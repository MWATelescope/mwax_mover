"""
Module hosting the MWAXCalvinProcessor for near-realtime
calibration
"""

import argparse
from configparser import ConfigParser
import datetime
import glob
import logging
import os
import shutil
import signal
import sys
import time
from typing import Optional
import coloredlogs
from itertools import repeat
from multiprocessing import Pool
from mwax_mover import (
    utils,
    version,
    mwax_calvin_utils,
    mwax_command,
    mwax_db,
)
from mwax_mover.mwa_archiver import copy_file_rsync
from mwax_mover.mwax_calvin_utils import CalvinJobType, estimate_birli_output_GB
from mwax_mover.mwax_calvin_solutions import process_solutions
from mwalib import MetafitsContext


class MWAXCalvinProcessor:
    """The main class processing calibration solutions"""

    def __init__(
        self,
    ):
        # General
        self.logger = logging.getLogger(__name__)
        self.log_path: str = ""
        self.log_level: str = ""
        self.hostname: str = ""
        self.db_handler_object: mwax_db.MWAXDBHandler

        # Metadata
        self.job_type: CalvinJobType
        self.mwa_asvo_download_url: str = ""
        self.obs_id: int = 0
        self.slurm_job_id: int = 0
        self.request_id_list: list[int] = []
        self.metafits_name: str = ""
        self.metafits_filename: str = ""
        self.uvfits_filename: str = ""
        self.ws_filenames: list[str] = []
        self.mwax_download_filenames: list[str] = []  # Only applicable for realtime
        self.data_downloaded: bool = False
        self.metafits_context: MetafitsContext

        # downloading
        self.download_retries: int = 0
        self.download_retry_wait: int = 0
        self.realtime_download_file_timeout: int = 0
        self.mwaasvo_download_obs_timeout: int = 0

        # processing
        self.job_input_path: str = (
            ""  # this is just /data/calvin/jobs/SLURM_JOB_ID_OBSID - where the visibility files are stored
        )
        self.working_path: str = (
            ""  # where does Birli write to? /data/calvin/jobs/SLURM_JOB_ID_OBSID or /tmp if the obs is small enough
        )
        self.job_output_path: str = ""  # e.g. /data/calvin/jobs/SLURM_JOB_ID_OBSID
        self.processing_error_path: str = ""
        self.source_list_filename: str = ""
        self.source_list_type: str = ""
        self.phase_fit_niter: int = 0
        self.produce_debug_plots: bool = True  # default to true- for now only off if running via pytest
        self.keep_completed_visibility_files: bool = False

        # birli
        self.birli_timeout: int = 0
        self.birli_binary_path: str = ""
        self.birli_max_mem_gib: int = 0
        self.birli_freq_res_khz: int = 0
        self.birli_int_time_res_sec: float = 0.0
        self.estimated_uvfits_GB: int = 0

        # hyperdrive
        self.hyperdrive_timeout: int = 0
        self.hyperdrive_binary_path: str = ""

    def start(self):
        """Start the processor"""
        self.running = True

        try:
            # Cleaning up /tmp in case there is a left over failed job that wasn't cleaned up
            if os.path.exists(self.temp_working_path):
                self.logger.info(f"Cleaning old working path ({self.temp_working_path}/)")
                shutil.rmtree(f"{self.temp_working_path}/")

            # creating database connection pool(s)
            self.logger.info("Starting database connection pool...")
            self.db_handler_object.start_database_pool()

            self.logger.info("Started process...")

            # Update this request with the slurm job_id
            self.logger.info(f"Assigning request to this host {self.hostname}")
            mwax_db.update_calibration_request_assigned_hostname(
                self.db_handler_object, self.slurm_job_id, self.hostname
            )

            # Set the data path and metadata
            # will be something like: /data/calvin/jobs/SLURM_JOB_ID_OBSID
            self.job_input_path = os.path.join(self.job_input_path, f"{str(self.slurm_job_id)}_{str(self.obs_id)}")

            # Ensure input data path exists
            os.makedirs(name=self.job_input_path, exist_ok=True)
            self.logger.info(f"Job Input Data will be downloaded to: {self.job_input_path}")

            self.metafits_filename = os.path.join(self.job_input_path, f"{self.obs_id}_metafits.fits")
            self.logger.info(f"Job metafits filename: {self.metafits_filename}")

            # Output dirs
            self.job_output_path = os.path.join(self.job_output_path, f"{str(self.slurm_job_id)}_{str(self.obs_id)}")

            # Ensure output path exists
            os.makedirs(name=self.job_output_path, exist_ok=True)
            self.logger.info(f"Job Output Data will be written to: {self.job_output_path}")

            # Get metafits file
            self.logger.info(f"Downloading metafits file: {self.metafits_filename}")
            try:
                utils.download_metafits_file(self.logger, self.obs_id, self.job_input_path)
            except Exception as catch_all_exception:  # pylint: disable=broad-except
                error_message = f"Metafits file {self.metafits_filename} did not exist and"
                " could not download one from web service. Exiting."
                f" {catch_all_exception}"
                self.logger.exception(error_message)
                self.fail_job_downloading(error_message)
                exit(-1)

            # Create metafits context
            self.logger.info(f"Reading metafits file {self.metafits_filename} with mwalib...")
            self.metafits_context = MetafitsContext(self.metafits_filename, None)

            # Get list of expected files from web service- wait if obs is still in progress!
            self.logger.info("Getting observation file list from web service...")
            result, error_message = self.get_observation_file_list()
            if not result:
                self.fail_job_downloading(error_message)
                exit(0)

            # next step depends on jobtype
            data_download_attempt: int = 1

            while not self.data_downloaded and data_download_attempt <= self.download_retries:
                self.logger.info(f"Download attempt {data_download_attempt} of {self.download_retries}...")
                if self.job_type == CalvinJobType.realtime:
                    # Download from MWAX boxes
                    self.data_downloaded, error_message = self.download_realtime_data()
                elif self.job_type == CalvinJobType.mwa_asvo:
                    # Download from MWA ASVO URL
                    self.data_downloaded, error_message = self.download_mwa_asvo_data()
                else:
                    error_message = f"Error- unknown job_type {self.job_type}. Aborting"
                    self.logger.error(error_message)
                    self.fail_job_downloading(error_message)
                    exit(-1)

                data_download_attempt += 1

                # Wait before trying again if we failed
                if not self.data_downloaded:
                    self.sleep(self.download_retry_wait)

            if not self.data_downloaded:
                self.fail_job_downloading(
                    f"Failed to download data after {self.download_retries} attempts. Error: {error_message}"
                )
                exit(0)

            # Working path for Birli / uvfits output is determined by calculating the size of the output visibilites:
            self.logger.info("Calculating observation output size...")
            self.estimated_uvfits_GB = estimate_birli_output_GB(
                self.metafits_context, self.birli_freq_res_khz, self.birli_int_time_res_sec
            )

            self.logger.info(
                f"Observation UVFITS output is estimated to be : {self.estimated_uvfits_GB} GB "
                f"({int(utils.gigabyte_to_gibibyte(self.estimated_uvfits_GB))} GiB)"
            )

            if self.estimated_uvfits_GB < 500:
                # Use temp_working_dir
                self.working_path = os.path.join(self.temp_working_path, f"{str(self.slurm_job_id)}_{str(self.obs_id)}")
                # Ensure input data path exists
                os.makedirs(name=self.working_path, exist_ok=True)

                # Override the birli allowed memory
                self.birli_max_mem_gib -= int(utils.gigabyte_to_gibibyte(self.estimated_uvfits_GB))
                self.logger.info(
                    f"Using temporary work dir {self.temp_working_path} for Birli output. Reduced "
                    f"allowed Birli memory to: {self.birli_max_mem_gib} GiB"
                )
            else:
                # Use output_dir
                self.working_path = self.job_output_path
                self.logger.info(f"Using work dir {self.working_path} for Birli output.")

            # Set uvfits filename
            self.uvfits_filename = os.path.join(self.working_path, f"{self.obs_id}.uvfits")
            self.logger.info(f"Job Output UVFITS file(s) will be created as: {self.uvfits_filename}")

            # All files we could get are now in the processing_path
            self.logger.info("Ensuring all data is ready for processing...")
            result, error_message = self.check_obs_is_ready_to_process()

            # Update database that we are processing this obsid
            if result:
                mwax_db.update_calsolution_request_calibration_started_status(
                    self.db_handler_object, self.obs_id, None, datetime.datetime.now()
                )
            else:
                self.fail_job_downloading(error_message)
                exit(-1)

            # We have all the files, so run birli
            result, error_message = self.run_birli()

            if not result:
                self.fail_job_processing(error_message)
                exit(-1)

            # Birli was successful so run hyperdrive!
            result, error_message = self.run_hyperdrive()

            if not result:
                self.fail_job_processing(error_message)
                exit(-1)

            # If that worked, process the solutions and insert into db
            self.logger.info("Processing solutions...")

            result, error_message, fit_id = process_solutions(
                self.logger,
                self.db_handler_object,
                self.obs_id,
                self.working_path,
                self.phase_fit_niter,
                self.produce_debug_plots,
            )

            if result:
                if fit_id:
                    self.succeed_job_processing(fit_id)
                else:
                    self.fail_job_processing("Error: process_solutions() did not return a fit_id")
                    exit(-1)
            else:
                self.fail_job_processing(error_message)
                exit(0)

            # This clean up only happens on success
            # if we get here, the whole calibration solution was inserted ok.
            if not self.keep_completed_visibility_files:
                # Remove visibilitiy files
                visibility_files = glob.glob(os.path.join(self.job_input_path, f"{self.obs_id}_*_*_*.fits"))
                for file_to_delete in visibility_files:
                    os.remove(file_to_delete)

                # Now remove uvfits too
                uvfits_files = glob.glob(os.path.join(self.job_input_path, "*.uvfits"))
                for file_to_delete in uvfits_files:
                    os.remove(file_to_delete)

            # Final log message
            self.logger.info("Completed Successfully")
        except Exception as e:
            # Something really bad went wrong!
            error_message = f"Unhandled Exception: {str(e)}"
            if self.data_downloaded:
                self.fail_job_processing(error_message)
            else:
                self.fail_job_downloading(error_message)

    def fail_job_downloading(self, error_message: str):
        # Update database
        try:
            mwax_db.update_calsolution_request_download_complete_status(
                self.db_handler_object, self.request_id_list, None, datetime.datetime.now(), error_message
            )
        except Exception as e:
            if self.logger:
                self.logger.info(f"Failed to update_calsolution_request_download_complete_status {str(e)}")

        try:
            self.stop()
        except Exception as e:
            if self.logger:
                self.logger.info(f"Failed to stop processor {str(e)}")

        if self.logger:
            self.logger.info("Completed with downloading errors")

    def fail_job_processing(self, error_message: str):
        # Update database
        try:
            mwax_db.update_calsolution_request_calibration_complete_status(
                self.db_handler_object, self.obs_id, None, None, None, datetime.datetime.now(), error_message
            )
        except Exception as e:
            if self.logger:
                self.logger.info(f"Failed to update_calsolution_request_calibration_complete_status {str(e)}")

        try:
            self.stop()
        except Exception as e:
            if self.logger:
                self.logger.info(f"Failed to stop processor {str(e)}")

        if self.logger:
            self.logger.info("Completed with processing errors")

    def succeed_job_processing(self, fit_id: int):
        # Update database
        mwax_db.update_calsolution_request_calibration_complete_status(
            self.db_handler_object, self.obs_id, None, datetime.datetime.now(), fit_id, None, None
        )
        self.stop()

    def get_observation_file_list(self) -> tuple[bool, str]:
        # Get the duration of the obs from the metafits and only proceed
        # if the current gps time is > the obs_id + duration + a constant
        exp_time = int(self.metafits_context.sched_duration_ms / 1000.0)
        current_gpstime: int = utils.get_gpstime_of_now()

        # We need to allow for some time for the observation to update the database,
        # so add an additional 120 seconds before we check
        OBS_FINISH_DELAY_SECONDS = 120
        OBS_FINISH_WAIT_SECONDS = 4
        while current_gpstime < (self.obs_id + exp_time + OBS_FINISH_DELAY_SECONDS):
            if not self.running:
                return False, "get_observation_file_list cancelled due to processor shutdown"

            self.logger.warning(
                f"{self.obs_id} Observation is still in progress:"
                f" {current_gpstime} < ({self.obs_id} - {int(self.obs_id) + exp_time + OBS_FINISH_DELAY_SECONDS})"
                f". Sleeping for {OBS_FINISH_WAIT_SECONDS} seconds..."
            )
            self.sleep(OBS_FINISH_WAIT_SECONDS)
            current_gpstime: int = utils.get_gpstime_of_now()

        # Ok, should be safe to get the list of files and hosts
        if self.running:
            try:
                ws_filenames_and_hosts: list[tuple[str, str]] = (
                    utils.get_data_files_with_hostname_for_obsid_from_webservice(self.logger, self.obs_id)
                )
            except Exception:
                # The previous call would have already logged tonnes of errors so no need to log anything specific here
                error_message = f"{self.obs_id} No webservice was able to provide list of data files."
                self.logger.error(error_message)
                return False, error_message

            # Assemble the filenames
            self.ws_filenames = []  # this is just filename
            self.mwax_download_filenames = []  # this is the full rsync user@host:/path/filename

            for filename, hostname in ws_filenames_and_hosts:
                self.mwax_download_filenames.append(
                    f"mwa@{hostname}:/{os.path.join("visdata/cal_outgoing/", filename)}"
                )
                self.ws_filenames.append(filename)

            return True, ""
        else:
            return False, "get_observation_file_list cancelled due to processor shutdown"

    def download_mwa_asvo_data(self) -> tuple[bool, str]:
        # Given the URL from command line args, download and untar the MWA ASVO data
        try:
            stdout = ""
            self.logger.info(
                f"{self.obs_id}: Attempting to download MWA ASVO data"
                f" {self.mwa_asvo_download_url} to {self.job_input_path}..."
            )

            cmdline = f'wget -q -O - "{self.mwa_asvo_download_url}" | tar -x -C {self.job_input_path}'

            try:
                # Submit the job
                return_val, stdout = mwax_command.run_command_ext(
                    self.logger, cmdline, None, self.mwaasvo_download_obs_timeout, True
                )

                if return_val:
                    self.logger.info(
                        f"{str(self.obs_id)} successfully downloaded "
                        f"from {self.mwa_asvo_download_url} into {self.job_input_path}"
                    )
                    return True, ""
                else:
                    error_message = f"{str(self.obs_id)} failed when running {cmdline} Error" f" {stdout}"
                    raise Exception(error_message)

            except Exception:
                self.logger.exception(
                    f"Failed to download and untar observation {self.obs_id} from "
                    f"{self.mwa_asvo_download_url}"
                    f" {stdout}"
                )
                raise

        except Exception as e:
            error_message = f"Failed to download and untar observation {self.obs_id} from {self.mwa_asvo_download_url}"
            f" Error ({str(e)})"
            self.logger.error(error_message)
            return False, error_message

    def download_realtime_data(self) -> tuple[bool, str]:
        # In parallel download all the files

        # Now download the files
        COPY_WORKERS = 24

        try:
            with Pool(processes=COPY_WORKERS) as pool:
                results = pool.starmap(
                    copy_file_rsync,
                    zip(
                        repeat(self.logger),
                        self.mwax_download_filenames,
                        repeat(self.job_input_path),
                        repeat(self.realtime_download_file_timeout),
                    ),
                )

            all_errors = ""
            for result, index in enumerate(results):
                if not result:
                    all_errors += f"{self.ws_filenames[index]} "

            if all_errors != "":
                error_message = f"Error downloading files: {all_errors}"
                self.logger.error(error_message)
                return False, error_message

            return True, ""
        except Exception as e:
            error_message = f"Exception downloading files: {str(e)}"
            self.logger.error(error_message)
            return False, error_message

    def check_obs_is_ready_to_process(self) -> tuple[bool, str]:
        """This routine checks to see if an observation is ready to be processed and
        also sums up the total size and determines if there is enough RAM to write
        the uvfits file to RAM or not"""
        # we need a list of files from the work dir
        # this first list has the full path
        # put a basic UNIX pattern so we don't pick up the metafits

        try:
            # Check for gpubox files (mwax OR legacy)
            glob_spec = "*.fits"
            data_dir_full_path_files = glob.glob(os.path.join(self.job_input_path, glob_spec))
            data_dir_filenames = [os.path.basename(i) for i in data_dir_full_path_files]
            data_dir_filenames.sort()
            # Remove any metafits files from the list
            for file in data_dir_filenames:
                if "metafits" in file:
                    data_dir_filenames.remove(file)

            # How does what we need compare to what we have?
            return_value = len(data_dir_filenames) == len(self.ws_filenames)

            message = f"{self.obs_id} check_obs_is_ready_to_process() =="
            f" {return_value} (WS: {len(self.ws_filenames)},"
            f" data_dir: {len(data_dir_filenames)})"

            self.logger.info(message)
            return return_value, message
        except Exception as e:
            return False, f"Exception in check_obs_is_ready_to_process: {str(e)}"

    def run_birli(self) -> tuple[bool, str]:
        """Run birli to produce uvfits file(s)"""
        birli_success: bool = False

        # Determine if the obs is oversampled
        oversampled = self.metafits_context.oversampled

        # Run Birli
        self.logger.info(f"{self.obs_id}: Running Birli...")
        birli_success = mwax_calvin_utils.run_birli(
            self.logger,
            self.job_input_path,
            self.metafits_filename,
            self.uvfits_filename,
            self.job_output_path,
            self.obs_id,
            oversampled,
            self.birli_binary_path,
            self.birli_max_mem_gib,
            self.birli_timeout,
            self.birli_freq_res_khz * 1000,
            self.birli_int_time_res_sec,
            self.birli_edge_width_khz * 1000,
        )

        if birli_success:
            return True, ""
        else:
            return False, "Birli run failed. See logs"

    def run_hyperdrive(self) -> tuple[bool, str]:
        """Run hyperdrive to produce solutions"""
        hyperdrive_success = False

        # If all good run hyperdrive- once per uvfits file created
        # N (where N>1) uvfits are generated if Birli sees the obs is picket fence
        # Therefore we need to run hyperdrive N times too
        #
        # get a list of the uvfits files
        uvfits_files = glob.glob(os.path.join(self.working_path, "*.uvfits"))

        # Run hyperdrive
        hyperdrive_success = mwax_calvin_utils.run_hyperdrive(
            self.logger,
            uvfits_files,
            self.metafits_filename,
            self.job_output_path,
            self.obs_id,
            self.hyperdrive_binary_path,
            self.source_list_filename,
            self.source_list_type,
            self.hyperdrive_timeout,
        )

        # Did we have N number of successful runs?
        if hyperdrive_success:
            # Run hyperdrive and get plots and stats
            mwax_calvin_utils.run_hyperdrive_stats(
                self.logger, uvfits_files, self.metafits_filename, self.obs_id, self.hyperdrive_binary_path
            )

        if hyperdrive_success:
            return True, ""
        else:
            return False, "Hyperdrive run failed. See logs"

    def stop(self):
        """Shutdown all processes"""
        # Close all database connections
        self.db_handler_object.stop_database_pool()
        self.running = False

    def signal_handler(self, _signum, _frame):
        """Handles SIGINT and SIGTERM"""
        self.logger.warning("Interrupted. Shutting down processor...")

        # Stop any Processors
        self.stop()

    def initialise(
        self,
        config_filename,
        obs_id: int,
        slurm_job_id: int,
        job_type: CalvinJobType,
        mwa_asvo_download_url: str,
        request_ids: list[int],
    ):
        """Initialise the processor from the command line"""
        # Get this hosts hostname
        self.hostname = utils.get_hostname()
        self.job_type = job_type
        self.mwa_asvo_download_url = mwa_asvo_download_url
        self.obs_id = obs_id
        self.slurm_job_id = slurm_job_id
        self.request_id_list = request_ids

        if not os.path.exists(config_filename):
            print(f"Configuration file location {config_filename} does not" " exist. Quitting.")
            sys.exit(1)

        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Parse config file
        config = ConfigParser()
        config.read_file(open(config_filename, "r", encoding="utf-8"))

        # read from config file
        self.log_path = config.get("mwax mover", "log_path")

        if not os.path.exists(self.log_path):
            print(f"log_path {self.log_path} does not exist. Quiting.")
            sys.exit(1)

        # Read log level
        config_file_log_level: Optional[str] = utils.read_optional_config(
            self.logger, config, "mwax mover", "log_level"
        )
        if config_file_log_level is None:
            self.log_level = "DEBUG"
            self.logger.warning(f"log_level not set in config file. Defaulting to {self.log_level} level logging.")
        else:
            self.log_level = config_file_log_level

        # It's now safe to start logging
        # start logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(self.log_level)
        console_log = logging.StreamHandler()
        console_log.setLevel(self.log_level)
        console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        self.logger.addHandler(console_log)

        if config.getboolean("mwax mover", "coloredlogs", fallback=False):
            coloredlogs.install(level="INFO", logger=self.logger)

        self.logger.info("Starting mwax_calvin_processor" f" processor...v{version.get_mwax_mover_version_string()}")
        self.logger.info(f"Reading config file: {config_filename}")

        #
        # MRO database
        #
        self.mro_metadatadb_host = utils.read_config(self.logger, config, "mro metadata database", "host")
        self.mro_metadatadb_db = utils.read_config(self.logger, config, "mro metadata database", "db")
        self.mro_metadatadb_user = utils.read_config(self.logger, config, "mro metadata database", "user")
        self.mro_metadatadb_pass = utils.read_config(self.logger, config, "mro metadata database", "pass", True)
        self.mro_metadatadb_port = int(utils.read_config(self.logger, config, "mro metadata database", "port"))

        # Initiate database connection for rmo metadata db
        self.db_handler_object = mwax_db.MWAXDBHandler(
            logger=self.logger,
            host=self.mro_metadatadb_host,
            port=self.mro_metadatadb_port,
            db_name=self.mro_metadatadb_db,
            user=self.mro_metadatadb_user,
            password=self.mro_metadatadb_pass,
        )

        #
        # Downloading
        #
        self.download_retries = int(utils.read_config(self.logger, config, "downloading", "download_retries"))
        self.download_retry_wait = int(utils.read_config(self.logger, config, "downloading", "download_retry_wait"))
        self.realtime_download_file_timeout = int(
            utils.read_config(self.logger, config, "downloading", "realtime_download_file_timeout")
        )
        self.mwaasvo_download_obs_timeout = int(
            utils.read_config(self.logger, config, "downloading", "mwaasvo_download_obs_timeout")
        )

        #
        # Birli
        #

        # Birli timeout
        self.birli_timeout = int(
            utils.read_config(
                self.logger,
                config,
                "birli",
                "timeout",
            )
        )

        # Get Birli max mem
        self.birli_max_mem_gib = int(
            utils.read_config(
                self.logger,
                config,
                "birli",
                "max_mem_gib",
            )
        )

        # Get the Birli binary
        self.birli_binary_path = utils.read_config(
            self.logger,
            config,
            "birli",
            "binary_path",
        )

        if not os.path.exists(self.birli_binary_path):
            self.logger.error("birli_binary_path location " f" {self.birli_binary_path} does not exist. Quitting.")
            sys.exit(1)

        # Get Birli freq res
        self.birli_freq_res_khz = int(
            utils.read_config(
                self.logger,
                config,
                "birli",
                "freq_res_khz",
            )
        )

        # Get Birli time res
        self.birli_int_time_res_sec = float(
            utils.read_config(
                self.logger,
                config,
                "birli",
                "int_time_res_sec",
            )
        )

        # Get Birli edge width
        self.birli_edge_width_khz = int(
            utils.read_config(
                self.logger,
                config,
                "birli",
                "edge_width_khz",
            )
        )

        #
        # Hyperdrive config
        #
        self.phase_fit_niter = int(
            utils.read_config(
                self.logger,
                config,
                "hyperdrive",
                "phase_fit_niter",
            )
        )

        self.source_list_filename = utils.read_config(
            self.logger,
            config,
            "hyperdrive",
            "source_list_filename",
        )

        if not os.path.exists(self.source_list_filename):
            self.logger.error(
                "source_list_filename location " f" {self.source_list_filename} does not exist. Quitting."
            )
            sys.exit(1)

        self.source_list_type = utils.read_config(
            self.logger,
            config,
            "hyperdrive",
            "source_list_type",
        )

        # hyperdrive timeout
        self.hyperdrive_timeout = int(
            utils.read_config(
                self.logger,
                config,
                "hyperdrive",
                "timeout",
            )
        )

        # Get the hyperdrive binary
        self.hyperdrive_binary_path = utils.read_config(
            self.logger,
            config,
            "hyperdrive",
            "binary_path",
        )

        if not os.path.exists(self.hyperdrive_binary_path):
            self.logger.error(
                "hyperdrive_binary_path location " f" {self.hyperdrive_binary_path} does not exist. Quitting."
            )
            sys.exit(1)

        #
        # processing config
        #
        # Get the job_input_path dir
        self.job_input_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "job_input_path",
        )

        if not os.path.exists(self.job_input_path):
            self.logger.error("job_input_path location " f" {self.job_input_path} does not exist. Quitting.")
            sys.exit(1)

        # Get the job_output_path dir
        self.job_output_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "job_output_path",
        )

        if not os.path.exists(self.job_output_path):
            self.logger.error("job_output_path location " f" {self.job_output_path} does not exist. Quitting.")
            sys.exit(1)

        # Get the temp working dir
        self.temp_working_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "temp_working_path",
        )

        if not os.path.exists(self.temp_working_path):
            if self.temp_working_path.startswith("/tmp"):
                # Create it (if it does not start with /tmp assume it is a permanent path in
                # which case user has to create it
                self.logger.debug(f"temp_working_path location {self.temp_working_path} does not exist, creating it.")
                os.makedirs(self.temp_working_path)
            else:
                self.logger.error("temp_working_path location " f" {self.temp_working_path} does not exist. Quitting.")
                sys.exit(1)

        self.keep_completed_visibility_files = utils.read_config_bool(
            self.logger,
            config,
            "processing",
            "keep_completed_visibility_files",
        )

    def initialise_from_command_line(self):
        """Initialise if initiated from command line"""

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_calvin_processor: a command line tool which is part of the"
            " MWA correlator for the MWA. It will be launched via sSLURM "
            " job and either download a realtime calibrator obs from MWAX or "
            "download data from an MWA ASVO URL. Either way it will then run "
            "Birli and Hyperdrive and then upload the calibration solution."
            f"mwax_mover v{version.get_mwax_mover_version_string()})\n"
        )

        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")
        parser.add_argument("-o", "--obs-id", required=True, type=int, help="ObservationID.\n")
        parser.add_argument("-s", "--slurm-job-id", required=True, type=int, help="This Slurm Job ID.\n")
        parser.add_argument(
            "-r", "--request-ids", required=True, type=str, help="A comma separated list of one or more request ids.\n"
        )
        parser.add_argument(
            "-j",
            "--job-type",
            required=True,
            type=CalvinJobType,
            choices=list(CalvinJobType),
            help="MWA ASVO or Realtime job.\n",
        )
        parser.add_argument(
            "-u",
            "--mwa-asvo-download-url",
            required=False,
            default="",
            help="For MWA ASVO processing- the download URL for the MWA ASVO job.\n",
        )

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]
        obs_id = args["obs_id"]

        if not utils.is_int(obs_id):
            print(f"ERROR: cmd line argument obs-id {obs_id} is not a number. Aborting.")
            exit(-1)

        slurm_job_id = args["slurm_job_id"]

        if not utils.is_int(slurm_job_id):
            print(f"ERROR: cmd line argument slurm-job-id {slurm_job_id} is not a number. Aborting.")
            exit(-1)

        job_type = args["job_type"]

        if not args["mwa_asvo_download_url"]:
            mwa_asvo_download_url = ""
        else:
            mwa_asvo_download_url = args["mwa_asvo_download_url"]

        if job_type == CalvinJobType.mwa_asvo and mwa_asvo_download_url == "":
            print(
                f"ERROR: cmd line argument job-type {job_type.value} is not a valid without "
                "the mwa_asvo_download_url being provided too. Aborting."
            )
            exit(-1)

        # Get a list of request ids
        request_ids: list[int] = []
        request_ids_string: str = args["request_ids"]
        request_ids_string_list: list[str] = request_ids_string.split(",")

        for request_id_str in request_ids_string_list:
            request_id_str = request_id_str.strip()
            if utils.is_int(request_id_str):
                request_ids.append(int())
            else:
                print(
                    f"ERROR: request-ids param '{request_ids_string}' must be one or more positive integers"
                    " separated by commas. Aborting."
                )
                exit(-1)
        # Check we got at least one
        if len(request_ids) == 0:
            print(f"ERROR: request-ids param '{request_ids_string}' must contain at least one request-id. Aborting.")
            exit(-1)

        self.initialise(
            config_filename,
            int(obs_id),
            int(slurm_job_id),
            job_type,
            mwa_asvo_download_url,
            request_ids,
        )

    def sleep(self, seconds):
        """This sleep function keeps an eye on self.running so that if we are in a long wait
        we will still respond to shutdown directives"""
        SECS_PER_INTERVAL: int = 5

        if self.running:
            if seconds <= SECS_PER_INTERVAL:
                time.sleep(seconds)
            else:
                integer_intervals, remainder_secs = divmod(seconds, SECS_PER_INTERVAL)

                while self.running and integer_intervals > 0:
                    time.sleep(SECS_PER_INTERVAL)
                    integer_intervals -= 1

                if self.running and remainder_secs > 0:
                    time.sleep(remainder_secs)


def main():
    """Mainline function"""
    processor = MWAXCalvinProcessor()

    try:
        processor.initialise_from_command_line()
        processor.start()
        sys.exit(0)
    except Exception as catch_all_exception:  # pylint: disable=broad-except
        if processor.logger:
            processor.logger.exception(str(catch_all_exception))
        else:
            print(str(catch_all_exception))


if __name__ == "__main__":
    main()
