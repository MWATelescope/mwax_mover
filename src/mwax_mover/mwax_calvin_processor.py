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
import signal
import sys
import time
from typing import Optional
import coloredlogs
from itertools import repeat
from multiprocessing import Pool
from tenacity import Retrying, RetryError, stop_after_attempt, wait_fixed
from astropy.io import fits
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

        # processing
        self.input_data_path: str = (
            ""  # this is just /data/calvin/jobs/OBSID/SLURM_JOB_ID - where the visibility files are stored
        )
        self.working_path: str = (
            ""  # where does Birli write to? /data/calvin/jobs/JOBID or /tmp if the obs is small enough
        )
        self.job_output_path: str = ""  # e.g. /data/calvin/jobs/JOBID
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

        # creating database connection pool(s)
        self.logger.info("Starting database connection pool...")
        self.db_handler_object.start_database_pool()

        self.logger.info("Started...")

        # Update this request with the slurm job_id
        mwax_db.update_calibration_request_assigned_hostname(self.db_handler_object, self.slurm_job_id, self.hostname)

        # Set the data path and metadata
        # will be something like: /data/calvin/jobs/OBSID/SLURM_JOB_ID
        self.input_data_path = os.path.join(
            os.path.join(self.input_data_path, str(self.obs_id)), str(self.slurm_job_id)
        )
        self.metafits_filename = os.path.join(self.input_data_path, f"{self.obs_id}_metafits.fits")
        # Ensure input data path exists
        os.makedirs(name=self.input_data_path, exist_ok=True)

        # Output dirs
        self.job_output_path = os.path.join(
            os.path.join(self.job_output_path, str(self.obs_id)), str(self.slurm_job_id)
        )
        # Ensure output path exists
        os.makedirs(name=self.job_output_path, exist_ok=True)

        # First step depends on jobtype
        data_downloaded: bool = False
        data_download_attempt: int = 1

        DOWNLOAD_RETRY_ATTEMPTS = 3
        DOWNLOAD_RETRY_WAIT_SECONDS = 240

        while not data_downloaded and data_download_attempt < DOWNLOAD_RETRY_ATTEMPTS:
            if self.job_type == CalvinJobType.realtime:
                # Download from MWAX boxes
                data_downloaded = self.download_realtime_data()
            elif self.job_type == CalvinJobType.mwa_asvo:
                # Download from MWA ASVO URL
                data_downloaded = self.download_mwa_asvo_data()
            else:
                self.logger.error(f"Error- unknown job_type {self.job_type}. Aborting")
                exit(-2)

            data_download_attempt += 1

            # Wait before trying again
            self.sleep(DOWNLOAD_RETRY_WAIT_SECONDS)

        # Working path for Birli / uvfits output is determined by calculating the size of the output visibilites:
        self.estimated_uvfits_GB = estimate_birli_output_GB(
            self.metafits_filename, self.birli_freq_res_khz, self.birli_int_time_res_sec
        )

        if self.estimated_uvfits_GB < 500:
            # Use temp_working_dir
            self.working_path = os.path.join(
                os.path.join(self.temp_working_path, str(self.obs_id)), str(self.slurm_job_id)
            )
            # Ensure input data path exists
            os.makedirs(name=self.working_path, exist_ok=True)

            # Override the birli allowed memory
            self.birli_max_mem_gib -= self.estimated_uvfits_GB
            self.logger.info(
                f"Using tempory work dir {self.temp_working_path} for Birli output. Reduced "
                f"allowed Birli memory to: {self.birli_max_mem_gib} GiB"
            )
        else:
            # Use output_dir
            self.working_path = self.job_output_path

        # All files we could get are now in the processing_path
        if not self.check_obs_is_ready_to_process():
            exit(-3)

        # We have all the files, so run birli
        if not self.run_birli():
            exit(-4)

        # Birli was successful so run hyperdrive!
        if not self.run_hyperdrive():
            exit(-5)

        # If that worked, process the solutions and insert into db
        if not process_solutions(
            self.logger,
            self.db_handler_object,
            self.obs_id,
            self.input_data_path,
            self.phase_fit_niter,
            self.produce_debug_plots,
        ):
            exit(-6)

        # clean up
        # if we get here, the whole calibration solution was inserted ok.
        # The transaction context will commit the transation
        if not self.keep_completed_visibility_files:
            # Remove visibilitiy files
            visibility_files = glob.glob(os.path.join(self.input_data_path, f"{self.obs_id}_*_*_*.fits"))
            for file_to_delete in visibility_files:
                os.remove(file_to_delete)

            # Now remove uvfits too
            uvfits_files = glob.glob(os.path.join(self.input_data_path, "*.uvfits"))
            for file_to_delete in uvfits_files:
                os.remove(file_to_delete)

        # Stop!
        self.stop()

        # Final log message
        self.logger.info("Completed Successfully")

    def download_mwa_asvo_data(self) -> bool:
        # Given the URL from command line args, download and untar the MWA ASVO data
        iteration = 0
        max_iterations = 3
        try:
            for attempt in Retrying(stop=stop_after_attempt(max_iterations), wait=wait_fixed(60)):
                iteration += 1
                with attempt:
                    stdout = ""
                    self.logger.info(
                        f"{self.obs_id}: Attempting to download MWA ASVO data ({iteration} of {max_iterations})"
                        f"{self.mwa_asvo_download_url} to {self.input_data_path}..."
                    )

                    cmdline = f'wget -q -O - "{self.mwa_asvo_download_url}" | tar -x -C {self.input_data_path}'

                    try:
                        # Submit the job
                        return_val, stdout = mwax_command.run_command_ext(self.logger, cmdline, None, 3600, True)

                        if return_val:
                            self.logger.info(
                                f"{str(self.obs_id)} successfully downloaded "
                                f"from {self.mwa_asvo_download_url} into  {self.input_data_path}"
                            )
                            return True
                        else:
                            error_message = f"{str(self.obs_id)} failed when running {cmdline} Error" f" {stdout}"
                            raise Exception(error_message)

                    except Exception:
                        self.logger.exception(
                            f"Failed to download and untar observation {self.obs_id} from {self.mwa_asvo_download_url}"
                            f" {stdout}"
                        )
                        raise
        except RetryError:
            self.logger.error(
                f"Failed to download and untar observation {self.obs_id} from {self.mwa_asvo_download_url} "
                "after all attempts. Giving up!"
            )
            return False
        return False

    def download_realtime_data(self) -> bool:
        # Get the list of files we need from the web service
        # In parallel download all the files
        #
        # Check we have a metafits
        #
        if not os.path.exists(self.metafits_filename):
            # download the metafits file to the data dir for the obs
            try:
                self.logger.info(f"{self.obs_id} Downloading metafits file...")
                utils.download_metafits_file(self.logger, self.obs_id, self.input_data_path)
                self.logger.info(f"{self.obs_id} metafits downloaded successfully")
            except Exception as catch_all_exception:
                self.logger.error(
                    f"Metafits file {self.metafits_filename} did not exist"
                    " and could not download one from web"
                    f" service. {catch_all_exception}"
                )
                return False

        # Get the duration of the obs from the metafits and only proceed
        # if the current gps time is > the obs_id + duration + a constant
        exp_time = int(utils.get_metafits_value(self.metafits_filename, "EXPOSURE"))
        current_gpstime: int = utils.get_gpstime_of_now()

        # We need to allow for some time for the observation to update the database,
        # so add an additional 120 seconds before we check
        OBS_FINISH_DELAY_SECONDS = 120
        OBS_FINISH_WAIT_SECONDS = 4
        while current_gpstime < (self.obs_id + exp_time + OBS_FINISH_DELAY_SECONDS):
            self.logger.warning(
                f"{self.obs_id} Observation is still in progress:"
                f" {current_gpstime} < ({self.obs_id} - {int(self.obs_id) + exp_time + OBS_FINISH_DELAY_SECONDS})"
                f". Sleeping for {OBS_FINISH_WAIT_SECONDS} seconds..."
            )
            self.sleep(OBS_FINISH_WAIT_SECONDS)
            current_gpstime: int = utils.get_gpstime_of_now()

        # Ok, should be safe to get the list of files and hosts
        try:
            ws_filenames_and_hosts: list[tuple[str, str]] = (
                utils.get_data_files_with_hostname_for_obsid_from_webservice(self.logger, self.obs_id)
            )
        except Exception:
            # The previous call would have already logged tonnes of errors so no need to log anything specific here
            self.logger.error(f"{self.obs_id} No webservice was able to provide list of data files.")
            return False

        # Assemble the filenames
        self.ws_filenames = []  # this is just filename
        download_filenames = []  # this is the full rsync user@host:/path/filename

        for filename, hostname in ws_filenames_and_hosts:
            download_filenames.append(f"mwa@{hostname}:/{os.path.join("visdata/cal_outgoing/", filename)}")
            self.ws_filenames.append(filename)

        # Now download the files
        RSYNC_TIMEOUT_SECONDS = 600
        COPY_WORKERS = 24

        with Pool(processes=COPY_WORKERS) as pool:
            results = pool.starmap(
                copy_file_rsync,
                zip(
                    repeat(self.logger), download_filenames, repeat(self.input_data_path), repeat(RSYNC_TIMEOUT_SECONDS)
                ),
            )

        all_errors = ""
        for result, index in enumerate(results):
            if not result:
                all_errors += f"{self.ws_filenames[index]} "

        if all_errors != "":
            self.logger.error(f"Error downloading files: {all_errors}")
            return False

        return True

    def check_obs_is_ready_to_process(self) -> bool:
        """This routine checks to see if an observation is ready to be processed and
        also sums up the total size and determines if there is enough RAM to write
        the uvfits file to RAM or not"""
        # we need a list of files from the work dir
        # this first list has the full path
        # put a basic UNIX pattern so we don't pick up the metafits

        # Check for gpubox files (mwax OR legacy)
        glob_spec = "*.fits"
        data_dir_full_path_files = glob.glob(os.path.join(self.input_data_path, glob_spec))
        data_dir_filenames = [os.path.basename(i) for i in data_dir_full_path_files]
        data_dir_filenames.sort()
        # Remove the metafits file
        if self.metafits_name in data_dir_filenames:
            data_dir_filenames.remove(self.metafits_name)

        # How does what we need compare to what we have?
        return_value = set(data_dir_filenames).issuperset(self.ws_filenames)

        self.logger.debug(
            f"{self.obs_id} check_obs_is_ready_to_process() =="
            f" {return_value} (WS: {len(self.ws_filenames)},"
            f" data_dir: {len(data_dir_filenames)})"
        )
        return return_value

    def run_birli(self) -> bool:
        """Run birli to produce uvfits file(s)"""
        birli_success: bool = False
        error_message: str = ""

        # Update database that we are processing this obsid
        mwax_db.update_calsolution_request_calibration_started_status(
            self.db_handler_object, self.obs_id, None, datetime.datetime.now()
        )

        # Determine if the obs is oversampled
        try:
            with fits.open(self.metafits_filename) as hdus:  # type: ignore
                oversampled: bool = int(hdus["PRIMARY"].header["OVERSAMP"]) == 1
        except KeyError:
            # No OVERSAMP key? Then it is not oversampled!
            oversampled: bool = False

        # Run Birli
        self.logger.info(f"{self.obs_id}: Running Birli...")
        birli_success = mwax_calvin_utils.run_birli(
            self.logger,
            self.input_data_path,
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

        if not birli_success:
            error_message = "Birli run failed. See logs"

            # Update database
            mwax_db.update_calsolution_request_calibration_complete_status(
                self.db_handler_object, self.obs_id, None, None, None, datetime.datetime.now(), error_message
            )
            return False
        else:
            return True

    def run_hyperdrive(self) -> bool:
        """Run hyperdrive to produce solutions"""
        error_message: str = ""
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

        if not hyperdrive_success:
            error_message = "Hyperdrive run failed. See logs"

            # Update database
            mwax_db.update_calsolution_request_calibration_complete_status(
                self.db_handler_object, self.obs_id, None, None, None, datetime.datetime.now(), error_message
            )
            return False
        else:
            return True

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
