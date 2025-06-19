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
import numpy as np
import traceback
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
from mwax_mover.mwax_db import insert_calibration_fits_row, insert_calibration_solutions_row
from mwax_mover.mwax_calvin_utils import (
    CalvinJobType,
    HyperfitsSolution,
    HyperfitsSolutionGroup,
    Metafits,
    debug_phase_fits,
    PhaseFitInfo,
    GainFitInfo,
    write_readme_file,
)


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
        self.processing_path: str = ""  # e.g. /data/calvin/jobs
        self.data_path: str = ""  # this is just /data/calvin/jobs/OBSID/SLURM_JOB_ID
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
        self.data_path = os.path.join(os.path.join(self.processing_path, str(self.obs_id)), str(self.slurm_job_id))
        self.metafits_name = f"{self.obs_id}_metafits.fits"
        self.metafits_filename = os.path.join(self.data_path, self.metafits_name)
        self.uvfits_filename = os.path.join(self.data_path, str(self.obs_id) + ".uvfits")

        # Ensure data path exists
        os.makedirs(name=self.data_path, exist_ok=True)

        # First step depends on jobtype
        if self.job_type == CalvinJobType.realtime:
            # Download from MWAX boxes
            self.download_realtime_data()
        elif self.job_type == CalvinJobType.mwa_asvo:
            # Download from MWA ASVO URL
            self.download_mwa_asvo_data()
        else:
            self.logger.error(f"Error- unknown job_type {self.job_type}. Aborting")
            exit(-2)

        # All files we could get are now in the processing_path
        if not self.check_obs_is_ready_to_process():
            exit(-3)

        # We have all the files, so run birli and hyperdrive!
        if not self.process_observation():
            exit(-4)

        # If that worked, process the solutions and insert into db
        if not self.process_solutions():
            exit(-5)

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
                        f"{self.mwa_asvo_download_url} to {self.data_path}..."
                    )

                    cmdline = f'wget -q -O - "{self.mwa_asvo_download_url}" | tar -x -C {self.data_path}'

                    try:
                        # Submit the job
                        return_val, stdout = mwax_command.run_command_ext(self.logger, cmdline, None, 3600, False)

                        if return_val:
                            self.logger.info(
                                f"{str(self.obs_id)} successfully downloaded "
                                f"from {self.mwa_asvo_download_url} into  {self.data_path}"
                            )
                            return True
                        else:
                            error_message = f"{str(self.obs_id)} failed to be submitted to SLURM. Error" f" {stdout}"
                            self.logger.error(error_message)
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
                utils.download_metafits_file(self.logger, self.obs_id, self.data_path)
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
        while current_gpstime < (self.obs_id + exp_time + OBS_FINISH_DELAY_SECONDS):
            self.logger.warning(
                f"{self.obs_id} Observation is still in progress:"
                f" {current_gpstime} < ({self.obs_id} - {int(self.obs_id) + exp_time + OBS_FINISH_DELAY_SECONDS})"
                ". Sleeping for 10 seconds..."
            )
            self.sleep(10)
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
        max_workers = 24

        with Pool(processes=max_workers) as pool:
            results = pool.starmap(
                copy_file_rsync,
                zip(repeat(self.logger), download_filenames, repeat(self.data_path), repeat(RSYNC_TIMEOUT_SECONDS)),
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
        """This routine checks to see if an observation is ready to be processed"""
        #
        # perform web service call to get list of data files from obsid
        #
        if not self.ws_filenames:
            try:
                self.ws_filenames = utils.get_data_files_for_obsid_from_webservice(self.logger, self.obs_id)
            except Exception:
                # The previous call would have already logged tonnes of errors so no need to log anything specific here
                self.logger.warning(f"{self.obs_id} No webservice was able to provide list of data files- requeueing")
                return False

        if self.ws_filenames:
            # we need a list of files from the work dir
            # this first list has the full path
            # put a basic UNIX pattern so we don't pick up the metafits

            # Check for gpubox files (mwax OR legacy)
            glob_spec = "*.fits"
            data_dir_full_path_files = glob.glob(os.path.join(self.data_path, glob_spec))
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
        else:
            # Web service didn't return any files
            # This is usually because there ARE no files in the database
            # Best to fail
            self.logger.error(f"utils.get_data_files_for_obsid_from_webservice({self.obs_id} did not return any files.")
            return False

    def process_observation(self) -> bool:
        """Have birli then hyperdrive run on it"""
        birli_success: bool = False
        error_message: str = ""

        # Update database that we are processing this obsid
        mwax_db.update_calsolution_request_calibration_started_status(
            self.db_handler_object, self.obs_id, None, datetime.datetime.now()
        )

        # Determine if the obs is oversampled
        try:
            with fits.open(self.metafits_filename) as hdus:
                oversampled: bool = int(hdus["PRIMARY"].header["OVERSAMP"]) == 1
        except KeyError:
            # No OVERSAMP key? Then it is not oversampled!
            oversampled: bool = False

        hyperdrive_success = False

        # Run Birli
        self.logger.info(f"{self.obs_id}: Running Birli...")
        birli_success = mwax_calvin_utils.run_birli(
            self, self.metafits_filename, self.uvfits_filename, self.obs_id, self.data_path, oversampled
        )

        if birli_success:
            # If all good run hyperdrive- once per uvfits file created
            # N (where N>1) uvfits are generated if Birli sees the obs is picket fence
            # Therefore we need to run hyperdrive N times too
            #
            # get a list of the uvfits files
            uvfits_files = glob.glob(os.path.join(self.data_path, "*.uvfits"))

            # Run hyperdrive
            hyperdrive_success = mwax_calvin_utils.run_hyperdrive(
                self, uvfits_files, self.metafits_filename, self.obs_id, self.data_path
            )

            # Did we have N number of successful runs?
            if hyperdrive_success:
                # Run hyperdrive and get plots and stats
                mwax_calvin_utils.run_hyperdrive_stats(
                    self, uvfits_files, self.metafits_filename, self.obs_id, self.data_path
                )

        if not (birli_success and hyperdrive_success):
            if not birli_success:
                error_message = "Birli run failed. See logs"
            elif not hyperdrive_success:
                error_message = "Hyperdrive run failed. See logs"

            # Update database
            mwax_db.update_calsolution_request_calibration_complete_status(
                self.db_handler_object, self.obs_id, None, None, None, datetime.datetime.now(), error_message
            )
            return False
        else:
            return True

    def process_solutions(self) -> bool:
        """Will deal with completed hyperdrive solutions
        by getting them into a format we can insert into
        the calibration database"""

        conn = None
        try:
            metafits_files = glob.glob(os.path.join(self.data_path, "*_metafits.fits"))
            # if len(metafits_files) > 1:
            #     self.logger.warning(f"{item} - more than one metafits file found.")

            self.logger.debug(f"{self.data_path} - {metafits_files=}")
            fits_solution_files = sorted(glob.glob(os.path.join(self.data_path, "*_solutions.fits")))
            # _bin_solution_files = glob.glob(os.path.join(item, "*_solutions.bin"))
            self.logger.debug(f"{self.data_path} - uploading {fits_solution_files=}")

            soln_group = HyperfitsSolutionGroup(
                [Metafits(f) for f in metafits_files], [HyperfitsSolution(f) for f in fits_solution_files]
            )

            # get tiles
            tiles = soln_group.metafits_tiles_df
            self.logger.debug(f"{self.data_path} - metafits tiles:\n{tiles.to_string(max_rows=999)}")

            # determine refant
            unflagged_tiles = tiles[tiles.flag == 0]
            if not len(unflagged_tiles):
                raise ValueError("No unflagged tiles found")
            refant = unflagged_tiles.sort_values(by=["id"]).iloc[0]
            self.logger.debug(f"{self.data_path} - {refant['name']=} ({refant['id']})")

            # get channel info
            chaninfo = soln_group.metafits_chan_info
            self.logger.debug(f"{self.data_path} - {chaninfo=}")
            all_coarse_chan_ranges = chaninfo.coarse_chan_ranges

            if len(fits_solution_files) != len(all_coarse_chan_ranges):
                raise RuntimeError(
                    f"{self.data_path} - number of solution files ({len(fits_solution_files)})"
                    f" does not match number of coarse chan ranges in metafits {len(all_coarse_chan_ranges)}"
                )

            chanblocks_per_coarse = soln_group.chanblocks_per_coarse
            # all_chanblocks_hz = soln_group.all_chanblocks_hz
            all_chanblocks_hz = np.concatenate(soln_group.all_chanblocks_hz)
            self.logger.debug(f"{self.data_path} - {chanblocks_per_coarse=}, {all_chanblocks_hz=}")

            soln_tile_ids, all_xx_solns_noref, all_yy_solns_noref = soln_group.get_solns()
            _, all_xx_solns, all_yy_solns = soln_group.get_solns(refant["name"])

            weights = soln_group.weights

            phase_fits = mwax_calvin_utils.process_phase_fits(
                self.logger,
                self.data_path,
                unflagged_tiles,
                all_chanblocks_hz,
                all_xx_solns,
                all_yy_solns,
                weights,
                soln_tile_ids,
                self.phase_fit_niter,
            )
            gain_fits = mwax_calvin_utils.process_gain_fits(
                self.logger,
                self.data_path,
                unflagged_tiles,
                all_chanblocks_hz,
                all_xx_solns_noref,
                all_yy_solns_noref,
                weights,
                soln_tile_ids,
                chanblocks_per_coarse,
            )

            # if ~np.any(np.isfinite(phase_fits["length"])):
            #     self.logger.warning(f"{item} - no valid phase fits found, continuing anyway")

            # Matplotlib stuff seems to break pytest so we will
            # pass false in for pytest stuff (or if we don't want debug)
            if self.produce_debug_plots:
                # This line was:
                # phase_fits_pivot = debug_phase_fits(...
                # But the phase_fits_pivot return value is not used
                debug_phase_fits(
                    phase_fits,
                    tiles,
                    all_chanblocks_hz,
                    all_xx_solns[0],
                    all_yy_solns[0],
                    weights,
                    prefix=f"{self.data_path}/{self.obs_id}_",
                    plot_residual=True,
                )
            # phase_fits_pivot = pivot_phase_fits(phase_fits, tiles)
            # self.logger.debug(f"{item} - fits:\n{phase_fits_pivot.to_string(max_rows=512)}")
            success = True

            # get a database connection, unless we are using dummy connection (for testing)
            transaction_cursor = None
            with self.db_handler_object.pool.connection() as conn:
                # Start a transaction
                with conn.transaction():
                    # Create a cursor
                    transaction_cursor = conn.cursor()

                    (success, fit_id) = insert_calibration_fits_row(
                        self.db_handler_object,
                        transaction_cursor,
                        obs_id=self.obs_id,
                        code_version=version.get_mwax_mover_version_string(),
                        creator="calvin",
                        fit_niter=self.phase_fit_niter,
                        fit_limit=20,
                    )

                    if fit_id is None or not success:
                        self.logger.error(f"{self.data_path} - failed to insert calibration fit")

                        # This will trigger a rollback of the calibration_fit row
                        raise Exception(f"{self.data_path} - failed to insert calibration fit")

                    for tile_id in soln_tile_ids:
                        some_fits = False
                        try:
                            x_gains = gain_fits[(gain_fits.tile_id == tile_id) & (gain_fits.pol == "XX")].iloc[0]
                            some_fits = True
                        except IndexError:
                            x_gains = GainFitInfo.nan()

                        try:
                            y_gains = gain_fits[(gain_fits.tile_id == tile_id) & (gain_fits.pol == "YY")].iloc[0]
                            some_fits = True
                        except IndexError:
                            y_gains = GainFitInfo.nan()

                        try:
                            x_phase = phase_fits[(phase_fits.tile_id == tile_id) & (phase_fits.pol == "XX")].iloc[0]
                            some_fits = True
                        except IndexError:
                            x_phase = PhaseFitInfo.nan()

                        try:
                            y_phase = phase_fits[(phase_fits.tile_id == tile_id) & (phase_fits.pol == "YY")].iloc[0]
                            some_fits = True
                        except IndexError:
                            y_phase = PhaseFitInfo.nan()

                        if not some_fits:
                            # we could `continue` here, which avoids inserting an empty row in the
                            # database, however we want to stick to the old behaviour for now.
                            # continue
                            pass

                        success = insert_calibration_solutions_row(
                            self.db_handler_object,
                            transaction_cursor,
                            int(fit_id),
                            int(self.obs_id),
                            int(tile_id),
                            -1 * x_phase.length,  # legacy calibration pipeline used inverse convention
                            x_phase.intercept,
                            x_gains.gains,
                            -1 * y_phase.length,  # legacy calibration pipeline used inverse convention
                            y_phase.intercept,
                            y_gains.gains,
                            x_gains.pol1,
                            y_gains.pol1,
                            x_phase.sigma_resid,
                            x_phase.chi2dof,
                            x_phase.quality,
                            y_phase.sigma_resid,
                            y_phase.chi2dof,
                            y_phase.quality,
                            x_gains.quality,
                            y_gains.quality,
                            x_gains.sigma_resid,
                            y_gains.sigma_resid,
                            x_gains.pol0,
                            y_gains.pol0,
                        )

                        if not success:
                            self.logger.error(
                                f"{self.data_path} - failed to insert calibration solution for tile {tile_id}"
                            )

                            # This will trigger a rollback of the calibration_fit row and any
                            # calibration_solutions child rows
                            raise Exception(
                                f"{self.data_path} - failed to insert calibration solution for tile {tile_id}"
                            )

            # if we get here, the whole calibration solution was inserted ok.
            # The transaction context will commit the transation
            if not self.keep_completed_visibility_files:
                # Remove visibilitiy files
                visibility_files = glob.glob(os.path.join(self.data_path, f"{self.obs_id}_*_*_*.fits"))
                for file_to_delete in visibility_files:
                    os.remove(file_to_delete)

                # Now remove uvfits too
                uvfits_files = glob.glob(os.path.join(self.data_path, "*.uvfits"))
                for file_to_delete in uvfits_files:
                    os.remove(file_to_delete)

            #
            # If this cal solution was a requested one, update it to completed
            #
            mwax_db.update_calsolution_request_calibration_complete_status(
                self.db_handler_object, self.obs_id, None, datetime.datetime.now(), int(fit_id), None, None
            )

            return True
        except Exception:
            error_text = f"{self.data_path} - Error in upload_handler:\n{traceback.format_exc()}"
            self.logger.exception(error_text)

            # Write an error readme
            write_readme_file(
                self.logger,
                os.path.join(self.data_path, "readme_error.txt"),
                f"upload_handler({self.data_path})",
                -999,
                "",
                error_text,
            )

            #
            # If this cal solution was a requested one, update it to failed
            #
            mwax_db.update_calsolution_request_calibration_complete_status(
                self.db_handler_object,
                self.obs_id,
                None,
                None,
                None,
                datetime.datetime.now(),
                error_text.replace("\n", " "),
            )

            return False

    def stop(self):
        """Shutdown all processes"""
        # Close all database connections
        self.db_handler_object.stop_database_pool()

    def signal_handler(self, _signum, _frame):
        """Handles SIGINT and SIGTERM"""
        self.logger.warning("Interrupted. Shutting down processor...")
        self.running = False

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
        # processing config
        #

        # Get the processing dir
        self.processing_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "processing_path",
        )

        if not os.path.exists(self.processing_path):
            self.logger.error("processing_path location " f" {self.processing_path} does not exist. Quitting.")
            sys.exit(1)

        self.phase_fit_niter = int(
            utils.read_config(
                self.logger,
                config,
                "processing",
                "phase_fit_niter",
            )
        )

        #
        # Hyperdrive config
        #
        self.source_list_filename = utils.read_config(
            self.logger,
            config,
            "processing",
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
            "processing",
            "source_list_type",
        )

        # hyperdrive timeout
        self.hyperdrive_timeout = int(
            utils.read_config(
                self.logger,
                config,
                "processing",
                "hyperdrive_timeout",
            )
        )

        # Get the hyperdrive binary
        self.hyperdrive_binary_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "hyperdrive_binary_path",
        )

        if not os.path.exists(self.hyperdrive_binary_path):
            self.logger.error(
                "hyperdrive_binary_path location " f" {self.hyperdrive_binary_path} does not exist. Quitting."
            )
            sys.exit(1)

        # Birli timeout
        self.birli_timeout = int(
            utils.read_config(
                self.logger,
                config,
                "processing",
                "birli_timeout",
            )
        )

        # Get Birli max mem
        self.birli_max_mem_gib = int(
            utils.read_config(
                self.logger,
                config,
                "processing",
                "birli_max_mem_gib",
            )
        )

        # Get the Birli binary
        self.birli_binary_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "birli_binary_path",
        )

        if not os.path.exists(self.birli_binary_path):
            self.logger.error("birli_binary_path location " f" {self.birli_binary_path} does not exist. Quitting.")
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

        try:
            job_type = CalvinJobType[args["job_type"]]
        except ValueError:
            print(f"ERROR: cmd line argument job-type {args["job_type"]} is not a valid job type. Aborting.")
            exit(-1)

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
        request_ids_string: str = args["request-ids"]
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

        self.initialise(config_filename, int(obs_id), int(slurm_job_id), job_type, mwa_asvo_download_url, request_ids)

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
