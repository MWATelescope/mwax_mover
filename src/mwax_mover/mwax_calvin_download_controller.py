"""
Module hosting the MWAXCalvinDownloadProcessor to
handle any unprocessed MWA calibration solution
requests
"""

import argparse
import datetime
import coloredlogs
from configparser import ConfigParser
import json
import logging
import os
import signal
import sys
import threading
from typing import Optional
import time
from mwax_mover import version, mwax_db, utils, mwax_asvo_helper
from mwax_mover.mwax_calvin_utils import submit_sbatch, create_sbatch_script, CalvinJobType

SLEEP_MWA_ASVO_OUTAGE_SECS = 2 * 60


class MWAXCalvinDownloadProcessor:
    """The main class handling calibration solution requests and
    submitting MWA ASVO jobs to downloading data. NOTE: no downloading
    is done in this code. It is handled by CalvinProcessor as it will
    be running on HPC nodes"""

    def __init__(
        self,
    ):
        # General
        self.logger = logging.getLogger(__name__)
        self.log_path: str = ""
        self.log_level: str = ""
        self.hostname: str = ""
        self.db_handler_object: mwax_db.MWAXDBHandler

        # health
        self.health_multicast_interface_ip: str = ""
        self.health_multicast_interface_name: str = ""
        self.health_multicast_ip: str = ""
        self.health_multicast_port: int = 0
        self.health_multicast_hops: int = 1

        # mwa asvo
        self.check_interval_seconds: int = 0
        self.mwa_asvo_longest_wait_time_seconds: int = 0
        self.giant_squid_binary_path: str = ""
        self.giant_squid_list_timeout_seconds: int = 0
        self.giant_squid_submitvis_timeout_seconds: int = 0

        # SBatch stuff
        self.script_path = ""

        # Helper for MWA ASVO interactions and job record keeping
        self.mwax_asvo_helper: mwax_asvo_helper.MWAASVOHelper = mwax_asvo_helper.MWAASVOHelper()

        self.running: bool = False
        self.ready_to_exit: bool = False

    def add_new_job(self, request_id: int, obs_id: int):
        """Starts tracking a new MWAASVOJob and, if not submitted already,
        submits the job to MWA ASVO

        Parameters:
            request_id (int): the request_id for this job
            obs_id (int): the obs_id for this job

        Returns:
            Nothing. Exceptions can be raised though
        """

        # Check if we have this obs_id tracked
        asvo_job = self.mwax_asvo_helper.get_first_job_for_obs_id(obs_id)

        # If this obs exists in another job AND we have not yet submitted it to slurm
        # Then just add this request onto the existing job
        if asvo_job and not asvo_job.download_slurm_job_submitted:
            # Found!
            asvo_job.request_ids.append(request_id)

            # Update database
            #
            # The point of this is:
            # If we are already handling obsid X, then another bunch of requests come through
            # we should "catch them up" to the current status in the database
            mwax_db.update_calsolution_request_submit_mwa_asvo_job(
                self.db_handler_object,
                asvo_job.request_ids,
                asvo_job.submitted_datetime,
                asvo_job.job_id,
            )
        else:
            # Not found
            try:
                # Submit job and add to the ones we are tracking
                new_job = self.mwax_asvo_helper.submit_download_job(request_id, obs_id)
            except mwax_asvo_helper.GiantSquidMWAASVOOutageException:
                # Handle me!
                self.logger.info(
                    "MWA ASVO has an outage. Doing nothing this loop, and sleeping "
                    f"for {SLEEP_MWA_ASVO_OUTAGE_SECS} seconds."
                )
                self.sleep(SLEEP_MWA_ASVO_OUTAGE_SECS)
                return
            except Exception as e:
                # Some other fatal error occurred, let's log it and update the db
                error_message = f"Error submitting job for ObsID {obs_id} RequestID {request_id}."
                self.logger.exception(error_message)
                error_message = error_message + f" {str(e)}"
                mwax_db.update_calsolution_request_download_complete_status(
                    self.db_handler_object,
                    [
                        request_id,
                    ],
                    None,
                    datetime.datetime.now(),
                    error_message,
                )
                return

            # We submmited a new MWA ASVO job, update the request table so we know we're on it!
            # Update database
            mwax_db.update_calsolution_request_submit_mwa_asvo_job(
                self.db_handler_object,
                new_job.request_ids,
                mwa_asvo_submitted_datetime=new_job.submitted_datetime,
                mwa_asvo_job_id=new_job.job_id,
            )

    def main_loop_handler(self):
        self.get_new_requests()
        self.update_all_tracked_jobs()
        self.submit_slurm_for_ready_mwa_asvo_jobs()
        pass

    def get_new_requests(self):
        """This code checks for any unassigned requests and assigns them to this host,
        adding them to our tracked jobs in self.mwa_asvo_helper.current_asvo_jobs"""

        if self.running:
            self.logger.debug("Querying database for unattempted calsolution_requests...")

            # 1. Get the an outstanding calibration_requests from the db
            # returned fields: Tuple[id, calid] or None (the obsid we are calibrating)
            result = mwax_db.assign_next_unattempted_calsolution_request(self.db_handler_object, self.hostname)

            if result:
                # get the id of the request
                request_id = int(result[0])

                # Get the obs_id
                obs_id = int(result[1])

                # If we are not already dealing with this obsid, add it!
                # This prevents us pulling in dupes
                if self.mwax_asvo_helper.get_first_job_for_obs_id(obs_id) is None:
                    self.logger.info(f"Resuming RequestID: {request_id} ObsID: {obs_id}")
                    self.add_new_job(request_id, obs_id)

    def update_all_tracked_jobs(self):
        # Find out the status of all this user's jobs in MWA ASVO
        # Get the job list from giant-squid, populating current_asvo_jobs
        # If we find a job in giant-squid which we don't know about,
        # DON'T include it in the list we track
        if self.running:
            self.logger.debug("Getting latest MWA ASVO job statuses...")
            try:
                self.mwax_asvo_helper.update_all_job_status()

            except mwax_asvo_helper.GiantSquidMWAASVOOutageException:
                # Handle me!
                self.logger.info(
                    "MWA ASVO has an outage. Doing nothing this loop, and "
                    f"sleeping for {SLEEP_MWA_ASVO_OUTAGE_SECS} seconds."
                )
                self.sleep(SLEEP_MWA_ASVO_OUTAGE_SECS)

            except Exception:
                # TODO - maybe some exceptions we should back off instead of exiting?
                self.logger.exception("Fatal exception- exiting!")
                self.running = False
                self.stop()

    def submit_slurm_for_ready_mwa_asvo_jobs(self):
        """This code will check for any jobs which can be downloaded and submit
        sbatch script to SLURM for them to be handled/downloaded/processed by
        the calvin processor"""
        error_message: str = ""

        if self.running:
            for job in self.mwax_asvo_helper.current_asvo_jobs:
                if not job.download_slurm_job_submitted:
                    if job.job_state == mwax_asvo_helper.MWAASVOJobState.Error:
                        # MWA ASVO completed this job with error
                        error_message = "MWA ASVO completed this job with an Error state"
                        self.logger.warning(f"{job}: {error_message}")

                        job.download_error_datetime = datetime.datetime.now()
                        job.download_error_message = error_message

                        # Update database
                        mwax_db.update_calsolution_request_download_complete_status(
                            self.db_handler_object,
                            job.request_ids,
                            None,
                            job.download_error_datetime,
                            job.download_error_message,
                        )

                    elif job.job_state == mwax_asvo_helper.MWAASVOJobState.Ready:
                        try:
                            self.logger.info(f"{job}: Submitting slurm job")

                            script = create_sbatch_script(
                                self.config_filename,
                                job.obs_id,
                                CalvinJobType.mwa_asvo,
                                self.log_path,
                                f"--mwa-asvo-download-url={job.download_url}",
                            )

                            # submit sbatch script
                            submit_sbatch(self.logger, self.script_path, script, job.obs_id)

                            # all is good- remove job from current jobs to be checked
                            job.download_slurm_job_submitted = True
                            job.download_slurm_job_submitted_datetime = datetime.datetime.now()

                        except Exception:
                            # Something went wrong!
                            self.logger.exception(f"{job}: Error submitting sbtach to SLURM.")

    def start(self):
        """Start the processor"""
        self.running = True

        # creating database connection pool(s)
        self.logger.info("Starting database connection pool...")
        self.db_handler_object.start_database_pool()

        # create a health thread
        self.logger.info("Starting health_thread...")
        health_thread = threading.Thread(name="health_thread", target=self.health_loop, daemon=True)
        health_thread.start()

        self.logger.info("Started...")

        # Main loop
        while self.running:
            self.main_loop_handler()

            self.logger.debug("Currently tracking jobs:")
            for job in self.mwax_asvo_helper.current_asvo_jobs:
                if not (job.download_slurm_job_submitted or job.download_error_datetime):
                    self.logger.debug(
                        f"{job} {job.job_state}; "
                        f"elapsed: {job.elapsed_time_seconds()} s; "
                        f"last_seen={job.last_seen_datetime}"
                    )

            self.logger.debug("History of completed jobs:")
            for job in self.mwax_asvo_helper.current_asvo_jobs:
                if job.download_slurm_job_submitted:
                    self.logger.debug(f"{job} succeeded: {job.download_slurm_job_submitted_datetime} ")
                elif job.download_error_datetime:
                    self.logger.debug(f"{job} failed: {job.download_error_datetime} {job.download_error_message}")

            self.logger.debug(f"Sleeping for {self.check_interval_seconds} seconds")
            self.sleep(self.check_interval_seconds)

        #
        # Finished- do some clean up
        #
        while not self.ready_to_exit:
            self.sleep(1)

        # Final log message
        self.logger.info("Completed Successfully")

    def stop(self):
        """Shutsdown all processes"""

        self.logger.warning("Stopping...")

        # Close all database connections
        self.db_handler_object.stop_database_pool()

        self.ready_to_exit = True

    def health_loop(self):
        """Send health information via UDP multicast"""
        while self.running:
            # Code to run by the health thread
            status_dict = self.get_status()

            # Convert the status to bytes
            status_bytes = json.dumps(status_dict).encode("utf-8")

            # Send the bytes
            try:
                utils.send_multicast(
                    self.health_multicast_interface_ip,
                    self.health_multicast_ip,
                    self.health_multicast_port,
                    status_bytes,
                    self.health_multicast_hops,
                )
            except Exception as catch_all_exception:  # pylint: disable=broad-except
                self.logger.warning("health_handler: Failed to send health information." f" {catch_all_exception}")

            # Sleep for a second
            self.sleep(1)

    def get_status(self) -> dict:
        """Returns status of process as a dictionary"""
        main_status = {
            "Unix timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
        }

        job_status_list = []
        for job in self.mwax_asvo_helper.current_asvo_jobs:
            job_status_list.append(job.get_status())

        status = {"main": main_status, "jobs": job_status_list}

        return status

    def signal_handler(self, _signum, _frame):
        """Handles SIGINT and SIGTERM"""
        self.logger.warning("Interrupted. Shutting down processor...")
        self.running = False

        # Stop any Processors
        self.stop()

    def initialise(self, config_filename: str):
        """Initialise the processor from the command line"""
        self.config_filename = config_filename

        # Get this hosts hostname
        self.hostname = utils.get_hostname()

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

        self.logger.info("Starting mwax_calvin_download_processor" f" ...v{version.get_mwax_mover_version_string()}")

        self.logger.info(f"Reading config file: {config_filename}")

        # health
        self.health_multicast_ip = utils.read_config(self.logger, config, "mwax mover", "health_multicast_ip")
        self.health_multicast_port = int(utils.read_config(self.logger, config, "mwax mover", "health_multicast_port"))
        self.health_multicast_hops = int(utils.read_config(self.logger, config, "mwax mover", "health_multicast_hops"))
        self.health_multicast_interface_name = utils.read_config(
            self.logger,
            config,
            "mwax mover",
            "health_multicast_interface_name",
        )

        # get this hosts primary network interface ip
        self.health_multicast_interface_ip = utils.get_ip_address(self.health_multicast_interface_name)
        self.logger.info(f"IP for sending multicast: {self.health_multicast_interface_ip}")

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
        # MWA ASVO config
        #

        # How long between iterations of the main loop (in seconds)
        self.check_interval_seconds = int(utils.read_config(self.logger, config, "mwa_asvo", "check_interval_seconds"))

        # How many secs do we wait for MWA ASVO to get us a completed job??
        self.mwa_asvo_longest_wait_time_seconds = int(
            utils.read_config(self.logger, config, "mwa_asvo", "mwa_asvo_longest_wait_time_seconds")
        )

        # Get the giant squid binary
        self.giant_squid_binary_path = utils.read_config(
            self.logger,
            config,
            "mwa_asvo",
            "giant_squid_binary_path",
        )

        if not os.path.exists(self.giant_squid_binary_path):
            self.logger.error(
                "giant_squid_binary_path location " f" {self.giant_squid_binary_path} does not exist. Quitting."
            )
            sys.exit(1)

        # How long do we wait for giant-squid to execute a list subcommand
        self.giant_squid_list_timeout_seconds = int(
            utils.read_config(self.logger, config, "mwa_asvo", "giant_squid_list_timeout_seconds")
        )

        # How long do we wait for giant-squid to execute a submit-vis subcommand
        self.giant_squid_submitvis_timeout_seconds = int(
            utils.read_config(self.logger, config, "mwa_asvo", "giant_squid_submitvis_timeout_seconds")
        )

        # script path (path for keeping all sbatch scripts)
        self.script_path = config.get("mwax mover", "script_path")

        if not os.path.exists(self.script_path):
            print(f"script_path {self.script_path} does not exist. Quiting.")
            sys.exit(1)

        # Setup the MWA ASVO Helper
        # Note dummy values for download timeout and download path as in this
        # use of the helper, we don't do any downloading!
        self.mwax_asvo_helper.initialise(
            self.logger,
            self.giant_squid_binary_path,
            self.giant_squid_list_timeout_seconds,
            self.giant_squid_submitvis_timeout_seconds,
        )

    def initialise_from_command_line(self):
        """Initialise if initiated from command line"""

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_calvin_download_processor: a command line tool which is part of the"
            " MWA calvin calibration service for the MWA. It checks for unprocessed records in"
            " the calibration_requests table, submits and manages an MWA ASVO job for"
            " each, then once ready, submits a slurm job for the data to be downloaded and "
            "processed by the calvin_processor on a hpc node"
            f" calibrate. (mwax_mover v{version.get_mwax_mover_version_string()})\n"
        )

        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        self.initialise(config_filename)

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
    processor = MWAXCalvinDownloadProcessor()

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
