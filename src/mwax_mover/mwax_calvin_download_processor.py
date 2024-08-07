"""
Module hosting the MWAXCalvinDownloadProcessor to
download any unprocessed MWA calibration solution
requests
"""

import argparse
import datetime
import coloredlogs
from configparser import ConfigParser
import json
import logging
import logging.handlers
import os
import signal
import sys
import threading
import time
from mwax_mover import version, mwax_db, utils, mwax_asvo_helper


class MWAXCalvinDownloadProcessor:
    """The main class processing calibration solution requests and downloading data"""

    def __init__(
        self,
    ):
        # General
        self.logger = logging.getLogger(__name__)
        self.log_path = None
        self.hostname = None
        self.db_handler_object: mwax_db.MWAXDBHandler = None

        # health
        self.health_multicast_interface_ip = None
        self.health_multicast_interface_name = None
        self.health_multicast_ip = None
        self.health_multicast_port = None
        self.health_multicast_hops = None

        # mwa asvo
        self.download_path = None
        self.check_interval_seconds = None
        self.mwa_asvo_longest_wait_time_seconds = None
        self.giant_squid_binary_path = None
        self.giant_squid_list_timeout_seconds = None
        self.giant_squid_submitvis_timeout_seconds = None
        self.giant_squid_download_timeout_seconds = None

        # Helper for MWA ASVO interactions and job record keeping
        self.mwax_asvo_helper: mwax_asvo_helper.MWAASVOHelper = mwax_asvo_helper.MWAASVOHelper()

        self.running = False
        self.ready_to_exit = False

    def main_loop_handler(self):
        """This is the main loop handler for this process.
        1. check and handle MWA ASVO jobs in progress
        2. check and handle new requests in the database"""

        # Background- this code needs to handle the following cases:
        # a) Normal operation. Process has been running and keeps running.
        # b) Process stopped (for some reason). Then process restarts.
        #    Need to deal with:
        #    * Cal requests which are already under way- in ASVO
        #    * or already processing on calvin
        #    * or already processed and failed on calvin
        if self.running:
            self.logger.debug("Querying database for unattempted calsolution_requests...")
            # 1. Get the an outstanding calibration_requests from the db
            # returned fields: Tuple[id, calid] or None (the obsid we are calibrating)
            result = mwax_db.assign_next_unattempted_calsolution_request(self.db_handler_object, self.hostname)

            if result:
                self.logger.debug(f"Found {len(result)} unattempted calsolution_requests to process.")

                # get the id of the request
                request_id = int(result[0])

                # Get the obs_id
                obs_id = int(result[1])

                # Check if we have this obs_id tracked
                asvo_job = None

                for job in self.mwax_asvo_helper.current_asvo_jobs:
                    if job.obs_id == obs_id:
                        asvo_job = job

                        asvo_job.request_ids.append(request_id)

                        # Update database
                        mwax_db.update_calsolution_request_submit_mwa_asvo_job(
                            self.db_handler_object,
                            asvo_job.request_ids,
                            mwa_asvo_submitted_datetime=asvo_job.submitted_datetime,
                            mwa_asvo_job_id=asvo_job.job_id,
                        )
                        break

                if not asvo_job:
                    try:
                        # Submit job and add to the ones we are tracking
                        new_job = self.mwax_asvo_helper.submit_download_job(request_id, obs_id)
                    except mwax_asvo_helper.GiantSquidMWAASVOOutageException:
                        # Handle me!
                        self.logger.info("MWA ASVO has an outage. Doing nothing this loop, and sleeping for 10 mins.")
                        time.sleep(10 * 60 * 60)
                        return
                    except Exception:
                        # TODO - maybe some exceptions we should back off instead of exiting?
                        self.logger.exception("Fatal exception- exiting!")
                        self.running = False
                        self.stop()
                        return

                    # We submmited a new MWA ASVO job, update the request table so we know we're on it!
                    # Update database
                    mwax_db.update_calsolution_request_submit_mwa_asvo_job(
                        self.db_handler_object,
                        new_job.request_ids,
                        mwa_asvo_submitted_datetime=new_job.submitted_datetime,
                        mwa_asvo_job_id=new_job.job_id,
                    )

        # 2. Find out the status of all this user's jobs in MWA ASVO
        # Get the job list from giant-squid, populating current_asvo_jobs
        # If we find a job in giant-squid which we don't know about,
        # DON'T include it in the list we track
        if self.running:
            self.logger.debug("Getting latest MWA ASVO job statuses...")
            try:
                self.mwax_asvo_helper.update_all_job_status()

            except mwax_asvo_helper.GiantSquidMWAASVOOutageException:
                # Handle me!
                self.logger.info("MWA ASVO has an outage. Doing nothing this loop, and sleeping for 10 mins.")
                time.sleep(10 * 60 * 60)
                return
            except Exception:
                # TODO - maybe some exceptions we should back off instead of exiting?
                self.logger.exception("Fatal exception- exiting!")
                self.running = False
                self.stop()

        # 3. See if any in the list need actioning
        if self.running:
            self.logger.debug("Checking to see if any jobs are ready to download...")
            self.handle_mwa_asvo_jobs()

    def handle_mwa_asvo_jobs(self):
        """This code will check for any jobs which can be downloaded and start
        downloading them"""
        DOWNLOAD_RETRIES: int = 3

        for job in self.mwax_asvo_helper.current_asvo_jobs:
            # Check job is in Ready state
            if job.job_state == mwax_asvo_helper.MWAASVOJobState.Ready:
                try:
                    self.logger.info(
                        f"{job}: Attempting to download (attempt: {job.download_retries + 1}/{DOWNLOAD_RETRIES})"
                    )

                    job.download_started_datetime = datetime.datetime.now()
                    # Update database
                    mwax_db.update_calsolution_request_download_started_status(
                        self.db_handler_object, job.request_ids, job.download_started
                    )

                    # Download the data (blocks until data is downloaded or exception fired)
                    self.mwax_asvo_helper.download_asvo_job(job)

                    # Update database
                    mwax_db.update_calsolution_request_download_complete_status(
                        self.db_handler_object, job.request_ids, datetime.datetime.now(), None, None
                    )

                except mwax_asvo_helper.GiantSquidMWAASVOOutageException:
                    # Handle me!
                    self.logger.info("MWA ASVO has an outage. Doing nothing this loop, and sleeping for 10 mins.")
                    time.sleep(10 * 60 * 60)
                    return

                except Exception as e:
                    # Something went wrong!
                    self.logger.exception(f"{job}: Error downloading Job.")
                    error_message = f"{job}: Error downloading Job: {str(e)}"

                # Remove it if the job if successful!
                # If not, it should retry in the next "handle_mwa_asvo_jobs loop"
                if job.download_completed:
                    self.mwax_asvo_helper.current_asvo_jobs.remove(job)
                    self.logger.info(f"{job}: downloaded successfully. Removing from current jobs.")
                else:
                    # Download failed, increment retries counter
                    job.download_retries += 1

                    # Check if we've had too many retries
                    if job.download_retries > DOWNLOAD_RETRIES:
                        self.logger.error(
                            f"{job}: Fatal exception- too many retries {DOWNLOAD_RETRIES} when trying to download."
                            "Exiting!"
                        )

                        # Update database
                        error_message = error_message + f"-(too many retries {DOWNLOAD_RETRIES})"
                        mwax_db.update_calsolution_request_download_complete_status(
                            self.db_handler_object, job.request_ids, None, datetime.datetime.now(), error_message
                        )

                        self.running = False
                        self.stop()
                        return

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

            for job in self.mwax_asvo_helper.current_asvo_jobs:
                self.logger.debug(f"{job} {job.job_state}")

            self.logger.debug(f"Sleeping for {self.check_interval_seconds} seconds")
            time.sleep(self.check_interval_seconds)

        #
        # Finished- do some clean up
        #
        while not self.ready_to_exit:
            time.sleep(1)

        # Final log message
        self.logger.info("Completed Successfully")

    def stop(self):
        """Shutsdown all processes"""

        self.logger.warning("Stopping...")

        # Close all database connections
        if not self.db_handler_object.dummy:
            self.db_handler_object.pool = None

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
            time.sleep(1)

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

    def initialise(self, config_filename):
        """Initialise the processor from the command line"""
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

        # It's now safe to start logging
        # start logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        console_log = logging.StreamHandler()
        console_log.setLevel(logging.DEBUG)
        console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        self.logger.addHandler(console_log)

        if config.getboolean("mwax mover", "coloredlogs", fallback=False):
            coloredlogs.install(level="DEBUG", logger=self.logger)

        # Removing file logging for now
        # file_log = logging.FileHandler(filename=os.path.join(self.log_path, "calvin_download_processor_main.log"))
        # file_log.setLevel(logging.DEBUG)
        # file_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        # self.logger.addHandler(file_log)

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

        if self.mro_metadatadb_host != mwax_db.DUMMY_DB:
            self.mro_metadatadb_db = utils.read_config(self.logger, config, "mro metadata database", "db")
            self.mro_metadatadb_user = utils.read_config(self.logger, config, "mro metadata database", "user")
            self.mro_metadatadb_pass = utils.read_config(self.logger, config, "mro metadata database", "pass", True)
            self.mro_metadatadb_port = utils.read_config(self.logger, config, "mro metadata database", "port")
        else:
            self.mro_metadatadb_db = None
            self.mro_metadatadb_user = None
            self.mro_metadatadb_pass = None
            self.mro_metadatadb_port = None

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

        # Get the incoming dir
        self.download_path = utils.read_config(
            self.logger,
            config,
            "mwa_asvo",
            "download_path",
        )

        if not os.path.exists(self.download_path):
            self.logger.error("download_path location " f" {self.download_path} does not exist. Quitting.")
            sys.exit(1)

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

        # How long do we wait for giant-squid to execute a download subcommand
        self.giant_squid_download_timeout_seconds = int(
            utils.read_config(self.logger, config, "mwa_asvo", "giant_squid_download_timeout_seconds")
        )

        # Setup the MWA ASVO Helper
        self.mwax_asvo_helper.initialise(
            self.logger,
            self.giant_squid_binary_path,
            self.giant_squid_list_timeout_seconds,
            self.giant_squid_submitvis_timeout_seconds,
            self.giant_squid_download_timeout_seconds,
            self.download_path,
        )

    def initialise_from_command_line(self):
        """Initialise if initiated from command line"""

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_calvin_download_processor: a command line tool which is part of the"
            " MWA calvin calibration service for the MWA. It checks for unprocessed records in"
            " the calibration_requests table, submits and manages an MWA ASVO job for"
            " each, then once ready, downloads the data for the calvin_processor to"
            f" calibrate. (mwax_mover v{version.get_mwax_mover_version_string()})\n"
        )

        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        self.initialise(config_filename)


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
