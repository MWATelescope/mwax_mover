"""
Module managing the MWAXCalvinProcessor for near-realtime
calibration form the calvin HPC cluster

* Realtime Calibration Jobs
1. Detect new calibration observation completed
2. Create SBATCH script:
   * Copy data from all MWAX boxes for all files of the observation
   * Let MWAX boxes know they can release the files
   * Process and upload the solution
   * Clean up

* MWA ASVO Calibration Jobs
1. Detect new calibration requested by MWA ASVO
2. Create SBATCH script:
   * Submit MWA ASVO job
   * Download files
   * Process and upload the solution
   * Clean up
"""

import argparse
from configparser import ConfigParser
from datetime import datetime, timedelta
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Optional
from mwax_mover import utils, version, mwax_asvo_helper
from mwax_mover.mwax_db import (
    MWAXDBHandler,
    get_unattempted_calsolution_requests,
    update_calsolution_request_submit_mwa_asvo_job_status,
    update_calibration_request_slurm_status,
    get_unattempted_unrequested_cal_obsids,
    insert_calibration_request_row,
)
from mwax_mover.mwax_calvin_utils import submit_sbatch, create_sbatch_script, CalvinJobType


class CalibrationRequest:
    def __init__(self):
        self.obs_id: int = 0
        self.request_id: int = 0
        self.type: CalvinJobType


class MWAXCalvinController:
    """The main class managing calvin processes:

    realtime: Get new realtime calibration requests, copies data from mwax
    hosts, then submits job to process them and upload solutions to the db.

    mwa_asvo: Gets new MWA ASVO calibration requests, submits MWA ASVO jobs to download
    data, then submits job to process them and upload solutions to the db.
    NOTE: no downloading is done in this code. It is handled by CalvinProcessor as it
    will be running on the calvin HPC nodes"""

    def __init__(
        self,
    ):
        # General
        self.logger = logging.getLogger(__name__)
        self.log_path: str = ""
        self.log_level: str = ""
        self.hostname: str = ""
        self.db_handler_object: MWAXDBHandler
        self.config_filename: str = ""
        self.worker_config_filename: str = ""
        self.running: bool = False
        self.ready_to_exit: bool = False

        # health
        self.health_multicast_interface_ip: str = ""
        self.health_multicast_interface_name: str = ""
        self.health_multicast_ip: str = ""
        self.health_multicast_port: int = 0
        self.health_multicast_hops: int = 0

        self.realtime_slurm_jobs_submitted: int = 0
        self.mwa_asvo_slurm_jobs_submitted: int = 0
        self.giant_squid_errors: int = 0
        self.database_errors: int = 0
        self.slurm_errors: int = 0
        self.mwa_asvo_errors: int = 0

        # calvin settings
        self.check_interval_seconds: int = 0
        self.script_path = ""

        # giant-squid
        self.mwa_asvo_outage_check_seconds: int = 0
        self.mwa_asvo_longest_wait_time_seconds: int = 0
        self.giant_squid_binary_path: str = ""
        self.giant_squid_list_timeout_seconds: int = 0
        self.giant_squid_submitvis_timeout_seconds: int = 0

        # Helper for MWA ASVO interactions and job record keeping
        self.mwax_asvo_helper: mwax_asvo_helper.MWAASVOHelper = mwax_asvo_helper.MWAASVOHelper()

    def start(self):
        """Start the controller"""
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

            tracked_asvo_job_count: int = 0
            for job in self.mwax_asvo_helper.current_asvo_jobs:
                if job.download_slurm_job_submitted is False and job.download_error_datetime is None:
                    tracked_asvo_job_count += 1
                    self.logger.debug(
                        f"{job} {job.job_state}; "
                        f"elapsed: {job.elapsed_time_seconds()} s; "
                        f"last_seen={job.last_seen_datetime}"
                    )
            if tracked_asvo_job_count > 0:
                self.logger.debug(f"Currently tracking {tracked_asvo_job_count} MWA ASVO jobs.")
            else:
                self.logger.debug("Not tracking any MWA ASVO jobs.")

            # If we're still running, wait before we do the next loop
            if self.running:
                self.logger.debug(f"Sleeping for {self.check_interval_seconds} seconds")
                self.sleep(self.check_interval_seconds)
        #
        # Finished- do some clean up
        #
        while not self.ready_to_exit:
            self.sleep(1)

        # Final log message
        self.logger.info("Completed Successfully")

    def main_loop_handler(self):
        # Look at the schedule and create cal requests for any unattempted calibrator observations
        try:
            self.realtime_create_requests_for_unattempted_cal_obs()
        except Exception:
            self.logger.exception("Error creating requests for unattempted cal obs")
            self.database_errors += 1

        # get new requests
        try:
            realtime_requests_list, asvo_requests_list = self.get_new_calibration_requests()

            # Handle realtime requests first!
            for cal_request in realtime_requests_list:
                try:
                    self.realtime_submit_to_slurm(cal_request)
                    self.realtime_slurm_jobs_submitted += 1
                except Exception:
                    self.logger.exception("Error submitting realtime slurm job")
                    self.slurm_errors += 1

            # now handle ASVO
            if self.mwax_asvo_helper.mwa_asvo_outage_datetime:
                # There was an outage at some point.
                # If it's been long enough reset the outage and retry
                elapsed: timedelta = datetime.now() - self.mwax_asvo_helper.mwa_asvo_outage_datetime
                if elapsed.total_seconds() >= self.mwa_asvo_outage_check_seconds:
                    # Reset the MWA ASVO outage so we retry
                    self.mwax_asvo_helper.mwa_asvo_outage_datetime = None

            for cal_request in asvo_requests_list:
                # For mwa_asvo, if we are not already dealing with this obsid, add it!
                # This prevents us pulling in dupes
                try:
                    self.mwa_asvo_add_new_asvo_job(cal_request.request_id, cal_request.obs_id)
                    self.mwa_asvo_slurm_jobs_submitted += 1
                except Exception:
                    self.logger.exception("Error submitting asvo slurm job")
                    self.slurm_errors += 1

        except Exception:
            self.logger.exception("Error retrieving new calibration requests")
            self.database_errors += 1

        # For mwa_asvo requests, if we're not in an MWA ASVO outage, update jobs check for ready ones
        if self.mwax_asvo_helper.mwa_asvo_outage_datetime is None:
            self.mwa_asvo_update_tracked_jobs()
            self.mwa_asvo_submit_ready_asvo_jobs_to_slurm()

    def realtime_create_requests_for_unattempted_cal_obs(self):
        # First get the obs's which need requests created
        obs_ids_to_request: Optional[list[int]] = get_unattempted_unrequested_cal_obsids(self.db_handler_object)

        if obs_ids_to_request:
            # Insert them all as requests
            for obs_id in obs_ids_to_request:
                insert_calibration_request_row(self.db_handler_object, obs_id)

    def realtime_submit_to_slurm(self, realtime_request: CalibrationRequest):
        # Create a sbatch script
        script = create_sbatch_script(
            self.worker_config_filename,
            realtime_request.obs_id,
            CalvinJobType.realtime,
            self.log_path,
            [str(realtime_request.request_id)],
            "",
        )

        success: bool = False
        slurm_job_id: Optional[int] = None

        # submit sbatch script
        try:
            (success, slurm_job_id) = submit_sbatch(self.logger, self.script_path, script, realtime_request.obs_id)
        except Exception:
            self.logger.exception(
                f"{str(realtime_request.obs_id)}: Unable to submit a realtime calibration "
                "sbatch job. Will retry next loop"
            )
            return

        if success and slurm_job_id is not None:
            # Success- update request to ensure it does not get picked up next loop
            # i.e. we have already submitted it, so don't do it again!
            update_calibration_request_slurm_status(
                self.db_handler_object,
                [
                    realtime_request.request_id,
                ],
                slurm_job_id,
                datetime.now().astimezone(),
                None,
                None,
            )
        else:
            error_message = (
                f"Unable to submit {realtime_request.obs_id} to SLURM for realtime calibration. Will retry next loop"
            )
            self.logger.error(error_message)

    def mwa_asvo_submit_ready_asvo_jobs_to_slurm(self):
        """This code will check for any jobs which can be downloaded and submit
        sbatch script to SLURM for them to be handled/downloaded/processed by
        the calvin processor"""
        error_message: str = ""

        for job in self.mwax_asvo_helper.current_asvo_jobs:
            if not job.download_slurm_job_submitted:
                if job.job_state == mwax_asvo_helper.MWAASVOJobState.Error:
                    # MWA ASVO completed this job with error
                    error_message = "MWA ASVO completed this job with an Error state"
                    self.logger.warning(f"{job}: {error_message}")

                    self.mwa_asvo_errors += 1

                    job.download_error_datetime = datetime.now().astimezone()
                    job.download_error_message = error_message

                    # Update database
                    try:
                        update_calsolution_request_submit_mwa_asvo_job_status(
                            self.db_handler_object,
                            job.request_ids,
                            job.job_id,
                            None,
                            job.download_error_datetime,
                            job.download_error_message,
                        )
                    except Exception:
                        self.logger.exception("Unable to update calibration_request table")
                        self.database_errors += 1

                elif job.job_state == mwax_asvo_helper.MWAASVOJobState.Ready:
                    try:
                        self.logger.info(f"{job}: Submitting slurm job")

                        script = create_sbatch_script(
                            self.worker_config_filename,
                            job.obs_id,
                            CalvinJobType.mwa_asvo,
                            self.log_path,
                            [str(r) for r in job.request_ids],
                            f'--mwa-asvo-download-url="{job.download_url}"',
                        )

                        # submit sbatch script
                        success = False
                        slurm_job_id = None
                        try:
                            (success, slurm_job_id) = submit_sbatch(self.logger, self.script_path, script, job.obs_id)
                        except Exception:
                            self.logger.exception("Unable to submit MWA ASVO slurm job")
                            self.slurm_errors += 1

                        # all is good
                        if success and slurm_job_id is not None:
                            job.download_slurm_job_submitted = True
                            job.download_slurm_job_id = slurm_job_id
                            job.download_slurm_job_submitted_datetime = datetime.now().astimezone()

                            # Now update the database with the jobid
                            update_calibration_request_slurm_status(
                                self.db_handler_object,
                                job.request_ids,
                                slurm_job_id,
                                job.download_slurm_job_submitted_datetime,
                                None,
                                None,
                            )

                    except Exception:
                        # Something went wrong!
                        self.logger.exception(
                            f"{job}: Exception submitting sbtach to SLURM, will try again in next loop"
                        )
                        self.slurm_errors += 1

    def stop(self):
        """Shutsdown all processes"""

        self.running = False

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
        """Returns status of all process as a dictionary"""
        main_status = {
            "Unix timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
            "realtime_slurm_jobs_submitted": self.realtime_slurm_jobs_submitted,
            "mwa_asvo_slurm_jobs_submitted": self.mwa_asvo_slurm_jobs_submitted,
            "giant_squid_errors": self.giant_squid_errors,
            "mwa_asvo_errors": self.mwa_asvo_errors,
            "database_errors": self.database_errors,
            "slurm_erors": self.slurm_errors,
        }

        status = {"main": main_status}

        return status

    def signal_handler(self, _signum, _frame):
        """Handles SIGINT and SIGTERM"""
        self.logger.warning("Interrupted. Shutting down processor...")

        # Stop any Processors
        self.stop()

    def get_new_calibration_requests(self) -> tuple[list[CalibrationRequest], list[CalibrationRequest]]:
        """This code checks for any unassigned requests and assigns them to this host.
        For mwaasvo jobs, it also adds them to our tracked jobs in
        self.mwa_asvo_helper.current_asvo_jobs

        returns a tuple of realtime requests and asvo requests respectively"""

        return_list_realtime: list[CalibrationRequest] = []
        return_list_asvo: list[CalibrationRequest] = []

        if self.running:
            self.logger.debug("Querying database for unattempted calsolution_requests...")

            # 1. Get the an outstanding calibration_requests from the db
            # 2. Also update the database to prevent the next call from picking up the same
            # request(s)
            #
            # returned fields: list[Tuple[id, calid, realtime]] or None (the obsid we are calibrating)
            results = get_unattempted_calsolution_requests(self.db_handler_object)

            if results:
                for result in results:
                    new_request = CalibrationRequest()

                    # get the id of the request
                    new_request.request_id = result[0]

                    # Get the obs_id
                    new_request.obs_id = result[1]

                    # Get the type
                    if result[2]:
                        new_request.type = CalvinJobType.realtime
                        return_list_realtime.append(new_request)
                    else:
                        new_request.type = CalvinJobType.mwa_asvo
                        return_list_asvo.append(new_request)

        return return_list_realtime, return_list_asvo

    def mwa_asvo_add_new_asvo_job(self, request_id: int, obs_id: int):
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
            if request_id not in asvo_job.request_ids:
                asvo_job.request_ids.append(request_id)

                # Update database
                #
                # The point of this is:
                # If we are already handling obsid X, then another bunch of requests come through
                # we should "catch them up" to the current status in the database
                update_calsolution_request_submit_mwa_asvo_job_status(
                    self.db_handler_object,
                    asvo_job.request_ids,
                    asvo_job.job_id,
                    asvo_job.submitted_datetime,
                    None,
                    None,
                )
            else:
                # We already are tracking this request- nothing to do
                pass
        else:
            # Not found
            try:
                # Submit job and add to the ones we are tracking
                new_job = self.mwax_asvo_helper.submit_download_job(request_id, obs_id)

                # We submmited a new MWA ASVO job, update the request table so we know we're on it!
                # Update database
                update_calsolution_request_submit_mwa_asvo_job_status(
                    self.db_handler_object, new_job.request_ids, new_job.job_id, new_job.submitted_datetime, None, None
                )
            except mwax_asvo_helper.GiantSquidMWAASVOOutageException:
                # Handle me!
                self.logger.warning(
                    f"RequestID: {request_id} ObsID: {obs_id} Cannot submit new download job: MWA ASVO has an outage"
                )
                return
            except Exception as e:
                # Some other fatal error occurred, let's log it and update the db
                error_message = f"Error submitting job for ObsID {obs_id} RequestID {request_id}."
                self.logger.exception(error_message)
                error_message = error_message + f" {str(e)}"
                update_calsolution_request_submit_mwa_asvo_job_status(
                    self.db_handler_object,
                    [
                        request_id,
                    ],
                    None,
                    None,
                    datetime.now().astimezone(),
                    error_message,
                )
                return

    def mwa_asvo_update_tracked_jobs(self):
        # Find out the status of all this user's jobs in MWA ASVO
        # Get the job list from giant-squid, populating current_asvo_jobs
        # If we find a job in giant-squid which we don't know about,
        # DON'T include it in the list we track
        if self.running:
            self.logger.debug("Getting latest MWA ASVO job statuses...")
            try:
                self.mwax_asvo_helper.update_all_job_status()

            except mwax_asvo_helper.GiantSquidMWAASVOOutageException:
                self.logger.warning("Cannot update MWA ASVO job states: MWA ASVO has an outage")

            except Exception:
                self.logger.exception("Error in update_all_job_status. Will retry next loop")

    def initialise(self, config_filename: str):
        """Initialise the processor from the command line"""
        self.config_filename = config_filename
        self.worker_config_filename = config_filename.replace("calvin_controller", "calvin_processor")

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

        self.logger.info(f"Starting mwax_calvin_controller...v{version.get_mwax_mover_version_string()}")
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
        self.db_handler_object = MWAXDBHandler(
            logger=self.logger,
            host=self.mro_metadatadb_host,
            port=self.mro_metadatadb_port,
            db_name=self.mro_metadatadb_db,
            user=self.mro_metadatadb_user,
            password=self.mro_metadatadb_pass,
        )

        #
        # calvin config
        #
        # How long between iterations of the main loop (in seconds)
        self.check_interval_seconds = int(utils.read_config(self.logger, config, "calvin", "check_interval_seconds"))

        # script path (path for keeping all sbatch scripts)
        self.script_path = config.get("calvin", "script_path")

        if not os.path.exists(self.script_path):
            print(f"script_path {self.script_path} does not exist. Quiting.")
            sys.exit(1)

        #
        # giant-squid config
        #
        # How many seconds do we wait before rechecking when giant squid says
        # MWA ASVO has an outage?
        self.mwa_asvo_outage_check_seconds = int(
            utils.read_config(self.logger, config, "giant squid", "mwa_asvo_outage_check_seconds")
        )

        # How many secs do we wait for MWA ASVO to get us a completed job??
        self.mwa_asvo_longest_wait_time_seconds = int(
            utils.read_config(self.logger, config, "giant squid", "mwa_asvo_longest_wait_time_seconds")
        )

        # Get the giant squid binary
        self.giant_squid_binary_path = utils.read_config(
            self.logger,
            config,
            "giant squid",
            "giant_squid_binary_path",
        )

        if not os.path.exists(self.giant_squid_binary_path):
            self.logger.error(
                "giant_squid_binary_path location " f" {self.giant_squid_binary_path} does not exist. Quitting."
            )
            sys.exit(1)

        # How long do we wait for giant-squid to execute a list subcommand
        self.giant_squid_list_timeout_seconds = int(
            utils.read_config(self.logger, config, "giant squid", "giant_squid_list_timeout_seconds")
        )

        # How long do we wait for giant-squid to execute a submit-vis subcommand
        self.giant_squid_submitvis_timeout_seconds = int(
            utils.read_config(self.logger, config, "giant squid", "giant_squid_submitvis_timeout_seconds")
        )

        # Setup the MWA ASVO Helper
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
            "mwax_calvin_controller: a command line tool which is part of the "
            "MWA correlator for the MWA. It will submit SBATCH jobs as needed "
            "to process real time calibration jobs."
            f"(mwax_mover v{version.get_mwax_mover_version_string()})\n"
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
    processor = MWAXCalvinController()

    try:
        processor.initialise_from_command_line()
        processor.start()
        sys.exit(0)
    except Exception as catch_all_exception:  # pylint: disable=broad-except
        if processor.logger:
            processor.logger.exception("Exception in main()")
        else:
            print(str(catch_all_exception))


if __name__ == "__main__":
    main()
