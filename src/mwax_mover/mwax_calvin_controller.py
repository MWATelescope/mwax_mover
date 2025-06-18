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
import json
import logging
import os
import queue
import signal
import sys
import threading
import time
from typing import Optional
from mwax_mover import (
    utils,
    version,
    mwax_queue_worker,
    mwax_db,
)
from mwax_mover.mwax_calvin_utils import submit_sbatch, create_sbatch_script, CalvinJobType


class MWAXCalvinController:
    """The main class managing calvin processes"""

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
        self.health_multicast_hops: int = 0

        self.running: bool = False
        self.ready_to_exit: bool = False

        # Workers
        self.queue_workers: list = []
        self.worker_threads: list = []

        # Queues
        self.realtime_queue: queue.Queue = queue.Queue()

        # SBatch stuff
        self.script_path = ""

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

        #
        # Create queue workers
        #

        # Create queueworker for the realtime queue
        self.logger.info("Creating workers...")
        new_worker = mwax_queue_worker.QueueWorker(
            name="realtime_worker",
            source_queue=self.realtime_queue,
            executable_path=None,
            event_handler=self.realtime_handler,
            log=self.logger,
            requeue_to_eoq_on_failure=True,
            exit_once_queue_empty=False,
            requeue_on_error=True,
        )
        self.queue_workers.append(new_worker)

        self.logger.info("Starting queue workers...")
        # Setup threads for processing items
        for i, worker in enumerate(self.queue_workers):
            queue_worker_thread = threading.Thread(name=f"worker_thread{i}", target=worker.start, daemon=True)
            self.worker_threads.append(queue_worker_thread)
            queue_worker_thread.start()

        self.logger.info("Started...")

        while self.running:
            for worker_thread in self.worker_threads:
                if worker_thread:
                    if worker_thread.is_alive():
                        self.sleep(0.2)
                    else:
                        self.logger.error(f"Worker {worker_thread.name} has died unexpectedly! Exiting!")
                        self.running = False
                        self.stop()
                        break

        #
        # Finished- do some clean up
        #
        while not self.ready_to_exit:
            self.sleep(1)

        # Final log message
        self.logger.info("Completed Successfully")

    def realtime_handler(self, item) -> bool:
        # item is an obsid
        obs_id: int = int(item)

        # Has to return True/False as it is a handler

        # Takes an item off the queue and actions it
        script = create_sbatch_script(obs_id, CalvinJobType.realtime, self.log_path, "")

        # submit sbatch script
        try:
            submit_sbatch(self.logger, self.script_path, script, obs_id)
            return True
        except Exception:
            self.logger.exception(f"{item}: Unable to submit a realtime calibration sbatch job")
            return False

    def stop(self):
        """Shutsdown all processes"""
        for queue_worker in self.queue_workers:
            queue_worker.stop()

        for worker_thread in self.worker_threads:
            if worker_thread:
                thread_name = worker_thread.name
                self.logger.info(f"QueueWorker {thread_name} Stopping...")
                if worker_thread.is_alive():
                    # Short timeout- everything other than a running hyperdrive
                    # instance should have joined by now.
                    worker_thread.join(timeout=10)
                self.logger.info(f"QueueWorker {thread_name} Stopped")

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
            "realtime_queue": self.realtime_queue.qsize(),
        }

        status = {"main": main_status}

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

        self.logger.info("Starting mwax_calvin_controller...v{version.get_mwax_mover_version_string()}")
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

        # script path (path for keeping all sbatch scripts)
        self.script_path = config.get("mwax mover", "script_path")

        if not os.path.exists(self.script_path):
            print(f"script_path {self.script_path} does not exist. Quiting.")
            sys.exit(1)

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
            processor.logger.exception(str(catch_all_exception))
        else:
            print(str(catch_all_exception))


if __name__ == "__main__":
    main()
