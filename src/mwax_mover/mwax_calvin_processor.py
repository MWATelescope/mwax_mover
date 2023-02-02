"""
Module hosting the MWAXCalvinProcessor for near-realtime
calibration
"""
import argparse
from configparser import ConfigParser
import json
import logging
import logging.handlers
import os
import queue
import signal
import sys
import threading
import time
from mwax_mover import (
    utils,
    version,
)


class MWAXCalvinProcessor:
    """The main class processing calibration solutions"""

    def __init__(
        self,
    ):
        # General
        self.logger = None
        self.log_path = None
        self.hostname = None

        # health
        self.health_multicast_interface_ip = None
        self.health_multicast_interface_name = None
        self.health_multicast_ip = None
        self.health_multicast_port = None
        self.health_multicast_hops = None

        self.running = False
        self.watch_path = None
        self.work_path = None
        self.watcher_threads = []
        self.worker_threads = []

        self.queue = queue.Queue()
        self.watchers = []
        self.queue_workers = []

    def start(self):
        """Start the processor"""
        self.running = True

        # create a health thread
        self.logger.info("Starting health_thread...")
        health_thread = threading.Thread(
            name="health_thread", target=self.health_handler, daemon=True
        )
        health_thread.start()

        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Create watchers
        self.logger.info("Creating watchers...")

        # Create queueworker archive queue
        self.logger.info("Creating workers...")

        self.logger.info("Waiting for all watchers to finish scanning....")
        count_of_watchers_still_scanning = len(self.watchers)
        while count_of_watchers_still_scanning > 0:
            count_of_watchers_still_scanning = 0
            for watcher in self.watchers:
                if not watcher.scan_completed:
                    count_of_watchers_still_scanning += 1
            time.sleep(1)  # hold off for another second
        self.logger.info("Watchers are finished scanning.")

        self.logger.info("Starting workers...")
        # Setup thread for processing items
        for i, worker in enumerate(self.queue_workers):
            queue_worker_thread = threading.Thread(
                name=f"worker_thread{i}", target=worker.start, daemon=True
            )
            self.worker_threads.append(queue_worker_thread)
            queue_worker_thread.start()

        self.logger.info("Started...")

        while self.running:
            for worker_thread in self.worker_threads:
                if worker_thread:
                    if worker_thread.is_alive():
                        time.sleep(1)
                    else:
                        self.running = False
                        break

        #
        # Finished- do some clean up
        #

        # Final log message
        self.logger.info("Completed Successfully")

    def stop(self):
        """Stops all processes"""
        for watcher in self.watchers:
            watcher.stop()

        if len(self.queue_workers) > 0:
            for queue_worker in self.queue_workers:
                queue_worker.stop()

        # Wait for threads to finish
        for watcher_thread in self.watcher_threads:
            if watcher_thread:
                thread_name = watcher_thread.name
                self.logger.debug(f"Watcher {thread_name} Stopping...")
                if watcher_thread.is_alive():
                    watcher_thread.join()
                self.logger.debug(f"Watcher {thread_name} Stopped")

        for worker_thread in self.worker_threads:
            if worker_thread:
                thread_name = worker_thread.name
                self.logger.debug(f"QueueWorker {thread_name} Stopping...")
                if worker_thread.is_alive():
                    worker_thread.join()
                self.logger.debug(f"QueueWorker {thread_name} Stopped")

    def health_handler(self):
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
                self.logger.warning(
                    "health_handler: Failed to send health information."
                    f" {catch_all_exception}"
                )

            # Sleep for a second
            time.sleep(1)

    def get_status(self) -> dict:
        """Returns status of all process as a dictionary"""
        main_status = {
            "Unix timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
        }

        watcher_list = []

        for watcher in self.watchers:
            status = dict({"name": "data_watcher"})
            status.update(watcher.get_status())
            watcher_list.append(status)

        worker_list = []

        if len(self.queue_workers) > 0:
            for i, worker in enumerate(self.queue_workers):
                status = dict({"name": f"archiver{i}"})
                status.update(worker.get_status())
                worker_list.append(status)

        processor_status_list = []
        processor = {
            "type": type(self).__name__,
            "watchers": watcher_list,
            "workers": worker_list,
        }
        processor_status_list.append(processor)

        status = {"main": main_status, "processors": processor_status_list}

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
            print(
                f"Configuration file location {config_filename} does not"
                " exist. Quitting."
            )
            sys.exit(1)

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
        console_log.setFormatter(
            logging.Formatter(
                "%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"
            )
        )
        self.logger.addHandler(console_log)

        file_log = logging.FileHandler(
            filename=os.path.join(self.log_path, "main.log")
        )
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(
            logging.Formatter(
                "%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"
            )
        )
        self.logger.addHandler(file_log)

        self.logger.info(
            "Starting mwax_calvin_processor"
            f" processor...v{version.get_mwax_mover_version_string()}"
        )

        # health
        self.health_multicast_ip = utils.read_config(
            self.logger, config, "mwax mover", "health_multicast_ip"
        )
        self.health_multicast_port = int(
            utils.read_config(
                self.logger, config, "mwax mover", "health_multicast_port"
            )
        )
        self.health_multicast_hops = int(
            utils.read_config(
                self.logger, config, "mwax mover", "health_multicast_hops"
            )
        )
        self.health_multicast_interface_name = utils.read_config(
            self.logger,
            config,
            "mwax mover",
            "health_multicast_interface_name",
        )

        # get this hosts primary network interface ip
        self.health_multicast_interface_ip = utils.get_ip_address(
            self.health_multicast_interface_name
        )
        self.logger.info(
            f"IP for sending multicast: {self.health_multicast_interface_ip}"
        )

        # Get the watch dir
        self.watch_path = utils.read_config(
            self.logger,
            config,
            "mwax mover",
            "watch_path",
        )

        if not os.path.exists(self.watch_path):
            self.logger.error(
                "watch_path location "
                f" {self.watch_path} does not exist. Quitting."
            )
            sys.exit(1)

        # Get the work dir
        self.work_path = utils.read_config(
            self.logger,
            config,
            "mwax mover",
            "work_path",
        )

        if not os.path.exists(self.work_path):
            self.logger.error(
                "work_path location "
                f" {self.work_path} does not exist. Quitting."
            )
            sys.exit(1)

    def initialise_from_command_line(self):
        """Initialise if initiated from command line"""

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_calvin_processor: a command line tool which is part of the"
            " MWA correlator for the MWA. It will monitor directories on"
            " each mwax server and, upon detecting a calibrator observation,"
            " will execute hyperdrive to generate calibration solutions."
            f" (mwax_mover v{version.get_mwax_mover_version_string()})\n"
        )

        parser.add_argument(
            "-c", "--cfg", required=True, help="Configuration file location.\n"
        )

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        self.initialise(config_filename)


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
