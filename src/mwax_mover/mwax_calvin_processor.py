"""
Module hosting the MWAXCalvinProcessor for near-realtime
calibration
"""
import argparse
from configparser import ConfigParser
import glob
import json
import logging
import logging.handlers
import os
import queue
import signal
import sys
import threading
import time
from mwax_mover import utils, version, mwax_watcher, mwax_queue_worker
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_NEW


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
        self.mwax_mover_mode = MODE_WATCH_DIR_FOR_NEW

        self.queue = queue.Queue()
        self.watchers = []
        self.queue_workers = []

        self.obsid_check_thread = None
        self.obsid_check_seconds = None

    def start(self):
        """Start the processor"""
        self.running = True

        # create a health thread
        self.logger.info("Starting health_thread...")
        health_thread = threading.Thread(
            name="health_thread", target=self.health_loop, daemon=True
        )
        health_thread.start()

        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Create watchers
        self.logger.info("Creating watchers...")
        # Create watcher for each data path queue
        new_watcher = mwax_watcher.Watcher(
            name="incoming_watcher",
            path=self.watch_path,
            dest_queue=self.queue,
            pattern=".fits",
            log=self.logger,
            mode=self.mwax_mover_mode,
            recursive=False,
            exclude_pattern=None,
        )
        self.watchers.append(new_watcher)

        # Create queueworker archive queue
        self.logger.info("Creating workers...")
        incoming_worker = mwax_queue_worker.QueueWorker(
            name="incoming_worker",
            source_queue=self.queue,
            executable_path=None,
            event_handler=self.incoming_handler,
            log=self.logger,
            requeue_to_eoq_on_failure=True,
            exit_once_queue_empty=False,
        )
        self.queue_workers.append(incoming_worker)

        self.logger.info("Starting watchers...")
        # Setup thread for watching filesystem
        for i, watcher in enumerate(self.watchers):
            watcher_thread = threading.Thread(
                name=f"watch_thread{i}", target=watcher.start, daemon=True
            )
            self.watcher_threads.append(watcher_thread)
            watcher_thread.start()

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

        self.logger.info("Starting obsid_check_thread...")
        obsid_check_thread = threading.Thread(
            name="obsid_check_thread",
            target=self.obsid_check_loop,
            daemon=True,
        )
        self.obsid_check_thread = obsid_check_thread
        obsid_check_thread.start()

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

    def incoming_handler(self, item) -> bool:
        """This is triggered each time a new fits file
        appears in the incoming_path"""
        self.logger.info(f"Handling... incoming FITS file {item}...")
        filename = os.path.basename(item)
        obs_id: int = int(filename[0:10])

        obsid_work_dir = os.path.join(self.work_path, str(obs_id))
        if not os.path.exists(obsid_work_dir):
            self.logger.info(
                f"{item} creating obs_id's work dir {obsid_work_dir}..."
            )
            # This is the first file of this obs_id to be seen
            os.mkdir(obsid_work_dir)

        # Relocate this file to the obs_id_work_dir
        new_filename = os.path.join(obsid_work_dir, filename)
        self.logger.info(
            f"{item} moving file into obs_id's work dir {new_filename}..."
        )
        os.rename(item, new_filename)
        return True

    def check_obs_is_ready_to_process(self, obs_id, obsid_work_dir) -> bool:
        """This routine checks to see if an observation is ready to be processed
        by hyperdrive"""
        # perform web service call to get list of data files from obsid
        json_metadata = utils.get_data_files_for_obsid_from_webservice(obs_id)

        # we need a list of files from the web service
        # this should just be the filenames
        web_service_filenames = [filename for filename in json_metadata]
        web_service_filenames.sort()

        # we need a list of files from the work dir
        # this first list has the full path
        work_dir_full_path_files = glob.glob(
            os.path.join(obsid_work_dir, "*.fits")
        )
        work_dir_filenames = [
            os.path.basename(i) for i in work_dir_full_path_files
        ]
        work_dir_filenames.sort()

        return_value = web_service_filenames == work_dir_filenames

        self.logger.debug(
            f"check_obs_is_ready_to_process({obs_id}) == {return_value} (WS:"
            f" {len(web_service_filenames)}, work_dir:"
            f" {len(work_dir_filenames)})"
        )
        return return_value

    def obsid_check_loop(self):
        """This thread sleeps most of the time, but wakes up
        to check if there are any complete sets of gpubox
        files which we should process"""
        while self.running:
            time.sleep(self.obsid_check_seconds)

            self.logger.debug(
                "Waking up and checking un-processed observations..."
            )

            obs_id_list = []

            # make a list of all obs_ids in the work path
            for filename in os.listdir(self.work_path):
                full_filename = os.path.join(self.work_path, filename)
                if os.path.isdir(full_filename):
                    obs_id_list.append(filename)

            # sort it
            obs_id_list.sort()

            # Check each one
            for obs_id in obs_id_list:
                if self.check_obs_is_ready_to_process(
                    obs_id, os.path.join(self.work_path, obs_id)
                ):
                    # do processing
                    self.logger.info(f"{obs_id} is ready for processing...")
        return True

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

        # Set obsid_check_seconds
        # How many secs between waiting for all gpubox files to arrive?
        self.obsid_check_seconds = int(
            utils.read_config(
                self.logger, config, "mwax mover", "obsid_check_seconds"
            )
        )

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
