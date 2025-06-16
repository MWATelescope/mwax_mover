"""Module file for MWACacheArchiveProcessor"""

import argparse
from configparser import ConfigParser
from glob import glob
import json
import logging
import logging.handlers
import os
import queue
import signal
import sys
import threading
import time
from typing import Optional
import astropy

# import boto3
# import botocore

# from boto3 import Session
from mwax_mover import (
    mwax_mover,
    mwax_db,
    mwa_archiver,
    utils,
    version,
)
from mwax_mover.mwax_priority_watcher import PriorityWatcher
from mwax_mover.mwax_ceph_priority_queue_worker import CephPriorityQueueWorker
from mwax_mover.utils import ValidationData, ArchiveLocation
from mwax_mover.mwax_db import DataFileRow


class MWACacheArchiveProcessor:
    """
    A class representing an instance which sends
    MWAX data products from the mwacache servers
    to the Pawsey LTS.
    """

    def __init__(self):
        self.logger = logging.getLogger(__name__)

        self.hostname: str = ""
        self.log_path: str = ""
        self.log_level: str = ""
        self.metafits_path: str = ""
        self.archive_to_location: ArchiveLocation = ArchiveLocation.Unknown
        self.concurrent_archive_workers: int = 0
        self.archive_command_timeout_sec: int = 0
        self.recursive: bool = False

        # database config
        self.remote_metadatadb_db: str = ""
        self.remote_metadatadb_host: str = ""
        self.remote_metadatadb_user: str = ""
        self.remote_metadatadb_pass: str = ""
        self.remote_metadatadb_port: int = 5432

        self.mro_metadatadb_db: str = ""
        self.mro_metadatadb_host: str = ""
        self.mro_metadatadb_user: str = ""
        self.mro_metadatadb_pass: str = ""
        self.mro_metadatadb_port: int = 5432

        # s3 config
        self.s3_profile: str = ""
        self.s3_ceph_endpoints: list[str] = []
        self.s3_multipart_threshold_bytes: Optional[int] = None
        self.s3_chunk_size_bytes: Optional[int] = None
        self.s3_max_concurrency: Optional[int] = None

        self.health_multicast_interface_ip: str = ""
        self.health_multicast_ip: str = ""
        self.health_multicast_port: int = 0
        self.health_multicast_hops: int = 1
        self.health_multicast_interface_name: str = ""

        self.high_priority_correlator_projectids: list[str] = []
        self.high_priority_vcs_projectids: list[str] = []

        # MWAX servers will copy in a temp file, then rename once it is good
        self.mwax_mover_mode: str = mwax_mover.MODE_WATCH_DIR_FOR_RENAME
        self.archiving_paused: bool = False
        self.running: bool = False

        self.mro_db_handler_object: mwax_db.MWAXDBHandler
        self.remote_db_handler_object: mwax_db.MWAXDBHandler

        self.watcher_threads: list[threading.Thread] = []
        self.worker_threads: list[threading.Thread] = []

        self.watch_dirs: list[str] = []
        self.queue: queue.PriorityQueue = queue.PriorityQueue()
        self.watchers: list[PriorityWatcher] = []
        self.queue_workers: list[CephPriorityQueueWorker] = []

    def start(self):
        """This method is used to start the processor"""
        self.running = True

        # creating database connection pool(s)
        self.logger.info("Starting MRO database connection pool...")
        self.mro_db_handler_object.start_database_pool()

        self.logger.info("Starting remotedb database connection pool...")
        self.remote_db_handler_object.start_database_pool()

        # create a health thread
        self.logger.info("Starting health_thread...")
        health_thread = threading.Thread(name="health_thread", target=self.health_handler, daemon=True)
        health_thread.start()

        self.logger.info("Creating watchers...")
        for watcher_no, watch_dir in enumerate(self.watch_dirs):
            #
            # Remove any partial files first if they are old
            #
            partial_files = glob(os.path.join(watch_dir, "*.part*"))
            for partial_file in partial_files:
                # Ensure now minus the last mod time of the partial file
                # is > 60 mins, it is definitely safe to delete
                # In theory we could be starting up as mwax is sending
                # us a new file and we don't want to delete an real
                # in progress file.
                min_partial_purge_age_secs = 3600

                if time.time() - os.path.getmtime(partial_file) > min_partial_purge_age_secs:
                    self.logger.warning(
                        f"Partial file {partial_file} is older than"
                        f" {min_partial_purge_age_secs} seconds and will be"
                        " removed..."
                    )
                    os.remove(partial_file)
                    self.logger.warning(f"Partial file {partial_file} deleted")
                else:
                    self.logger.warning(
                        f"Partial file {partial_file} is newer than"
                        f" {min_partial_purge_age_secs} seconds so will NOT be"
                        " removed this time"
                    )

            # Create watcher for each data path queue
            new_watcher = PriorityWatcher(
                name=f"incoming_watcher{watcher_no}",
                path=watch_dir,
                dest_queue=self.queue,
                pattern=".*",
                log=self.logger,
                mode=self.mwax_mover_mode,
                recursive=self.recursive,
                metafits_path=self.metafits_path,
                list_of_correlator_high_priority_projects=self.high_priority_correlator_projectids,
                list_of_vcs_high_priority_projects=self.high_priority_vcs_projectids,
                exclude_pattern=".part*",
            )
            self.watchers.append(new_watcher)

        # Create queueworker archive queue
        self.logger.info("Creating workers...")

        for archive_worker in range(0, self.concurrent_archive_workers):
            new_worker = CephPriorityQueueWorker(
                name=f"Archiver{archive_worker}",
                source_queue=self.queue,
                executable_path=None,
                event_handler=self.archive_handler,
                log=self.logger,
                requeue_to_eoq_on_failure=True,
                ceph_profile=self.s3_profile,
                exit_once_queue_empty=False,
            )
            self.queue_workers.append(new_worker)

        self.logger.info("Starting watchers...")
        # Setup thread for watching filesystem
        for i, watcher in enumerate(self.watchers):
            watcher_thread = threading.Thread(name=f"watch_thread{i}", target=watcher.start, daemon=True)
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
            queue_worker_thread = threading.Thread(name=f"worker_thread{i}", target=worker.start, daemon=True)
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

    def archive_handler(self, item: str) -> bool:
        """Handles sending files to Pawsey"""
        self.logger.info(f"{item}- archive_handler() Started...")

        # validate the filename
        val: ValidationData = utils.validate_filename(self.logger, item, self.metafits_path)

        # do some sanity checks!
        if val.valid:
            # Get the file size
            actual_file_size = os.stat(item).st_size
            self.logger.debug(f"{item}- archive_handler() file size on disk is" f" {actual_file_size} bytes")

            # Lookup file from db
            data_files_row: DataFileRow = mwax_db.get_data_file_row(self.remote_db_handler_object, item, val.obs_id)

            database_file_size = data_files_row.size

            # Check for 0 size
            if actual_file_size == 0:
                # File size is 0- lets just blow it away
                self.logger.warning(f"{item}- archive_handler() File size is 0 bytes. Deleting" " file")
                utils.remove_file(self.logger, item, raise_error=False)

                # even though its a problem,we return true as we are finished
                # with the item and it should not be requeued
                return True
            elif actual_file_size != database_file_size:
                # File size is incorrect- lets just blow it away
                self.logger.warning(
                    f"{item}- archive_handler() File size"
                    f" {actual_file_size} does not match {database_file_size}."
                    " Deleting file"
                )
                utils.remove_file(self.logger, item, raise_error=False)

                # even though its a problem,we return true as we are finished
                # with the item and it should not be requeued
                return True

            self.logger.debug(f"{item}- archive_handler() File size matches metadata" " database")

            # Determine where to archive it
            bucket = utils.determine_bucket(
                item,
                self.archive_to_location,
            )

            archive_success = False

            if (
                self.archive_to_location == ArchiveLocation.AcaciaIngest
                or self.archive_to_location == ArchiveLocation.Banksia
                or self.archive_to_location == ArchiveLocation.AcaciaMWA
            ):  # Acacia or Banksia
                archive_success = mwa_archiver.archive_file_rclone(
                    self.logger,
                    self.s3_profile,
                    self.s3_ceph_endpoints,
                    item,
                    bucket,
                    data_files_row.checksum,
                )
            else:
                raise NotImplementedError(f"Location {self.archive_to_location.value} not implemented")

            if archive_success:
                # Update record in metadata database
                if not mwax_db.update_data_file_row_as_archived(
                    self.mro_db_handler_object,
                    val.obs_id,
                    item,
                    self.archive_to_location,
                    bucket,
                    None,
                ):
                    # if something went wrong, requeue
                    return False

                # If all is well, we have the file safely archived and the
                # database updated, so remove the file
                self.logger.debug(f"{item}- archive_handler() Deleting file")
                utils.remove_file(self.logger, item, raise_error=False)

                self.logger.info(f"{item}- archive_handler() Finished")
                return True
            else:
                return False
        else:
            # The filename was not valid
            self.logger.error(f"{item}- archive_handler() {val.validation_message}")
            return False

    def pause_archiving(self, paused: bool):
        """Pauses all processes"""
        if self.archiving_paused != paused:
            if paused:
                self.logger.info("Pausing archiving")
            else:
                self.logger.info("Resuming archiving")

            if len(self.queue_workers) > 0:
                for queue_worker in self.queue_workers:
                    queue_worker.pause(paused)

            self.archiving_paused = paused

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

        # Close all database connections
        self.remote_db_handler_object.stop_database_pool()
        self.mro_db_handler_object.stop_database_pool()

    def health_handler(self):
        """Send multicast health data"""
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
        """Returns status of all process as a dictionary"""
        if self.archiving_paused:
            archiving = "paused"
        else:
            archiving = "running"

        main_status = {
            "Unix timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
            "archiving": archiving,
            "cmdline": " ".join(sys.argv[1:]),
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
        """Catches SIG INT and SIG TERM then stops the processor"""
        self.logger.warning("Interrupted. Shutting down processor...")
        self.running = False

        # Stop any Processors
        self.stop()

    def initialise(self, config_filename):
        """Do initial setup"""
        if not os.path.exists(config_filename):
            print(f"Configuration file location {config_filename} does not" " exist. Quitting.")
            sys.exit(1)

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
        self.logger.setLevel(self.log_level)
        console_log = logging.StreamHandler()
        console_log.setLevel(self.log_level)
        console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        self.logger.addHandler(console_log)

        self.logger.info(
            "Starting mwacache_archive_processor" f" processor...v{version.get_mwax_mover_version_string()}"
        )

        # Get this hosts hostname
        if self.hostname == "":
            self.hostname = utils.get_hostname()
        self.logger.info(f"hostname: {self.hostname}")

        # Dump some diagnostic info
        py_version = sys.version  # contains a \n, so get rid of it
        py_version = py_version.replace("\n", " ")
        self.logger.info(f"Python v{py_version}")
        self.logger.info(f"astropy v{astropy.__version__}")

        self.logger.info(f"Reading config file: {config_filename}")

        i = 1
        self.watch_dirs = []

        # Common config options
        self.metafits_path = utils.read_config(self.logger, config, "mwax mover", "metafits_path")

        if not os.path.exists(self.metafits_path):
            self.logger.error("Metafits file location " f" {self.metafits_path} does not exist. Quitting.")
            sys.exit(1)

        self.archive_to_location = ArchiveLocation(
            int(utils.read_config(self.logger, config, "mwax mover", "archive_to_location"))
        )
        self.concurrent_archive_workers = int(
            utils.read_config(self.logger, config, "mwax mover", "concurrent_archive_workers")
        )
        self.archive_command_timeout_sec = int(
            utils.read_config(
                self.logger,
                config,
                "mwax mover",
                "archive_command_timeout_sec",
            )
        )

        # Get list of projectids which are to be given
        # high priority when archiving
        self.high_priority_correlator_projectids = utils.read_config_list(
            self.logger,
            config,
            "mwax mover",
            "high_priority_correlator_projectids",
        )
        self.high_priority_vcs_projectids = utils.read_config_list(
            self.logger,
            config,
            "mwax mover",
            "high_priority_vcs_projectids",
        )

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

        # We set different s3 options based on the location
        # 2 == Acacia Ingest
        # 3 == Banksia
        # 4 == Acacia MWA
        if self.archive_to_location == ArchiveLocation.AcaciaIngest:
            s3_section = "acacia_ingest"
        elif self.archive_to_location == ArchiveLocation.Banksia:
            s3_section = "banksia"
        elif self.archive_to_location == ArchiveLocation.AcaciaMWA:
            s3_section = "acacia_mwa"
        else:
            raise NotImplementedError(
                "archive to location should be 2 (acacia ingest) or 3 (banksia) or 4 (acacia_mwa)"
            )

        # s3 options
        self.s3_profile = utils.read_config(self.logger, config, s3_section, "profile")

        self.s3_ceph_endpoints = utils.read_config_list(
            self.logger,
            config,
            s3_section,
            "ceph_endpoints",
        )

        s3_multipart_threshold_bytes = utils.read_optional_config(
            self.logger, config, s3_section, "multipart_threshold_bytes"
        )
        if s3_multipart_threshold_bytes is not None:
            self.s3_multipart_threshold_bytes = int(s3_multipart_threshold_bytes)

        s3_chunk_size_bytes = utils.read_optional_config(self.logger, config, s3_section, "chunk_size_bytes")
        if s3_chunk_size_bytes is not None:
            self.s3_chunk_size_bytes = int(s3_chunk_size_bytes)

        s3_max_concurrency = utils.read_optional_config(self.logger, config, s3_section, "max_concurrency")
        if s3_max_concurrency is not None:
            self.s3_max_concurrency = int(s3_max_concurrency)

        #
        # Options specified per host
        #

        # Look for data_path1.. data_pathN
        while config.has_option(self.hostname, f"incoming_path{i}"):
            new_incoming_path = utils.read_config(self.logger, config, self.hostname, f"incoming_path{i}")
            if not os.path.exists(new_incoming_path):
                self.logger.error(
                    f"incoming file location in incoming_path{i} -" f" {new_incoming_path} does not exist. Quitting."
                )
                sys.exit(1)
            self.watch_dirs.append(new_incoming_path)
            i += 1

        if len(self.watch_dirs) == 0:
            self.logger.error(
                "No incoming data file locations were not present in config"
                " file. Use incoming_path1 .. incoming_pathN in the"
                " [<hostname>] section (where <hostname> is the lowercase"
                " hostname of the machine running this). This host's name is:"
                f" '{self.hostname}'. Quitting."
            )
            sys.exit(1)

        self.recursive = utils.read_config_bool(self.logger, config, self.hostname, "recursive")

        #
        # MRO database - this is one we will update
        #
        self.mro_metadatadb_host = utils.read_config(self.logger, config, "mro metadata database", "host")

        self.mro_metadatadb_db = utils.read_config(self.logger, config, "mro metadata database", "db")
        self.mro_metadatadb_user = utils.read_config(self.logger, config, "mro metadata database", "user")
        self.mro_metadatadb_pass = utils.read_config(self.logger, config, "mro metadata database", "pass", True)
        self.mro_metadatadb_port = int(utils.read_config(self.logger, config, "mro metadata database", "port"))

        # Initiate database connection for rmo metadata db
        self.mro_db_handler_object = mwax_db.MWAXDBHandler(
            logger=self.logger,
            host=self.mro_metadatadb_host,
            port=self.mro_metadatadb_port,
            db_name=self.mro_metadatadb_db,
            user=self.mro_metadatadb_user,
            password=self.mro_metadatadb_pass,
        )

        #
        # Remote metadata db is ready only- just used to query file size and
        # date info
        #
        self.remote_metadatadb_host = utils.read_config(self.logger, config, "remote metadata database", "host")

        self.remote_metadatadb_db = utils.read_config(self.logger, config, "remote metadata database", "db")
        self.remote_metadatadb_user = utils.read_config(self.logger, config, "remote metadata database", "user")
        self.remote_metadatadb_pass = utils.read_config(self.logger, config, "remote metadata database", "pass", True)
        self.remote_metadatadb_port = int(utils.read_config(self.logger, config, "remote metadata database", "port"))

        # Initiate database connection for remote metadata db
        self.remote_db_handler_object = mwax_db.MWAXDBHandler(
            logger=self.logger,
            host=self.remote_metadatadb_host,
            port=self.remote_metadatadb_port,
            db_name=self.remote_metadatadb_db,
            user=self.remote_metadatadb_user,
            password=self.remote_metadatadb_pass,
        )

        # Make sure we can Ctrl-C / kill out of this
        self.logger.info("Initialising signal handlers")
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        self.logger.info("Ready to start...")

    def initialise_from_command_line(self):
        """Initialise if initiated from command line"""

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwacache_archive_processor: a command line tool which is part of"
            " the MWA correlator for the MWA. It will monitor various"
            " directories on each mwacache server and, upon detecting a file,"
            " send it to Pawsey's LTS. It will then remove the file from the"
            " local disk. (mwax_mover"
            f" v{version.get_mwax_mover_version_string()})\n"
        )

        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        self.initialise(config_filename)


def main():
    """Mainline function"""
    processor = MWACacheArchiveProcessor()

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
