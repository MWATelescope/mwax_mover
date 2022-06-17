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
from mwax_mover import (
    mwax_mover,
    mwax_db,
    mwax_ceph_queue_worker,
    mwax_watcher,
    mwa_archiver,
    utils,
    version,
)
from mwax_mover.mwax_db import DataFileRow


class MWAXCalvinProcessor:
    def __init__(
        self,
        logger,
        hostname: str,
        archive_to_location: int,
        concurrent_archive_workers: int,
        archive_command_timeout_sec: int,
        incoming_paths: list,
        recursive: bool,
        mro_db_handler_object,
        remote_db_handler_object,
        health_multicast_interface_ip,
        health_multicast_ip,
        health_multicast_port,
        health_multicast_hops,
        acacia_profile: str,
        acacia_ceph_endpoint: str,
        acacia_multipart_threshold_bytes: int,
        acacia_chunk_size_bytes: int,
        acacia_max_concurrency: int,
    ):

        self.logger = logger

        self.hostname = hostname
        self.archive_to_location = archive_to_location
        self.concurrent_archive_workers = concurrent_archive_workers
        self.archive_command_timeout_sec = archive_command_timeout_sec
        self.recursive = recursive

        # acacia config
        self.acacia_profile = acacia_profile
        self.acacia_ceph_endpoint = acacia_ceph_endpoint
        self.acacia_multipart_threshold_bytes = (
            acacia_multipart_threshold_bytes
        )
        self.acacia_chunk_size_bytes = acacia_chunk_size_bytes
        self.acacia_max_concurrency = acacia_max_concurrency

        self.health_multicast_interface_ip = health_multicast_interface_ip
        self.health_multicast_ip = health_multicast_ip
        self.health_multicast_port = health_multicast_port
        self.health_multicast_hops = health_multicast_hops

        # MWAX servers will copy in a temp file, then rename once it is good
        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_RENAME
        self.archiving_paused = False
        self.running = False

        self.mro_db_handler_object = mro_db_handler_object
        self.remote_db_handler_object = remote_db_handler_object

        self.watcher_threads = []
        self.worker_threads = []

        self.watch_dirs = incoming_paths
        self.queue = queue.Queue()
        self.watchers = []
        self.queue_workers = []

    def initialise(self):
        self.running = True

        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Start everything
        self.start()

        while self.running:
            for t in self.worker_threads:
                if t:
                    if t.isAlive():
                        time.sleep(1)
                    else:
                        self.running = False
                        break

        #
        # Finished- do some clean up
        #

        # Final log message
        self.logger.info("Completed Successfully")

    def start(self):
        # create a health thread
        self.logger.info("Starting health_thread...")
        health_thread = threading.Thread(
            name="health_thread", target=self.health_handler, daemon=True
        )
        health_thread.start()

        self.logger.info("Creating watchers...")
        for watch_dir in self.watch_dirs:
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
                MIN_PARTIAL_PURGE_AGE_SECS = 3600

                if (
                    time.time() - os.path.getmtime(partial_file)
                    > MIN_PARTIAL_PURGE_AGE_SECS
                ):
                    self.logger.warning(
                        f"Partial file {partial_file} is older than"
                        f" {MIN_PARTIAL_PURGE_AGE_SECS} seconds and will be"
                        " removed..."
                    )
                    os.remove(partial_file)
                    self.logger.warning(f"Partial file {partial_file} deleted")
                else:
                    self.logger.warning(
                        f"Partial file {partial_file} is newer than"
                        f" {MIN_PARTIAL_PURGE_AGE_SECS} seconds so will NOT be"
                        " removed this time"
                    )

            # Create watcher for each data path queue
            new_watcher = mwax_watcher.Watcher(
                path=watch_dir,
                q=self.queue,
                pattern=".*",
                log=self.logger,
                mode=self.mwax_mover_mode,
                recursive=self.recursive,
                exclude_pattern=".part*",
            )
            self.watchers.append(new_watcher)

        # Create queueworker archive queue
        self.logger.info("Creating workers...")

        for n in range(0, self.concurrent_archive_workers):
            new_worker = mwax_ceph_queue_worker.CephQueueWorker(
                label=f"Archiver{n}",
                q=self.queue,
                executable_path=None,
                event_handler=self.archive_handler,
                log=self.logger,
                requeue_to_eoq_on_failure=True,
                exit_once_queue_empty=False,
                backoff_initial_seconds=20,
                backoff_factor=2,
                backoff_limit_seconds=40,
                ceph_endpoint=self.acacia_ceph_endpoint,
                ceph_profile=self.acacia_profile,
            )
            self.queue_workers.append(new_worker)

        self.logger.info("Starting watchers...")
        # Setup thread for watching filesystem
        for i, watcher in enumerate(self.watchers):
            watcher_thread = threading.Thread(
                name=f"watch_thread{i}", target=watcher.start, daemon=True
            )
            self.watcher_threads.append(watcher_thread)
            watcher_thread.start()

        self.logger.info("Starting workers...")
        # Setup thread for processing items
        for i, worker in enumerate(self.queue_workers):
            queue_worker_thread = threading.Thread(
                name=f"worker_thread{i}", target=worker.start, daemon=True
            )
            self.worker_threads.append(queue_worker_thread)
            queue_worker_thread.start()

        self.logger.info("Started...")

    def archive_handler(self, item: str, ceph_session) -> bool:
        self.logger.info(f"{item}- archive_handler() Started...")

        # validate the filename
        (
            valid,
            obs_id,
            filetype,
            file_ext,
            validation_message,
        ) = mwa_archiver.validate_filename(item)

        # do some sanity checks!
        if valid:
            # Get the file size
            actual_file_size = os.stat(item).st_size
            self.logger.debug(
                f"{item}- archive_handler() file size on disk is"
                f" {actual_file_size} bytes"
            )

            # Lookup file from db
            data_files_row: DataFileRow = mwax_db.get_data_file_row(
                self.remote_db_handler_object, item
            )
            database_file_size = data_files_row.size

            # Check for 0 size
            if actual_file_size == 0:
                # File size is 0- lets just blow it away
                self.logger.warning(
                    f"{item}- archive_handler() File size is 0 bytes. Deleting"
                    " file"
                )
                mwax_mover.remove_file(self.logger, item, raise_error=False)

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
                mwax_mover.remove_file(self.logger, item, raise_error=False)

                # even though its a problem,we return true as we are finished
                # with the item and it should not be requeued
                return True

            self.logger.debug(
                f"{item}- archive_handler() File size matches metadata"
                " database"
            )

            # Determine where to archive it
            bucket, folder = mwa_archiver.determine_bucket_and_folder(
                item,
                self.archive_to_location,
            )

        if valid:
            archive_success = False

            if self.archive_to_location == 2:  # Ceph
                archive_success = mwa_archiver.archive_file_ceph(
                    self.logger,
                    ceph_session,
                    item,
                    bucket,
                    data_files_row.checksum,
                    self.acacia_profile,
                    self.acacia_ceph_endpoint,
                    self.acacia_multipart_threshold_bytes,
                    self.acacia_chunk_size_bytes,
                    self.acacia_max_concurrency,
                )
            else:
                raise NotImplementedError(
                    f"Location {self.archive_to_location} not implemented"
                )

            if archive_success:
                # Update record in metadata database
                if not mwax_db.update_data_file_row_as_archived(
                    self.mro_db_handler_object,
                    obs_id,
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
                mwax_mover.remove_file(self.logger, item, raise_error=False)

                self.logger.info(f"{item}- archive_handler() Finished")
                return True
            else:
                return False
        else:
            # The filename was not valid
            self.logger.error(
                f"{item}- archive_handler() {validation_message}"
            )
            return False

    def pause_archiving(self, paused: bool):
        if self.archiving_paused != paused:
            if paused:
                self.logger.info("Pausing archiving")
            else:
                self.logger.info("Resuming archiving")

            if len(self.queue_workers) > 0:
                for qw in self.queue_workers:
                    qw.pause(paused)

            self.archiving_paused = paused

    def stop(self):
        for watcher in self.watchers:
            watcher.stop()

        if len(self.queue_workers) > 0:
            for qw in self.queue_workers:
                qw.stop()

        # Wait for threads to finish
        for t in self.watcher_threads:
            if t:
                thread_name = t.name
                self.logger.debug(f"Watcher {thread_name} Stopping...")
                if t.isAlive:
                    t.join()
                self.logger.debug(f"Watcher {thread_name} Stopped")

        for t in self.worker_threads:
            if t:
                thread_name = t.name
                self.logger.debug(f"QueueWorker {thread_name} Stopping...")
                if t.isAlive():
                    t.join()
                self.logger.debug(f"QueueWorker {thread_name} Stopped")

    def health_handler(self):
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
            except Exception as e:
                self.logger.warning(
                    f"health_handler: Failed to send health information. {e}"
                )

            # Sleep for a second
            time.sleep(1)

    def get_status(self) -> dict:
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

    def signal_handler(self, signum, frame):
        self.logger.warning("Interrupted. Shutting down processor...")
        self.running = False

        # Stop any Processors
        self.stop()


def main():
    # Get this hosts hostname
    hostname = utils.get_hostname()

    # Get command line args
    parser = argparse.ArgumentParser()
    parser.description = (
        "mwacache_archive_processor: a command line tool which is part of the"
        " MWA correlator for the MWA. It will monitor various directories on"
        " each mwacache server and, upon detecting a file, send it to"
        " Pawsey's LTS. It will then remove the file from the local disk."
        f" (mwax_mover v{version.get_mwax_mover_version_string()})\n"
    )

    parser.add_argument(
        "-c", "--cfg", required=True, help="Configuration file location.\n"
    )

    args = vars(parser.parse_args())

    # Check that config file exists
    config_filename = args["cfg"]

    if not os.path.exists(config_filename):
        print(
            f"Configuration file location {config_filename} does not exist."
            " Quitting."
        )
        exit(1)

    # Parse config file
    config = ConfigParser()
    config.read_file(open(config_filename, "r"))

    # read from config file
    cfg_log_path = config.get("mwax mover", "log_path")

    if not os.path.exists(cfg_log_path):
        print(f"log_path {cfg_log_path} does not exist. Quiting.")
        exit(1)

    # It's now safe to start logging
    # start logging
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    console_log = logging.StreamHandler()
    console_log.setLevel(logging.DEBUG)
    console_log.setFormatter(
        logging.Formatter(
            "%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"
        )
    )
    logger.addHandler(console_log)

    file_log = logging.FileHandler(
        filename=os.path.join(cfg_log_path, "main.log")
    )
    file_log.setLevel(logging.DEBUG)
    file_log.setFormatter(
        logging.Formatter(
            "%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"
        )
    )
    logger.addHandler(file_log)

    logger.info(
        "Starting mwax_calvin_processor"
        f" processor...v{version.get_mwax_mover_version_string()}"
    )

    i = 1
    cfg_incoming_paths = []

    # Common config options
    cfg_archive_to_location = int(
        utils.read_config(logger, config, "mwax mover", "archive_to_location")
    )
    cfg_concurrent_archive_workers = int(
        utils.read_config(
            logger, config, "mwax mover", "concurrent_archive_workers"
        )
    )
    cfg_archive_command_timeout_sec = int(
        utils.read_config(
            logger, config, "mwax mover", "archive_command_timeout_sec"
        )
    )

    # health
    cfg_health_multicast_ip = utils.read_config(
        logger, config, "mwax mover", "health_multicast_ip"
    )
    cfg_health_multicast_port = int(
        utils.read_config(
            logger, config, "mwax mover", "health_multicast_port"
        )
    )
    cfg_health_multicast_hops = int(
        utils.read_config(
            logger, config, "mwax mover", "health_multicast_hops"
        )
    )
    cfg_health_multicast_interface_name = utils.read_config(
        logger, config, "mwax mover", "health_multicast_interface_name"
    )
    # get this hosts primary network interface ip
    cfg_health_multicast_interface_ip = utils.get_ip_address(
        cfg_health_multicast_interface_name
    )
    logger.info(
        f"IP for sending multicast: {cfg_health_multicast_interface_ip}"
    )

    # acacia options
    cfg_acacia_profile = utils.read_config(logger, config, "acacia", "profile")
    cfg_acacia_ceph_endpoint = utils.read_config(
        logger, config, "acacia", "ceph_endpoint"
    )
    cfg_acacia_multipart_threshold_bytes = int(
        utils.read_config(
            logger, config, "acacia", "multipart_threshold_bytes"
        )
    )
    cfg_acacia_chunk_size_bytes = int(
        utils.read_config(logger, config, "acacia", "chunk_size_bytes")
    )
    cfg_acacia_max_concurrency = int(
        utils.read_config(logger, config, "acacia", "max_concurrency")
    )

    #
    # Options specified per host
    #

    # Look for data_path1.. data_pathN
    while config.has_option(hostname, f"incoming_path{i}"):
        new_incoming_path = utils.read_config(
            logger, config, hostname, f"incoming_path{i}"
        )
        if not os.path.exists(new_incoming_path):
            logger.error(
                f"incoming file location in incoming_path{i} -"
                f" {new_incoming_path} does not exist. Quitting."
            )
            exit(1)
        cfg_incoming_paths.append(new_incoming_path)
        i += 1

    if len(cfg_incoming_paths) == 0:
        logger.error(
            "No incoming data file locations were not present in config file."
            " Use incoming_path1 .. incoming_pathN in the [<hostname>]"
            " section (where <hostname> is the lowercase hostname of the"
            f" machine running this). This host's name is: '{hostname}'."
            " Quitting."
        )
        exit(1)

    cfg_recursive = utils.read_config_bool(
        logger, config, hostname, "recursive"
    )

    #
    # MRO database - this is one we will update
    #
    cfg_mro_metadatadb_host = utils.read_config(
        logger, config, "mro metadata database", "host"
    )

    if cfg_mro_metadatadb_host != mwax_db.DUMMY_DB:
        cfg_mro_metadatadb_db = utils.read_config(
            logger, config, "mro metadata database", "db"
        )
        cfg_mro_metadatadb_user = utils.read_config(
            logger, config, "mro metadata database", "user"
        )
        cfg_mro_metadatadb_pass = utils.read_config(
            logger, config, "mro metadata database", "pass", True
        )
        cfg_mro_metadatadb_port = utils.read_config(
            logger, config, "mro metadata database", "port"
        )
    else:
        cfg_mro_metadatadb_db = None
        cfg_mro_metadatadb_user = None
        cfg_mro_metadatadb_pass = None
        cfg_mro_metadatadb_port = None

    # Initiate database connection for rmo metadata db
    mro_db_handler = mwax_db.MWAXDBHandler(
        logger=logger,
        host=cfg_mro_metadatadb_host,
        port=cfg_mro_metadatadb_port,
        db=cfg_mro_metadatadb_db,
        user=cfg_mro_metadatadb_user,
        password=cfg_mro_metadatadb_pass,
    )

    #
    # Remote metadata db is ready only- just used to query file size and date
    # info
    #
    cfg_remote_metadatadb_host = utils.read_config(
        logger, config, "remote metadata database", "host"
    )

    if cfg_remote_metadatadb_host != mwax_db.DUMMY_DB:
        cfg_remote_metadatadb_db = utils.read_config(
            logger, config, "remote metadata database", "db"
        )
        cfg_remote_metadatadb_user = utils.read_config(
            logger, config, "remote metadata database", "user"
        )
        cfg_remote_metadatadb_pass = utils.read_config(
            logger, config, "remote metadata database", "pass", True
        )
        cfg_remote_metadatadb_port = utils.read_config(
            logger, config, "remote metadata database", "port"
        )
    else:
        cfg_remote_metadatadb_db = None
        cfg_remote_metadatadb_user = None
        cfg_remote_metadatadb_pass = None
        cfg_remote_metadatadb_port = None

    # Initiate database connection for remote metadata db
    remote_db_handler = mwax_db.MWAXDBHandler(
        logger=logger,
        host=cfg_remote_metadatadb_host,
        port=cfg_remote_metadatadb_port,
        db=cfg_remote_metadatadb_db,
        user=cfg_remote_metadatadb_user,
        password=cfg_remote_metadatadb_pass,
    )

    # Create the processor
    processor = MWAXCalvinProcessor(
        logger,
        hostname,
        cfg_archive_to_location,
        cfg_concurrent_archive_workers,
        cfg_archive_command_timeout_sec,
        cfg_incoming_paths,
        cfg_recursive,
        mro_db_handler,
        remote_db_handler,
        cfg_health_multicast_interface_ip,
        cfg_health_multicast_ip,
        cfg_health_multicast_port,
        cfg_health_multicast_hops,
        cfg_acacia_profile,
        cfg_acacia_ceph_endpoint,
        cfg_acacia_multipart_threshold_bytes,
        cfg_acacia_chunk_size_bytes,
        cfg_acacia_max_concurrency,
    )

    try:
        processor.initialise()
        sys.exit(0)

    except Exception as e:
        if processor.logger:
            processor.logger.exception(str(e))
        else:
            print(str(e))


if __name__ == "__main__":
    main()
