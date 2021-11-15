from mwax_mover import mwax_mover, mwax_db, mwax_queue_worker, mwax_watcher, mwa_archiver, utils, version
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


class MWACacheArchiveProcessor:
    def __init__(self,
                 logger,
                 hostname: str,
                 ceph_endpoint: str,
                 incoming_paths: list,
                 recursive: bool,
                 db_handler_object,
                 health_multicast_interface_ip,
                 health_multicast_ip,
                 health_multicast_port,
                 health_multicast_hops):

        self.logger = logger

        self.hostname = hostname
        self.ceph_endpoint = ceph_endpoint
        self.recursive = recursive

        self.health_multicast_interface_ip = health_multicast_interface_ip
        self.health_multicast_ip = health_multicast_ip
        self.health_multicast_port = health_multicast_port
        self.health_multicast_hops = health_multicast_hops

        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_NEW
        self.archiving_paused = False
        self.running = False

        self.db_handler_object = db_handler_object

        self.watcher_threads = []
        self.worker_threads = []

        self.watch_dirs = incoming_paths
        self.queue = queue.Queue()
        self.watchers = []
        self.queue_worker = None

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
            name="health_thread", target=self.health_handler, daemon=True)
        health_thread.start()

        self.logger.info("Creating watchers...")
        for watch_dir in self.watch_dirs:
            # Create watcher for each data path queue
            new_watcher = mwax_watcher.Watcher(path=watch_dir, q=self.queue,
                                               pattern=".*", log=self.logger,
                                               mode=self.mwax_mover_mode,
                                               recursive=self.recursive)
            self.watchers.append(new_watcher)

        # Create queueworker archive queue
        self.logger.info("Creating workers...")
        self.queue_worker = mwax_queue_worker.QueueWorker(label="Archiver",
                                                          q=self.queue,
                                                          executable_path=None,
                                                          event_handler=self.archive_handler,
                                                          log=self.logger,
                                                          requeue_to_eoq_on_failure=True,
                                                          exit_once_queue_empty=False)

        self.logger.info("Starting watchers...")
        # Setup thread for watching filesystem
        for i, watcher in enumerate(self.watchers):
            watcher_thread = threading.Thread(
                name=f"watch_thread{i}", target=watcher.start, daemon=True)
            self.watcher_threads.append(watcher_thread)
            watcher_thread.start()

        self.logger.info("Starting workers...")
        # Setup thread for processing items
        queue_worker_thread = threading.Thread(name="worker_thread", target=self.queue_worker.start,
                                               daemon=True)
        self.worker_threads.append(queue_worker_thread)
        queue_worker_thread.start()

        self.logger.info("Started...")

    def archive_handler(self, item: str) -> bool:
        self.logger.info(f"{item}- archive_handler() Started...")

        # For now archive to DMF
        location = 1  # DMF

        # validate the filename
        (valid, obs_id, filetype, file_ext, prefix, dmf_host, validation_message) = \
            mwa_archiver.validate_filename(item, location)

        if valid:
            archive_success = False

            if location == 1:  # DMF
                # Now copy the file into dmf
                archive_success = mwa_archiver.archive_file_rsync(self.logger,
                                                                  item,
                                                                  -1,  # cache boxes do not have numa architecture
                                                                  f"ngas@{dmf_host}",
                                                                  prefix,
                                                                  120)

            elif location == 2:  # Ceph
                archive_success = mwa_archiver.archive_file_ceph(self.logger,
                                                                 item,
                                                                 self.ceph_endpoint)

            if archive_success:
                # Update record in metadata database
                if not mwax_db.upsert_data_file_row(self.db_handler_object, item, filetype, self.hostname,
                                                    True, location, prefix, None, None):
                    # if something went wrong, requeue
                    return False

                # If all is well, we have the file safely archived and the database updated, so remove the file
                self.logger.debug(f"{item}- archive_handler() Deleting file")
                mwax_mover.remove_file(self.logger, item, raise_error=False)

                self.logger.info(f"{item}- archive_handler() Finished")
                return True
            else:
                return False
        else:
            # The filename was not valid
            self.logger.error(f"{item}- db_handler() {validation_message}")
            return False

    def pause_archiving(self, paused: bool):
        if self.archiving_paused != paused:
            if paused:
                self.logger.info(f"Pausing archiving")
            else:
                self.logger.info(f"Resuming archiving")

            if self.queue_worker:
                self.queue_worker.pause(paused)

            self.archiving_paused = paused

    def stop(self):
        for watcher in self.watchers:
            watcher.stop()

        self.queue_worker.stop()

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
            status_bytes = json.dumps(status_dict).encode('utf-8')

            # Send the bytes
            try:
                utils.send_multicast(self.health_multicast_interface_ip,
                                     self.health_multicast_ip,
                                     self.health_multicast_port,
                                     status_bytes,
                                     self.health_multicast_hops)
            except Exception as e:
                self.logger.warning(
                    f"health_handler: Failed to send health information. {e}")

            # Sleep for a second
            time.sleep(1)

    def get_status(self) -> dict:
        if self.archiving_paused:
            archiving = "paused"
        else:
            archiving = "running"

        main_status = {"Unix timestamp": time.time(),
                       "process": type(self).__name__,
                       "version": version.get_mwax_mover_version_string(),
                       "host": self.hostname,
                       "running": self.running,
                       "archiving": archiving, }

        watcher_list = []

        for watcher in self.watchers:
            status = dict({"name": "data_watcher"})
            status.update(watcher.get_status())
            watcher_list.append(status)

        worker_list = []

        if self.queue_worker:
            status = dict({"name": "archiver"})
            status.update(self.queue_worker.get_status())
            worker_list.append(status)

        processor_status_list = []
        processor = {"type": type(self).__name__,
                     "watchers": watcher_list,
                     "workers": worker_list}
        processor_status_list.append(processor)

        status = {"main": main_status,
                  "processors": processor_status_list}

        return status

    def signal_handler(self, signum, frame):
        self.logger.warning(f"Interrupted. Shutting down processor...")
        self.running = False

        # Stop any Processors
        self.stop()


def initialise():
    # Get this hosts hostname
    hostname = utils.get_hostname()

    # Get command line args
    parser = argparse.ArgumentParser()
    parser.description = "mwacache_archive_processor: a command line tool which is part of the MWA " \
                         "correlator for the MWA. It will monitor various directories on each mwacache " \
                         "server and, upon detecting a file, send it to Pawsey's LTS. It will then " \
        f"remove the file from the local disk. " \
        f"(mwax_mover v{version.get_mwax_mover_version_string()})\n"

    parser.add_argument("-c", "--cfg", required=True,
                        help="Configuration file location.\n")

    args = vars(parser.parse_args())

    # Check that config file exists
    config_filename = args["cfg"]

    if not os.path.exists(config_filename):
        print(
            f"Configuration file location {config_filename} does not exist. Quitting.")
        exit(1)

    # Parse config file
    config = ConfigParser()
    config.read_file(open(config_filename, 'r'))

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
    console_log.setFormatter(logging.Formatter(
        '%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
    logger.addHandler(console_log)

    file_log = logging.FileHandler(
        filename=os.path.join(cfg_log_path, "main.log"))
    file_log.setLevel(logging.DEBUG)
    file_log.setFormatter(logging.Formatter(
        '%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
    logger.addHandler(file_log)

    logger.info(
        f"Starting mwacache_archive_processor processor...v{version.get_mwax_mover_version_string()}")

    i = 1
    cfg_incoming_paths = []

    # Common config options
    cfg_ceph_endpoint = utils.read_config(
        logger, config, "mwax mover", "ceph_endpoint")

    cfg_health_multicast_ip = utils.read_config(
        logger, config, "mwax mover", "health_multicast_ip")
    cfg_health_multicast_port = int(utils.read_config(
        logger, config, "mwax mover", "health_multicast_port"))
    cfg_health_multicast_hops = int(utils.read_config(
        logger, config, "mwax mover", "health_multicast_hops"))
    cfg_health_multicast_interface_name = utils.read_config(
        logger, config, "mwax mover", "health_multicast_interface_name")
    # get this hosts primary network interface ip
    cfg_health_multicast_interface_ip = utils.get_ip_address(
        cfg_health_multicast_interface_name)
    logger.info(
        f"IP for sending multicast: {cfg_health_multicast_interface_ip}")

    #
    # Options specified per host
    #

    # Look for data_path1.. data_pathN
    while config.has_option(hostname, f"incoming_path{i}"):
        new_incoming_path = utils.read_config(
            logger, config, hostname, f"incoming_path{i}")
        if not os.path.exists(new_incoming_path):
            logger.error(
                f"incoming file location in incoming_path{i} - {new_incoming_path} does not exist. Quitting.")
            exit(1)
        cfg_incoming_paths.append(new_incoming_path)
        i += 1

    if len(cfg_incoming_paths) == 0:
        logger.error(f"No incoming data file locations were not present in config file. "
                     f"Use incoming_path1 .. incoming_pathN "
                     f"in the [<hostname>] section (where <hostname> is the lowercase hostname of the machine running "
                     f"this). This host's name is: '{hostname}'. Quitting.")
        exit(1)

    cfg_recursive = utils.read_config_bool(
        logger, config, hostname, "recursive")

    cfg_metadatadb_host = utils.read_config(
        logger, config, "mwa metadata database", "host")

    if cfg_metadatadb_host != mwax_db.DUMMY_DB:
        cfg_metadatadb_db = utils.read_config(
            logger, config, "mwa metadata database", "db")
        cfg_metadatadb_user = utils.read_config(
            logger, config, "mwa metadata database", "user")
        cfg_metadatadb_pass = utils.read_config(
            logger, config, "mwa metadata database", "pass", True)
        cfg_metadatadb_port = utils.read_config(
            logger, config, "mwa metadata database", "port")
    else:
        cfg_metadatadb_db = None
        cfg_metadatadb_user = None
        cfg_metadatadb_pass = None
        cfg_metadatadb_port = None

        # Initiate database connection pool for metadata db
    db_handler = mwax_db.MWAXDBHandler(logger=logger,
                                       host=cfg_metadatadb_host,
                                       port=cfg_metadatadb_port,
                                       db=cfg_metadatadb_db,
                                       user=cfg_metadatadb_user,
                                       password=cfg_metadatadb_pass)

    return logger, hostname, cfg_ceph_endpoint, cfg_incoming_paths, cfg_recursive, db_handler, \
        cfg_health_multicast_interface_ip, cfg_health_multicast_ip, cfg_health_multicast_port, \
        cfg_health_multicast_hops


def main():
    (logger, hostname, ceph_endpoint, incoming_paths, recursive, db_handler,
     health_multicast_interface_ip, health_multicast_ip, health_multicast_port, health_multicast_hops) = initialise()

    p = MWACacheArchiveProcessor(logger,
                                 hostname,
                                 ceph_endpoint,
                                 incoming_paths,
                                 recursive,
                                 db_handler,
                                 health_multicast_interface_ip,
                                 health_multicast_ip,
                                 health_multicast_port,
                                 health_multicast_hops)

    try:
        p.initialise()
        sys.exit(0)

    except Exception as e:
        if p.logger:
            p.logger.exception(str(e))
        else:
            print(str(e))


if __name__ == '__main__':
    main()
