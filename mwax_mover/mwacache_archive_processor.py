from mwax_mover import mwax_mover, mwax_queue_worker, mwax_watcher, utils
import argparse
from configparser import ConfigParser
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
                 archive_host: str,
                 archive_port: str,
                 incoming_paths: list,
                 recursive: bool):

        self.logger = logger

        self.hostname = hostname
        self.archive_destination_host = archive_host
        self.archive_destination_port = archive_port
        self.recursive = recursive

        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_NEW
        self.archiving_paused = False
        self.running = False

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
        for watch_dir in self.watch_dirs:
            # Create watcher for each data path queue
            new_watcher = mwax_watcher.Watcher(path=watch_dir, q=self.queue,
                                               pattern=".*", log=self.logger,
                                               mode=self.mwax_mover_mode,
                                               recursive=self.recursive)
            self.watchers.append(new_watcher)

        # Create queueworker archive queue
        self.queue_worker = mwax_queue_worker.QueueWorker(label="Archiver",
                                                          q=self.queue,
                                                          executable_path=None,
                                                          event_handler=self.archive_handler,
                                                          mode=self.mwax_mover_mode,
                                                          log=self.logger,
                                                          requeue_to_eoq_on_failure=True)

        # Setup thread for watching filesystem
        for i, watcher in enumerate(self.watchers):
            watcher_thread = threading.Thread(name=f"watch_thread{i}", target=watcher.start, daemon=True)
            self.watcher_threads.append(watcher_thread)
            watcher_thread.start()

        # Setup thread for processing items
        queue_worker_thread = threading.Thread(name="worker_thread", target=self.queue_worker.start,
                                               daemon=True)
        self.worker_threads.append(queue_worker_thread)
        queue_worker_thread.start()

    def archive_handler(self, item: str) -> bool:
        self.logger.info(f"{item}- archive_handler() Started...")

        if utils.archive_file_xrootd(self.logger,
                                     item,
                                     None,
                                     self.archive_destination_host,
                                     120) is not True:
            return False

        self.logger.debug(f"{item}- archive_handler() Deleting file")
        mwax_mover.remove_file(self.logger, item)

        self.logger.info(f"{item}- archive_handler() Finished")
        return True

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

    def get_status(self) -> dict:
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

        if self.archiving_paused:
            archiving = "paused"
        else:
            archiving = "running"

        return_status = {
                         "host": self.hostname,
                         "running": self.running,
                         "type": type(self).__name__,
                         "archiving": archiving,
                         "watchers": watcher_list,
                         "workers": worker_list
                        }

        return return_status

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
    parser.description = "archbox_archive_processor: a command line tool which is part of the MWA " \
                         "correlator for the MWA. It will monitor the /data directory on each arch box " \
                         "and upon detecting a file, send it to a mwacache server via xrootd. It will then " \
                         "remove the file from the arch box NGAS instance."

    parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

    args = vars(parser.parse_args())

    # Check that config file exists
    config_filename = args["cfg"]

    if not os.path.exists(config_filename):
        print(f"Configuration file location {config_filename} does not exist. Quitting.")
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
    console_log.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
    logger.addHandler(console_log)

    file_log = logging.FileHandler(filename=os.path.join(cfg_log_path, "main.log"))
    file_log.setLevel(logging.DEBUG)
    file_log.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
    logger.addHandler(file_log)

    logger.info("Starting mwax_subfile_distributor processor...")

    i = 1
    cfg_incoming_paths = []

    #
    # Look for data_path1.. data_pathN
    #
    while config.has_option("mwax mover", f"incoming_path{i}"):
        new_incoming_path = utils.read_config(logger, config, "mwax mover", f"incoming_path{i}")
        if not os.path.exists(new_incoming_path):
            logger.error(f"incoming file location in incoming_path{i} - {new_incoming_path} does not exist. Quitting.")
            exit(1)
        cfg_incoming_paths.append(new_incoming_path)
        i += 1

    if len(cfg_incoming_paths) == 0:
        logger.error(f"No incoming data file locations were not present in config file. "
                     f"Use incoming_path1 .. incoming_pathN "
                     f"in the mwax_mover section. Quitting.")
        exit(1)

    cfg_recursive = utils.read_config_bool(logger, config, "mwax mover", "recursive")

    # Read config specific to this host
    cfg_archive_destination_host = utils.read_config(logger, config, hostname, "destination_host")
    cfg_archive_destination_port = utils.read_config(logger, config, hostname, "destination_port")

    return logger, hostname, cfg_archive_destination_host, cfg_archive_destination_port, cfg_incoming_paths, cfg_recursive


def main():
    (logger, hostname, archive_host, archive_port, incoming_paths, recursive) = initialise()

    p = MWACacheArchiveProcessor(logger,
                                 hostname,
                                 archive_host,
                                 archive_port,
                                 incoming_paths,
                                 recursive)

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
