"""
Module hosting the MWAXCalvinProcessor for near-realtime
calibration
"""
import argparse
from configparser import ConfigParser
import logging
import logging.handlers
import os
import queue
import signal
import sys
import time
from mwax_mover import (
    mwax_mover,
    mwax_db,
    utils,
    version,
)


class MWAXCalvinProcessor:
    """The main class processing calibration solutions"""

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
        """Function to setup processor"""
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
        """Start the processor"""
        pass

    def stop(self):
        """Stop the processor"""
        pass

    def health_handler(self):
        """Send health information via UDP multicast"""
        pass

    def get_status(self) -> dict:
        """Return the status as a dictionary"""
        pass

    def signal_handler(self, signum, frame):
        """Handles SIGINT and SIGTERM"""
        self.logger.warning("Interrupted. Shutting down processor...")
        self.running = False

        # Stop any Processors
        self.stop()


def main():
    """Main routine for the processor"""
    # Get this hosts hostname
    hostname = utils.get_hostname()

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
