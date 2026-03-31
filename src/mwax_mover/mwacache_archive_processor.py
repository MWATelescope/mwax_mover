"""Entry point and main class for the mwacache_archive_processor daemon.

MWACacheArchiveProcessor runs on the mwacache servers at Curtin. It monitors one
or more incoming directories for files sent from MWAX boxes, validates their size
and checksum against the remote metadata database, archives them to Pawsey Long-Term
Storage (Acacia or Banksia) via rclone, updates the MRO metadata database to
confirm successful archival, then deletes the local copy.
"""

from mwax_mover.mwax_db import MWAXDBHandler

import argparse
from configparser import ConfigParser
from glob import glob
import json
import logging
import os
import signal
import sys
import threading
import time
from typing import Optional
import astropy

from mwax_mover import (
    mwax_db,
    utils,
    version,
)
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker
from mwax_mover.mwax_wqw_pawsey_outgoing import PawseyOutgoingProcessor
from mwax_mover.utils import ArchiveLocation

# Setup root logger
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(name)s.%(funcName)s, %(message)s"))
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


class MWACacheArchiveProcessor:
    """
    A class representing an instance which sends
    MWAX data products from the mwacache servers
    to the Pawsey LTS.
    """

    def __init__(self):
        """Initialize MWACacheArchiveProcessor with default values.

        Sets up instance variables for database connections, archiving configuration,
        health monitoring, and worker management.
        """
        if utils.running_under_pytest():
            # pretend I am mwacache99
            self.hostname = "mwacache99"
        else:
            self.hostname: str = utils.get_hostname()

        self.metafits_path: str = ""
        self.archive_to_location: ArchiveLocation = ArchiveLocation.Unknown
        self.concurrent_archive_workers: int = 0
        self.archive_command_timeout_sec: int = 0

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

        self.health_multicast_interface_ip: str = ""
        self.health_multicast_ip: str = ""
        self.health_multicast_port: int = 0
        self.health_multicast_hops: int = 1
        self.health_multicast_interface_name: str = ""

        self.high_priority_correlator_projectids: list[str] = []
        self.high_priority_vcs_projectids: list[str] = []

        # MWAX servers will copy in a temp file, then rename once it is good
        self.archiving_paused: bool = False
        self.running: bool = False

        self.mro_db_handler: mwax_db.MWAXDBHandler
        self.remote_db_handler: mwax_db.MWAXDBHandler

        self.watch_dirs: list[str] = []

        # This list helps us keep track of all the workers
        self.workers: list[MWAXPriorityWatchQueueWorker] = list()

    def start(self):
        """Start the processor and begin monitoring for archive operations.

        Initializes database connection pools, starts health monitoring and worker
        threads, and enters main monitoring loop.
        """
        self.running = True

        # creating database connection pool(s)
        if self.mro_metadatadb_host != "dummy":
            logger.info("Starting MRO database connection pool...")
            self.mro_db_handler.start_database_pool()

        if self.remote_metadatadb_host != "dummy":
            logger.info("Starting remotedb database connection pool...")
            self.remote_db_handler.start_database_pool()

        # create a health thread
        logger.info("Starting health_thread...")
        health_thread = threading.Thread(name="health_thread", target=self.health_handler, daemon=True)
        health_thread.start()

        logger.info("Cleaning up old temp files...")
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
                min_partial_purge_age_secs = 3600

                if time.time() - os.path.getmtime(partial_file) > min_partial_purge_age_secs:
                    logger.warning(
                        f"Partial file {partial_file} is older than"
                        f" {min_partial_purge_age_secs} seconds and will be"
                        " removed..."
                    )
                    os.remove(partial_file)
                    logger.warning(f"Partial file {partial_file} deleted")
                else:
                    logger.warning(
                        f"Partial file {partial_file} is newer than"
                        f" {min_partial_purge_age_secs} seconds so will NOT be"
                        " removed this time"
                    )

        logger.info("Starting workers...")

        for w in self.workers:
            w.start()

        logger.info("Started...")

        time.sleep(1)  # give things time to start!

        logger.info("Entering main loop...")

        while self.running:
            for w in self.workers:
                if self.running:
                    if not w.is_running():
                        logger.error(f"Worker {w.name} has stopped unexpectedly.")
                        self.running = False
                        break

            time.sleep(0.1)

        # Final log message
        logger.info("Completed Successfully")

    def pause_archiving(self, paused: bool):
        """Pause or resume archiving operations across all workers.

        Args:
            paused: True to pause archiving, False to resume.
        """
        if self.archiving_paused != paused:
            if paused:
                logger.info("Pausing archiving")
            else:
                logger.info("Resuming archiving")

            for worker in self.workers:
                if worker:
                    worker.pause(paused)

            self.archiving_paused = paused

    def stop(self):
        """Stop the processor and shutdown all workers and connections.

        Stops worker threads and closes database connections.
        """
        self.running = False

        # Stop any Processors
        for w in self.workers:
            if w.is_running():
                w.stop()

        # Close database connections
        if self.mro_db_handler:
            self.mro_db_handler.close()

        if self.remote_db_handler:
            self.remote_db_handler.close()

    def health_handler(self):
        """Periodically send health status via UDP multicast.

        Runs in a separate thread and sends status information every second while
        the processor is running.
        """
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
                logger.warning(f"health_handler: Failed to send health information. {catch_all_exception}")

            # Sleep for a second
            time.sleep(1)

    def get_status(self) -> dict:
        """Return status of the processor and all workers as a dictionary.

        Returns:
            A dictionary containing main processor status and individual worker statuses.
        """
        main_status = {
            "unix_timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
            "cmdline": " ".join(sys.argv[1:]),
        }

        worker_status_list = []

        for w in self.workers:
            worker_status_list.append(w.get_status())

        status = {"main": main_status, "workers": worker_status_list}

        return status

    def signal_handler(self, _signum, _frame):
        """Handle SIGINT and SIGTERM signals for graceful shutdown.

        Args:
            _signum: Signal number (unused).
            _frame: Stack frame (unused).
        """
        logger.warning("Interrupted. Shutting down processor...")

        # Stop any Processors
        self.stop()

    def initialise(
        self,
        config_filename,
        override_mro_db_handler: Optional[MWAXDBHandler] = None,
        override_remote_db_handler: Optional[MWAXDBHandler] = None,
    ):
        """Initialize the processor from a configuration file.

        Args:
            config_filename: Path to the configuration file.
            override_mro_db_handler: If present, this will override the default MWAXDBHandler (this is used for testing via tests/tests_fakedb.py FakeMWAXDBHandler). Defaults to None.
            override_remote_db_handler: If present, this will override the default MWAXDBHandler (this is used for testing via tests/tests_fakedb.py FakeMWAXDBHandler). Defaults to None.
        """
        if not os.path.exists(config_filename):
            print(f"Configuration file location {config_filename} does not exist. Quitting.")
            sys.exit(1)

        # Parse config file
        config = ConfigParser()
        config.read_file(open(config_filename, "r", encoding="utf-8"))

        # Read log level
        config_file_log_level: Optional[str] = utils.read_optional_config(config, "mwax mover", "log_level")
        if config_file_log_level:
            # It's now safe to start logging
            # start logging
            logger.setLevel(config_file_log_level)

        logger.info(f"Starting mwacache_archive_processor processor...v{version.get_mwax_mover_version_string()}")

        logger.info(f"hostname: {self.hostname}")

        # Dump some diagnostic info
        py_version = sys.version  # contains a \n, so get rid of it
        py_version = py_version.replace("\n", " ")
        logger.info(f"Python v{py_version}")
        logger.info(f"astropy v{astropy.__version__}")

        logger.info(f"Reading config file: {config_filename}")

        i = 1
        self.watch_dirs = []

        # Common config options
        self.metafits_path = utils.read_config(config, "mwax mover", "metafits_path")

        if not os.path.exists(self.metafits_path):
            logger.error(f"Metafits file location  {self.metafits_path} does not exist. Quitting.")
            sys.exit(1)

        self.archive_to_location = ArchiveLocation(int(utils.read_config(config, "mwax mover", "archive_to_location")))
        self.concurrent_archive_workers = int(utils.read_config(config, "mwax mover", "concurrent_archive_workers"))
        self.archive_command_timeout_sec = int(
            utils.read_config(
                config,
                "mwax mover",
                "archive_command_timeout_sec",
            )
        )

        # Get list of projectids which are to be given
        # high priority when archiving
        self.high_priority_correlator_projectids = utils.read_config_list(
            config,
            "mwax mover",
            "high_priority_correlator_projectids",
        )
        self.high_priority_vcs_projectids = utils.read_config_list(
            config,
            "mwax mover",
            "high_priority_vcs_projectids",
        )

        # health
        self.health_multicast_ip = utils.read_config(config, "mwax mover", "health_multicast_ip")
        self.health_multicast_port = int(utils.read_config(config, "mwax mover", "health_multicast_port"))
        self.health_multicast_hops = int(utils.read_config(config, "mwax mover", "health_multicast_hops"))
        self.health_multicast_interface_name = utils.read_config(
            config,
            "mwax mover",
            "health_multicast_interface_name",
        )
        # get this hosts primary network interface ip
        self.health_multicast_interface_ip = utils.get_ip_address(self.health_multicast_interface_name)
        logger.info(f"IP for sending multicast: {self.health_multicast_interface_ip}")

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
        self.s3_profile = utils.read_config(config, s3_section, "profile")

        self.s3_ceph_endpoints = utils.read_config_list(
            config,
            s3_section,
            "ceph_endpoints",
        )

        #
        # Options specified per host
        #

        # Look for data_path1.. data_pathN
        while config.has_option(self.hostname, f"incoming_path{i}"):
            new_incoming_path = utils.read_config(config, self.hostname, f"incoming_path{i}")
            if not os.path.exists(new_incoming_path):
                logger.error(
                    f"incoming file location in incoming_path{i} - {new_incoming_path} does not exist. Quitting."
                )
                sys.exit(1)
            self.watch_dirs.append(new_incoming_path)
            i += 1

        if len(self.watch_dirs) == 0:
            logger.error(
                "No incoming data file locations were not present in config"
                " file. Use incoming_path1 .. incoming_pathN in the"
                " [<hostname>] section (where <hostname> is the lowercase"
                " hostname of the machine running this). This host's name is:"
                f" '{self.hostname}'. Quitting."
            )
            sys.exit(1)

        self.recursive = utils.read_config_bool(config, self.hostname, "recursive")

        #
        # MRO database - this is one we will update
        #
        self.mro_metadatadb_host = utils.read_config(config, "mro metadata database", "host")

        self.mro_metadatadb_db = utils.read_config(config, "mro metadata database", "db")
        self.mro_metadatadb_user = utils.read_config(config, "mro metadata database", "user")

        self.mro_metadatadb_pass = utils.read_config(
            config, "mro metadata database", "pass", self.mro_metadatadb_host != "dummy"
        )

        self.mro_metadatadb_port = int(utils.read_config(config, "mro metadata database", "port"))

        # Initiate database connection for mro metadata db
        if override_mro_db_handler:
            self.mro_db_handler = override_mro_db_handler
        else:
            self.mro_db_handler = mwax_db.MWAXDBHandler(
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
        self.remote_metadatadb_host = utils.read_config(config, "remote metadata database", "host")

        self.remote_metadatadb_db = utils.read_config(config, "remote metadata database", "db")
        self.remote_metadatadb_user = utils.read_config(config, "remote metadata database", "user")
        self.remote_metadatadb_pass = utils.read_config(
            config, "remote metadata database", "pass", self.remote_metadatadb_db != "dummy"
        )
        self.remote_metadatadb_port = int(utils.read_config(config, "remote metadata database", "port"))

        # Initiate database connection for remote metadata db
        if override_remote_db_handler:
            self.remote_db_handler = override_remote_db_handler
        else:
            self.remote_db_handler = mwax_db.MWAXDBHandler(
                host=self.remote_metadatadb_host,
                port=self.remote_metadatadb_port,
                db_name=self.remote_metadatadb_db,
                user=self.remote_metadatadb_user,
                password=self.remote_metadatadb_pass,
            )

        # Assemble paths and extensions
        paths_and_exts = [(s, ".*") for s in self.watch_dirs]

        # Create watch queue worker
        for i, p_and_e in enumerate(paths_and_exts):
            worker = PawseyOutgoingProcessor(
                f"PawseyOutgoingProcessor{i}",
                self.metafits_path,
                [p_and_e],
                self.high_priority_correlator_projectids,
                self.high_priority_vcs_projectids,
                self.mro_db_handler,
                self.remote_db_handler,
                self.s3_profile,
                self.s3_ceph_endpoints,
                self.archive_to_location,
            )
            self.workers.append(worker)

        # Make sure we can Ctrl-C / kill out of this
        logger.info("Initialising signal handlers")
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        logger.info("Ready to start...")

    def initialise_from_command_line(self):
        """Initialize the processor from command-line arguments.

        Parses command-line arguments and calls initialise() with the configuration
        file path.
        """

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwacache_archive_processor: a command line tool which is part of"
            " the MWA correlator for the MWA. It will monitor various"
            " directories on each mwacache server and, upon detecting a file,"
            " send it to Pawsey's LTS. It will then remove the file from the"
            " local disk."
        )

        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        self.initialise(config_filename)


def main():
    """Main entry point for the MWA cache archive processor."""
    processor = MWACacheArchiveProcessor()

    try:
        processor.initialise_from_command_line()
        processor.start()
        sys.exit(0)
    except Exception:
        logger.exception("Exited with error")


if __name__ == "__main__":
    main()
