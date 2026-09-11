"""Entry point and main class for the mwacache_archive_processor daemon.

MWACacheArchiveProcessor runs on the mwacache servers at Curtin. It monitors one
or more incoming directories for files sent from MWAX boxes, validates their size
and checksum against the metadata database, archives them to Pawsey Long-Term
Storage (Acacia or Banksia) via rclone, updates the metadata database to
confirm successful archival, then deletes the local copy.
"""

import logging
import os
import signal
import sys
import threading
import time
from configparser import ConfigParser
from glob import glob

import astropy

from mwax_mover import version
from mwax_mover.constants import (
    CONFIG_KEY_ARCHIVE_COMMAND_TIMEOUT_SEC,
    CONFIG_KEY_HIGH_PRIORITY_CORRELATOR_PROJECTIDS,
    CONFIG_KEY_HIGH_PRIORITY_VCS_PROJECTIDS,
    CONFIG_KEY_LOG_LEVEL,
    DEFAULT_POSTGRES_PORT,
    DUMMY_CONFIG_VALUE,
    EXIT_FAILURE,
    HEALTH_THREAD_NAME,
    LOG_FORMAT,
    SECONDS_PER_HOUR,
    SECTION_MWAX_MOVER,
)
from mwax_mover.core.config import read_config, read_config_bool, read_config_list, read_optional_config
from mwax_mover.core.env import get_hostname, running_under_pytest
from mwax_mover.db.handler import MWAXDBHandler
from mwax_mover.processors.daemon import MWAXDaemon
from mwax_mover.processors.pawsey_outgoing import PawseyOutgoingProcessor
from mwax_mover.queues.watch_queue_worker import MWAXPriorityWatchQueueWorker
from mwax_mover.filesystem.naming import ArchiveLocation

# Setup root logger
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter(LOG_FORMAT))
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


class MWACacheArchiveProcessor(MWAXDaemon):
    """
    A class representing an instance which sends
    MWAX data products from the mwacache servers
    to the Pawsey LTS.
    """

    DESCRIPTION = (
        "mwacache_archive_processor: a command line tool which is part of"
        " the MWA correlator for the MWA. It will monitor various"
        " directories on each mwacache server and, upon detecting a file,"
        " send it to Pawsey's LTS. It will then remove the file from the"
        " local disk."
    )

    def __init__(self):
        """Initialize MWACacheArchiveProcessor with default values.

        Sets up instance variables for database connections, archiving configuration,
        health monitoring, and worker management.
        """
        super().__init__()

        if running_under_pytest():
            # pretend I am mwacache99
            self.hostname = "mwacache99"
        else:
            self.hostname = get_hostname()

        self.cfg_metafits_path: str = ""
        self.archive_to_location: ArchiveLocation = ArchiveLocation.Unknown
        self.cfg_concurrent_archive_workers: int = 0
        self.cfg_archive_command_timeout_sec: int = 0
        self.cfg_rclone_check_wait_secs: int = 0

        # database config
        self.cfg_db_host: str = ""
        self.cfg_db_name: str = ""
        self.cfg_db_user: str = ""
        self.cfg_db_pass: str = ""
        self.cfg_db_port: int = DEFAULT_POSTGRES_PORT

        # s3 config
        self.s3_profile: str = ""

        self.cfg_health_multicast_hops: int = 1

        self.cfg_high_priority_correlator_projectids: list[str] = []
        self.cfg_high_priority_vcs_projectids: list[str] = []

        self.db_handler: MWAXDBHandler

        self.watch_dirs: list[str] = []
        self.recursive: bool = False

        # This list helps us keep track of all the workers
        self.workers: list[MWAXPriorityWatchQueueWorker] = list()

    def start(self):
        """Start the processor and begin monitoring for archive operations.

        Initializes database connection pools, starts health monitoring and worker
        threads, and enters main monitoring loop.
        """
        self.running = True

        # creating database connection pool
        if self.cfg_db_host != DUMMY_CONFIG_VALUE:
            logger.info("Starting database connection pool...")
            self.db_handler.start_database_pool()

        # create a health thread
        logger.info("Starting health_thread...")
        health_thread = threading.Thread(name=HEALTH_THREAD_NAME, target=self.health_loop, daemon=True)
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
                min_partial_purge_age_secs = SECONDS_PER_HOUR

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
                        self.request_fatal_shutdown(EXIT_FAILURE, f"Worker {w.name} has stopped unexpectedly.")
                        break

            time.sleep(0.1)

        # Final log message. NOTE: this used to unconditionally log "Completed
        # Successfully" even when we got here because a worker died, which
        # combined with main()'s sys.exit(0) made a fatal error look like a
        # clean shutdown.
        if self.fatal_exit_code:
            logger.error(f"Shutting down with exit code {self.fatal_exit_code}: {self.fatal_reason}")
        else:
            logger.info("Completed Successfully")

    def stop(self):
        """Stop the processor and shutdown all workers and connections.

        Stops worker threads and closes database connections.
        """
        self.running = False

        # Stop any Processors
        for w in self.workers:
            if w.is_running():
                w.stop()

        # Close database connection
        if self.db_handler:
            self.db_handler.close()

    def get_extra_status(self) -> dict:
        """No daemon-specific status keys beyond the base class's.

        Returns:
            An empty dict; the workers list is reported via get_worker_status().
        """
        return {}

    def get_worker_status(self) -> list[dict]:
        """Per-worker status, for get_status()'s "workers" key.

        Returns:
            A list of each worker's status dict.
        """
        return [w.get_status() for w in self.workers]

    def initialise(
        self,
        config_filename,
        override_db_handler: MWAXDBHandler | None = None,
    ):
        """Initialize the processor from a configuration file.

        Args:
            config_filename: Path to the configuration file.
            override_db_handler: If present, this will override the default
                MWAXDBHandler (this is used for testing via tests/tests_fakedb.py
                FakeMWAXDBHandler). Defaults to None.
        """
        if not os.path.exists(config_filename):
            print(f"Configuration file location {config_filename} does not exist. Quitting.")
            sys.exit(EXIT_FAILURE)

        # Parse config file
        config = ConfigParser()
        config.read_file(open(config_filename, "r", encoding="utf-8"))

        # Read log level
        config_file_log_level: str | None = read_optional_config(config, SECTION_MWAX_MOVER, CONFIG_KEY_LOG_LEVEL)
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
        self.cfg_metafits_path = read_config(config, SECTION_MWAX_MOVER, "metafits_path")

        if not os.path.exists(self.cfg_metafits_path):
            logger.error(f"Metafits file location  {self.cfg_metafits_path} does not exist. Quitting.")
            sys.exit(EXIT_FAILURE)

        self.archive_to_location = ArchiveLocation(int(read_config(config, SECTION_MWAX_MOVER, "archive_to_location")))
        self.cfg_concurrent_archive_workers = int(read_config(config, SECTION_MWAX_MOVER, "concurrent_archive_workers"))
        self.cfg_archive_command_timeout_sec = int(
            read_config(
                config,
                SECTION_MWAX_MOVER,
                CONFIG_KEY_ARCHIVE_COMMAND_TIMEOUT_SEC,
            )
        )

        # Seconds to wait between rclone copy and rclone check to ensure Banksia VSS nodes have synced
        self.cfg_rclone_check_wait_secs = int(
            read_config(
                config,
                SECTION_MWAX_MOVER,
                "rclone_check_wait_secs",
            )
        )

        # Get list of projectids which are to be given
        # high priority when archiving
        self.cfg_high_priority_correlator_projectids = read_config_list(
            config,
            SECTION_MWAX_MOVER,
            CONFIG_KEY_HIGH_PRIORITY_CORRELATOR_PROJECTIDS,
        )
        self.cfg_high_priority_vcs_projectids = read_config_list(
            config,
            SECTION_MWAX_MOVER,
            CONFIG_KEY_HIGH_PRIORITY_VCS_PROJECTIDS,
        )

        # health
        self._read_health_config(config)

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
        self.s3_profile = read_config(config, s3_section, "profile")

        #
        # Options specified per host
        #

        # Look for data_path1.. data_pathN
        while config.has_option(self.hostname, f"incoming_path{i}"):
            new_incoming_path = read_config(config, self.hostname, f"incoming_path{i}")
            if not os.path.exists(new_incoming_path):
                logger.error(
                    f"incoming file location in incoming_path{i} - {new_incoming_path} does not exist. Quitting."
                )
                sys.exit(EXIT_FAILURE)
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
            sys.exit(EXIT_FAILURE)

        self.recursive = read_config_bool(config, self.hostname, "recursive")

        #
        # MWA database
        #
        db_handler_from_config = MWAXDBHandler.from_config(config)
        self.cfg_db_host = db_handler_from_config.host
        self.cfg_db_name = db_handler_from_config.db_name
        self.cfg_db_user = db_handler_from_config.user
        self.cfg_db_pass = db_handler_from_config.password
        self.cfg_db_port = db_handler_from_config.port

        # Initiate database connection
        self.db_handler = override_db_handler if override_db_handler else db_handler_from_config

        # Assemble paths and extensions
        paths_and_exts = [(s, ".*") for s in self.watch_dirs]

        # Create watch queue worker
        for i, p_and_e in enumerate(paths_and_exts):
            worker = PawseyOutgoingProcessor(
                f"PawseyOutgoingProcessor{i}",
                self.cfg_metafits_path,
                [p_and_e],
                self.cfg_high_priority_correlator_projectids,
                self.cfg_high_priority_vcs_projectids,
                self.db_handler,
                self.s3_profile,
                self.archive_to_location,
                self.cfg_rclone_check_wait_secs,
                self.recursive,
            )
            self.workers.append(worker)

        # Make sure we can Ctrl-C / kill out of this
        logger.info("Initialising signal handlers")
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        logger.info("Ready to start...")


def main():
    """Main entry point for the MWA cache archive processor."""
    processor = MWACacheArchiveProcessor()

    try:
        processor.initialise_from_command_line()
        processor.start()
    except Exception:
        logger.exception("Exited with error")
        sys.exit(EXIT_FAILURE)

    # Surface a worker thread's fatal exit code (see request_fatal_shutdown).
    # This used to be an unconditional sys.exit(0).
    sys.exit(processor.fatal_exit_code)


if __name__ == "__main__":
    main()
