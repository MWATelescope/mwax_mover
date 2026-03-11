"""Module for mwax_subfile_distributor"""

import argparse
from configparser import ConfigParser
import queue
from flask import Flask, request
import json
import logging
import glob
import os
import signal
import sys
import shutil
import time
import threading

from mwax_mover import (
    mwax_db,
    utils,
)
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker, MWAXWatchQueueWorker
from typing import Optional
from mwax_mover import version
from mwax_mover.mwax_wqw_bf_stitching_processor import BfStitchingProcessor
from mwax_mover.mwax_wqw_checksum_and_db import ChecksumAndDBProcessor
from mwax_mover.mwax_wqw_outgoing import OutgoingProcessor
from mwax_mover.mwax_wqw_packet_stats_processor import PacketStatsProcessor
from mwax_mover.mwax_wqw_subfile_incoming_processor import SubfileIncomingProcessor
from mwax_mover.mwax_wqw_vis_cal_outgoing import VisCalOutgoingProcessor
from mwax_mover.mwax_wqw_vis_stats import VisStatsProcessor

flask_app: Flask = Flask(__name__)


class MWAXSubfileDistributor:
    """Class for MWAXSubfileDistributor- the main engine of MWAX"""

    def __init__(self):
        # init the logging subsystem
        self.logger = logging.getLogger("mwax_mover")
        self.cfg_log_level: str = ""

        self.subfile_dist_mode: utils.MWAXSubfileDistirbutorMode = utils.MWAXSubfileDistirbutorMode.UNKNOWN

        # Config parser
        self.config: ConfigParser

        # init vars
        if utils.running_under_pytest():
            # pretend I am mwax99
            self.hostname = "mwax99"
        else:
            self.hostname: str = utils.get_hostname()
        self.running: bool = False

        # Web server
        self.flask_thread: threading.Thread

        # This list helps us keep track of all the workers
        self.workers: list[MWAXWatchQueueWorker | MWAXPriorityWatchQueueWorker] = list()

        #
        # Config file vars
        #

        # Common
        self.cfg_webserver_port: int = 0
        self.cfg_voltdata_dont_archive_path: str = ""
        self.cfg_subfile_incoming_path: str = ""
        self.cfg_voltdata_incoming_path: str = ""
        self.cfg_voltdata_outgoing_path: str = ""
        self.cfg_always_keep_subfiles: bool = False
        self.cfg_archive_command_timeout_sec: int = 0
        self.cfg_psrdada_timeout_sec: int = 0
        self.cfg_copy_subfile_to_disk_timeout_sec: int = 0
        self.cfg_archiving_enabled: bool = False
        self.cfg_health_multicast_interface_ip: str = ""
        self.cfg_health_multicast_interface_name: str = ""
        self.cfg_health_multicast_ip: str = ""
        self.cfg_health_multicast_port: int = 0
        self.cfg_health_multicast_hops: int = 1
        self.cfg_packet_stats_dump_dir: str = ""
        self.cfg_packet_stats_destination_dir: str = ""

        # Voltage buffer dump vars
        self.dump_start_gps = None
        self.dump_end_gps = None
        self.dump_trigger_id = None
        self.dump_keep_file_queue = queue.Queue()

        # Correlator
        self.cfg_corr_input_ringbuffer_key: str = ""
        self.cfg_corr_diskdb_numa_node: int = -1
        self.cfg_corr_archive_command_numa_node: int = -1
        self.cfg_corr_visdata_dont_archive_path: str = ""
        self.cfg_corr_visdata_incoming_path: str = ""
        self.cfg_corr_visdata_outgoing_path: str = ""
        self.cfg_corr_high_priority_correlator_projectids: list[str] = []
        self.cfg_corr_high_priority_vcs_projectids: list[str] = []
        # Archiving settings for correlator
        self.cfg_corr_archive_destination_host: str = ""
        self.cfg_corr_archive_destination_port: int = 0
        self.cfg_corr_archive_destination_enabled: int = 0
        # processing_stats config
        self.cfg_corr_mwax_stats_timeout_sec: int = 0
        self.cfg_corr_mwax_stats_dump_dir: str = ""
        self.cfg_corr_mwax_stats_binary_dir: str = ""
        self.cfg_corr_visdata_processing_stats_path: str = ""
        # calibration config
        self.cfg_corr_calibrator_outgoing_path: str = ""
        self.cfg_corr_calibrator_destination_enabled: int = 0
        self.cfg_corr_metafits_path: str = ""

        # Connection info for metadata db
        self.cfg_metadatadb_host: str = ""
        self.cfg_metadatadb_db: str = ""
        self.cfg_metadatadb_user: str = ""
        self.cfg_metadatadb_pass: str = ""
        self.cfg_metadatadb_port: int = 5432

        # Archiving stuff
        self.archiving_paused: bool = False

        # Since our watcher needs a queue, we'll just get the queue to dump the filenames
        # into this list so we can easily remove them when release_cal_obs is called
        # by a calvin
        self.outgoing_cal_list: list[str] = list()

        # Database handler for metadata db
        self.db_handler: mwax_db.MWAXDBHandler

    def initialise_from_command_line(self):
        """Called if run from command line"""

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_subfile_distributor: a command line tool which is part of"
            " the mwax suite for the MWA. It will perform different tasks"
            " based on the configuration file.\nIn addition, it will"
            " automatically archive files in /voltdata and /visdata to the"
            " mwacache servers at the Curtin Data Centre. (mwax_mover"
            f" v{version.get_mwax_mover_version_string()})\n"
        )

        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

        parser.add_argument(
            "--mode",
            choices=["c", "b", "C", "B"],
            required=True,
            help="Mode of operation: C (correlator) or B (beamformer)",
        )

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        # mode
        config_mode: utils.MWAXSubfileDistirbutorMode = utils.MWAXSubfileDistirbutorMode.UNKNOWN
        mode_str = ""
        try:
            mode_str = str(args["mode"])
            config_mode: utils.MWAXSubfileDistirbutorMode = utils.MWAXSubfileDistirbutorMode(mode_str.upper())
        except Exception:
            print(f"--mode {mode_str} is not supported")
            exit(-1)

        self.initialise(config_filename, config_mode)

    def initialise(self, config_filename: str, config_mode: utils.MWAXSubfileDistirbutorMode):
        """Initialise common code"""
        if not os.path.exists(config_filename):
            self.logger.error(f"Configuration file location {config_filename} does not exist. Quitting.")
            sys.exit(1)

        # Parse config file
        self.config = ConfigParser()
        self.config.read_file(open(config_filename, "r", encoding="utf-8"))

        # Read log level
        config_file_log_level: Optional[str] = utils.read_optional_config(
            self.logger, self.config, "mwax mover", "log_level"
        )
        if config_file_log_level is None:
            self.cfg_log_level = "DEBUG"
            self.logger.warning(f"log_level not set in config file. Defaulting to {self.cfg_log_level} level logging.")
        else:
            self.cfg_log_level = config_file_log_level

        # It's now safe to start logging
        # start logging
        self.logger.setLevel(self.cfg_log_level)
        console_log = logging.StreamHandler()
        console_log.setLevel(self.cfg_log_level)
        console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        self.logger.addHandler(console_log)

        self.logger.info(f"Starting mwax_subfile_distributor processor...v{version.get_mwax_mover_version_string()}")

        self.logger.info("==========================================================================================")

        self.subfile_dist_mode = config_mode
        if self.subfile_dist_mode == utils.MWAXSubfileDistirbutorMode.CORRELATOR:
            self.logger.info("running in CORRELATOR mode: ignoring Beamforming observations")
        elif self.subfile_dist_mode == utils.MWAXSubfileDistirbutorMode.BEAMFORMER:
            self.logger.info("running in BEAMFORMER mode: ignoring VCS and Correlator observations")
        else:
            self.logger.warning(f"Incorrect mode: {self.subfile_dist_mode.value} exiting")
            exit(2)

        self.logger.info("==========================================================================================")

        self.logger.info(f"Reading config file: {config_filename}")

        self.cfg_webserver_port = int(utils.read_config(self.logger, self.config, "mwax mover", "webserver_port"))
        self.cfg_voltdata_dont_archive_path = utils.read_config(
            self.logger,
            self.config,
            "mwax mover",
            "voltdata_dont_archive_path",
        )
        self.cfg_subfile_incoming_path = utils.read_config(
            self.logger, self.config, "mwax mover", "subfile_incoming_path"
        )
        self.cfg_voltdata_incoming_path = utils.read_config(
            self.logger, self.config, "mwax mover", "voltdata_incoming_path"
        )
        self.cfg_voltdata_outgoing_path = utils.read_config(
            self.logger, self.config, "mwax mover", "voltdata_outgoing_path"
        )
        self.cfg_health_multicast_interface_name = utils.read_config(
            self.logger,
            self.config,
            "mwax mover",
            "health_multicast_interface_name",
        )
        self.cfg_health_multicast_ip = utils.read_config(self.logger, self.config, "mwax mover", "health_multicast_ip")
        self.cfg_health_multicast_port = int(
            utils.read_config(self.logger, self.config, "mwax mover", "health_multicast_port")
        )
        self.cfg_health_multicast_hops = int(
            utils.read_config(self.logger, self.config, "mwax mover", "health_multicast_hops")
        )

        self.cfg_psrdada_timeout_sec = int(
            utils.read_config(self.logger, self.config, "mwax mover", "psrdada_timeout_sec")
        )
        self.cfg_copy_subfile_to_disk_timeout_sec = int(
            utils.read_config(
                self.logger,
                self.config,
                "mwax mover",
                "copy_subfile_to_disk_timeout_sec",
            )
        )

        self.cfg_archive_command_timeout_sec = int(
            utils.read_config(
                self.logger,
                self.config,
                "mwax mover",
                "archive_command_timeout_sec",
            )
        )

        # get this hosts primary network interface ip
        self.cfg_health_multicast_interface_ip = utils.get_ip_address(self.cfg_health_multicast_interface_name)
        self.logger.info(f"IP for sending multicast: {self.cfg_health_multicast_interface_ip}")

        if not os.path.exists(self.cfg_voltdata_dont_archive_path):
            self.logger.error(
                f"'Voltdata Dont Archive' location {self.cfg_voltdata_dont_archive_path} does not exist. Quitting."
            )
            sys.exit(1)

        if not os.path.exists(self.cfg_subfile_incoming_path):
            self.logger.error(f"Subfile file location {self.cfg_subfile_incoming_path} does not exist. Quitting.")
            sys.exit(1)

        if not os.path.exists(self.cfg_voltdata_incoming_path):
            self.logger.error(f"Voltdata file location {self.cfg_voltdata_incoming_path} does not exist. Quitting.")
            sys.exit(1)

        if not os.path.exists(self.cfg_voltdata_outgoing_path):
            self.logger.error(f"Voltdata file location {self.cfg_voltdata_outgoing_path} does not exist. Quitting.")
            sys.exit(1)

        self.cfg_always_keep_subfiles = (
            int(
                utils.read_config(
                    self.logger,
                    self.config,
                    self.hostname,
                    "always_keep_subfiles",
                )
            )
            == 1
        )

        if self.cfg_always_keep_subfiles:
            self.logger.info(
                "Will keep subfiles after they are used in:"
                f" {self.cfg_voltdata_incoming_path}... ** NOTE: this should"
                " be for DEBUG only as it will be slow and may not keep"
                " up! **"
            )

        self.cfg_packet_stats_dump_dir = utils.read_config(
            self.logger, self.config, "mwax mover", "packet_stats_dump_dir"
        )
        if self.cfg_packet_stats_dump_dir == "":
            self.logger.warning("packet_stats_dump_dir is blank, so no packet stats will be written.")
        else:
            if not os.path.exists(self.cfg_packet_stats_dump_dir):
                self.logger.error(f"packet_stats_dump_dir {self.cfg_packet_stats_dump_dir} does not exist. Quitting.")
                sys.exit(1)

        self.cfg_packet_stats_destination_dir = utils.read_config(
            self.logger, self.config, "mwax mover", "packet_stats_destination_dir"
        )

        if self.cfg_packet_stats_destination_dir == "" and self.cfg_packet_stats_dump_dir != "":
            self.logger.warning(
                "packet_stats_destination_dir is blank, so no packet stats will be moved from dump_dir "
                f"{self.cfg_packet_stats_dump_dir} to destination (e.g. vulcan)."
            )
        elif self.cfg_packet_stats_destination_dir == "" and self.cfg_packet_stats_dump_dir == "":
            pass
        else:
            # We have a destination dir, so ensure it exists
            if not os.path.exists(self.cfg_packet_stats_destination_dir):
                self.logger.error(
                    f"packet_stats_destination_dir {self.cfg_packet_stats_destination_dir} does not exist. Quitting."
                )
                sys.exit(1)

        # read correlator config
        self.cfg_corr_input_ringbuffer_key = utils.read_config(
            self.logger, self.config, "correlator", "input_ringbuffer_key"
        )
        self.cfg_corr_visdata_incoming_path = utils.read_config(
            self.logger, self.config, "correlator", "visdata_incoming_path"
        )
        self.cfg_corr_visdata_dont_archive_path = utils.read_config(
            self.logger,
            self.config,
            "correlator",
            "visdata_dont_archive_path",
        )
        self.cfg_corr_visdata_processing_stats_path = utils.read_config(
            self.logger,
            self.config,
            "correlator",
            "visdata_processing_stats_path",
        )
        self.cfg_corr_visdata_outgoing_path = utils.read_config(
            self.logger, self.config, "correlator", "visdata_outgoing_path"
        )
        self.cfg_corr_mwax_stats_binary_dir = utils.read_config(
            self.logger, self.config, "correlator", "mwax_stats_binary_dir"
        )
        self.cfg_corr_mwax_stats_dump_dir = utils.read_config(
            self.logger, self.config, "correlator", "mwax_stats_dump_dir"
        )
        self.cfg_corr_mwax_stats_timeout_sec = int(
            utils.read_config(
                self.logger,
                self.config,
                "correlator",
                "mwax_stats_timeout_sec",
            )
        )

        # calibration processing sections
        self.cfg_corr_calibrator_outgoing_path = utils.read_config(
            self.logger,
            self.config,
            "correlator",
            "calibrator_outgoing_path",
        )
        self.cfg_corr_calibrator_destination_enabled = int(
            utils.read_config(
                self.logger,
                self.config,
                "correlator",
                "calibrator_destination_enabled",
            )
        )

        self.cfg_corr_metafits_path = utils.read_config(self.logger, self.config, "correlator", "metafits_path")

        # Get list of projectids which are to be given
        # high priority when archiving
        self.cfg_corr_high_priority_correlator_projectids = utils.read_config_list(
            self.logger,
            self.config,
            "correlator",
            "high_priority_correlator_projectids",
        )
        self.cfg_corr_high_priority_vcs_projectids = utils.read_config_list(
            self.logger,
            self.config,
            "correlator",
            "high_priority_vcs_projectids",
        )

        if not os.path.exists(self.cfg_corr_visdata_incoming_path):
            self.logger.error(f"Visdata file location {self.cfg_corr_visdata_incoming_path} does not exist. Quitting.")
            sys.exit(1)

        if not os.path.exists(self.cfg_corr_visdata_dont_archive_path):
            self.logger.error(
                f"'Visdata Dont Archive' location {self.cfg_corr_visdata_dont_archive_path} does not exist. Quitting."
            )
            sys.exit(1)

        if not os.path.exists(self.cfg_corr_visdata_processing_stats_path):
            self.logger.error(
                f"Visdata file location {self.cfg_corr_visdata_processing_stats_path} does not exist. Quitting."
            )
            sys.exit(1)

        if not os.path.exists(self.cfg_corr_visdata_outgoing_path):
            self.logger.error(f"Visdata file location {self.cfg_corr_visdata_outgoing_path} does not exist. Quitting.")
            sys.exit(1)

        if not os.path.exists(self.cfg_corr_mwax_stats_binary_dir):
            self.logger.error(f"mwax_stats binary dir {self.cfg_corr_mwax_stats_binary_dir} does not exist. Quitting.")
            sys.exit(1)

        if not os.path.exists(self.cfg_corr_calibrator_outgoing_path):
            self.logger.error(
                f"calibrator outgoing location {self.cfg_corr_calibrator_outgoing_path} does not exist. Quitting."
            )
            sys.exit(1)

        if not os.path.exists(self.cfg_corr_metafits_path):
            self.logger.error(f"metafits location {self.cfg_corr_metafits_path} does not exist. Quitting.")
            sys.exit(1)

        self.cfg_metadatadb_host = utils.read_config(self.logger, self.config, "mwa metadata database", "host")
        self.cfg_metadatadb_db = utils.read_config(self.logger, self.config, "mwa metadata database", "db")
        self.cfg_metadatadb_user = utils.read_config(self.logger, self.config, "mwa metadata database", "user")
        # Only read the password if host is not dummy
        if self.cfg_metadatadb_host != "dummy":
            self.cfg_metadatadb_pass = utils.read_config(
                self.logger,
                self.config,
                "mwa metadata database",
                "pass",
                True,
            )
        self.cfg_metadatadb_port = int(utils.read_config(self.logger, self.config, "mwa metadata database", "port"))

        # Read config specific to this host
        self.cfg_corr_archive_destination_host = utils.read_config(
            self.logger,
            self.config,
            self.hostname,
            "mwax_destination_host",
        )
        self.cfg_corr_archive_destination_port = int(
            utils.read_config(
                self.logger,
                self.config,
                self.hostname,
                "mwax_destination_port",
            )
        )
        self.cfg_corr_archive_destination_enabled = (
            int(
                utils.read_config(
                    self.logger,
                    self.config,
                    self.hostname,
                    "mwax_destination_enabled",
                )
            )
            == 1
        )
        self.cfg_corr_diskdb_numa_node = int(
            utils.read_config(
                self.logger,
                self.config,
                self.hostname,
                "dada_disk_db_numa_node",
            )
        )
        self.cfg_corr_archive_command_numa_node = int(
            utils.read_config(
                self.logger,
                self.config,
                self.hostname,
                "archive_command_numa_node",
            )
        )

        # beamformer options
        self.cfg_bf_redis_host = utils.read_config(
            self.logger,
            self.config,
            "beamformer",
            "bf_redis_host",
        )

        self.cfg_bf_redis_queue_key = utils.read_config(
            self.logger,
            self.config,
            "beamformer",
            "bf_redis_queue_key",
        )

        self.cfg_bf_aocal_path = utils.read_config(
            self.logger,
            self.config,
            "beamformer",
            "bf_aocal_path",
        )

        if not os.path.exists(self.cfg_bf_aocal_path):
            self.logger.error(f"bf_aocal_path location {self.cfg_bf_aocal_path} does not exist. Quitting.")
            sys.exit(1)

        self.cfg_bf_incoming_path = utils.read_config(
            self.logger,
            self.config,
            "beamformer",
            "bf_incoming_path",
        )

        if not os.path.exists(self.cfg_bf_incoming_path):
            self.logger.error(f"bf_incoming_path location {self.cfg_bf_incoming_path} does not exist. Quitting.")
            sys.exit(1)

        self.cfg_bf_stitching_path = utils.read_config(
            self.logger,
            self.config,
            "beamformer",
            "bf_stitching_path",
        )

        if not os.path.exists(self.cfg_bf_stitching_path):
            self.logger.error(f"bf_stitching_path location {self.cfg_bf_stitching_path} does not exist. Quitting.")
            sys.exit(1)

        self.cfg_bf_outgoing_path = utils.read_config(
            self.logger,
            self.config,
            "beamformer",
            "bf_outgoing_path",
        )

        if not os.path.exists(self.cfg_bf_outgoing_path):
            self.logger.error(f"bf_outgoing_path location {self.cfg_bf_outgoing_path} does not exist. Quitting.")
            sys.exit(1)

        self.cfg_bf_dont_archive_path = utils.read_config(
            self.logger,
            self.config,
            "beamformer",
            "bf_dont_archive_path",
        )
        if not os.path.exists(self.cfg_bf_dont_archive_path):
            self.logger.error(
                f"bf_dont_archive_path location {self.cfg_bf_dont_archive_path} does not exist. Quitting."
            )
            sys.exit(1)

        self.cfg_bf_keep_original_files_after_stitching = utils.read_config_bool(
            self.logger, self.config, "beamformer", "bf_keep_original_files_after_stitching"
        )

        # Initiate database connection pool for metadata db
        self.db_handler = mwax_db.MWAXDBHandler(
            logger=self.logger,
            host=self.cfg_metadatadb_host,
            port=self.cfg_metadatadb_port,
            db_name=self.cfg_metadatadb_db,
            user=self.cfg_metadatadb_user,
            password=self.cfg_metadatadb_pass,
        )

        # Read master archiving enabled option
        self.cfg_archiving_enabled = (
            int(
                utils.read_config(
                    self.logger,
                    self.config,
                    "mwax mover",
                    "archiving_enabled",
                )
            )
            == 1
        )

        # If master archiving is disabled, then disable the corr and bf
        # archiving settings otherwise just use those settings as necessary
        if not self.cfg_archiving_enabled:
            self.logger.warning(
                "Master archving ('archiving_enabled') is set to FALSE."
                " Nothing will be archived and nothing will be sent for"
                " calibration."
            )
            self.cfg_corr_archive_destination_enabled = False
            self.cfg_corr_calibrator_destination_enabled = False

        # Create and start web server
        self.logger.info(f"Starting http server on port {self.cfg_webserver_port}...")
        self.flask_thread = threading.Thread(name="webserver", target=self.start_flask_web_server, daemon=True)
        self.flask_thread.start()

        #
        # Subfile handling watch-queue-workers
        #
        # Each server will use a unique queue key. The queue key we read in the cfg file
        # is just the base- we then add the server name.
        # e.g. mwax25 will have a queue key of "bfq_mwax25"

        # Get the last 2 digits of the hostname
        bf_redis_queue_key = f"{self.cfg_bf_redis_queue_key}{self.hostname}"

        self.logger.debug(f"Using redis queue key: {bf_redis_queue_key}")

        # Create watch queue worker
        subfile_incoming_worker = SubfileIncomingProcessor(
            self.logger,
            self,
            self.cfg_subfile_incoming_path,
            ".sub",
            ".free",
            ".keep",
            self.cfg_voltdata_incoming_path,
            self.cfg_bf_aocal_path,
            self.cfg_bf_redis_host,
            bf_redis_queue_key,
            self.cfg_packet_stats_dump_dir,
            self.cfg_corr_mwax_stats_binary_dir,
            1 if self.cfg_corr_diskdb_numa_node == 0 else 0,
            self.cfg_copy_subfile_to_disk_timeout_sec,
            self.cfg_corr_input_ringbuffer_key,
            self.cfg_corr_diskdb_numa_node,
            self.cfg_psrdada_timeout_sec,
            self.cfg_always_keep_subfiles,
            self.cfg_corr_archive_destination_enabled,
            self.cfg_corr_metafits_path,
            self.subfile_dist_mode,
        )
        self.workers.append(subfile_incoming_worker)

        if self.cfg_packet_stats_destination_dir != "" and self.cfg_packet_stats_dump_dir != "":
            packet_stats_worker = PacketStatsProcessor(
                self.logger, self.cfg_packet_stats_dump_dir, ".dat", self.cfg_packet_stats_destination_dir
            )
            self.workers.append(packet_stats_worker)

        #
        # Archiving watch-queue-workers
        #

        # Watch:
        #   watch_dir_incoming_vis
        #   watch_dir_incoming_volt
        #   watch_dir_stitching_bf
        # Do:
        #   Checksum and insert into database (if archiving)
        # Then:
        #   Move file to outgoing dir (if archiving) or dont_archive dir (if not archiving)
        self.checksum_and_db_processor = ChecksumAndDBProcessor(
            self.logger,
            self.cfg_corr_metafits_path,
            self.cfg_corr_visdata_incoming_path,
            self.cfg_corr_visdata_processing_stats_path,
            self.cfg_corr_visdata_outgoing_path,
            self.cfg_corr_visdata_dont_archive_path,
            self.cfg_voltdata_incoming_path,
            self.cfg_voltdata_outgoing_path,
            self.cfg_voltdata_dont_archive_path,
            self.cfg_bf_stitching_path,
            self.cfg_bf_outgoing_path,
            self.cfg_bf_dont_archive_path,
            self.cfg_corr_high_priority_correlator_projectids,
            self.cfg_corr_high_priority_vcs_projectids,
            self.db_handler,
            self.cfg_corr_archive_destination_enabled == 1,
        )
        self.workers.append(self.checksum_and_db_processor)

        # Watch:
        #   watch_dir_processing_stats_vis
        # Do:
        #   Run mwax_stats
        # Then:
        #   Move file to outgoing cal dir (if archiving & calibrator), outgoing dir (if archiving and not calibrator) or dont_archive dir (if not archiving)
        self.vis_stats_processor = VisStatsProcessor(
            self.logger,
            self.cfg_corr_metafits_path,
            self.cfg_corr_visdata_processing_stats_path,
            self.cfg_corr_mwax_stats_binary_dir,
            self.cfg_corr_mwax_stats_timeout_sec,
            self.cfg_corr_mwax_stats_dump_dir,
            self.cfg_corr_archive_destination_enabled == 1,
            self.cfg_corr_visdata_outgoing_path,
            self.cfg_corr_calibrator_outgoing_path,
            self.cfg_corr_visdata_dont_archive_path,
        )
        self.workers.append(self.vis_stats_processor)

        # Watch:
        #   watch_dir_incoming_bf
        # Do:
        #   Stitch the files and save them into watch_dir_stitching_bf
        #   (Optionally copy the pre-stitched files to dont_archive_path_bf if bf_keep_original_files_after_stitching is True)
        # Then:
        #   ChecksumAndDB processor will pick up the new files in watch_dir_stitching_bf
        self.bf_stitching_processor = BfStitchingProcessor(
            self.logger,
            self.cfg_corr_metafits_path,
            self.cfg_bf_incoming_path,
            self.cfg_bf_stitching_path,
            self.cfg_bf_dont_archive_path,
            self.cfg_corr_high_priority_correlator_projectids,
            self.cfg_corr_high_priority_vcs_projectids,
            self.cfg_corr_archive_destination_enabled == 1,
            self.cfg_bf_keep_original_files_after_stitching,
        )
        self.workers.append(self.bf_stitching_processor)

        if self.cfg_corr_archive_destination_enabled == 1:
            # Only start these processors if we are archiving

            # Watch:
            #   watch_dir_outgoing_cal
            # Do:
            #   Add to the outgoing cal list so that when release_cal_obs is called by calvin, we can remove the file from the list and archive the file
            #
            self.vis_cal_outgoing_processor = VisCalOutgoingProcessor(
                self.logger,
                self.cfg_corr_calibrator_outgoing_path,
                self.outgoing_cal_list,
            )
            self.workers.append(self.vis_cal_outgoing_processor)

            # Watch:
            #   watch_dir_outgoing_vis
            #   watch_dir_outgoing_volt
            #   watch_dir_outgoing_bf
            # Do:
            #   Use xrootd to transfer the files to the mwacache servers
            self.outgoing_processor = OutgoingProcessor(
                self.logger,
                self.cfg_corr_metafits_path,
                self.cfg_corr_visdata_outgoing_path,
                self.cfg_voltdata_outgoing_path,
                self.cfg_bf_outgoing_path,
                self.cfg_corr_high_priority_correlator_projectids,
                self.cfg_corr_high_priority_vcs_projectids,
                self.cfg_corr_archive_command_numa_node,
                self.cfg_corr_archive_destination_host,
                self.cfg_archive_command_timeout_sec,
            )
            self.workers.append(self.outgoing_processor)

        else:
            # We have disabled archiving, so use a different
            # handler for incoming data
            # which just moves the files elsewhere

            # First check to ensure there are no existing unarchived files on
            # our watching dirs
            if utils.running_under_pytest:
                # Ignore this check if we're testing
                pass
            else:
                if (
                    len(next(os.walk(self.cfg_voltdata_incoming_path))[2]) > 0
                    or len(next(os.walk(self.cfg_corr_visdata_incoming_path))[2]) > 0
                    or len(next(os.walk(self.cfg_voltdata_outgoing_path))[2]) > 0
                    or len(next(os.walk(self.cfg_corr_visdata_outgoing_path))[2]) > 0
                    or len(next(os.walk(self.cfg_corr_calibrator_outgoing_path))[2]) > 0
                    or len(next(os.walk(self.cfg_corr_visdata_processing_stats_path))[2]) > 0
                    or len(next(os.walk(self.cfg_bf_incoming_path))[2]) > 0
                    or len(next(os.walk(self.cfg_bf_outgoing_path))[2]) > 0
                ):
                    self.logger.error(
                        "Error- voltage incoming/outgoing and/or visibility "
                        "incoming/processing/outgoing/cal/bf dirs are not empty! "
                        "Watched paths must be empty before starting with  "
                        "archiving disabled to prevent inadvertent data loss. "
                        "Exiting."
                    )
                    sys.exit(-2)

        # self.archive_processor = mwax_archive_processor.MWAXArchiveProcessor(
        #     self,
        #     self.hostname,
        #     self.cfg_corr_archive_destination_enabled,
        #     self.cfg_corr_archive_command_numa_node,
        #     self.cfg_corr_archive_destination_host,
        #     self.cfg_corr_archive_destination_port,
        #     self.cfg_archive_command_timeout_sec,
        #     self.cfg_corr_mwax_stats_binary_dir,
        #     self.cfg_corr_mwax_stats_dump_dir,
        #     self.cfg_corr_mwax_stats_timeout_sec,
        #     self.db_handler,
        #     self.cfg_voltdata_incoming_path,
        #     self.cfg_voltdata_outgoing_path,
        #     self.cfg_corr_visdata_incoming_path,
        #     self.cfg_corr_visdata_processing_stats_path,
        #     self.cfg_corr_visdata_outgoing_path,
        #     self.cfg_corr_calibrator_outgoing_path,
        #     self.cfg_corr_calibrator_destination_enabled,
        #     self.cfg_corr_metafits_path,
        #     self.cfg_corr_visdata_dont_archive_path,
        #     self.cfg_voltdata_dont_archive_path,
        #     self.cfg_corr_high_priority_correlator_projectids,
        #     self.cfg_corr_high_priority_vcs_projectids,
        #     self.cfg_bf_incoming_path,
        #     self.cfg_bf_stitching_path,
        #     self.cfg_bf_outgoing_path,
        #     self.cfg_bf_dont_archive_path,
        #     self.cfg_bf_keep_original_files_after_stitching,
        # )

        # # Add this processor to list of processors we manage
        # self.processors.append(self.archive_processor)

        # Make sure we can Ctrl-C / kill out of this
        self.logger.info("Initialising signal handlers")
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info("Ready to start...")

    def release_cal_obs(self, obs_id: int):
        try:
            # Release any cal_outgoing files- this is triggered by a calvin server finishing processing
            # and calling the release_cal_obs web service endpoint on this host
            obs_files = glob.glob(os.path.join(self.cfg_corr_calibrator_outgoing_path, f"{obs_id}*.fits"))

            if len(obs_files) == 0:
                self.logger.debug(f"{obs_id}: release_cal_obs()- no files found for this obs_id")

            # For file in the cal_outgoing dir for this obs_id
            for item in obs_files:
                # Does the file exist?
                if os.path.exists(item):
                    # Is this host doing archiving?
                    if self.cfg_corr_archive_destination_enabled == 1:
                        # Validate and get info about the obs
                        obs_info: utils.ValidationData = utils.validate_filename(
                            self.logger, item, self.cfg_corr_metafits_path
                        )

                        # Should this project be archived?
                        if utils.should_project_be_archived(obs_info.project_id):
                            # Send to vis_outgoing
                            # Take the input filename - strip the path, then append the output path
                            outgoing_filename = os.path.join(
                                self.cfg_corr_visdata_outgoing_path, os.path.basename(item)
                            )
                            self.logger.debug(f"{obs_id}- release_cal_obs() moving {item} to outgoing vis dir")
                            os.rename(item, outgoing_filename)
                        else:
                            # No this project doesn't get archived
                            outgoing_filename = os.path.join(
                                self.cfg_corr_visdata_dont_archive_path, os.path.basename(item)
                            )
                            self.logger.debug(
                                f"{item}- release_cal_obs() moving file to {self.cfg_corr_visdata_dont_archive_path}"
                            )
                            os.rename(item, outgoing_filename)
                    else:
                        # This host is not doing any archiving
                        outgoing_filename = os.path.join(
                            self.cfg_corr_visdata_dont_archive_path, os.path.basename(item)
                        )
                        self.logger.debug(
                            f"{item}- release_cal_obs() moving file to {self.cfg_corr_visdata_dont_archive_path}"
                        )
                        os.rename(item, outgoing_filename)
                else:
                    self.logger.exception(f"{obs_id}: release_cal_obs()- failed to archive {item}- file does not exist")

                # Remove item from queue
                try:
                    self.outgoing_cal_list.remove(item)
                except Exception:
                    # Don't want an exception if file is already gone from list
                    pass
        except Exception:
            self.logger.exception(f"{obs_id}: release_cal_obs()- something went wrong when releasing this obs_id")

    def pause_archiving(self, paused: bool):
        """Pauses archiving"""
        if self.archiving_paused != paused:
            if paused:
                self.logger.info("Pausing archiving")
            else:
                self.logger.info("Resuming archiving")

            for worker in self.workers:
                if worker:
                    worker.pause(paused)

            self.archiving_paused = paused

    def start_flask_web_server(self):
        # threaded=True lets Flask handle multiple requests concurrently
        flask_app.add_url_rule("/shutdown", "shutdown", self.endpoint_shutdown, methods=["POST", "GET"])
        flask_app.add_url_rule("/status", "status", self.endpoint_status, methods=["GET"])
        flask_app.add_url_rule(
            "/pause_archiving", "pause_archiving", self.endpoint_pause_archiving, methods=["POST", "GET"]
        )
        flask_app.add_url_rule(
            "/resume_archiving", "resume_archiving", self.endpoint_resume_archiving, methods=["POST", "GET"]
        )
        flask_app.add_url_rule(
            "/release_cal_obs", "release_cal_obs", self.endpoint_release_cal_obs, methods=["POST", "GET"]
        )
        flask_app.add_url_rule("/dump_voltages", "dump_voltages", self.endpoint_dump_voltages, methods=["POST", "GET"])

        flask_app.run(debug=False, host="0.0.0.0", port=self.cfg_webserver_port, use_reloader=False, threaded=True)

    def endpoint_shutdown(self):
        self.stop()
        return b"OK", 200

    def endpoint_status(self):
        data = json.dumps(self.get_status())
        return data.encode("utf-8"), 200

    def endpoint_pause_archiving(self):
        self.pause_archiving(paused=True)
        return b"OK", 200

    def endpoint_resume_archiving(self):
        self.pause_archiving(paused=False)
        return b"OK", 200

    def endpoint_release_cal_obs(self):
        try:
            self.logger.info("Recieved call to release_cal_obs()")

            obs_id = request.args.get("obs_id")  # returns None if missing

            if obs_id is None:
                raise ValueError("obs_id parameter missing from release_cal_obs() call")
            else:
                if utils.is_int(obs_id):
                    self.logger.info(
                        f"{obs_id}: release_cal_obs(): calling archive_processor.release_cal_obs({obs_id})"
                    )
                    self.release_cal_obs(int(obs_id))
                    return b"OK", 200
                else:
                    raise ValueError(f"obs_id {obs_id} passed to release_cal_obs() is not an int")

        except Exception as ws_exception:
            return f"ERROR: {ws_exception}".encode("utf-8"), 500

    def endpoint_dump_voltages(self):
        # Check for correct params
        try:
            starttime = request.args.get("start")
            if starttime is None:
                raise ValueError("start parameter missing from dump_voltages() call")
            else:
                if utils.is_int(starttime):
                    starttime = int(starttime)
                else:
                    raise ValueError("start parameter is not an integer")

            endtime = request.args.get("end")
            if endtime is None:
                raise ValueError("end parameter missing from dump_voltages() call")
            else:
                if utils.is_int(endtime):
                    endtime = int(endtime)
                else:
                    raise ValueError("end parameter is not an integer")

            trigger_id = request.args.get("trigger_id")
            if trigger_id is None:
                raise ValueError("trigger_id parameter missing from dump_voltages() call")
            else:
                if utils.is_int(trigger_id):
                    trigger_id = int(trigger_id)
                else:
                    raise ValueError("trigger_id parameter is not an integer")

            # Special test mode- if start and end == 0 just return 200
            if starttime == endtime == 0:
                return b"OK", 200
            else:
                if len(str(starttime)) != 10 and starttime != 0:
                    raise ValueError("start must be gps seconds and length 10 (or 0 for as early as possible)")

                if len(str(endtime)) != 10:
                    raise ValueError("end must be gps seconds and length 10")

                if endtime - starttime <= 0:
                    raise ValueError("end must be after start")

                # Check to see if we aren't already doing a dump
                if self.dump_start_gps is None and self.dump_end_gps is None:
                    # Now call the method to dump the voltages
                    if self.dump_voltages(starttime, endtime, trigger_id):
                        return b"OK", 200
                    else:
                        return b"Failed to start Voltage Buffer Dump", 400
                else:
                    # Reject this request
                    return b"Voltage Buffer Dump already in progress. Request canceled.", 400

        except ValueError as parameters_exception:  # pylint: disable=broad-except
            return f"Value Error: {parameters_exception}".encode("utf-8"), 400

        except Exception as dump_voltages_exception:  # pylint: disable=broad-except
            return f"ERROR: {dump_voltages_exception}".encode("utf-8"), 500

    def dump_voltages(self, start_gps_time: int, end_gps_time: int, trigger_id: int) -> bool:
        """Dump whatever subfiles we have from /dev/shm to disk"""
        # Set module level variables
        self.dump_start_gps = start_gps_time  # note, this may be 0! meaning 'earliest'
        self.dump_end_gps = end_gps_time
        self.dump_trigger_id = trigger_id

        self.logger.info(
            f"dump_voltages: from {str(start_gps_time)} to {str(end_gps_time)} for trigger {trigger_id}..."
        )

        # Look for any .free files which have the first 10 characters of
        # filename from starttime to endtime
        free_file_list = sorted(glob.glob(f"{self.cfg_subfile_incoming_path}/*.free"))

        #
        # We need to keep at least N free files
        # Otherwise we have no way to deal
        # with buffer stress
        free_files_to_retain = 2

        if len(free_file_list) > free_files_to_retain:
            # Remove the first two from the list
            free_file_list = free_file_list[free_files_to_retain:]
        else:
            # We don't have enough free files to do a dump, so exit
            self.logger.warning("dump_voltages: not enough free files for voltage dump.")
            return True

        for free_filename in free_file_list:
            # Get just the filename, and then see if we have a gps time
            #
            # Filenames are:  1234567890_1234567890_xxx.free
            #
            filename_only = os.path.basename(free_filename)
            # file_obsid = int(filename_only[0:10])
            file_gps_time = int(filename_only[11:21])

            # If the start of the voltage dump is 0, we use it's subobsid
            # as the real start
            if self.dump_start_gps == 0:
                self.dump_start_gps = file_gps_time

            # See if the file_gps_time is between start and end time
            if start_gps_time <= file_gps_time <= end_gps_time:
                # Now we need to check they are no VCS observations.
                # If so, they are already archived so we don't bother
                # archivng them again
                if utils.read_subfile_value(free_filename, utils.PSRDADA_MODE) != utils.CorrelatorMode.MWAX_VCS.value:
                    self.logger.info(
                        f"dump_voltages: keeping {free_filename}, and updating subfile header "
                        f"with 'TRIGGER_ID {trigger_id}'"
                    )

                    # See if there already is a TRIGGER_ID keyword in the subfile- if so
                    # don't overwrite it. We must have overlapping triggers happening
                    if not utils.read_subfile_trigger_value(free_filename):
                        # No TRIGGER_ID yet, so add it
                        utils.inject_subfile_header(free_filename, f"{utils.PSRDADA_TRIGGER_ID} {trigger_id}\n")

                    # For any that exist, rename them immediately to .keep
                    keep_filename = free_filename.replace(".free", ".keep")
                    shutil.move(free_filename, keep_filename)

                    # append to queue so it can be copied off when in NO_CAPTURE mode
                    self.dump_keep_file_queue.put(keep_filename)
                else:
                    self.logger.info(f"dump_voltages: NOT keeping {free_filename} as it is a MWAX_VCS subobservation")

        self.logger.info("dump_voltages: complete")
        return True

    def health_handler(self):
        """Sends health data via UDP multicast"""
        while self.running:
            # Code to run by the health thread
            status_dict = self.get_status()

            # Convert the status to bytes
            status_bytes = json.dumps(status_dict).encode("utf-8")

            # Send the bytes
            try:
                utils.send_multicast(
                    self.cfg_health_multicast_interface_ip,
                    self.cfg_health_multicast_ip,
                    self.cfg_health_multicast_port,
                    status_bytes,
                    self.cfg_health_multicast_hops,
                )
            except Exception as catch_all_exception:  # pylint: disable=broad-except
                self.logger.warning(f"health_handler: Failed to send health information. {catch_all_exception}")

            # Sleep for a second
            time.sleep(1)

    def get_status(self) -> dict:
        """Returns processor status as a dictionary"""
        main_status = {
            "Unix timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
            "beamformer": False,
            "beamformer archiving": False,
            "correlator": True,
            "correlator archiving": self.cfg_corr_archive_destination_enabled,
            "cal sending:": self.cfg_corr_calibrator_destination_enabled,
            "cmdline": " ".join(sys.argv[1:]),
        }

        worker_status_list = []

        for w in self.workers:
            worker_status_list.append(w.get_status())

        status = {"main": main_status, "workers": worker_status_list}

        return status

    def signal_handler(self, _signum, _frame):
        """Handle SIGINT, SIGTERM"""
        self.logger.warning(f"Interrupted. Shutting down {len(self.workers)} workers...")
        self.stop()

    def start(self):
        """Start the processor"""
        self.running = True

        # creating database connection pool(s)
        self.logger.info("Starting database connection pool...")

        if self.cfg_metadatadb_host != "dummy":
            # Dont start it if we are "dummy"- we are probably doing
            # a unit test which does not need a db
            self.db_handler.start_database_pool()

        # create a health thread
        self.logger.info("Starting health_thread...")
        health_thread = threading.Thread(name="health_thread", target=self.health_handler, daemon=True)
        health_thread.start()
        self.logger.info("health_thread started.")

        self.logger.info("MWAX Subfile Distributor started and will run workers.")

        for w in self.workers:
            w.start()

        time.sleep(1)  # give things time to start!

        self.logger.info("Entering main loop...")

        while self.running:
            for w in self.workers:
                if self.running:
                    if not w.is_running():
                        self.logger.error(f"Worker {w.name} has stopped unexpectedly.")
                        self.running = False
                        break

            time.sleep(0.1)

        # Final log message
        self.logger.info("Completed Successfully")

    def stop(self):
        #
        # Finished
        #
        self.running = False

        # Stop any Processors
        for w in self.workers:
            w.stop()

        # do some clean up of the web server
        #
        try:
            shutdown_func = request.environ.get("werkzeug.server.shutdown")
            if shutdown_func is None:
                self.logger.warning("Not running with the Werkzeug Server")
            else:
                shutdown_func()
                self.logger.debug("Flask web server shut down successfully.")
        except Exception:
            # If this fails just ignore for now
            pass


def main():
    """Main function"""
    processor = MWAXSubfileDistributor()

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
