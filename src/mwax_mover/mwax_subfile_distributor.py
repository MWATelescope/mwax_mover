"""Module for mwax_subfile_distributor"""

import argparse
from configparser import ConfigParser
from flask import Flask, request
import json
import logging
import os
import signal
import sys
import time
import threading
from typing import Optional
from mwax_mover import mwax_archive_processor
from mwax_mover import mwax_db
from mwax_mover import mwax_subfile_processor
from mwax_mover import utils
from mwax_mover import version

flask_app: Flask = Flask(__name__)


class MWAXSubfileDistributor:
    """Class for MWAXSubfileDistributor- the main engine of MWAX"""

    def __init__(self):
        # init the logging subsystem
        self.logger = logging.getLogger("mwax_mover")
        self.cfg_log_level: str = ""

        # Config parser
        self.config: ConfigParser

        # init vars
        self.hostname: str = utils.get_hostname()
        self.running: bool = False
        self.processors: list = []
        self.archive_processor = None
        self.filterbank_processor = None
        self.subfile_processor = None

        # Web server
        self.flask_thread: threading.Thread

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

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        self.initialise(config_filename)

    def initialise(self, config_filename):
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
        self.cfg_bf_pipe_path = utils.read_config(
            self.logger,
            self.config,
            "beamformer",
            "bf_pipe_path",
        )

        if not os.path.exists(self.cfg_bf_pipe_path):
            self.logger.error(f"bf_pipe_path location {self.cfg_bf_pipe_path} does not exist. Quitting.")
            sys.exit(1)

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

        # Start the processors
        self.subfile_processor = mwax_subfile_processor.SubfileProcessor(
            self,
            self.cfg_subfile_incoming_path,
            self.cfg_voltdata_incoming_path,
            self.cfg_always_keep_subfiles,
            self.cfg_corr_input_ringbuffer_key,
            self.cfg_corr_diskdb_numa_node,
            self.cfg_psrdada_timeout_sec,
            self.cfg_copy_subfile_to_disk_timeout_sec,
            self.cfg_corr_mwax_stats_binary_dir,
            self.cfg_packet_stats_dump_dir,
            self.cfg_packet_stats_destination_dir,
            self.hostname,
            self.cfg_bf_pipe_path,
            self.cfg_bf_aocal_path,
        )

        # Add this processor to list of processors we manage
        self.processors.append(self.subfile_processor)

        if self.cfg_corr_archive_destination_enabled is False:
            self.logger.warning(
                "'mwax_destination_enabled' is FALSE. Nothing will be"
                " archived and nothing will be sent for calibration."
            )

        self.archive_processor = mwax_archive_processor.MWAXArchiveProcessor(
            self,
            self.hostname,
            self.cfg_corr_archive_destination_enabled,
            self.cfg_corr_archive_command_numa_node,
            self.cfg_corr_archive_destination_host,
            self.cfg_corr_archive_destination_port,
            self.cfg_archive_command_timeout_sec,
            self.cfg_corr_mwax_stats_binary_dir,
            self.cfg_corr_mwax_stats_dump_dir,
            self.cfg_corr_mwax_stats_timeout_sec,
            self.db_handler,
            self.cfg_voltdata_incoming_path,
            self.cfg_voltdata_outgoing_path,
            self.cfg_corr_visdata_incoming_path,
            self.cfg_corr_visdata_processing_stats_path,
            self.cfg_corr_visdata_outgoing_path,
            self.cfg_corr_calibrator_outgoing_path,
            self.cfg_corr_calibrator_destination_enabled,
            self.cfg_corr_metafits_path,
            self.cfg_corr_visdata_dont_archive_path,
            self.cfg_voltdata_dont_archive_path,
            self.cfg_corr_high_priority_correlator_projectids,
            self.cfg_corr_high_priority_vcs_projectids,
            self.cfg_bf_incoming_path,
            self.cfg_bf_outgoing_path,
            self.cfg_bf_dont_archive_path,
        )

        # Add this processor to list of processors we manage
        self.processors.append(self.archive_processor)

        # Make sure we can Ctrl-C / kill out of this
        self.logger.info("Initialising signal handlers")
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info("Ready to start...")

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
        if self.archive_processor:
            self.archive_processor.pause_archiving(paused=True)
        return b"OK", 200

    def endpoint_resume_archiving(self):
        if self.archive_processor:
            self.archive_processor.pause_archiving(paused=False)
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
                    if self.archive_processor:
                        self.archive_processor.release_cal_obs(int(obs_id))
                        return b"OK", 200
                    else:
                        raise ValueError("archive_processor not initialised")
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
                if self.subfile_processor:
                    if self.subfile_processor.dump_start_gps is None and self.subfile_processor.dump_end_gps is None:
                        # Now call the method to dump the voltages
                        if self.subfile_processor.dump_voltages(starttime, endtime, trigger_id):
                            return b"OK", 200
                        else:
                            return b"Failed to start Voltage Buffer Dump", 400
                    else:
                        # Reject this request
                        return b"Voltage Buffer Dump already in progress. Request canceled.", 400
                else:
                    return b"Subfile processor not initialised. Cannot dump voltages.", 400

        except ValueError as parameters_exception:  # pylint: disable=broad-except
            return f"Value Error: {parameters_exception}".encode("utf-8"), 400

        except Exception as dump_voltages_exception:  # pylint: disable=broad-except
            return f"ERROR: {dump_voltages_exception}".encode("utf-8"), 500

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

        processor_status_list = []

        for processor in self.processors:
            processor_status_list.append(processor.get_status())

        status = {"main": main_status, "processors": processor_status_list}

        return status

    def signal_handler(self, _signum, _frame):
        """Handle SIGINT, SIGTERM"""
        self.logger.warning(f"Interrupted. Shutting down {len(self.processors)} processors...")
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

        self.logger.info("MWAX Subfile Distributor started successfully.")

        for processor in self.processors:
            processor.start()

        time.sleep(1)  # give things time to start!

        # create a health thread
        self.logger.info("Starting health_thread...")
        health_thread = threading.Thread(name="health_thread", target=self.health_handler, daemon=True)
        health_thread.start()

        while self.running:
            for processor in self.processors:
                for worker_threads in processor.worker_threads:
                    if worker_threads:
                        if not worker_threads.is_alive():
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
        for processor in self.processors:
            processor.stop()

        # do some clean up of the web server
        #
        shutdown_func = request.environ.get("werkzeug.server.shutdown")
        if shutdown_func is None:
            self.logger.warning("Not running with the Werkzeug Server")
        else:
            shutdown_func()
            self.logger.debug("Flask web server shut down successfully.")


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
