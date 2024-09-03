"""Module for mwax_subfile_distributor"""

import argparse
from configparser import ConfigParser
from http.server import BaseHTTPRequestHandler, HTTPServer
import json
import logging
import logging.handlers
import os
import signal
import sys
import time
import threading
import typing
from urllib.parse import urlparse, parse_qs
from mwax_mover import mwax_archive_processor
from mwax_mover import mwax_db
from mwax_mover import mwax_filterbank_processor
from mwax_mover import mwax_subfile_processor
from mwax_mover import utils
from mwax_mover import version


class MWAXHTTPServer(HTTPServer):
    """Class representing a web server for web service control"""

    def __init__(self, *args, **kw):
        HTTPServer.__init__(self, *args, **kw)
        self.context: typing.Optional[MWAXSubfileDistributor] = None


class MWAXHTTPGetHandler(BaseHTTPRequestHandler):
    """Class for handling GET requests"""

    def do_GET(self):  # pylint: disable=invalid-name
        """Process a web service GET"""
        # This is the path (e.g. /status) but with no parameters
        parsed_path = urlparse(self.path.lower()).path

        # Check for ending /'s and remove them all
        while parsed_path[-1] == "/":
            parsed_path = parsed_path[:-1]

        # This is a dictionary of key value pairs of all parameters
        parameter_list = parse_qs(urlparse(self.path.lower()).query)

        try:
            if parsed_path == "/shutdown":
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")
                self.server.context.signal_handler(signal.SIGINT, None)

            elif parsed_path == "/status":
                data = json.dumps(self.server.context.get_status())

                self.send_response(200)
                self.end_headers()
                self.wfile.write(data.encode())

            elif parsed_path == "/pause_archiving":
                self.server.context.archive_processor.pause_archiving(paused=True)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")

            elif parsed_path == "/resume_archiving":
                self.server.context.archive_processor.pause_archiving(paused=False)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(b"OK")

            elif parsed_path == "/dump_voltages":
                # Check for correct params
                try:
                    starttime = int(parameter_list["start"][0])
                    endtime = int(parameter_list["end"][0])
                    trigger_id = int(parameter_list["trigger_id"][0])

                    # Special test mode- if start and end == 0 just return 200
                    if starttime == endtime == 0:
                        self.send_response(200)
                        self.end_headers()
                        self.wfile.write(b"OK")
                    else:
                        if len(str(starttime)) != 10 and starttime != 0:
                            raise ValueError(
                                "start must be gps seconds and length 10 (or 0" " for as early as possible)"
                            )

                        if len(str(endtime)) != 10:
                            raise ValueError("end must be gps seconds and length 10")

                        if endtime - starttime <= 0:
                            raise ValueError("end must be after start")

                        # Check to see if we aren't already doing a dump
                        if (
                            self.server.context.subfile_processor.dump_start_gps is None
                            and self.server.context.subfile_processor.dump_end_gps is None
                        ):
                            # Now call the method to dump the voltages
                            if self.server.context.subfile_processor.dump_voltages(starttime, endtime, trigger_id):
                                self.send_response(200)
                                self.end_headers()
                                self.wfile.write(b"OK")
                            else:
                                self.send_response(400)
                                self.end_headers()
                                self.wfile.write(b"Failed")
                        else:
                            # Reject this request
                            self.send_response(400)
                            self.end_headers()
                            self.wfile.write(b"Voltage Buffer Dump already in progress." b" Request canceled.")

                except ValueError as parameters_exception:  # pylint: disable=broad-except
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(f"Value Error: {parameters_exception}".encode("utf-8"))

                except Exception as dump_voltages_exception:  # pylint: disable=broad-except
                    self.send_response(400)
                    self.end_headers()
                    self.wfile.write(
                        "Unhandled exception running dump_voltages" f" {dump_voltages_exception}".encode("utf-8")
                    )

            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(f"Unknown command {parsed_path}".encode("utf-8"))

        except Exception as catch_all_exception:  # pylint: disable=broad-except
            self.server.context.logger.error(f"GET: Error {str(catch_all_exception)}")
            self.send_response(400)
            self.end_headers()

    def log_message(self, format: str, *args):
        """Logs a message"""
        self.server.context.logger.debug(f"{self.address_string()} {format % args}")
        return


class MWAXSubfileDistributor:
    """Class for MWAXSubfileDistributor- the main engine of MWAX"""

    def __init__(self):
        # init the logging subsystem
        self.logger = logging.getLogger("mwax_mover")

        # Config parser
        self.config = None

        # init vars
        self.hostname = utils.get_hostname()
        self.running = False
        self.processors = []
        self.archive_processor = None
        self.filterbank_processor = None
        self.subfile_processor = None

        # Web server
        self.web_server = None
        self.web_server_thread = None

        #
        # Config file vars
        #

        # Common
        self.cfg_log_path = None
        self.cfg_webserver_port = None
        self.cfg_voltdata_dont_archive_path = None
        self.cfg_subfile_incoming_path = None
        self.cfg_voltdata_incoming_path = None
        self.cfg_voltdata_outgoing_path = None
        self.cfg_always_keep_subfiles = False
        self.cfg_archive_command_timeout_sec = None
        self.cfg_psrdada_timeout_sec = None
        self.cfg_copy_subfile_to_disk_timeout_sec = None
        self.cfg_archiving_enabled: bool = False
        self.cfg_health_multicast_interface_ip = None
        self.cfg_health_multicast_interface_name = None
        self.cfg_health_multicast_ip = None
        self.cfg_health_multicast_port = None
        self.cfg_health_multicast_hops = None

        # Beamformer
        self.cfg_bf_enabled = False
        self.cfg_bf_ringbuffer_key = None
        self.cfg_bf_numa_node = None
        self.cfg_bf_fildata_path = None
        self.cfg_bf_settings_path = None
        self.cfg_bf_archive_command_numa_node = None
        # Archiving settings for beamformer
        self.cfg_bf_archive_destination_host = None
        self.cfg_bf_archive_destination_port = None
        self.cfg_bf_archive_destination_enabled = False

        # Correlator
        self.cfg_corr_enabled = False
        self.cfg_corr_input_ringbuffer_key = None
        self.cfg_corr_diskdb_numa_node = None
        self.cfg_corr_archive_command_numa_node = None
        self.cfg_corr_visdata_dont_archive_path = None
        self.cfg_corr_visdata_incoming_path = None
        self.cfg_corr_visdata_outgoing_path = None
        self.cfg_corr_high_priority_correlator_projectids = None
        self.cfg_corr_high_priority_vcs_projectids = None
        # Archiving settings for correlator
        self.cfg_corr_archive_destination_host = None
        self.cfg_corr_archive_destination_port = None
        self.cfg_corr_archive_destination_enabled = False
        # processing_stats config
        self.cfg_corr_mwax_stats_timeout_sec = None
        self.cfg_corr_mwax_stats_dump_dir = None
        self.cfg_corr_mwax_stats_executable = None
        self.cfg_corr_visdata_processing_stats_path = None
        # calibration config
        self.cfg_corr_calibrator_outgoing_path = None
        self.cfg_corr_calibrator_destination_host = None
        self.cfg_corr_calibrator_destination_port = None
        self.cfg_corr_calibrator_destination_enabled = False
        self.cfg_corr_metafits_path = None

        # Connection info for metadata db
        self.cfg_metadatadb_host = None
        self.cfg_metadatadb_db = None
        self.cfg_metadatadb_user = None
        self.cfg_metadatadb_pass = None
        self.cfg_metadatadb_port = None

        # Database handler for metadata db
        self.db_handler = None

    def initialise_from_command_line(self):
        """Called if run from command line"""

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_subfile_distributor: a command line tool which is part of"
            " the mwax suite for the MWA. It will perform different tasks"
            " based on the configuration file.\nIn addition, it will"
            " automatically archive files in /voltdata and /visdata to the"
            " mwacache servers at the Curtin Data Centre. In Beamformer mode,"
            " filterbank files generated will be copied to a remote host"
            " running Fredda.  (mwax_mover"
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
            self.logger.error(f"Configuration file location {config_filename} does not" " exist. Quitting.")
            sys.exit(1)

        # Parse config file
        self.config = ConfigParser()
        self.config.read_file(open(config_filename, "r", encoding="utf-8"))

        # read from config file
        self.cfg_log_path = self.config.get("mwax mover", "log_path")

        if not os.path.exists(self.cfg_log_path):
            self.logger.error(f"log_path {self.cfg_log_path} does not exist. Quiting.")
            sys.exit(1)

        # It's now safe to start logging
        # start logging
        self.logger.setLevel(logging.DEBUG)
        console_log = logging.StreamHandler()
        console_log.setLevel(logging.DEBUG)
        console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        self.logger.addHandler(console_log)

        # Removing file logging for now
        # file_log = logging.FileHandler(filename=os.path.join(self.cfg_log_path, "subfile_distributor_main.log"))
        # file_log.setLevel(logging.DEBUG)
        # file_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        # self.logger.addHandler(file_log)

        self.logger.info("Starting mwax_subfile_distributor" f" processor...v{version.get_mwax_mover_version_string()}")
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
                "'Voltdata Dont Archive' location"
                f" {self.cfg_voltdata_dont_archive_path} does not exist."
                " Quitting."
            )
            sys.exit(1)

        if not os.path.exists(self.cfg_subfile_incoming_path):
            self.logger.error(f"Subfile file location {self.cfg_subfile_incoming_path} does" " not exist. Quitting.")
            sys.exit(1)

        if not os.path.exists(self.cfg_voltdata_incoming_path):
            self.logger.error("Voltdata file location" f" {self.cfg_voltdata_incoming_path} does not exist. Quitting.")
            sys.exit(1)

        if not os.path.exists(self.cfg_voltdata_outgoing_path):
            self.logger.error("Voltdata file location" f" {self.cfg_voltdata_outgoing_path} does not exist. Quitting.")
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

        # Check to see if we have a beamformer section
        if self.config.has_section("beamformer"):
            self.cfg_bf_ringbuffer_key = utils.read_config(
                self.logger, self.config, "beamformer", "input_ringbuffer_key"
            )
            self.cfg_bf_fildata_path = utils.read_config(self.logger, self.config, "beamformer", "fildata_path")

            if not os.path.exists(self.cfg_bf_fildata_path):
                self.logger.error(f"Fildata file location {self.cfg_bf_fildata_path} does" " not exist. Quitting.")
                sys.exit(1)

            self.cfg_bf_settings_path = utils.read_config(
                self.logger,
                self.config,
                "beamformer",
                "beamformer_settings_path",
            )
            if not os.path.exists(self.cfg_bf_settings_path):
                self.logger.error(
                    "Beamformer settings file location" f" {self.cfg_bf_settings_path} does not exist. Quitting."
                )
                sys.exit(1)

            self.logger.info("Beam settings will be read from:" f" {self.cfg_bf_settings_path} at runtime.")

            # Read filterbank config specific to this host
            self.cfg_bf_archive_destination_host = utils.read_config(
                self.logger, self.config, self.hostname, "fil_destination_host"
            )
            self.cfg_bf_archive_destination_port = int(
                utils.read_config(self.logger, self.config, self.hostname, "fil_destination_port")
            )
            self.cfg_bf_archive_destination_enabled = (
                int(
                    utils.read_config(
                        self.logger,
                        self.config,
                        self.hostname,
                        "fil_destination_enabled",
                    )
                )
                == 1
            )
            self.cfg_bf_numa_node = int(
                utils.read_config(
                    self.logger,
                    self.config,
                    self.hostname,
                    "dada_disk_db_numa_node",
                )
            )
            self.cfg_bf_archive_command_numa_node = int(
                utils.read_config(
                    self.logger,
                    self.config,
                    self.hostname,
                    "archive_command_numa_node",
                )
            )

            self.cfg_bf_enabled = True
        else:
            self.cfg_bf_enabled = False

        # read correlator config
        if self.config.has_section("correlator"):
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
            self.cfg_corr_mwax_stats_executable = utils.read_config(
                self.logger, self.config, "correlator", "mwax_stats_executable"
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
            self.cfg_corr_calibrator_destination_host = utils.read_config(
                self.logger,
                self.config,
                "correlator",
                "calibrator_destination_host",
            )
            self.cfg_corr_calibrator_destination_port = int(
                utils.read_config(
                    self.logger,
                    self.config,
                    "correlator",
                    "calibrator_destination_port",
                )
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
                self.logger.error(
                    "Visdata file location" f" {self.cfg_corr_visdata_incoming_path} does not exist." " Quitting."
                )
                sys.exit(1)

            if not os.path.exists(self.cfg_corr_visdata_dont_archive_path):
                self.logger.error(
                    "'Visdata Dont Archive' location"
                    f" {self.cfg_corr_visdata_dont_archive_path} does not"
                    " exist. Quitting."
                )
                sys.exit(1)

            if not os.path.exists(self.cfg_corr_visdata_processing_stats_path):
                self.logger.error(
                    "Visdata file location"
                    f" {self.cfg_corr_visdata_processing_stats_path} does not"
                    " exist. Quitting."
                )
                sys.exit(1)

            if not os.path.exists(self.cfg_corr_visdata_outgoing_path):
                self.logger.error(
                    "Visdata file location" f" {self.cfg_corr_visdata_outgoing_path} does not exist." " Quitting."
                )
                sys.exit(1)

            if not os.path.exists(self.cfg_corr_mwax_stats_executable):
                self.logger.error(
                    "mwax_stats executable" f" {self.cfg_corr_mwax_stats_executable} does not exist." " Quitting."
                )
                sys.exit(1)

            if not os.path.exists(self.cfg_corr_calibrator_outgoing_path):
                self.logger.error(
                    "calibrator outgoing location"
                    f" {self.cfg_corr_calibrator_outgoing_path} does not"
                    " exist. Quitting."
                )
                sys.exit(1)

            if not os.path.exists(self.cfg_corr_metafits_path):
                self.logger.error(f"metafits location {self.cfg_corr_metafits_path} does not" " exist. Quitting.")
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

            # Initiate database connection pool for metadata db
            self.db_handler = mwax_db.MWAXDBHandler(
                logger=self.logger,
                host=self.cfg_metadatadb_host,
                port=self.cfg_metadatadb_port,
                db_name=self.cfg_metadatadb_db,
                user=self.cfg_metadatadb_user,
                password=self.cfg_metadatadb_pass,
            )

            self.cfg_corr_enabled = True
        else:
            self.cfg_corr_enabled = False

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
            self.cfg_bf_archive_destination_enabled = False
            self.cfg_corr_archive_destination_enabled = False
            self.cfg_corr_calibrator_destination_enabled = False

        # Create and start web server
        self.logger.info(f"Starting http server on port {self.cfg_webserver_port}...")
        self.web_server = MWAXHTTPServer(("", int(self.cfg_webserver_port)), MWAXHTTPGetHandler)
        self.web_server.context = self
        self.web_server_thread = threading.Thread(
            name="webserver", target=self.web_server_loop, args=(self.web_server,), daemon=True
        )
        self.web_server_thread.start()

        # Start the processors
        self.subfile_processor = mwax_subfile_processor.SubfileProcessor(
            self,
            self.cfg_subfile_incoming_path,
            self.cfg_voltdata_incoming_path,
            self.cfg_always_keep_subfiles,
            self.cfg_bf_enabled,
            self.cfg_bf_ringbuffer_key,
            self.cfg_bf_numa_node,
            self.cfg_bf_fildata_path,
            self.cfg_bf_settings_path,
            self.cfg_corr_enabled,
            self.cfg_corr_input_ringbuffer_key,
            self.cfg_corr_diskdb_numa_node,
            self.cfg_psrdada_timeout_sec,
            self.cfg_copy_subfile_to_disk_timeout_sec,
        )

        # Add this processor to list of processors we manage
        self.processors.append(self.subfile_processor)

        if self.cfg_corr_enabled:
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
                self.cfg_corr_mwax_stats_executable,
                self.cfg_corr_mwax_stats_dump_dir,
                self.cfg_corr_mwax_stats_timeout_sec,
                self.db_handler,
                self.cfg_voltdata_incoming_path,
                self.cfg_voltdata_outgoing_path,
                self.cfg_corr_visdata_incoming_path,
                self.cfg_corr_visdata_processing_stats_path,
                self.cfg_corr_visdata_outgoing_path,
                self.cfg_corr_calibrator_outgoing_path,
                self.cfg_corr_calibrator_destination_host,
                self.cfg_corr_calibrator_destination_port,
                self.cfg_corr_calibrator_destination_enabled,
                self.cfg_corr_metafits_path,
                self.cfg_corr_visdata_dont_archive_path,
                self.cfg_voltdata_dont_archive_path,
                self.cfg_corr_high_priority_correlator_projectids,
                self.cfg_corr_high_priority_vcs_projectids,
            )

            # Add this processor to list of processors we manage
            self.processors.append(self.archive_processor)

        if self.cfg_bf_enabled:
            if self.cfg_bf_archive_destination_enabled:
                self.filterbank_processor = mwax_filterbank_processor.FilterbankProcessor(
                    self,
                    self.hostname,
                    self.cfg_bf_fildata_path,
                    self.cfg_bf_archive_destination_host,
                    self.cfg_bf_archive_destination_port,
                    self.cfg_bf_archive_command_numa_node,
                )

                # Add this processor to list of processors we manage
                self.processors.append(self.filterbank_processor)
            else:
                self.logger.info(
                    "Filterbank Archiving is disabled due to configuration" " setting `fil_destination_enabled`."
                )

        # Make sure we can Ctrl-C / kill out of this
        self.logger.info("Initialising signal handlers")
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        self.logger.info("Ready to start...")

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
                self.logger.warning("health_handler: Failed to send health information." f" {catch_all_exception}")

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
            "beamformer": self.cfg_bf_enabled,
            "beamformer archiving": self.cfg_bf_archive_destination_enabled,
            "correlator": self.cfg_corr_enabled,
            "correlator archiving": self.cfg_corr_archive_destination_enabled,
            "cal sending:": self.cfg_corr_calibrator_destination_enabled,
            "cmdline": " ".join(sys.argv[1:]),
        }

        processor_status_list = []

        for processor in self.processors:
            processor_status_list.append(processor.get_status())

        status = {"main": main_status, "processors": processor_status_list}

        return status

    def web_server_loop(self, webserver):
        """Method to start the webserver serving"""
        webserver.serve_forever()

    def signal_handler(self, _signum, _frame):
        """Handle SIGINT, SIGTERM"""
        self.logger.warning(f"Interrupted. Shutting down {len(self.processors)} processors...")
        self.running = False

        # Stop any Processors
        for processor in self.processors:
            processor.stop()

    def start(self):
        """Start the processor"""
        self.running = True

        if self.cfg_bf_enabled:
            self.logger.info("Beamformer Enabled")
        else:
            self.logger.info("Beamformer disabled")

        if self.cfg_corr_enabled:
            self.logger.info("Correlator Enabled")
        else:
            self.logger.info("Correlator disabled")

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
                        if worker_threads.is_alive():
                            time.sleep(0.2)
                        else:
                            self.running = False
                            break

        #
        # Finished- do some clean up
        #

        # End the web server
        self.logger.info("Stopping webserver...")
        self.web_server.socket.close()
        self.web_server.server_close()
        self.web_server_thread.join(timeout=4)

        # Final log message
        self.logger.info("Completed Successfully")


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
