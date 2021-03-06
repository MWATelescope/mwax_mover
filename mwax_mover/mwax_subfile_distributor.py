from mwax_mover import mwax_archive_processor
from mwax_mover import mwax_db
from mwax_mover import mwax_filterbank_processor
from mwax_mover import mwax_subfile_processor
from mwax_mover import utils
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
from urllib.parse import urlparse, parse_qs


class MWAXHTTPServer(HTTPServer):
    def __init__(self, *args, **kw):
        HTTPServer.__init__(self, *args, **kw)
        self.context = None


class MWAXHTTPGetHandler(BaseHTTPRequestHandler):

    def do_GET(self):
        # This is the path (e.g. /status) but with no parameters
        parsed_path = urlparse(self.path.lower()).path

        # Check for ending /'s and remove them all
        while parsed_path[-1] == "/":
            parsed_path = parsed_path[:-1]

        # This is a dictionary of key value pairs of all parameters
        parameter_list = parse_qs(urlparse(self.path.lower()).query)

        try:
            if parsed_path == "/status":
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
                    starttime = int(parameter_list['start'][0])

                    if len(str(starttime)) != 10:
                        raise ValueError("start must be gps seconds and length 10")

                    try:
                        endtime = int(parameter_list['end'][0])

                        if len(str(endtime)) != 10:
                            raise ValueError("end must be gps seconds and length 10")

                        if endtime - starttime <= 0:
                            raise ValueError("end must be after start")

                        # Now call the method to dump the voltages
                        if self.server.context.subfile_processor.dump_voltages(starttime, endtime):
                            self.send_response(200)
                            self.end_headers()
                            self.wfile.write(b"OK")
                        else:
                            self.send_response(400)
                            self.end_headers()
                            self.wfile.write(b"Failed")

                    except Exception as dump_exception_end:
                        self.send_response(400)
                        self.end_headers()
                        self.wfile.write(f"Missing or invalid 'end' parameter {dump_exception_end}".encode("utf-8"))

                except Exception as dump_exception_start:
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write(f"Missing or invalid 'start' parameter {dump_exception_start}".encode("utf-8"))

            else:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(f"Unknown command {parsed_path}".encode('utf-8'))

        except Exception as e:
            self.server.context.logger.error(f"GET: Error {str(e)}")
            self.send_response(400)
            self.end_headers()

    def log_message(self, format: str, *args):
        self.server.context.logger.debug(f"{self.address_string()} {format % args}")
        return


class MWAXSubfileDistributor:
    def __init__(self):
        # init the logging subsystem
        self.logger = logging.getLogger("mwax_mover")

        # Config parser
        self.config = None

        # init vars
        self.hostname = None
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
        self.cfg_subfile_path = None
        self.cfg_voltdata_path = None

        # Beamformer
        self.cfg_bf_enabled = None
        self.cfg_bf_ringbuffer_key = None
        self.cfg_bf_numa_node = None
        self.cfg_bf_fildata_path = None
        self.cfg_bf_archive_command_numa_node = None
        # Archiving settings for beamformer
        self.cfg_bf_archive_destination_host = None
        self.cfg_bf_archive_destination_port = None

        # Correlator
        self.cfg_corr_enabled = None
        self.cfg_corr_ringbuffer_key = None
        self.cfg_corr_diskdb_numa_node = None
        self.cfg_corr_archive_command_numa_node = None
        self.cfg_corr_visdata_path = None
        # Archiving settings for correlator
        self.cfg_corr_archive_destination_host = None
        self.cfg_corr_archive_destination_port = None

        # Connection info for metadata db
        self.cfg_metadatadb_host = None
        self.cfg_metadatadb_db = None
        self.cfg_metadatadb_user = None
        self.cfg_metadatadb_pass = None
        self.cfg_metadatadb_port = None

        # Database handler for metadata db
        self.db_handler = None

    def initialise(self):
        # Get this hosts hostname
        self.hostname = utils.get_hostname()

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = "mwax_subfile_distributor: a command line tool which is part of the mwax " \
                             "suite for the MWA. It will perform different tasks based on the configuration file.\n" \
                             "In addition, it will automatically archive files in /voltdata and /visdata to the " \
                             "mwacache servers at the Curtin Data Centre. In Beamformer mode, filterbank files " \
                             "generated will be copied to a remote host running Fredda.\n"

        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        if not os.path.exists(config_filename):
            self.logger.error(f"Configuration file location {config_filename} does not exist. Quitting.")
            exit(1)

        # Parse config file
        self.config = ConfigParser()
        self.config.read_file(open(config_filename, 'r'))

        # read from config file
        self.cfg_log_path = self.config.get("mwax mover", "log_path")

        if not os.path.exists(self.cfg_log_path):
            self.logger.error(f"log_path {self.cfg_log_path} does not exist. Quiting.")
            exit(1)

        # It's now safe to start logging
        # start logging
        self.logger.setLevel(logging.DEBUG)
        console_log = logging.StreamHandler()
        console_log.setLevel(logging.DEBUG)
        console_log.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
        self.logger.addHandler(console_log)

        file_log = logging.FileHandler(filename=os.path.join(self.cfg_log_path, "main.log"))
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
        self.logger.addHandler(file_log)

        self.logger.info("Starting mwax_subfile_distributor processor...")
        self.cfg_webserver_port = utils.read_config(self.logger, self.config, "mwax mover", "webserver_port")
        self.cfg_subfile_path = utils.read_config(self.logger, self.config,"mwax mover", "subfile_path")
        self.cfg_voltdata_path = utils.read_config(self.logger, self.config,"mwax mover", "voltdata_path")

        if not os.path.exists(self.cfg_subfile_path):
            self.logger.error(f"Subfile file location {self.cfg_subfile_path} does not exist. Quitting.")
            exit(1)

        if not os.path.exists(self.cfg_voltdata_path):
            self.logger.error(f"Voltdata file location {self.cfg_voltdata_path} does not exist. Quitting.")
            exit(1)

        # Check to see if we have a beamformer section
        if self.config.has_section("beamformer"):
            self.cfg_bf_ringbuffer_key = utils.read_config(self.logger, self.config,"beamformer", "input_ringbuffer_key")
            self.cfg_bf_numa_node = utils.read_config(self.logger, self.config,"beamformer", "dada_disk_db_numa_node")
            self.cfg_bf_fildata_path = utils.read_config(self.logger, self.config,"beamformer", "fildata_path")
            self.cfg_bf_archive_command_numa_node = utils.read_config(self.logger, self.config, "beamformer",
                                                                        "archive_command_numa_node")

            if not os.path.exists(self.cfg_bf_fildata_path):
                self.logger.error(f"Fildata file location {self.cfg_bf_fildata_path} does not exist. Quitting.")
                exit(1)

            # Read filterbank config specific to this host
            self.cfg_bf_archive_destination_host = utils.read_config(self.logger, self.config, self.hostname,
                                                                       "destination_host")
            self.cfg_bf_archive_destination_port = utils.read_config(self.logger, self.config, self.hostname,
                                                                       "destination_port")

            self.cfg_bf_enabled = True
        else:
            self.cfg_bf_enabled = False

        # read metadata database config
        if self.config.has_section("correlator"):
            self.cfg_corr_ringbuffer_key = utils.read_config(self.logger, self.config,"correlator", "input_ringbuffer_key")
            self.cfg_corr_diskdb_numa_node = utils.read_config(self.logger, self.config,"correlator", "dada_disk_db_numa_node")
            self.cfg_corr_archive_command_numa_node = utils.read_config(self.logger, self.config,"correlator", "archive_command_numa_node")
            self.cfg_corr_visdata_path = utils.read_config(self.logger, self.config,"correlator", "visdata_path")

            if not os.path.exists(self.cfg_corr_visdata_path):
                self.logger.error(f"Fildata file location {self.cfg_corr_visdata_path} does not exist. Quitting.")
                exit(1)

            self.cfg_metadatadb_host = utils.read_config(self.logger, self.config,"mwa metadata database", "host")

            if self.cfg_metadatadb_host != mwax_db.DUMMY_DB:
                self.cfg_metadatadb_db = utils.read_config(self.logger, self.config,"mwa metadata database", "db")
                self.cfg_metadatadb_user = utils.read_config(self.logger, self.config,"mwa metadata database", "user")
                self.cfg_metadatadb_pass = utils.read_config(self.logger, self.config,"mwa metadata database", "pass", True)
                self.cfg_metadatadb_port = utils.read_config(self.logger, self.config,"mwa metadata database", "port")

            # Read config specific to this host
            self.cfg_corr_archive_destination_host = utils.read_config(self.logger, self.config, self.hostname, "destination_host")
            self.cfg_corr_archive_destination_port = utils.read_config(self.logger, self.config, self.hostname, "destination_port")

            # Initiate database connection pool for metadata db
            self.db_handler = mwax_db.MWAXDBHandler(host=self.cfg_metadatadb_host,
                                                    port=self.cfg_metadatadb_port,
                                                    db=self.cfg_metadatadb_db,
                                                    user=self.cfg_metadatadb_user,
                                                    password=self.cfg_metadatadb_pass)

            self.cfg_corr_enabled = True
        else:
            self.cfg_corr_enabled = False

        # Create and start web server
        self.logger.info(f"Starting http server on port {self.cfg_webserver_port}...")
        self.web_server = MWAXHTTPServer(('', int(self.cfg_webserver_port)), MWAXHTTPGetHandler)
        self.web_server.context = self
        self.web_server_thread = threading.Thread(name='webserver',
                                                  target=self.web_server_loop,
                                                  args=(self.web_server,))
        self.web_server_thread.setDaemon(True)
        self.web_server_thread.start()

        # Start the processors
        self.subfile_processor = mwax_subfile_processor.SubfileProcessor(self,
                                                                         self.cfg_subfile_path,
                                                                         self.cfg_voltdata_path,
                                                                         self.cfg_bf_enabled,
                                                                         self.cfg_bf_ringbuffer_key,
                                                                         self.cfg_bf_numa_node,
                                                                         self.cfg_corr_enabled,
                                                                         self.cfg_corr_ringbuffer_key,
                                                                         self.cfg_corr_diskdb_numa_node)

        # Add this processor to list of processors we manage
        self.processors.append(self.subfile_processor)

        if self.cfg_corr_enabled:
            self.archive_processor = mwax_archive_processor.ArchiveProcessor(self,
                                                                             self.hostname,
                                                                             self.cfg_corr_archive_command_numa_node,
                                                                             self.cfg_corr_archive_destination_host,
                                                                             self.cfg_corr_archive_destination_port,
                                                                             self.db_handler,
                                                                             self.cfg_voltdata_path,
                                                                             self.cfg_corr_visdata_path)

            # Add this processor to list of processors we manage
            self.processors.append(self.archive_processor)

        if self.cfg_bf_enabled:
            self.filterbank_processor = mwax_filterbank_processor.FilterbankProcessor(self,
                                                                                      self.hostname,
                                                                                      self.cfg_bf_fildata_path,
                                                                                      self.cfg_bf_archive_destination_host,
                                                                                      self.cfg_bf_archive_destination_port,
                                                                                      self.cfg_bf_archive_command_numa_node)

            # Add this processor to list of processors we manage
            self.processors.append(self.filterbank_processor)

    def get_status(self) -> dict:
        main_status = {"host": self.hostname,
                       "beamformer": self.cfg_bf_enabled,
                       "correlator": self.cfg_corr_enabled,
                       "running": self.running}

        processor_status_list = []

        for processor in self.processors:
            processor_status_list.append(processor.get_status())

        status = {"main": main_status,
                  "processors": processor_status_list}

        return status

    def web_server_loop(self, webserver):
        webserver.serve_forever()

    def signal_handler(self, signum, frame):
        self.logger.warning(f"Interrupted. Shutting down {len(self.processors)} processors...")
        self.running = False

        # Stop any Processors
        for processor in self.processors:
            processor.stop()

    def start(self):
        self.running = True

        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        if self.cfg_bf_enabled:
            self.logger.info(f"Beamformer Enabled")
        else:
            self.logger.info(f"Beamformer disabled")

        if self.cfg_corr_enabled:
            self.logger.info(f"Correlator Enabled")
        else:
            self.logger.info(f"Correlator disabled")

        for processor in self.processors:
            processor.start()

        while self.running:
            for processor in self.processors:
                for t in processor.worker_threads:
                    if t:
                        if t.isAlive():
                            time.sleep(1)
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
    p = MWAXSubfileDistributor()

    try:
        p.initialise()
        p.start()
        sys.exit(0)
    except Exception as e:
        if p.logger:
            p.logger.exception(str(e))
        else:
            print(str(e))


if __name__ == '__main__':
    main()
