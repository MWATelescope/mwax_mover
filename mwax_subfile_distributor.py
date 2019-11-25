import mwax_mover
import argparse
import crc32c
import logging
import logging.handlers
import os
import queue
import signal
import socket
import subprocess
import sys
import threading
import time
from urllib.parse import urlencode
from urllib.request import urlopen
import urllib.error

MODE_MWAX_CORRELATOR = "MWAX_CORRELATOR"
MODE_MWAX_BEAMFORMER = "MWAX_BEAMFORMER"

SUBFILE_MODE_IDLE = "IDLE"
SUBFILE_MODE_CCAPTURE = "CCAPTURE"
SUBFILE_MODE_VCAPTURE = "VCAPTURE"

COMMAND_DADA_DISKDB = "dada_diskdb"
PSRDADA_MAX_HEADER_LINES = 16           # number of lines of the PSRDADA header to read looking for keywords

VOLTDATA_PATH = "/voltdata"
VISDATA_PATH = "/visdata"


def calculate_checksum(filename):
    crc32 = 0
    block_size = 1048576

    with open(filename, "rb") as binaryfile:
        while True:
            block = binaryfile.read(block_size)
            if not block:
                break
            crc32 = crc32c.crc32(block, crc32)

    return crc32


def get_hostname():
    hostname = socket.gethostname()

    # ensure we remove anything after a . in case we got the fqdn
    split_hostname = hostname.split(".")[0]

    return split_hostname


def run_command(logger, filename, command, command_timeout):
    tries = 1
    max_retries = 3

    while tries <= max_retries:
        try:
            # launch the process
            if tries == 1:
                logger.info(f"{filename}- Launching {command}")
            else:
                logger.info(f"{filename}- Retrying {command} {tries} of {max_retries}")

            subprocess.run(f"{command}",
                           shell=True, check=True, timeout=command_timeout)
            return True

        except subprocess.CalledProcessError as process_error:
            logger.error(f"{filename}- Error launching {command}. "
                         f"Return code {process_error.returncode},"
                         f"Output {process_error.output} {process_error.stderr}")

        except Exception as unknown_exception:
            logger.error(f"{filename}- Unknown error launching {command}. {unknown_exception}")

        tries = tries + 1

    logger.error(f"{filename}- Max retries exceeded trying to launch {command}. Failing.")
    return False


class SubfileProcessor:
    def __init__(self, logger, mode, ringbuffer_key, numa_node):
        self.logger = logger

        self.mode = mode
        self.ext_to_watch_for = ".sub"
        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_RENAME
        self.watch_dir = "/dev/shm"
        self.ext_done = ".free"

        self.watcher_threads = []
        self.worker_threads = []

        self.queue = queue.Queue()
        self.watcher = None
        self.queue_worker = None

        self.ringbuffer_key = ringbuffer_key    # PSRDADA ringbuffer key for INPUT into correlator or beamformer
        self.numa_node = numa_node              # Numa node to use to do a file copy

    def start(self):
        # Create watcher
        self.watcher = mwax_mover.Watcher(path=self.watch_dir, q=self.queue,
                                          pattern=f"{self.ext_to_watch_for}", log=self.logger,
                                          mode=self.mwax_mover_mode)

        # Create queueworker
        self.queue_worker = mwax_mover.QueueWorker(label="Subfile Input Queue",
                                                   q=self.queue,
                                                   executable_path=None,
                                                   event_handler=self.handler,
                                                   mode=self.mwax_mover_mode,
                                                   log=self.logger)

        # Setup thread for watching filesystem
        watcher_thread = threading.Thread(name="watch_sub", target=self.watcher.start, daemon=True)
        self.watcher_threads.append(watcher_thread)
        watcher_thread.start()

        # Setup thread for processing items
        queue_worker_thread = threading.Thread(name="work_sub", target=self.queue_worker.start, daemon=True)
        self.worker_threads.append(queue_worker_thread)
        queue_worker_thread.start()

    def handler(self, item):
        self.logger.info(f"{item}- SubfileProcessor.handler is handling {item}...")

        try:
            subfile_mode = self._read_subfile_mode(item)

            if self.mode == MODE_MWAX_CORRELATOR:
                # 1. Read header of subfile.
                # 2. If mode==CCAPTURE then
                #       load into PSRDADA ringbuffer for correlator input
                #    else if mode==VCAPTURE then
                #       Ensure archiving to Pawsey is stopped while we capture
                #       copy subfile onto /voltdata where it will eventually get archived
                # 3. Rename .sub file to .free so that udpgrab can reuse it
                if subfile_mode == SUBFILE_MODE_CCAPTURE:
                    self._load_psrdada_ringbuffer(item, self.ringbuffer_key)

                elif subfile_mode == SUBFILE_MODE_VCAPTURE:
                    self._copy_subfile_to_voltdata(item, self.numa_node)

                else:
                    self.logger.error(f"{item}- Unknown subfile mode {subfile_mode}, ignoring.")

            elif self.mode == MODE_MWAX_BEAMFORMER:
                # 1. load file into PSRDADA ringbuffer for beamformer input
                # 2. Rename .sub file to .free so that udpgrab can reuse it
                self._load_psrdada_ringbuffer(item, self.ringbuffer_key)

            else:
                self.logger.error(f"{item}- Unknown MWAX Mover mode, ignoring.")
        finally:
            # Rename subfile so that udpgrab can reuse it
            free_filename = str(item).replace(self.ext_to_watch_for, self.ext_done)
            if not run_command(self.logger, item, f"mv {item} {free_filename}", 2):
                self.logger.error(f"{item}- Could not reame {item} back to {free_filename}")
                exit(2)

            self.logger.info(f"{item}- SubfileProcessor.handler finished handling.")

    def stop(self):
        self.watcher.stop()
        self.queue_worker.stop()

        # Wait for threads to finish
        for t in self.watcher_threads:
            if t:
                if t.isAlive:
                    t.join()

        for t in self.worker_threads:
            if t:
                if t.isAlive():
                    t.join()

    def _read_subfile_mode(self, filename):
        subfile_mode = None

        with open(filename, "r") as subfile:
            line_no = 1

            for line in subfile:
                # Don't search the whole file, just the first 16 lines
                if line_no <= PSRDADA_MAX_HEADER_LINES:
                    split_line = line.split(" ")

                    # We should have 2 items, keyword and value
                    if len(split_line) == 2:
                        keyword = split_line[0].strip()
                        value = split_line[1].strip()

                        if keyword == "MODE":
                            subfile_mode = value
                            break

                line_no = line_no + 1

        return subfile_mode

    def _load_psrdada_ringbuffer(self, filename, ringbuffer_key):
        self.logger.info(f"{filename}- Loading file into PSRDADA ringbuffer {ringbuffer_key}")

        command = f"dada_diskdb -k {ringbuffer_key} -f {filename}"
        return run_command(self.logger, filename, command, 32)

    def _copy_subfile_to_voltdata(self, filename, numa_node):
        self.logger.info(f"{filename}- Copying file into {VOLTDATA_PATH}")

        command = f"numactl --cpunodebind={numa_node} --membind={numa_node} cp {filename} {VOLTDATA_PATH}/."
        return run_command(self.logger, filename, command, 120)


class ArchiveProcessor:
    def __init__(self, logger, mode, archive_host, archive_port):
        self.logger = logger

        self.archive_destination_host = archive_host
        self.archive_destination_port = archive_port

        self.mode = mode
        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_NEW

        self.queue_db = queue.Queue()
        self.queue_worker_db = None

        self.watcher_threads = []
        self.worker_threads = []

        self.watch_dir_volt = "/voltdata"
        self.queue_volt = queue.Queue()
        self.watcher_volt = None
        self.queue_worker_volt = None

        self.watch_dir_vis = "/visdata"
        self.queue_vis = queue.Queue()
        self.watcher_vis = None
        self.queue_worker_vis = None

        self.watch_dir_fil = "/visdata"
        self.queue_fil = queue.Queue()
        self.watcher_fil = None
        self.queue_worker_fil = None

    def start(self):
        # Create watcher for voltage data -> db queue
        self.watcher_volt = mwax_mover.Watcher(path=self.watch_dir_volt, q=self.queue_db,
                                               pattern=".sub", log=self.logger,
                                               mode=self.mwax_mover_mode)

        # Create watcher for visibility data -> db queue
        self.watcher_vis = mwax_mover.Watcher(path=self.watch_dir_vis, q=self.queue_db,
                                              pattern=".fits", log=self.logger,
                                              mode=self.mwax_mover_mode)

        # Create watcher for filterbank data -> db queue
        self.watcher_fil = mwax_mover.Watcher(path=self.watch_dir_fil, q=self.queue_db,
                                              pattern=".fil", log=self.logger,
                                              mode=self.mwax_mover_mode)

        # Create queueworker for the db queue
        self.queue_worker_db = mwax_mover.QueueWorker(label="MWA Metadata DB",
                                                      q=self.queue_db,
                                                      executable_path=None,
                                                      event_handler=self.db_handler,
                                                      mode=self.mwax_mover_mode,
                                                      log=self.logger)

        # Create queueworker for voltage queue
        self.queue_worker_volt = mwax_mover.QueueWorker(label="Subfile Archive",
                                                        q=self.queue_volt,
                                                        executable_path=None,
                                                        event_handler=self.archive_handler,
                                                        mode=self.mwax_mover_mode,
                                                        log=self.logger)

        # Create queueworker for visibility queue
        self.queue_worker_vis = mwax_mover.QueueWorker(label="Visibility Archive",
                                                       q=self.queue_vis,
                                                       executable_path=None,
                                                       event_handler=self.archive_handler,
                                                       mode=self.mwax_mover_mode,
                                                       log=self.logger)

        # Create queueworker for filterbank queue
        self.queue_worker_fil = mwax_mover.QueueWorker(label="Filterbank Archive",
                                                       q=self.queue_fil,
                                                       executable_path=None,
                                                       event_handler=self.archive_handler,
                                                       mode=self.mwax_mover_mode,
                                                       log=self.logger)

        # Setup thread for watching filesystem
        watcher_volt_thread = threading.Thread(name="watch_volt", target=self.watcher_volt.start, daemon=True)
        self.watcher_threads.append(watcher_volt_thread)
        watcher_volt_thread.start()

        # Setup thread for processing items
        queue_worker_volt_thread = threading.Thread(name="work_volt", target=self.queue_worker_volt.start, daemon=True)
        self.worker_threads.append(queue_worker_volt_thread)
        queue_worker_volt_thread.start()

        # Setup thread for watching filesystem
        watcher_vis_thread = threading.Thread(name="watch_vis", target=self.watcher_vis.start, daemon=True)
        self.watcher_threads.append(watcher_vis_thread)
        watcher_vis_thread.start()

        # Setup thread for processing items
        queue_worker_vis_thread = threading.Thread(name="work_vis", target=self.queue_worker_vis.start, daemon=True)
        self.worker_threads.append(queue_worker_vis_thread)
        queue_worker_vis_thread.start()

        # Setup thread for watching filesystem
        watcher_fil_thread = threading.Thread(name="watch_fil", target=self.watcher_fil.start, daemon=True)
        self.watcher_threads.append(watcher_fil_thread)
        watcher_fil_thread.start()

        # Setup thread for processing items
        queue_worker_fil_thread = threading.Thread(name="work_fil", target=self.queue_worker_fil.start, daemon=True)
        self.worker_threads.append(queue_worker_fil_thread)
        queue_worker_fil_thread.start()

        # Setup thread for processing items from db queue
        queue_worker_db_thread = threading.Thread(name="work_db", target=self.queue_worker_db.start, daemon=True)
        self.worker_threads.append(queue_worker_db_thread)
        queue_worker_db_thread.start()

    def db_handler(self, item, origin_queue):
        self.logger.info(f"{item}- ArchiveProcessor.handler is handling {item}...")

        # immediately add this file to the db so we insert a record into metadata data_files table
        # Get info
        filename = os.path.basename(item)
        obsid = filename[0:10]
        file_ext = os.path.splitext(item)[1]
        file_size = os.stat(item).st_size
        filetype = None
        hostname = get_hostname()
        remote_archived = False     # This gets set to True by NGAS at Pawsey
        site_path = f"http://mwangas/RETRIEVE?file_id={obsid}"

        if file_ext.lower() == ".sub":
            filetype = 17
        elif file_ext.lower() == ".fits":
            filetype = 8
        elif file_ext.lower() == ".fil":
            filetype = 18
        else:
            # Error - unknown filetype
            self.logger.error(f"{item}- Could not handle unknown extension {file_ext}")
            exit(3)

        # Insert record into metadata database
        self.insert_data_file_row(obsid, filetype, file_size, filename, site_path, hostname, remote_archived)

        # if something went wrong, requeue
        #origin_queue.append(item)

        # immediately add this file (and a ptr to it's queue) to the voltage or vis queue which will deal with archiving
        if file_ext.lower() == ".sub":
            self.queue_volt.put(item)
        elif file_ext.lower() == ".fits":
            self.queue_vis.put(item)
        elif file_ext.lower() == ".fil":
            self.queue_fil.put(item)

        self.logger.info(f"{item}- ArchiveProcessor.db_handler finished handling.")

    def archive_handler(self, item, origin_queue):
        self.logger.info(f"{item}- ArchiveProcessor.archive_handler is handling {item}...")

        if self.archive_file(item) != 200:
            # Retry
            origin_queue.put(item)

        self.logger.info(f"{item}- ArchiveProcessor.archive_handler finished handling.")

    def stop(self):
        self.watcher_volt.stop()
        self.watcher_vis.stop()
        self.watcher_fil.stop()

        self.queue_worker_db.stop()
        self.queue_worker_volt.stop()
        self.queue_worker_vis.stop()
        self.queue_worker_fil.stop()

        # Wait for threads to finish
        for t in self.watcher_threads:
            if t:
                if t.isAlive:
                    t.join()

        for t in self.worker_threads:
            if t:
                if t.isAlive():
                    t.join()

    def insert_data_file_row(self, obsid, filetype, file_size, filename, site_path, hostname, remote_archived):
        self.logger.info(f"INSERT INTO data_files (observation_num, filetype, size, filename, "
                         f"site_path, host, remote_archived) "
                         f"VALUES ({obsid}, {filetype}, {file_size}, {filename}, "
                         f"{site_path}, {hostname}, {remote_archived})")

    def archive_file(self, full_filename):
        self.logger.info(f"{full_filename} attempting archive_file...")

        self.logger.info(f"{full_filename} calculating checksum...")
        checksum = calculate_checksum(full_filename)
        self.logger.debug(f"{full_filename} checksum == {checksum}")

        query_args = {'fileUri': f'ngas@{self.archive_destination_host}:{full_filename}',
                      'bport': self.archive_destination_port,
                      'bwinsize': '=32m',
                      'bnum_streams': 12,
                      'mimeType': 'application/octet-stream',
                      'bchecksum': str(checksum)}

        encoded_args = urlencode(query_args)

        bbcpurl = f"http://{self.archive_destination_host}/BBCPARC?{encoded_args}"

        resp = None
        try:
            resp = urlopen(bbcpurl, timeout=7200)
            data = []
            while True:
                buff = resp.read()
                if not buff:
                    break
                data.append(buff.decode('utf-8'))

            return 200, '', ''.join(data)
        except urllib.error.URLError as url_error:
            self.logger.error(f"{full_filename} failed to archive ({url_error.errno} {url_error.reason}")
            return url_error.errno, '', url_error.reason

        finally:
            if resp:
                resp.close()
            self.logger.info(f"{full_filename} archive_file completed")


class MWAXSubfileDistributor:
    def __init__(self):
        # init the logging subsystem
        self.logger = logging.getLogger('mwax_subfile_distributor')

        # init vars
        self.mode = None
        self.running = False
        self.processors = []
        self.ringbuffer_key = None
        self.numa_node = None

    def initialise(self):

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = "mwax_subfile_distributor: a command line tool which is part of the mwax " \
                             "suite for the MWA. It will perform different tasks based on --mode option.\n" \
                             "In addition, it will automatically archive files in /voltdata and /visdata to the " \
                             "mwacache servers at the Curtin Data Centre. Files in /voltdata/beams will be sent to" \
                             "Fredda.\n"
        parser.add_argument("-m", "--mode", required=True, default=None,
                            choices=[MODE_MWAX_CORRELATOR, MODE_MWAX_BEAMFORMER, ],
                            help=f"Mode to run:\n"
                            f"{MODE_MWAX_CORRELATOR}: watch for new sub files. If correlator then pass onto ringbuffer"
                                 f", if voltage capture then store to disk and disable archiver.\n" 
                            f"{MODE_MWAX_BEAMFORMER}: watch for new sub files, pass onto ringbuffer.\n")

        parser.add_argument("-k", "--key", required=True, default=None,
                            help=f"PSRDADA Ringbuffer key:\n"
                                 f"When mode = {MODE_MWAX_CORRELATOR}, this is the correlator input ringbuffer key.\n"
                                 f"When mode = {MODE_MWAX_BEAMFORMER}, this is the beamformer input ringbuffer key.\n")

        parser.add_argument("-n", "--numa_node", required=True, default=None,
                            help=f"NUMA node to use when copying subfiles to destinations.\n")

        args = vars(parser.parse_args())

        # Check args
        self.mode = args["mode"]
        self.ringbuffer_key = args["key"]
        self.numa_node = args["numa_node"]

        # start logging
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
        self.logger.addHandler(ch)

        self.logger.info("Starting mwax_subfile_distributor processor...")

        if self.mode == MODE_MWAX_CORRELATOR or self.mode == MODE_MWAX_BEAMFORMER:
            processor = SubfileProcessor(self.logger, self.mode, self.ringbuffer_key, self.numa_node)

            # Add this processor to list of processors we manage
            self.processors.append(processor)

            processor = ArchiveProcessor(self.logger, self.mode, "mwacache10", 7700)

            # Add this processor to list of processors we manage
            self.processors.append(processor)

        else:
            self.logger.error("Unknown running mode. Quitting.")
            exit(1)

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

        self.logger.info(f"Running in mode {self.mode}")

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

        # Finished
        self.logger.info("Completed Successfully")


if __name__ == '__main__':
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
