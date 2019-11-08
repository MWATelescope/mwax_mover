import mwax_mover
import argparse
import logging
import logging.handlers
import queue
import signal
import subprocess
import sys
import threading
import time

MODE_MWAX_CORRELATOR = "MWAX_CORRELATOR"
MODE_MWAX_BEAMFORMER = "MWAX_BEAMFORMER"

SUBFILE_MODE_IDLE = "IDLE"
SUBFILE_MODE_CCAPTURE = "CCAPTURE"
SUBFILE_MODE_VCAPTURE = "VCAPTURE"

COMMAND_DADA_DISKDB = "dada_diskdb"
PSRDADA_MAX_HEADER_LINES = 16           # number of lines of the PSRDADA header to read looking for keywords

VOLTDATA_PATH = "/voltdata"
VISDATA_PATH = "/visdata"


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
        self.queue = None
        self.watcher = None
        self.queue_worker = None
        self.ext_done = ".free"
        self.queue_worker_thread = None
        self.watcher_thread = None
        self.ringbuffer_key = ringbuffer_key    # PSRDADA ringbuffer key for INPUT into correlator or beamformer
        self.numa_node = numa_node              # Numa node to use to do a file copy

    def start(self):
        # Create a queue for dealing with sub files
        self.queue = queue.Queue()

        # Create watcher
        self.watcher = mwax_mover.Watcher(path=self.watch_dir, q=self.queue,
                                          pattern=f"{self.ext_to_watch_for}", log=self.logger,
                                          mode=self.mwax_mover_mode)

        # Create queueworker
        self.queue_worker = mwax_mover.QueueWorker(q=self.queue,
                                                   executable_path=None,
                                                   event_handler=self.handler,
                                                   mode=self.mwax_mover_mode,
                                                   log=self.logger)

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
        ch.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
        self.logger.addHandler(ch)

        self.logger.info("Starting mwax_subfile_distributor processor...")

        if self.mode == MODE_MWAX_CORRELATOR or self.mode == MODE_MWAX_BEAMFORMER:
            processor = SubfileProcessor(self.logger, self.mode, self.ringbuffer_key, self.numa_node)

            # Add this processor to list of processors we manage
            self.processors.append(processor)

            # Start this processor
            processor.start()

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
            self.logger.info(f"Scanning {processor.watch_dir} for files matching {'*' + processor.ext_to_watch_for}...")

            # Setup thread for watching filesystem
            processor.watcher_thread = threading.Thread(target=processor.watcher.start, daemon=True)

            # Start watcher
            processor.watcher_thread.start()

            # Setup thread for processing items
            processor.queue_worker_thread = threading.Thread(target=processor.queue_worker.start, daemon=True)

            # Start queue worker
            processor.queue_worker_thread.start()

        while self.running:
            for processor in self.processors:
                if processor.queue_worker_thread.isAlive():
                    time.sleep(1)
                else:
                    self.running = False
                    break

        for processor in self.processors:
            # Wait for threads to finish
            if processor.watcher_thread:
                processor.watcher_thread.join()

            if processor.queue_worker_thread.isAlive():
                processor.queue_worker_thread.join()

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
