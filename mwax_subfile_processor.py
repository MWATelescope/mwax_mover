import mwax_archive_processor
import mwax_mover
import mwax_queue_worker
import mwax_watcher
import argparse
import logging
import logging.handlers
import queue
import signal
import sys
import threading
import time


SUBFILE_MODE_IDLE = "IDLE"
SUBFILE_MODE_CCAPTURE = "CCAPTURE"
SUBFILE_MODE_VCAPTURE = "VCAPTURE"

COMMAND_DADA_DISKDB = "dada_diskdb"
PSRDADA_MAX_HEADER_LINES = 16           # number of lines of the PSRDADA header to read looking for keywords

VOLTDATA_PATH = "/voltdata"
VISDATA_PATH = "/visdata"


def read_subfile_mode(filename):
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
        self.watcher = mwax_watcher.Watcher(path=self.watch_dir, q=self.queue,
                                            pattern=f"{self.ext_to_watch_for}", log=self.logger,
                                            mode=self.mwax_mover_mode)

        # Create queueworker
        self.queue_worker = mwax_queue_worker.QueueWorker(label="Subfile Input Queue",
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
            subfile_mode = read_subfile_mode(item)

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
            if not mwax_mover.run_command(item, f"mv {item} {free_filename}", 2):
                self.logger.error(f"{item}- Could not reame {item} back to {free_filename}")
                exit(2)

            self.logger.info(f"{item}- SubfileProcessor.handler finished handling.")

    def stop(self):
        self.watcher.stop()
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

    def _load_psrdada_ringbuffer(self, filename, ringbuffer_key):
        self.logger.info(f"{filename}- Loading file into PSRDADA ringbuffer {ringbuffer_key}")

        command = f"dada_diskdb -k {ringbuffer_key} -f {filename}"
        return mwax_mover.run_command(filename, command, 32)

    def _copy_subfile_to_voltdata(self, filename, numa_node):
        self.logger.info(f"{filename}- Copying file into {VOLTDATA_PATH}")

        command = f"numactl --cpunodebind={numa_node} --membind={numa_node} cp {filename} {VOLTDATA_PATH}/."
        return mwax_mover.run_command(filename, command, 120)

