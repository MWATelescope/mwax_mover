from mwax_mover import mwax_mover
from mwax_mover import mwax_queue_worker
from mwax_mover import mwax_watcher
import glob
import logging
import logging.handlers
import os
import queue
import shutil
import threading
from enum import Enum


class CorrelatorMode(Enum):
    NO_CAPTURE = "NO_CAPTURE"
    CORR_MODE_CHANGE = "CORR_MODE_CHANGE"
    HW_LFILES = "HW_LFILES"
    MWAX_CORRELATOR = "MWAX_CORRELATOR"
    VOLTAGE_START = "VOLTAGE_START"
    VOLTAGE_BUFFER = "VOLTAGE_BUFFER"
    VOLTAGE_STOP = "VOLTAGE_STOP"
    MWAX_VCS = "MWAX_VCS"

    @staticmethod
    def is_no_capture(mode_string: str) -> bool:
        return mode_string in [CorrelatorMode.NO_CAPTURE.value, CorrelatorMode.CORR_MODE_CHANGE.value, CorrelatorMode.VOLTAGE_STOP.value, ]

    @staticmethod
    def is_correlator(mode_string: str) -> bool:
        return mode_string in [CorrelatorMode.HW_LFILES.value, CorrelatorMode.MWAX_CORRELATOR.value, ]

    @staticmethod
    def is_vcs(mode_string: str) -> bool:
        return mode_string in [CorrelatorMode.VOLTAGE_START.value, CorrelatorMode.VOLTAGE_BUFFER.value, CorrelatorMode.MWAX_VCS.value, ]


COMMAND_DADA_DISKDB = "dada_diskdb"
PSRDADA_MAX_HEADER_LINES = 16           # number of lines of the PSRDADA header to read looking for keywords


def read_subfile_mode(filename: str) -> str:
    subfile_mode = None

    with open(filename, "rb") as subfile:
        line_no = 1

        subfile_text = subfile.read(4096).decode()
        subfile_text_lines = subfile_text.splitlines()

        for line in subfile_text_lines:
            # Don't search the whole file, just the first 16 lines
            if line_no <= PSRDADA_MAX_HEADER_LINES:
                split_line = line.split()

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
    def __init__(self, context,
                 subfile_path: str, voltdata_path: str,
                 bf_enabled: bool, bf_ringbuffer_key: str, bf_numa_node: int,
                 corr_enabled: bool, corr_ringbuffer_key: str, corr_diskdb_numa_node: int):
        self.subfile_distributor_context = context

        # Setup logging
        self.logger = logging.getLogger(__name__)
        self.logger.propagate = True  # pass all logged events to the parent (subfile distributor/main log)
        self.logger.setLevel(logging.DEBUG)
        file_log = logging.FileHandler(filename=os.path.join(self.subfile_distributor_context.cfg_log_path,
                                                             f"{__name__}.log"))
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
        self.logger.addHandler(file_log)

        self.ext_to_watch_for = ".sub"
        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_RENAME
        self.subfile_path = subfile_path
        self.voltdata_path = voltdata_path
        self.watch_dir = self.subfile_path
        self.ext_done = ".free"

        self.watcher_threads = []
        self.worker_threads = []

        # Use a priority queue to ensure earliest to oldest order of subfiles by obsid and second
        self.queue = queue.PriorityQueue()
        self.watcher = None
        self.queue_worker = None

        self.bf_enabled = bf_enabled
        self.bf_ringbuffer_key = bf_ringbuffer_key    # PSRDADA ringbuffer key for INPUT into correlator or beamformer
        self.bf_numa_node = bf_numa_node              # Numa node to use to do a file copy

        self.corr_enabled = corr_enabled
        self.corr_ringbuffer_key = corr_ringbuffer_key  # PSRDADA ringbuffer key for INPUT into correlator or beamformer
        self.corr_diskdb_numa_node = corr_diskdb_numa_node  # Numa node to use to do a file copy

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

    def handler(self, item: str) -> bool:
        success = False

        self.logger.info(f"{item}- SubfileProcessor.handler is handling {item}...")

        try:
            subfile_mode = read_subfile_mode(item)

            if self.corr_enabled:
                # 1. Read header of subfile.
                # 2. If mode==HW_LFILES then
                #       load into PSRDADA ringbuffer for correlator input
                #    else if mode==VOLTAGE_START then
                #       Ensure archiving to Pawsey is stopped while we capture
                #       copy subfile onto /voltdata where it will eventually get archived
                #    else
                #       Ignore the subfile
                # 3. Rename .sub file to .free so that udpgrab can reuse it
                if CorrelatorMode.is_correlator(subfile_mode):
                    self.subfile_distributor_context.archive_processor.pause_archiving(False)

                    self._load_psrdada_ringbuffer(item, self.corr_ringbuffer_key, self.corr_diskdb_numa_node)
                    success = True

                elif CorrelatorMode.is_vcs(subfile_mode):
                    # Pause archiving so we have the disk to ourselves
                    self.subfile_distributor_context.archive_processor.pause_archiving(True)

                    self._copy_subfile_to_voltdata(item, self.corr_diskdb_numa_node)
                    success = True

                elif CorrelatorMode.is_no_capture(subfile_mode):
                    self.logger.info(f"{item}- ignoring due to mode: {subfile_mode}")
                    self.subfile_distributor_context.archive_processor.pause_archiving(False)
                    success = True

                else:
                    self.logger.error(f"{item}- Unknown subfile mode {subfile_mode}, ignoring.")
                    success = False

            if self.bf_enabled:
                # Don't run beamformer is we are in correlator mode too and we are doing a voltage capture!
                # Otherwise:
                # 1. load file into PSRDADA ringbuffer for beamformer input
                # 2. Rename .sub file to .free so that udpgrab can reuse it
                if self.corr_enabled and CorrelatorMode.is_vcs(subfile_mode):
                    self.logger.warning(f"{item}- beamformer mode enabled and is in {subfile_mode} mode, ignoring this"
                                        f" beamformer job.")
                else:
                    self._load_psrdada_ringbuffer(item, self.bf_ringbuffer_key, self.corr_diskdb_numa_node)

                success = True

        except Exception as handler_exception:
            self.logger.error(f"{item} {handler_exception}")
            success = False

        finally:
            if success:
                # Rename subfile so that udpgrab can reuse it
                free_filename = str(item).replace(self.ext_to_watch_for, self.ext_done)

                try:
                    shutil.move(item, free_filename)
                except Exception as move_exception:
                    self.logger.error(f"{item}- Could not rename {item} back to {free_filename}. Error "
                                      f"{move_exception}")
                    exit(2)

            self.logger.info(f"{item}- SubfileProcessor.handler finished handling.")

        return success

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

    def _load_psrdada_ringbuffer(self, filename: str, ringbuffer_key: str, numa_node: int) -> bool:
        self.logger.info(f"{filename}- Loading file into PSRDADA ringbuffer {ringbuffer_key}")

        command = f"numactl --cpunodebind={str(numa_node)} --membind={str(numa_node)} dada_diskdb -k {ringbuffer_key} -f {filename}"
        return mwax_mover.run_command(command, 32)

    def _copy_subfile_to_voltdata(self, filename: str, numa_node: int) -> bool:
        self.logger.info(f"{filename}- Copying file into {self.voltdata_path}")

        command = f"numactl --cpunodebind={str(numa_node)} --membind={str(numa_node)} cp {filename} {self.voltdata_path}/."
        return mwax_mover.run_command(command, 120)

    def dump_voltages(self, start_gps_time: int, end_gps_time: int) -> bool:
        self.logger.info(f"dump_voltages: from {str(start_gps_time)} to {str(end_gps_time)}...")

        # disable archiving
        archiving = self.subfile_distributor_context.archive_processor.archiving_paused
        self.subfile_distributor_context.archive_processor.pause_archiving(True)

        # Look for any .free files which have the first 10 characters of filename from starttime to endtime
        free_file_list = sorted(glob.glob(f"{self.subfile_path}/*.free"))

        keep_file_list = []

        for free_filename in free_file_list:
            # Get just the filename, and then see if we have a gps time
            #
            # Filenames are:  1234567890_1234567890_xxx.free
            #
            filename_only = os.path.basename(free_filename)
            file_obsid = int(filename_only[0:10])
            file_gps_time = int(filename_only[11:21])

            # See if the file_gps_time is between start and end time
            if start_gps_time <= file_gps_time <= end_gps_time:
                self.logger.info(f"dump_voltages: keeping {free_filename}")

                # For any that exist, rename them immediately to .keep
                keep_filename = free_filename.replace(".free", ".keep")
                shutil.move(free_filename, keep_filename)
                keep_file_list.append(keep_filename)

        # Then copy all .keep to /voltdata/*.sub
        for keep_filename in keep_file_list:
            self.logger.info(f"dump_voltages: copying {keep_filename} to {self.voltdata_path}...")
            keep_filename_only = os.path.basename(keep_filename)
            sub_filename_only = keep_filename_only.replace(".keep", ".sub")
            sub_filename = os.path.join(self.voltdata_path, sub_filename_only)

            # Only copy if we don't already have it in /voltdata
            if not os.path.exists(sub_filename):
                shutil.copyfile(keep_filename, sub_filename)
                self.logger.info(f"dump_voltages: copy of {keep_filename} to {self.voltdata_path} complete")

        # Then rename all .keep files to .free
        for keep_filename in keep_file_list:
            free_filename = keep_filename.replace(".keep", ".free")
            self.logger.info(f"dump_voltages: renaming {keep_filename} to {free_filename}")
            shutil.move(keep_filename, free_filename)

        # reenable archiving (if we're not in voltage capture mode)
        self.subfile_distributor_context.archive_processor.pause_archiving(archiving)

        self.logger.info(f"dump_voltages: complete")
        return True

    def get_status(self) -> dict:
        watcher_list = []

        if self.watcher:
            status = dict({"name": "subfile watcher"})
            status.update(self.watcher.get_status())
            watcher_list.append(status)

        worker_list = []

        if self.queue_worker:
            status = dict({"name": "subfile worker"})
            status.update(self.queue_worker.get_status())
            worker_list.append(status)

        return_status = {"type": type(self).__name__,
                         "watchers": watcher_list,
                         "workers": worker_list}

        return return_status
