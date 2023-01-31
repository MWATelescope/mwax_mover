"""Module for subfile processor"""
import glob
import logging
import logging.handlers
import os
import queue
import shutil
import sys
import threading
import time
from enum import Enum
from mwax_mover import (
    mwax_mover,
    utils,
    mwax_queue_worker,
    mwax_watcher,
    mwax_command,
)

COMMAND_DADA_DISKDB = "dada_diskdb"


class CorrelatorMode(Enum):
    """Class representing correlator mode"""

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
        """Returns true if mode is a no capture"""
        return mode_string in [
            CorrelatorMode.NO_CAPTURE.value,
            CorrelatorMode.CORR_MODE_CHANGE.value,
            CorrelatorMode.VOLTAGE_STOP.value,
        ]

    @staticmethod
    def is_correlator(mode_string: str) -> bool:
        """Returns true if mode is a correlator obs"""
        return mode_string in [
            CorrelatorMode.HW_LFILES.value,
            CorrelatorMode.MWAX_CORRELATOR.value,
        ]

    @staticmethod
    def is_vcs(mode_string: str) -> bool:
        """Returns true if mode is a vcs obs"""
        return mode_string in [
            CorrelatorMode.VOLTAGE_START.value,
            CorrelatorMode.VOLTAGE_BUFFER.value,
            CorrelatorMode.MWAX_VCS.value,
        ]


class SubfileProcessor:
    """Class representing the SubfileProcessor"""

    def __init__(
        self,
        context,
        subfile_incoming_path: str,
        voltdata_incoming_path: str,
        always_keep_subfiles: bool,
        bf_enabled: bool,
        bf_ringbuffer_key: str,
        bf_numa_node,
        bf_fildata_path: str,
        bf_settings_path: str,
        corr_enabled: bool,
        corr_ringbuffer_key: str,
        corr_diskdb_numa_node,
        psrdada_timeout_sec: int,
        copy_subfile_to_disk_timeout_sec: int,
    ):
        self.subfile_distributor_context = context

        # Setup logging
        self.logger = logging.getLogger(__name__)
        # pass all logged events to the parent (subfile distributor/main log)
        self.logger.propagate = True
        self.logger.setLevel(logging.DEBUG)
        file_log = logging.FileHandler(
            filename=os.path.join(
                self.subfile_distributor_context.cfg_log_path,
                f"{__name__}.log",
            )
        )
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(
            logging.Formatter(
                "%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"
            )
        )
        self.logger.addHandler(file_log)

        self.ext_to_watch_for = ".sub"
        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_RENAME
        self.subfile_incoming_path = subfile_incoming_path
        self.voltdata_incoming_path = voltdata_incoming_path
        self.always_keep_subfiles = always_keep_subfiles
        self.ext_done = ".free"

        # Voltage buffer dump vars
        self.dump_start_gps = None
        self.dump_end_gps = None
        self.dump_keep_file_queue = queue.Queue()

        # Watchers and workers
        self.watcher_threads = []
        self.worker_threads = []

        # Use a priority queue to ensure earliest to oldest order of subfiles
        # by obsid and second
        self.subfile_queue = queue.PriorityQueue()
        self.subfile_watcher = None
        self.subfile_queue_worker = None

        self.bf_enabled = bf_enabled
        # PSRDADA ringbuffer key for INPUT into correlator or beamformer
        self.bf_ringbuffer_key = bf_ringbuffer_key
        self.bf_numa_node = bf_numa_node  # Numa node to use to do a file copy
        # Path where filterbank files are written to
        # (and sub files in debug mode)
        self.bf_fildata_path = bf_fildata_path
        # Path to text file containing the PSRDADA Beamformer header info
        self.bf_settings_path = bf_settings_path

        self.corr_enabled = corr_enabled
        # PSRDADA ringbuffer key for INPUT into correlator or beamformer
        self.corr_ringbuffer_key = corr_ringbuffer_key
        # Numa node to use to do a file copy
        self.corr_diskdb_numa_node = corr_diskdb_numa_node

        self.psrdada_timeout_sec = psrdada_timeout_sec
        self.copy_subfile_to_disk_timeout_sec = (
            copy_subfile_to_disk_timeout_sec
        )

    def start(self):
        """Start the processor"""
        # Create watcher for the subfiles
        self.subfile_watcher = mwax_watcher.Watcher(
            name="subfile_watcher",
            path=self.subfile_incoming_path,
            dest_queue=self.subfile_queue,
            pattern=f"{self.ext_to_watch_for}",
            log=self.logger,
            mode=self.mwax_mover_mode,
            recursive=False,
        )

        # Create subfile queueworker
        self.subfile_queue_worker = mwax_queue_worker.QueueWorker(
            name="Subfile Input Queue",
            source_queue=self.subfile_queue,
            executable_path=None,
            event_handler=self.subfile_handler,
            log=self.logger,
            requeue_to_eoq_on_failure=False,
            exit_once_queue_empty=False,
        )

        # Setup thread for watching filesystem
        watcher_thread = threading.Thread(
            name="watch_sub", target=self.subfile_watcher.start, daemon=True
        )
        self.watcher_threads.append(watcher_thread)
        watcher_thread.start()

        # Setup thread for processing subfile items
        queue_worker_thread = threading.Thread(
            name="work_sub",
            target=self.subfile_queue_worker.start,
            daemon=True,
        )
        self.worker_threads.append(queue_worker_thread)
        queue_worker_thread.start()

    def handle_next_keep_file(self) -> bool:
        """When we need to offload a keep file, this handles it"""
        success = True

        # Get next keep file off the queue (if any)
        if self.dump_keep_file_queue.qsize() > 0:
            keep_filename = self.dump_keep_file_queue.get()

            self.logger.info(
                "SubfileProcessor.handle_next_keep_file is handling"
                f" {keep_filename}..."
            )

            # Copy the .keep file to the voltdata incoming dir
            copy_success = self.copy_subfile_to_disk(
                keep_filename,
                self.corr_diskdb_numa_node,
                self.voltdata_incoming_path,
                self.copy_subfile_to_disk_timeout_sec,
            )

            if copy_success:
                # Rename kept subfile so that mwax_u2s can reuse it
                free_filename = keep_filename.replace(".keep", self.ext_done)

                try:
                    shutil.move(keep_filename, free_filename)
                except Exception as move_exception:  # pylint: disable=broad-except
                    self.logger.error(
                        f"Could not rename {keep_filename} back to"
                        f" {free_filename}. Error {move_exception}"
                    )
                    sys.exit(2)
            else:
                # Reqeuue file to try again later
                self.dump_keep_file_queue.put(keep_filename)

            self.logger.info(
                "SubfileProcessor.handle_next_keep_file finished handling"
                f" {keep_filename}. Remaining .keep files:"
                f" {self.dump_keep_file_queue.qsize()}."
            )

        return success

    def subfile_handler(self, item: str) -> bool:
        """When subfile detected this handles it"""
        success = False

        self.logger.info(
            f"{item}- SubfileProcessor.subfile_handler is handling {item}..."
        )

        # `keep_subfiles_path` is used after doing the main work. If we are
        # running in with `always_keep_subfiles`=1,
        # then we will always keep the voltages and not delete them.
        # If so, we put them into the voltdata path. But if we are in VCS mode
        # there is no need to do this as we
        # already keep the sub files.
        #
        # If not in `always_keep_subfiles`=1 mode, this value will be None and
        # will be ignored.
        #
        keep_subfiles_path = None

        try:
            subfile_mode = utils.read_subfile_value(item, "MODE")

            if self.corr_enabled:
                # 1. Read header of subfile.
                # 2. If mode==HW_LFILES then
                #       load into PSRDADA ringbuffer for correlator input
                #    else if mode==VOLTAGE_START then
                #       Ensure archiving to Pawsey is stopped while we capture
                #       copy subfile onto /voltdata where it will eventually
                #       get archived
                #    else
                #       Ignore the subfile
                # 3. Rename .sub file to .free so that udpgrab can reuse it
                if CorrelatorMode.is_correlator(subfile_mode):
                    if (
                        self.subfile_distributor_context.cfg_corr_archive_destination_enabled  # pylint: disable=line-too-long
                    ):
                        self.subfile_distributor_context.archive_processor.pause_archiving(  # pylint: disable=line-too-long
                            False
                        )

                    success = utils.load_psrdada_ringbuffer(
                        self.logger,
                        item,
                        self.corr_ringbuffer_key,
                        self.corr_diskdb_numa_node,
                        self.psrdada_timeout_sec,
                    )

                    if self.always_keep_subfiles:
                        keep_subfiles_path = self.voltdata_incoming_path

                elif CorrelatorMode.is_vcs(subfile_mode):
                    # Pause archiving so we have the disk to ourselves
                    if (
                        self.subfile_distributor_context.cfg_corr_archive_destination_enabled  # pylint: disable=line-too-long
                    ):
                        self.subfile_distributor_context.archive_processor.pause_archiving(  # pylint: disable=line-too-long
                            True
                        )

                    self.copy_subfile_to_disk(
                        item,
                        self.corr_diskdb_numa_node,
                        self.voltdata_incoming_path,
                        self.copy_subfile_to_disk_timeout_sec,
                    )
                    success = True

                elif CorrelatorMode.is_no_capture(subfile_mode):
                    self.logger.info(
                        f"{item}- ignoring due to mode: {subfile_mode}"
                    )

                    #
                    # This is our opportunity to write any "keep" files to disk
                    # which were held
                    #

                    if (
                        self.subfile_distributor_context.cfg_corr_archive_destination_enabled  # pylint: disable=line-too-long
                    ):
                        self.subfile_distributor_context.archive_processor.pause_archiving(  # pylint: disable=line-too-long
                            False
                        )

                    # Since we're not doing anything with this subfile we can
                    # try and handle any remaining keep files
                    self.handle_next_keep_file()

                    success = True

                else:
                    self.logger.error(
                        f"{item}- Unknown subfile mode {subfile_mode},"
                        " ignoring."
                    )
                    success = False

            if self.bf_enabled:
                # Don't run beamformer if we are in correlator mode too and we
                # are doing a voltage capture!
                if self.corr_enabled and CorrelatorMode.is_vcs(subfile_mode):
                    self.logger.warning(
                        f"{item}- beamformer mode enabled and is in"
                        f" {subfile_mode} mode, ignoring this beamformer job."
                    )
                    success = True
                else:
                    # Otherwise:
                    # 1. If mode is correlator or vcs, then:
                    # 2. Inject the relevant beamformer keywords into the
                    # header of the sub file
                    # 3. load file into PSRDADA ringbuffer for beamformer input
                    # 4. Rename .sub file to .free so that udpgrab can reuse it
                    if CorrelatorMode.is_correlator(
                        subfile_mode
                    ) or CorrelatorMode.is_vcs(subfile_mode):
                        # Get the settings we want for the beamformer from the
                        # text file NOTE: this is read per subfile, and the
                        # idea is it can be updated at runtime but this makes
                        # it a bit less efficient than reading it once and
                        # using it many times.
                        with open(
                            self.bf_settings_path, "r", encoding="utf-8"
                        ) as bf_settings_file:
                            beamformer_settings = bf_settings_file.read()

                        # It is possible the beamformer settings are already
                        # added into the sub file (e.g. a failed load into ringbuffer)
                        # So check first, before appending them again!
                        if (
                            utils.read_subfile_value(
                                item, "NUM_INCOHERENT_BEAMS"
                            )
                            is None
                        ):
                            self.logger.info(
                                f"{item}- injecting beamformer header into"
                                " subfile..."
                            )
                            utils.inject_beamformer_headers(
                                item, beamformer_settings
                            )
                        else:
                            self.logger.info(
                                f"{item}- beamformer header exists in subfile."
                            )

                        success = utils.load_psrdada_ringbuffer(
                            self.logger,
                            item,
                            self.bf_ringbuffer_key,
                            self.bf_numa_node,
                            self.psrdada_timeout_sec,
                        )

                        if self.always_keep_subfiles:
                            keep_subfiles_path = self.voltdata_incoming_path

                    elif CorrelatorMode.is_no_capture(subfile_mode):
                        self.logger.info(
                            f"{item}- ignoring due to mode: {subfile_mode}"
                        )
                        success = True

                    else:
                        self.logger.error(
                            f"{item}- Unknown subfile mode {subfile_mode},"
                            " ignoring."
                        )
                        success = False

        except Exception as handler_exception:  # pylint: disable=broad-except
            self.logger.error(f"{item} {handler_exception}")
            success = False

        finally:
            if success:
                # Check if need to keep subfiles, if so we need to copy
                # them off
                if keep_subfiles_path:
                    # we use -1 for numa node for now as this is really for
                    # debug so it does not matter
                    self.copy_subfile_to_disk(
                        item,
                        -1,
                        keep_subfiles_path,
                        self.copy_subfile_to_disk_timeout_sec,
                    )

                # Rename subfile so that udpgrab can reuse it
                free_filename = str(item).replace(
                    self.ext_to_watch_for, self.ext_done
                )

                try:
                    shutil.move(item, free_filename)
                except Exception as move_exception:  # pylint: disable=broad-except
                    self.logger.error(
                        f"{item}- Could not rename {item} back to"
                        f" {free_filename}. Error {move_exception}"
                    )
                    sys.exit(2)

            self.logger.info(
                f"{item}- SubfileProcessor.subfile_handler finished handling."
            )

        return success

    def stop(self):
        """Stop the processor"""
        self.subfile_watcher.stop()
        self.subfile_queue_worker.stop()

        # Wait for threads to finish
        for watcher_thread in self.watcher_threads:
            if watcher_thread:
                thread_name = watcher_thread.name
                self.logger.debug(f"Watcher {thread_name} Stopping...")
                if watcher_thread.is_alive():
                    watcher_thread.join()
                self.logger.debug(f"Watcher {thread_name} Stopped")

        for worker_thread in self.worker_threads:
            if worker_thread:
                thread_name = worker_thread.name
                self.logger.debug(f"QueueWorker {thread_name} Stopping...")
                if worker_thread.is_alive():
                    worker_thread.join()
                self.logger.debug(f"QueueWorker {thread_name} Stopped")

    def copy_subfile_to_disk(
        self,
        filename: str,
        numa_node: int,
        destination_path: str,
        timeout: int,
    ) -> bool:
        """Copies the given filename to the destination path"""
        self.logger.info(f"{filename}- Copying file into {destination_path}")

        command = f"cp {filename} {destination_path}/."

        start_time = time.time()
        retval, stdout = mwax_command.run_command_ext(
            self.logger, command, numa_node, timeout, False
        )
        elapsed = time.time() - start_time

        if retval:
            self.logger.info(
                f"{filename}- Copying file into {destination_path} was"
                f" successful (took {elapsed:.3f} secs."
            )
        else:
            self.logger.error(
                f"{filename}- Copying file into {destination_path} failed with"
                f" error {stdout}"
            )

        return retval

    def dump_voltages(self, start_gps_time: int, end_gps_time: int) -> bool:
        """Dump whatever subfiles we have from /dev/shm to disk"""
        # Set module level variables
        self.dump_start_gps = start_gps_time
        self.dump_end_gps = end_gps_time

        self.logger.info(
            f"dump_voltages: from {str(start_gps_time)} to"
            f" {str(end_gps_time)}..."
        )

        # disable archiving
        archiving_was_paused = False

        # If we have an active archiver, check it's status
        if (
            self.subfile_distributor_context.cfg_corr_archive_destination_enabled  # noqa: E501
        ):
            archiving_was_paused = (
                self.subfile_distributor_context.archive_processor.archiving_paused  # noqa: E501
            )
            self.subfile_distributor_context.archive_processor.pause_archiving(
                True
            )

        # Look for any .free files which have the first 10 characters of
        # filename from starttime to endtime
        free_file_list = sorted(
            glob.glob(f"{self.subfile_incoming_path}/*.free")
        )

        keep_file_list = []

        for free_filename in free_file_list:
            # Get just the filename, and then see if we have a gps time
            #
            # Filenames are:  1234567890_1234567890_xxx.free
            #
            filename_only = os.path.basename(free_filename)
            # file_obsid = int(filename_only[0:10])
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
            self.logger.info(
                f"dump_voltages: copying {keep_filename} to"
                f" {self.voltdata_incoming_path}..."
            )
            keep_filename_only = os.path.basename(keep_filename)
            sub_filename_only = keep_filename_only.replace(".keep", ".sub")
            sub_filename = os.path.join(
                self.voltdata_incoming_path, sub_filename_only
            )

            # Only copy if we don't already have it in /voltdata
            if not os.path.exists(sub_filename):
                shutil.copyfile(keep_filename, sub_filename)
                self.logger.info(
                    f"dump_voltages: copy of {keep_filename} to"
                    f" {self.voltdata_incoming_path} complete"
                )

        # Then rename all .keep files to .free
        for keep_filename in keep_file_list:
            free_filename = keep_filename.replace(".keep", ".free")
            self.logger.info(
                f"dump_voltages: renaming {keep_filename} to {free_filename}"
            )
            shutil.move(keep_filename, free_filename)

        # reenable archiving (if we're not in voltage capture mode) and if an
        # archiver is running
        if (
            self.subfile_distributor_context.cfg_corr_archive_destination_enabled  # noqa: E501
        ):
            self.subfile_distributor_context.archive_processor.pause_archiving(
                archiving_was_paused
            )

        self.logger.info("dump_voltages: complete")
        return True

    def get_status(self) -> dict:
        """Return status as a dictionary"""
        watcher_list = []

        if self.subfile_watcher:
            status = dict(
                {"Unix timestamp": time.time(), "name": "subfile watcher"}
            )
            status.update(self.subfile_watcher.get_status())
            watcher_list.append(status)

        worker_list = []

        if self.subfile_queue_worker:
            status = dict({"name": "subfile worker"})
            status.update(self.subfile_queue_worker.get_status())
            worker_list.append(status)

        return_status = {
            "type": type(self).__name__,
            "watchers": watcher_list,
            "workers": worker_list,
        }

        return return_status
