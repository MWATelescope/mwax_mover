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
from mwax_mover import (
    mwax_mover,
    utils,
    mwax_queue_worker,
    mwax_watcher,
    mwax_command,
)
from mwax_mover.utils import CorrelatorMode

COMMAND_DADA_DISKDB = "dada_diskdb"


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

        self.ext_sub_file = ".sub"
        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_RENAME
        self.subfile_incoming_path = subfile_incoming_path
        self.voltdata_incoming_path = voltdata_incoming_path
        self.always_keep_subfiles = always_keep_subfiles
        self.ext_free_file = ".free"
        self.ext_keep_file = ".keep"

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
            pattern=f"{self.ext_sub_file}",
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

        # Get next keep file off the queue
        keep_filename = self.dump_keep_file_queue.get()

        self.logger.info(
            "SubfileProcessor.handle_next_keep_file is handling"
            f" {keep_filename}..."
        )

        # Copy the .keep file to the voltdata incoming dir
        # and ensure it is named as a ".sub" file
        copy_success = self.copy_subfile_to_disk(
            keep_filename,
            self.corr_diskdb_numa_node,
            self.voltdata_incoming_path,
            self.copy_subfile_to_disk_timeout_sec,
            os.path.basename(keep_filename).replace(
                self.ext_keep_file, self.ext_sub_file
            ),
        )

        if copy_success:
            # Rename kept subfile so that mwax_u2s can reuse it
            free_filename = keep_filename.replace(
                self.ext_keep_file, self.ext_free_file
            )

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
            subfile_mode = utils.read_subfile_value(item, utils.PSRDADA_MODE)

            if self.corr_enabled:
                #
                # We need to check we are not in a voltage dump. If so, whatever
                # observation is happening is ignored and instead we treat this
                # like a VCS observation
                #
                # Read the SUBOBSID from the subfile
                subobs_id = int(
                    utils.read_subfile_value(item, utils.PSRDADA_SUBOBS_ID)
                )

                if (
                    (
                        self.dump_start_gps is not None
                        and self.dump_end_gps is not None
                    )
                    and subobs_id >= self.dump_start_gps
                    and subobs_id < self.dump_end_gps
                ):
                    # We ARE in voltage dump and so we go and do a VCS capture instead
                    # of whatever else we were doing
                    self.logger.info(
                        f"{item}- ignoring existing mode: {subfile_mode} as we"
                        f" are within a voltage dump ({self.dump_start_gps} <"
                        f" {self.dump_end_gps}). Doing VCS instead."
                    )

                    # Pause archiving so we have the disk to ourselves
                    if (
                        self.subfile_distributor_context.cfg_corr_archive_destination_enabled  # pylint: disable=line-too-long
                    ):
                        self.subfile_distributor_context.archive_processor.pause_archiving(  # pylint: disable=line-too-long
                            True
                        )

                    success = self.copy_subfile_to_disk(
                        item,
                        self.corr_diskdb_numa_node,
                        self.voltdata_incoming_path,
                        self.copy_subfile_to_disk_timeout_sec,
                    )
                else:
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
                        # This is a normal MWAX_CORRELATOR obs, continue as normal
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

                        success = self.copy_subfile_to_disk(
                            item,
                            self.corr_diskdb_numa_node,
                            self.voltdata_incoming_path,
                            self.copy_subfile_to_disk_timeout_sec,
                        )

                    elif CorrelatorMode.is_no_capture(
                        subfile_mode
                    ) or CorrelatorMode.is_voltage_buffer(subfile_mode):
                        self.logger.info(
                            f"{item}- ignoring due to mode: {subfile_mode}"
                        )

                        #
                        # This is our opportunity to write any "keep" files to disk
                        # which were held from a voltage buffer dump
                        #
                        if self.dump_keep_file_queue.qsize() > 0:
                            # Pause archiving
                            if (
                                self.subfile_distributor_context.cfg_corr_archive_destination_enabled  # pylint: disable=line-too-long
                            ):
                                self.subfile_distributor_context.archive_processor.pause_archiving(  # pylint: disable=line-too-long
                                    True
                                )

                            # Since we're not doing anything with this subfile we can
                            # try and handle any remaining keep files
                            self.handle_next_keep_file()
                        else:
                            # Unpause archiving
                            if (
                                self.subfile_distributor_context.cfg_corr_archive_destination_enabled  # pylint: disable=line-too-long
                            ):
                                self.subfile_distributor_context.archive_processor.pause_archiving(  # pylint: disable=line-too-long
                                    False
                                )

                        success = True

                    else:
                        self.logger.error(
                            f"{item}- Unknown subfile mode {subfile_mode},"
                            " ignoring."
                        )
                        success = False

                    # There is a semi-rare case where in between the top of this code and now
                    # a voltage trigger has been received. If so THIS subfile may not have
                    # been added to the keep list, so deal with it now
                    if (
                        (
                            self.dump_start_gps is not None
                            and self.dump_end_gps is not None
                        )
                        and subobs_id >= self.dump_start_gps
                        and subobs_id < self.dump_end_gps
                    ):
                        # Rename it now to .keep
                        keep_filename = item.replace(
                            self.ext_sub_file, self.ext_keep_file
                        )

                        os.rename(item, keep_filename)

                        # add it to the queue
                        # Lucky for us it will add it to the bottom.
                        self.dump_keep_file_queue.put(keep_filename)

                # Check if we need to clear the dump info
                if self.dump_end_gps is not None:
                    if subobs_id >= self.dump_end_gps:
                        # Reset the dump start and end
                        self.dump_start_gps = None
                        self.dump_end_gps = None

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
                    # 1. If mode is correlator or vcs or voltage buffer, then:
                    # 2. Inject the relevant beamformer keywords into the
                    # header of the sub file
                    # 3. load file into PSRDADA ringbuffer for beamformer input
                    # 4. Rename .sub file to .free so that udpgrab can reuse it
                    if (
                        CorrelatorMode.is_correlator(subfile_mode)
                        or CorrelatorMode.is_vcs(subfile_mode)
                        or CorrelatorMode.is_voltage_buffer(subfile_mode)
                    ):
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
                    self.ext_sub_file, self.ext_free_file
                )

                try:
                    # Check it exists first- the dump process
                    # may have renamed it already to .keep
                    if os.path.exists(item):
                        shutil.move(item, free_filename)

                except (
                    Exception
                ) as move_exception:  # pylint: disable=broad-except
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
        destination_filename: str = ".",
    ) -> bool:
        """Copies the given filename to the destination path"""
        self.logger.info(f"{filename}- Copying file into {destination_path}")

        command = f"cp {filename} {destination_path}/{destination_filename}"

        start_time = time.time()
        retval, stdout = mwax_command.run_command_ext(
            self.logger, command, numa_node, timeout, False
        )
        elapsed = time.time() - start_time

        if retval:
            self.logger.info(
                f"{filename}- Copying file into"
                f" {destination_path}/{destination_filename} was successful"
                f" (took {elapsed:.3f} secs)."
            )
        else:
            self.logger.error(
                f"{filename}- Copying file into"
                f" {destination_path}/{destination_filename} failed with error"
                f" {stdout}"
            )

        return retval

    def dump_voltages(self, start_gps_time: int, end_gps_time: int) -> bool:
        """Dump whatever subfiles we have from /dev/shm to disk"""
        # Set module level variables
        self.dump_start_gps = (
            start_gps_time  # note, this may be 0! meaning 'earliest'
        )
        self.dump_end_gps = end_gps_time

        self.logger.info(
            f"dump_voltages: from {str(start_gps_time)} to"
            f" {str(end_gps_time)}..."
        )

        # Look for any .free files which have the first 10 characters of
        # filename from starttime to endtime
        free_file_list = sorted(
            glob.glob(f"{self.subfile_incoming_path}/*{self.ext_free_file}")
        )

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
            self.logger.warning(
                "dump_voltages: not enough free files for voltage dump."
            )
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
                if (
                    utils.read_subfile_value(free_filename, utils.PSRDADA_MODE)
                    != CorrelatorMode.MWAX_VCS.value
                ):
                    self.logger.info(f"dump_voltages: keeping {free_filename}")

                    # For any that exist, rename them immediately to .keep
                    keep_filename = free_filename.replace(
                        self.ext_free_file, self.ext_keep_file
                    )
                    shutil.move(free_filename, keep_filename)

                    # append to queue so it can be copied off when in NO_CAPTURE mode
                    self.dump_keep_file_queue.put(keep_filename)
                else:
                    self.logger.info(
                        f"dump_voltages: NOT keeping {free_filename} as it is"
                        " a MWAX_VCS subobservation"
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
            "dump_keep_file_queue": self.dump_keep_file_queue.qsize(),
            "dump_start_gps": self.dump_start_gps,
            "dump_end_gps": self.dump_end_gps,
            "watchers": watcher_list,
            "workers": worker_list,
        }

        return return_status
