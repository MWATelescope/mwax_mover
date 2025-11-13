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
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME, MODE_WATCH_DIR_FOR_NEW
from mwax_mover import utils, mwax_queue_worker, mwax_watcher, mwax_command
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
        mwax_stats_binary_dir: str,
        packet_stats_dump_dir: str,
        packet_stats_destination_dir: str,
        hostname: str,
    ):
        self.sd_ctx = context

        # Setup logging
        self.logger = logging.getLogger(__name__)
        # pass all logged events to the parent (subfile distributor/main log)
        self.logger.propagate = True
        self.logger.setLevel(logging.DEBUG)
        file_log = logging.FileHandler(
            filename=os.path.join(
                self.sd_ctx.cfg_log_path,
                f"{__name__}.log",
            )
        )
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        self.logger.addHandler(file_log)
        self.hostname = hostname

        self.ext_sub_file = ".sub"
        self.mwax_mover_mode = MODE_WATCH_DIR_FOR_RENAME
        self.packet_stats_mode = MODE_WATCH_DIR_FOR_NEW
        self.subfile_incoming_path = subfile_incoming_path
        self.voltdata_incoming_path = voltdata_incoming_path
        self.always_keep_subfiles = always_keep_subfiles
        self.ext_free_file = ".free"
        self.ext_keep_file = ".keep"
        self.ext_packet_stats_file = ".dat"

        # Voltage buffer dump vars
        self.dump_start_gps = None
        self.dump_end_gps = None
        self.dump_trigger_id = None
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
        # Numa node for ringbuffers
        self.corr_diskdb_numa_node = corr_diskdb_numa_node
        # Numa node for /dev/shm/mwax/*.free files (opposite of ringbuffers)
        self.corr_devshm_numa_node = 1 if corr_diskdb_numa_node == 0 else 0

        self.psrdada_timeout_sec = psrdada_timeout_sec
        self.copy_subfile_to_disk_timeout_sec = copy_subfile_to_disk_timeout_sec

        self.mwax_stats_binary_dir = mwax_stats_binary_dir
        self.packet_stats_dump_dir = packet_stats_dump_dir
        self.packet_stats_destination_dir = packet_stats_destination_dir
        self.packet_stats_destination_queue = queue.Queue()
        self.packet_stats_watcher = None
        self.packet_stats_queue_worker = None

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
        watcher_thread = threading.Thread(name="watch_sub", target=self.subfile_watcher.start, daemon=True)
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

        #
        # Setup watcher, queue, worker for transferring packet stats to destination (e.g vulcan)
        # But only if we have specified the destination in the config file (and the dump dir)
        #
        if self.packet_stats_destination_dir != "" and self.packet_stats_dump_dir != "":
            # Create watcher for the packet stats files
            self.packet_stats_watcher = mwax_watcher.Watcher(
                name="packet_stats_watcher",
                path=self.packet_stats_dump_dir,
                dest_queue=self.packet_stats_destination_queue,
                pattern=f"{self.ext_packet_stats_file}",
                log=self.logger,
                mode=self.packet_stats_mode,
                recursive=False,
            )

            # Create subfile queueworker
            self.packet_stats_destination_queue_worker = mwax_queue_worker.QueueWorker(
                name="Packet stats destination Queue",
                source_queue=self.packet_stats_destination_queue,
                executable_path=None,
                event_handler=self.packet_stats_destination_handler,
                log=self.logger,
                requeue_to_eoq_on_failure=False,
                exit_once_queue_empty=False,
            )

            # Setup thread for watching filesystem
            watcher_thread = threading.Thread(
                name="watch_packet_stats", target=self.packet_stats_watcher.start, daemon=True
            )
            self.watcher_threads.append(watcher_thread)
            watcher_thread.start()

            # Setup thread for processing subfile items
            queue_worker_thread = threading.Thread(
                name="work_packet_stats",
                target=self.packet_stats_destination_queue_worker.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_thread)
            queue_worker_thread.start()

    def packet_stats_destination_handler(self, item: str) -> bool:
        # This gets called by the queueworker who had put an "item"
        # on the queue from a watcher of the filesystem.
        # The "item" is the full path and filename of a packet stats data file
        # which now needs to be copied to the destination path (e.g. vulcan)
        # and the deleted once successful
        destination_filename: str = os.path.join(self.packet_stats_destination_dir, os.path.basename(item))

        try:
            self.logger.debug(f"{item}: Attempting to copy local packet stats file {item} to{destination_filename}")
            shutil.copy2(item, destination_filename)

            self.logger.debug(f"{item}: Copy success. Deleting local packet stats file {item}")

            # Success- now delete the file
            os.remove(item)

            self.logger.debug(f"{item}: Deleted local packet stats file {item}")

            return True
        except Exception:
            # Something went wrong- log it and requeue
            self.logger.exception(f"Unable to copy/delete {item} to {destination_filename}")
            return False

    def handle_next_keep_file(self) -> bool:
        """When we need to offload a keep file, this handles it"""
        success = True

        # Get next keep file off the queue
        keep_filename = self.dump_keep_file_queue.get()

        self.logger.info(f"SubfileProcessor.handle_next_keep_file is handling {keep_filename}...")

        # Read TRANSFER_SIZE from subfile header
        # We only use this when writing a subfile to disk in case the subfile is
        # bigger than the data
        transfer_size_str = utils.read_subfile_value(keep_filename, utils.PSRDADA_TRANSFER_SIZE)
        if transfer_size_str is None:
            raise ValueError(f"Keyword {utils.PSRDADA_TRANSFER_SIZE} not found in {keep_filename}")

        transfer_size = int(transfer_size_str)
        subfile_bytes_to_write = transfer_size + 4096  # We add the header to the transfer size

        # Copy the .keep file to the voltdata incoming dir
        # and ensure it is named as a ".sub" file
        copy_success = self.copy_subfile_to_disk_dd(
            keep_filename,
            self.corr_diskdb_numa_node,
            self.voltdata_incoming_path,
            self.copy_subfile_to_disk_timeout_sec,
            os.path.basename(keep_filename).replace(self.ext_keep_file, self.ext_sub_file),
            subfile_bytes_to_write,
        )

        if copy_success:
            # Rename kept subfile so that mwax_u2s can reuse it
            free_filename = keep_filename.replace(self.ext_keep_file, self.ext_free_file)

            try:
                shutil.move(keep_filename, free_filename)
            except Exception as move_exception:  # pylint: disable=broad-except
                self.logger.error(f"Could not rename {keep_filename} back to {free_filename}. Error {move_exception}")
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

        self.logger.info(f"{item}- SubfileProcessor.subfile_handler is handling {item}...")

        handler_starttime = time.time()

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

        # Read all the things from the subfile header here!
        subfile_header_values = utils.read_subfile_values(
            item,
            [
                utils.PSRDADA_SUBOBS_ID,
                utils.PSRDADA_TRANSFER_SIZE,
                utils.PSRDADA_MODE,
            ],
        )

        # Validate each one
        if subfile_header_values[utils.PSRDADA_SUBOBS_ID] is None:
            raise ValueError(f"Keyword {utils.PSRDADA_SUBOBS_ID} not found in {item}")
        subobs_id = int(subfile_header_values[utils.PSRDADA_SUBOBS_ID])

        # Read TRANSFER_SIZE from subfile header
        # We only use this when writing a subfile to disk in case the subfile is
        # bigger than the data
        # transfer_size_str = utils.read_subfile_value(item, utils.PSRDADA_TRANSFER_SIZE)
        if subfile_header_values[utils.PSRDADA_TRANSFER_SIZE] is None:
            raise ValueError(f"Keyword {utils.PSRDADA_TRANSFER_SIZE} not found in {item}")
        transfer_size = int(subfile_header_values[utils.PSRDADA_TRANSFER_SIZE])
        subfile_bytes_to_write = transfer_size + 4096  # We add the header to the transfer size

        # Get Mode
        if subfile_header_values[utils.PSRDADA_MODE] is None:
            raise ValueError(f"Keyword {utils.PSRDADA_MODE} not found in {item}")
        subfile_mode = subfile_header_values[utils.PSRDADA_MODE]

        # Only do packet stats if packet_stats_dump_dir is not an empty string
        if self.packet_stats_dump_dir != "":
            # For all subfiles we need to extract the packet stats:
            # Ignore failures
            utils.run_mwax_packet_stats(
                self.logger, self.mwax_stats_binary_dir, item, self.packet_stats_dump_dir, -1, 3
            )

        try:
            if self.corr_enabled:
                #
                # We need to check we are not in a voltage dump. If so, whatever
                # observation is happening is ignored and instead we treat this
                # like a VCS observation
                #
                if (
                    (self.dump_start_gps is not None and self.dump_end_gps is not None)
                    and subobs_id >= self.dump_start_gps
                    and subobs_id < self.dump_end_gps
                ):
                    # We ARE in voltage dump and so we go and do a VCS capture instead
                    # of whatever else we were doing
                    self.logger.info(
                        f"{item}- ignoring existing mode: {subfile_mode} as we"
                        f" are within a voltage dump ({self.dump_start_gps} <"
                        f" {self.dump_end_gps}) for trigger {self.dump_trigger_id}."
                        " Doing VCS instead."
                    )

                    # Pause archiving so we have the disk to ourselves
                    if self.sd_ctx.cfg_corr_archive_destination_enabled:  # pylint: disable=line-too-long
                        self.sd_ctx.archive_processor.pause_archiving(True)  # pylint: disable=line-too-long

                    # See if there already is a TRIGGER_ID keyword in the subfile- if so
                    # don't overwrite it. We must have overlapping triggers happening
                    if not utils.read_subfile_trigger_value(item):
                        # No TRIGGER_ID yet, so add it
                        self.logger.info(
                            f"{item}- injecting {utils.PSRDADA_TRIGGER_ID} {self.dump_trigger_id} into subfile..."
                        )
                        utils.inject_subfile_header(item, f"{utils.PSRDADA_TRIGGER_ID} {self.dump_trigger_id}\n")

                    success = self.copy_subfile_to_disk_dd(
                        item,
                        self.corr_devshm_numa_node,
                        self.voltdata_incoming_path,
                        self.copy_subfile_to_disk_timeout_sec,
                        os.path.split(item)[1],
                        subfile_bytes_to_write,
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
                        if self.sd_ctx.cfg_corr_archive_destination_enabled:  # pylint: disable=line-too-long
                            self.sd_ctx.archive_processor.pause_archiving(False)  # pylint: disable=line-too-long

                        success = utils.load_psrdada_ringbuffer(
                            self.logger,
                            item,
                            self.corr_ringbuffer_key,
                            -1,
                            self.psrdada_timeout_sec,
                        )

                        if self.always_keep_subfiles:
                            keep_subfiles_path = self.voltdata_incoming_path

                    elif CorrelatorMode.is_vcs(subfile_mode):
                        # Pause archiving so we have the disk to ourselves
                        if self.sd_ctx.cfg_corr_archive_destination_enabled:  # pylint: disable=line-too-long
                            self.sd_ctx.archive_processor.pause_archiving(True)  # pylint: disable=line-too-long

                        success = self.copy_subfile_to_disk_dd(
                            item,
                            self.corr_devshm_numa_node,
                            self.voltdata_incoming_path,
                            self.copy_subfile_to_disk_timeout_sec,
                            os.path.split(item)[1],
                            subfile_bytes_to_write,
                        )

                    elif CorrelatorMode.is_no_capture(subfile_mode) or CorrelatorMode.is_voltage_buffer(subfile_mode):
                        self.logger.info(f"{item}- ignoring due to mode: {subfile_mode}")

                        #
                        # This is our opportunity to write any "keep" files to disk
                        # which were held from a voltage buffer dump
                        #
                        if self.dump_keep_file_queue.qsize() > 0:
                            # Pause archiving
                            if self.sd_ctx.cfg_corr_archive_destination_enabled:  # pylint: disable=line-too-long
                                self.sd_ctx.archive_processor.pause_archiving(True)  # pylint: disable=line-too-long

                            # Since we're not doing anything with this subfile we can
                            # try and handle any remaining keep files
                            self.handle_next_keep_file()
                        else:
                            # Unpause archiving
                            if self.sd_ctx.cfg_corr_archive_destination_enabled:  # pylint: disable=line-too-long
                                self.sd_ctx.archive_processor.pause_archiving(False)  # pylint: disable=line-too-long

                        success = True

                    else:
                        self.logger.error(f"{item}- Unknown subfile mode {subfile_mode}, ignoring.")
                        success = True

                    # There is a semi-rare case where in between the top of this code and now
                    # a voltage trigger has been received. If so THIS subfile may not have been added to
                    # the keep list, so deal with it now

                    # Commenting out below for now as it is not necessary I think...
                    # if (
                    #    (
                    #        self.dump_start_gps is not None
                    #        and self.dump_end_gps is not None
                    #    )
                    #    and subobs_id >= self.dump_start_gps
                    #    and subobs_id < self.dump_end_gps
                    # ):
                    #    # Rename it now to .keep
                    #    keep_filename = item.replace(
                    #        self.ext_sub_file, self.ext_keep_file
                    #    )
                    #
                    #    os.rename(item, keep_filename)
                    #
                    #    # add it to the queue
                    #    # Lucky for us it will add it to the bottom.
                    #    self.dump_keep_file_queue.put(keep_filename)

                # Check if we need to clear the dump info
                if self.dump_end_gps is not None:
                    if subobs_id >= self.dump_end_gps:
                        # Reset the dump start and end
                        self.dump_start_gps = None
                        self.dump_end_gps = None
                        self.dump_trigger_id = None

            if self.bf_enabled:
                # Don't run beamformer if we are in correlator mode too and we
                # are doing a voltage capture!
                if self.corr_enabled and CorrelatorMode.is_vcs(subfile_mode):
                    self.logger.warning(
                        f"{item}- beamformer mode enabled and is in {subfile_mode} mode, ignoring this beamformer job."
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
                        with open(self.bf_settings_path, "r", encoding="utf-8") as bf_settings_file:
                            beamformer_settings = bf_settings_file.read()

                        # It is possible the beamformer settings are already
                        # added into the sub file (e.g. a failed load into ringbuffer)
                        # So check first, before appending them again!
                        if utils.read_subfile_value(item, "NUM_INCOHERENT_BEAMS") is None:
                            self.logger.info(f"{item}- injecting beamformer header into subfile...")
                            utils.inject_beamformer_headers(item, beamformer_settings)
                        else:
                            self.logger.info(f"{item}- beamformer header exists in subfile.")

                        success = utils.load_psrdada_ringbuffer(
                            self.logger,
                            item,
                            self.bf_ringbuffer_key,
                            -1,
                            self.psrdada_timeout_sec,
                        )

                        if self.always_keep_subfiles:
                            keep_subfiles_path = self.voltdata_incoming_path

                    elif CorrelatorMode.is_no_capture(subfile_mode):
                        self.logger.info(f"{item}- ignoring due to mode: {subfile_mode}")
                        success = True

                    else:
                        self.logger.error(f"{item}- Unknown subfile mode {subfile_mode}, ignoring.")
                        success = True

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
                    self.copy_subfile_to_disk_dd(
                        item,
                        self.corr_devshm_numa_node,
                        keep_subfiles_path,
                        self.copy_subfile_to_disk_timeout_sec,
                        os.path.split(item)[1],
                        subfile_bytes_to_write,
                    )

                # Rename subfile so that udpgrab can reuse it
                free_filename = str(item).replace(self.ext_sub_file, self.ext_free_file)

                try:
                    # Check it exists first- the dump process
                    # may have renamed it already to .keep
                    try:
                        shutil.move(item, free_filename)
                    except FileNotFoundError:
                        pass

                except Exception as move_exception:  # pylint: disable=broad-except
                    self.logger.error(
                        f"{item}- Could not rename {item} back to {free_filename}. Error {move_exception}"
                    )
                    sys.exit(2)

            handler_elapsed = time.time() - handler_starttime

            self.logger.info(
                f"{item}- SubfileProcessor.subfile_handler finished handling in {handler_elapsed:.3f} secs."
            )

        return success

    def stop(self):
        """Stop the processor"""
        if self.subfile_watcher:
            self.subfile_watcher.stop()

        if self.subfile_queue_worker:
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

    def copy_subfile_to_disk_cp(
        self,
        filename: str,
        numa_node: int,
        destination_path: str,
        timeout: int,
        destination_filename: str = ".",
    ) -> bool:
        """Copies the given filename to the destination path
        DEPRECATED FOR NOW- replaced with copy_subfile_to_disk_dd()"""
        self.logger.info(f"{filename}- Copying file into {destination_path}")

        command = f"cp {filename} {destination_path}/{destination_filename}"

        start_time = time.time()
        retval, stdout = mwax_command.run_command_ext(self.logger, command, numa_node, timeout, False)
        elapsed = time.time() - start_time

        if retval:
            self.logger.info(
                f"{filename}- Copying file into"
                f" {destination_path}/{destination_filename} was successful"
                f" (took {elapsed:.3f} secs)."
            )
        else:
            self.logger.error(
                f"{filename}- Copying file into {destination_path}/{destination_filename} failed with error {stdout}"
            )

        return retval

    def copy_subfile_to_disk_dd(
        self,
        filename: str,
        numa_node: int,
        destination_path: str,
        timeout: int,
        destination_filename: str,
        bytes_to_write: int,
    ) -> bool:
        """Copies the given filename to the destination path, trimming X bytes off the end of the file
        This is for the case where the subfiles are sized for e.g. 144T but the observation is only
        128T. When copied, dd will only copy the 128T worth of data (u2s would have already ensured
        that the data written is only 128T worth and that the remaining space is 'empty')

        filename: Abs or relative path and filename
        destination_path: Just the destination directory
        destination_filename: Just the destination filename (no path)- note- unlike with cp it cannot
                            be just a "."! dd doesn't like that - it has to be a real filename.
        bytes_to_write: only write the first N bytes"""
        self.logger.debug(f"{filename}- Copying first {bytes_to_write} bytes of file into {destination_path}")

        command = f"dd if={filename} of={destination_path}/{destination_filename} bs=4M oflag=direct iflag=count_bytes count={bytes_to_write}"

        start_time = time.time()
        retval, stdout = mwax_command.run_command_ext(self.logger, command, numa_node, timeout, False)

        if retval:
            elapsed = time.time() - start_time
            speed = (bytes_to_write / elapsed) / (1000.0 * 1000.0 * 1000.0)

            self.logger.info(
                f"{filename}- Copying first {bytes_to_write} bytes of file into"
                f" {destination_path}/{destination_filename} was successful"
                f" (took {elapsed:.3f} secs at {speed:.3f} GB/sec)."
            )
        else:
            self.logger.error(
                f"{filename}- Copying first {bytes_to_write} bytes of file into"
                f" {destination_path}/{destination_filename} failed with error"
                f" {stdout}"
            )

        return retval

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
        free_file_list = sorted(glob.glob(f"{self.subfile_incoming_path}/*{self.ext_free_file}"))

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
                if utils.read_subfile_value(free_filename, utils.PSRDADA_MODE) != CorrelatorMode.MWAX_VCS.value:
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
                    keep_filename = free_filename.replace(self.ext_free_file, self.ext_keep_file)
                    shutil.move(free_filename, keep_filename)

                    # append to queue so it can be copied off when in NO_CAPTURE mode
                    self.dump_keep_file_queue.put(keep_filename)
                else:
                    self.logger.info(f"dump_voltages: NOT keeping {free_filename} as it is a MWAX_VCS subobservation")

        self.logger.info("dump_voltages: complete")
        return True

    def get_status(self) -> dict:
        """Return status as a dictionary"""
        watcher_list = []

        if self.subfile_watcher:
            status = dict({"Unix timestamp": time.time(), "name": "subfile watcher"})
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
