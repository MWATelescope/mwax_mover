"""Module for subfile processor"""

import glob
import os
import queue
import shutil
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME
from mwax_mover import utils
from mwax_mover.utils import CorrelatorMode, MWAXSubfileDistirbutorMode
from mwax_mover.mwax_wqw_subfile_incoming_processor import SubfileIncomingProcessor
from mwax_mover.mwax_wqw_packet_stats_processor import PacketStatsProcessor
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker, MWAXWatchQueueWorker


class SubfileProcessor:
    """Class representing the SubfileProcessor"""

    def __init__(
        self,
        context,
        subfile_incoming_path: str,
        voltdata_incoming_path: str,
        always_keep_subfiles: bool,
        corr_ringbuffer_key: str,
        corr_diskdb_numa_node,
        psrdada_timeout_sec: int,
        copy_subfile_to_disk_timeout_sec: int,
        mwax_stats_binary_dir: str,
        packet_stats_dump_dir: str,
        packet_stats_destination_dir: str,
        hostname: str,
        bf_redis_host: str,
        bf_redis_queue_key: str,
        bf_aocal_path: str,
        archive_destination_enabled: bool,
        metafits_path: str,
        subfile_dist_mode: MWAXSubfileDistirbutorMode,
    ):
        self.sd_ctx = context
        self.workers: list[MWAXPriorityWatchQueueWorker | MWAXWatchQueueWorker] = []

        # Setup logging
        self.logger = context.logger.getChild("SubfileProcessor")
        self.hostname = hostname

        self.ext_sub_file = ".sub"
        self.mwax_mover_mode = MODE_WATCH_DIR_FOR_RENAME
        self.subfile_incoming_path = subfile_incoming_path
        self.voltdata_incoming_path = voltdata_incoming_path
        self.always_keep_subfiles = always_keep_subfiles
        self.ext_free_file = ".free"
        self.ext_keep_file = ".keep"
        self.ext_packet_stats_file = ".dat"
        self.packet_stats_dump_dir = packet_stats_dump_dir
        self.packet_stats_destination_dir = packet_stats_destination_dir

        # Voltage buffer dump vars
        self.dump_start_gps = None
        self.dump_end_gps = None
        self.dump_trigger_id = None
        self.dump_keep_file_queue = queue.Queue()

        self.archive_destination_enabled = archive_destination_enabled
        self.metafits_path = metafits_path
        self.subfile_dist_mode = subfile_dist_mode

        # Each server will use a unique queue key. The queue key we read in the cfg file
        # is just the base- we then add the server name.
        # e.g. mwax25 will have a queue key of "bfq_mwax25"

        # Get the last 2 digits of the hostname
        bf_redis_queue_key = f"{bf_redis_queue_key}{hostname}"

        self.logger.debug(f"Using redis queue key: {bf_redis_queue_key}")

        # Create watch queue worker
        subfile_incoming_worker = SubfileIncomingProcessor(
            self.logger,
            self,
            self.sd_ctx.archive_processor,
            subfile_incoming_path,
            self.ext_sub_file,
            self.ext_free_file,
            self.ext_keep_file,
            voltdata_incoming_path,
            bf_aocal_path,
            bf_redis_host,
            bf_redis_queue_key,
            packet_stats_dump_dir,
            mwax_stats_binary_dir,
            1 if corr_diskdb_numa_node == 0 else 0,
            copy_subfile_to_disk_timeout_sec,
            corr_ringbuffer_key,
            corr_diskdb_numa_node,
            psrdada_timeout_sec,
            always_keep_subfiles,
            self.archive_destination_enabled,
            self.metafits_path,
            self.subfile_dist_mode,
        )
        self.workers.append(subfile_incoming_worker)

        if packet_stats_destination_dir != "" and packet_stats_dump_dir != "":
            packet_stats_worker = PacketStatsProcessor(
                self.logger, packet_stats_dump_dir, self.ext_packet_stats_file, packet_stats_destination_dir
            )
            self.workers.append(packet_stats_worker)

    def start(self):
        """Start the processor"""
        self.logger.info("Starting SubfileProcessor...")

        for w in self.workers:
            w.start()

        self.logger.info("SubfileProcessor started.")

    def stop(self):
        """Stop the processor"""
        for w in self.workers:
            w.stop()

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
        worker_list = []

        for w in self.workers:
            status = dict({"name": w.name})
            status.update(w.get_status())
            worker_list.append(status)

        return_status = {
            "type": type(self).__name__,
            "dump_keep_file_queue": self.dump_keep_file_queue.qsize(),
            "dump_start_gps": self.dump_start_gps,
            "dump_end_gps": self.dump_end_gps,
            "workers": worker_list,
        }

        return return_status
