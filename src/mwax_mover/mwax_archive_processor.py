"""Module file for MWAXArchiveProcessor"""

import glob
import logging
import logging.handlers
import os
import queue
import sys
import threading
import time
from mwax_mover import (
    mwax_mover,
    mwax_db,
    mwax_queue_worker,
    mwax_priority_queue_worker,
    mwax_priority_watcher,
    mwax_watcher,
    mwa_archiver,
    utils,
)
from mwax_mover.mwax_watcher import Watcher
from mwax_mover.mwax_queue_worker import QueueWorker
from mwax_mover.mwax_priority_watcher import PriorityWatcher
from mwax_mover.mwax_priority_queue_worker import PriorityQueueWorker
from mwax_mover.utils import MWADataFileType, ValidationData


class MWAXArchiveProcessor:
    """
    A class representing an instance which sends
    MWAX data products to the mwacache servers.
    """

    def __init__(
        self,
        context,
        hostname: str,
        archive_destination_enabled: int,
        archive_command_numa_node: int,
        archive_host: str,
        archive_port: int,
        archive_command_timeout_sec: int,
        mwax_stats_binary_dir: str,
        mwax_stats_dump_dir: str,
        mwax_stats_timeout_sec: int,
        db_handler_object: mwax_db.MWAXDBHandler,
        voltdata_incoming_path: str,
        voltdata_outgoing_path: str,
        visdata_incoming_path: str,
        visdata_processing_stats_path: str,
        visdata_outgoing_path: str,
        visdata_cal_outgoing_path: str,
        calibrator_destination_enabled: int,
        metafits_path: str,
        visdata_dont_archive_path: str,
        voltdata_dont_archive_path: str,
        high_priority_correlator_projectids: list[str],
        high_priority_vcs_projectids: list[str],
    ):
        self.sd_ctx = context

        # Setup logging
        self.logger: logging.Logger = logging.getLogger(__name__)
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

        self.db_handler_object: mwax_db.MWAXDBHandler = db_handler_object

        self.hostname: str = hostname
        self.archive_destination_enabled: int = archive_destination_enabled
        self.archive_destination_host: str = archive_host
        self.archive_destination_port: int = archive_port
        self.archive_command_numa_node: int = archive_command_numa_node
        self.archive_command_timeout_sec: int = archive_command_timeout_sec

        # Full path to executable for mwax_stats
        self.mwax_stats_binary_dir: str = mwax_stats_binary_dir
        # Directory where to dump the stats files
        self.mwax_stats_dump_dir: str = mwax_stats_dump_dir
        self.mwax_stats_timeout_sec: int = mwax_stats_timeout_sec

        self.archiving_paused: bool = False

        self.watchers: list[PriorityWatcher | Watcher] = []
        self.watcher_threads: list[threading.Thread] = []

        self.workers: list[PriorityQueueWorker | QueueWorker] = []
        self.worker_threads: list[threading.Thread] = []

        self.dont_archive_path_vis: str = visdata_dont_archive_path
        self.queue_dont_archive_vis: queue.Queue = queue.Queue()

        self.dont_archive_path_volt: str = voltdata_dont_archive_path
        self.queue_dont_archive_volt: queue.Queue = queue.Queue()

        self.queue_checksum_and_db: queue.PriorityQueue = queue.PriorityQueue()

        self.watch_dir_incoming_volt: str = voltdata_incoming_path
        self.watch_dir_incoming_vis: str = visdata_incoming_path

        self.watch_dir_processing_stats_vis: str = visdata_processing_stats_path
        self.queue_processing_stats_vis: queue.Queue = queue.Queue()

        self.watch_dir_outgoing_volt: str = voltdata_outgoing_path
        self.watch_dir_outgoing_vis: str = visdata_outgoing_path
        self.queue_outgoing: queue.PriorityQueue = queue.PriorityQueue()

        self.watch_dir_outgoing_cal: str = visdata_cal_outgoing_path
        # this queue is simply for health stats- we don't actually "process"
        # files in the cal_outgoing_path- calvin servers take the files
        self.queue_outgoing_cal: queue.Queue = queue.Queue()
        # Since our watcher needs a queue, we'll just get the queue to dump the filenames
        # into this list so we can easily remove them when release_cal_obs is called
        # by a calvin
        self.outgoing_cal_list: list[str] = []

        self.calibrator_destination_enabled: int = calibrator_destination_enabled

        self.metafits_path: str = metafits_path
        self.list_of_correlator_high_priority_projects: list[str] = high_priority_correlator_projectids
        self.list_of_vcs_high_priority_projects: list[str] = high_priority_vcs_projectids

    def start(self):
        """This method is used to start the processor"""
        if self.archive_destination_enabled:
            # Create watcher for voltage data -> checksum+db queue
            watcher_incoming_volt = mwax_priority_watcher.PriorityWatcher(
                name="watcher_incoming_volt",
                path=self.watch_dir_incoming_volt,
                dest_queue=self.queue_checksum_and_db,
                pattern=".sub",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
                metafits_path=self.metafits_path,
                list_of_correlator_high_priority_projects=self.list_of_correlator_high_priority_projects,
                list_of_vcs_high_priority_projects=self.list_of_vcs_high_priority_projects,
                recursive=False,
            )
            self.watchers.append(watcher_incoming_volt)

            # Create watcher for visibility data -> checksum+db queue
            # This will watch for mwax visibilities being renamed OR
            # fits files being created
            # (e.g. metafits ppd files being copied into /visdata).
            watcher_incoming_vis = mwax_priority_watcher.PriorityWatcher(
                name="watcher_incoming_vis",
                path=self.watch_dir_incoming_vis,
                dest_queue=self.queue_checksum_and_db,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
                metafits_path=self.metafits_path,
                list_of_correlator_high_priority_projects=self.list_of_correlator_high_priority_projects,
                list_of_vcs_high_priority_projects=self.list_of_vcs_high_priority_projects,
                recursive=False,
            )
            self.watchers.append(watcher_incoming_vis)

            # Create watcher for visibility processing stats
            watcher_processing_stats_vis = mwax_watcher.Watcher(
                name="watcher_processing_stats_vis",
                path=self.watch_dir_processing_stats_vis,
                dest_queue=self.queue_processing_stats_vis,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                recursive=False,
            )
            self.watchers.append(watcher_processing_stats_vis)

            # Create watcher for calibration data
            # (only for health stats)
            watcher_outgoing_cal = mwax_watcher.Watcher(
                name="watcher_outgoing_cal",
                path=self.watch_dir_outgoing_cal,
                dest_queue=self.queue_outgoing_cal,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                recursive=False,
            )
            self.watchers.append(watcher_outgoing_cal)

            # Create watcher for archiving outgoing voltage data
            watcher_outgoing_volt = mwax_priority_watcher.PriorityWatcher(
                name="watcher_outgoing_volt",
                path=self.watch_dir_outgoing_volt,
                dest_queue=self.queue_outgoing,
                pattern=".sub",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                metafits_path=self.metafits_path,
                list_of_correlator_high_priority_projects=self.list_of_correlator_high_priority_projects,
                list_of_vcs_high_priority_projects=self.list_of_vcs_high_priority_projects,
                recursive=False,
            )
            self.watchers.append(watcher_outgoing_volt)

            # Create watcher for archiving outgoing visibility data
            watcher_outgoing_vis = mwax_priority_watcher.PriorityWatcher(
                name="watcher_outgoing_vis",
                path=self.watch_dir_outgoing_vis,
                dest_queue=self.queue_outgoing,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                metafits_path=self.metafits_path,
                list_of_correlator_high_priority_projects=self.list_of_correlator_high_priority_projects,
                list_of_vcs_high_priority_projects=self.list_of_vcs_high_priority_projects,
                recursive=False,
            )
            self.watchers.append(watcher_outgoing_vis)

            #
            # Create watcher threads
            #

            # Setup thread for watching incoming filesystem (volt)
            watcher_volt_incoming_thread = threading.Thread(
                name="watch_volt_incoming",
                target=watcher_incoming_volt.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_volt_incoming_thread)

            # Setup thread for watching incoming filesystem (vis)
            watcher_vis_incoming_thread = threading.Thread(
                name="watch_vis_incoming",
                target=watcher_incoming_vis.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_vis_incoming_thread)

            # Setup thread for watching processing_stats filesystem (vis)
            watcher_vis_processing_stats_thread = threading.Thread(
                name="watch_vis_processing_stats",
                target=watcher_processing_stats_vis.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_vis_processing_stats_thread)

            # Setup thread for watching cal_outgoing filesystem
            watcher_cal_outgoing_thread = threading.Thread(
                name="watch_cal_outgoing",
                target=watcher_outgoing_cal.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_cal_outgoing_thread)

            # Setup thread for watching outgoing filesystem (volt)
            watcher_volt_outgoing_thread = threading.Thread(
                name="watch_volt_outgoing",
                target=watcher_outgoing_volt.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_volt_outgoing_thread)

            # Setup thread for watching outgoing filesystem (vis)
            watcher_vis_outgoing_thread = threading.Thread(
                name="watch_vis_outgoing",
                target=watcher_outgoing_vis.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_vis_outgoing_thread)

            #
            # Create Workers
            #

            # Create queueworker for the checksum and db queue
            queue_worker_checksum_and_db = mwax_priority_queue_worker.PriorityQueueWorker(
                name="checksum and database worker",
                source_queue=self.queue_checksum_and_db,
                executable_path=None,
                event_handler=self.checksum_and_db_handler,
                log=self.logger,
                exit_once_queue_empty=False,
            )
            self.workers.append(queue_worker_checksum_and_db)

            # worker for visibility processing stats
            queue_worker_processing_stats_vis = mwax_queue_worker.QueueWorker(
                name="processing stats vis worker",
                source_queue=self.queue_processing_stats_vis,
                executable_path=None,
                event_handler=self.stats_handler,
                log=self.logger,
                exit_once_queue_empty=False,
            )
            self.workers.append(queue_worker_processing_stats_vis)

            # worker for cal_outgoing
            queue_worker_cal_outgoing = mwax_queue_worker.QueueWorker(
                name="outgoing cal vis worker",
                source_queue=self.queue_outgoing_cal,
                executable_path=None,
                event_handler=self.cal_handler,
                log=self.logger,
                exit_once_queue_empty=False,
            )
            self.workers.append(queue_worker_cal_outgoing)

            # Create queueworker for outgoing queue
            queue_worker_outgoing = mwax_priority_queue_worker.PriorityQueueWorker(
                name="outgoing worker",
                source_queue=self.queue_outgoing,
                executable_path=None,
                event_handler=self.archive_handler,
                log=self.logger,
                exit_once_queue_empty=False,
            )
            self.workers.append(queue_worker_outgoing)

            #
            # Setup queue worker threads
            #
            # Setup thread for processing items on the checksum and db queue
            queue_worker_checksum_and_db_thread = threading.Thread(
                name="work_checksum_and_db",
                target=queue_worker_checksum_and_db.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_checksum_and_db_thread)

            # Setup thread for processing items on the
            # processing stats vis queue
            queue_worker_vis_processing_stats_thread = threading.Thread(
                name="work_vis_processing_stats",
                target=queue_worker_processing_stats_vis.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_vis_processing_stats_thread)

            # Setup thread for processing items on the
            # cal outgoing queue
            queue_worker_cal_outgoing_thread = threading.Thread(
                name="work_vis_processing_stats",
                target=queue_worker_cal_outgoing.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_cal_outgoing_thread)

            # Setup thread for processing items on the outgoing queue
            queue_worker_outgoing_thread = threading.Thread(
                name="work_outgoing",
                target=queue_worker_outgoing.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_outgoing_thread)

        else:
            # We have disabled archiving, so use a different
            # handler for incoming data
            # which just moves the files elsewhere

            # First check to ensure there are no existing unarchived files on
            # our watching dirs
            if (
                len(next(os.walk(self.watch_dir_incoming_volt))[2]) > 0
                or len(next(os.walk(self.watch_dir_incoming_vis))[2]) > 0
                or len(next(os.walk(self.watch_dir_outgoing_volt))[2]) > 0
                or len(next(os.walk(self.watch_dir_outgoing_vis))[2]) > 0
                or len(next(os.walk(self.watch_dir_outgoing_cal))[2]) > 0
                or len(next(os.walk(self.watch_dir_processing_stats_vis))[2]) > 0
            ):
                self.logger.error(
                    "Error- voltage incoming/outgoing and/or visibility "
                    "incoming/processing/outgoing/cal dirs are not empty! "
                    "Watched paths must be empty before starting with  "
                    "archiving disabled to prevent inadvertent data loss. "
                    "Exiting."
                )
                sys.exit(-2)

            # Create watcher for voltage data -> dont_archive queue
            watcher_incoming_volt = mwax_watcher.Watcher(
                name="watcher_incoming_volt",
                path=self.watch_dir_incoming_volt,
                dest_queue=self.queue_dont_archive_volt,
                pattern=".sub",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
                recursive=False,
            )
            self.watchers.append(watcher_incoming_volt)

            # Create watcher for visibility data -> dont_archive queue
            # This will watch for mwax visibilities being renamed OR
            # fits files being created (e.g. metafits ppd files being copied
            # into /visdata).
            watcher_incoming_vis = mwax_watcher.Watcher(
                name="watcher_incoming_vis",
                path=self.watch_dir_incoming_vis,
                dest_queue=self.queue_dont_archive_vis,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
                recursive=False,
            )
            self.watchers.append(watcher_incoming_vis)

            # Create queueworker for the vis don't archive queue
            queue_worker_dont_archive_vis = mwax_queue_worker.QueueWorker(
                name="dont archive worker (vis)",
                source_queue=self.queue_dont_archive_vis,
                executable_path=None,
                event_handler=self.dont_archive_handler_vis,
                log=self.logger,
                exit_once_queue_empty=False,
            )
            self.workers.append(queue_worker_dont_archive_vis)

            # Create queueworker for the volt don't archive queue
            queue_worker_dont_archive_volt = mwax_queue_worker.QueueWorker(
                name="dont archive worker (volt)",
                source_queue=self.queue_dont_archive_volt,
                executable_path=None,
                event_handler=self.dont_archive_handler_volt,
                log=self.logger,
                exit_once_queue_empty=False,
            )
            self.workers.append(queue_worker_dont_archive_volt)

            #
            # Create watcher threads
            #

            # Setup thread for watching incoming filesystem (volt)
            watcher_volt_incoming_thread = threading.Thread(
                name="watch_volt_incoming",
                target=watcher_incoming_volt.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_volt_incoming_thread)

            # Setup thread for watching incoming filesystem (vis)
            watcher_vis_incoming_thread = threading.Thread(
                name="watch_vis_incoming",
                target=watcher_incoming_vis.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_vis_incoming_thread)

            # Setup thread for processing items on the dont archive queue
            queue_worker_dont_archive_thread_vis = threading.Thread(
                name="work_dont_archive_vis",
                target=queue_worker_dont_archive_vis.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_dont_archive_thread_vis)

            # Setup thread for processing items on the dont archive queue
            queue_worker_dont_archive_thread_volt = threading.Thread(
                name="work_dont_archive_volt",
                target=queue_worker_dont_archive_volt.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_dont_archive_thread_volt)

        #
        # Start Watcher threads
        #
        for watcher_thread in self.watcher_threads:
            if watcher_thread:
                watcher_thread.start()

        self.logger.info("Waiting for all watchers to finish scanning....")
        count_of_watchers_still_scanning = len(self.watchers)
        while count_of_watchers_still_scanning > 0:
            count_of_watchers_still_scanning = 0
            for watcher in self.watchers:
                if not watcher.scan_completed:
                    count_of_watchers_still_scanning += 1
            time.sleep(1)  # hold off for another second
        self.logger.info("Watchers are finished scanning.")

        #
        # Start worker threads
        #
        for worker_thread in self.worker_threads:
            if worker_thread:
                worker_thread.start()

    def dont_archive_handler_vis(self, item: str) -> bool:
        """This handles the visibility case where we have disabled archiving"""
        self.logger.debug(f"{item}- dont_archive_handler_vis() Started")

        outgoing_filename = os.path.join(self.dont_archive_path_vis, os.path.basename(item))

        self.logger.debug(f"{item}- dont_archive_handler_vis() moving file to" f" {self.dont_archive_path_vis}")
        os.rename(item, outgoing_filename)

        self.logger.info(
            f"{item}- dont_archive_handler_vis() moved file to"
            f" {self.dont_archive_path_vis} dir Queue size:"
            f" {self.queue_dont_archive_vis.qsize()}"
        )

        self.logger.debug(f"{item}- dont_archive_handler_vis() Finished")
        return True

    def dont_archive_handler_volt(self, item: str) -> bool:
        """This handles the voltage case where we have disabled archiving"""
        self.logger.debug(f"{item}- dont_archive_handler_volt() Started")

        outgoing_filename = os.path.join(self.dont_archive_path_volt, os.path.basename(item))

        self.logger.debug(f"{item}- dont_archive_handler_volt() moving file to" f" {self.dont_archive_path_volt}")
        os.rename(item, outgoing_filename)

        self.logger.info(
            f"{item}- dont_archive_handler_volt() moved file to"
            f" {self.dont_archive_path_volt} dir Queue size:"
            f" {self.queue_dont_archive_volt.qsize()}"
        )

        self.logger.debug(f"{item}- dont_archive_handler_volt() Finished")
        return True

    def checksum_and_db_handler(self, item: str) -> bool:
        """This is the first handler executed when we process a new file"""
        self.logger.info(f"{item}- checksum_and_db_handler() Started")

        # validate the filename
        val: ValidationData = utils.validate_filename(self.logger, item, self.metafits_path)

        if val.valid:
            try:
                # Determine file size
                file_size = os.stat(item).st_size

                # checksum then add this file to the db so we insert a record into
                # metadata data_files table
                checksum_type_id: int = 1  # MD5
                checksum: str = utils.do_checksum_md5(self.logger, item, int(self.archive_command_numa_node), 180)

                # if file is a VCS subfile, check if it is from a trigger and
                # grab the trigger_id as an int
                # If not found it will return None
                trigger_id = utils.read_subfile_trigger_value(item)
            except FileNotFoundError:
                # The filename was not valid
                self.logger.warning(f"{item}- checksum_and_db_handler() file was removed while" " processing.")
                return True

            # Insert record into metadata database
            if not mwax_db.insert_data_file_row(
                self.db_handler_object,
                val.obs_id,
                item,
                val.filetype_id,
                self.hostname,
                checksum_type_id,
                checksum,
                trigger_id,
                file_size,
            ):
                # if something went wrong, requeue
                return False

            # Check to see if the file is still there- the above call could have deleted it
            # if we got an FK error
            if not os.path.exists(item):
                # Return True, telling the queue worker we are done with this item
                return True

            #
            # If the project_id is the special code C123 then
            # do not archive it.
            #
            if utils.should_project_be_archived(val.project_id):
                # immediately add this file (and a ptr to it's queue) to the
                # voltage or vis queue which will deal with archiving
                if val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
                    # move to voltdata/outgoing
                    # Take the input filename - strip the path, then append the
                    # output path
                    outgoing_filename = os.path.join(self.watch_dir_outgoing_volt, os.path.basename(item))

                    self.logger.debug(f"{item}- checksum_and_db_handler() moving subfile to volt" " outgoing dir")
                    os.rename(item, outgoing_filename)

                    self.logger.info(
                        f"{item}- checksum_and_db_handler() moved subfile to volt"
                        " outgoing dir Queue size:"
                        f" {self.queue_checksum_and_db.qsize()}"
                    )
                elif val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
                    # move to visdata/processing_stats
                    # Take the input filename - strip the path, then append the
                    # output path
                    outgoing_filename = os.path.join(self.watch_dir_processing_stats_vis, os.path.basename(item))

                    self.logger.debug(
                        f"{item}- checksum_and_db_handler() moving visibility file" " to vis processing stats dir"
                    )
                    os.rename(item, outgoing_filename)

                    self.logger.info(
                        f"{item}- checksum_and_db_handler() moved visibility file"
                        " to vis processing stats dir Queue size:"
                        f" {self.queue_checksum_and_db.qsize()}"
                    )

                elif val.filetype_id == MWADataFileType.MWA_PPD_FILE.value:
                    # move to visdata/outgoing
                    # Take the input filename - strip the path, then append the
                    # output path
                    outgoing_filename = os.path.join(self.watch_dir_outgoing_vis, os.path.basename(item))

                    self.logger.debug(f"{item}- checksum_and_db_handler() moving metafits file" " to vis outgoing dir")
                    os.rename(item, outgoing_filename)

                    self.logger.info(
                        f"{item}- checksum_and_db_handler() moved metafits file to"
                        " vis outgoing dir Queue size:"
                        f" {self.queue_checksum_and_db.qsize()}"
                    )
                else:
                    self.logger.error(
                        f"{item}- checksum_and_db_handler() - not a valid file"
                        f" extension {val.filetype_id} / {val.file_ext}"
                    )
                    return False
            else:
                # This is a "do not archive" project
                # If it is visibilities, then produce stats, otherwise move to dont_archive
                if val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
                    # move to visdata/processing_stats
                    # Take the input filename - strip the path, then append the
                    # output path
                    outgoing_filename = os.path.join(self.watch_dir_processing_stats_vis, os.path.basename(item))

                    self.logger.debug(
                        f"{item}- checksum_and_db_handler() moving visibility file" " to vis processing stats dir"
                    )
                    os.rename(item, outgoing_filename)

                    self.logger.info(
                        f"{item}- checksum_and_db_handler() moved visibility file"
                        " to vis processing stats dir Queue size:"
                        f" {self.queue_checksum_and_db.qsize()}"
                    )
                    # Note that the cal_handler code needs to ensure it does not
                    # archvive any do not archive projects
                elif val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
                    self.dont_archive_handler_volt(item)
                elif val.filetype_id == MWADataFileType.MWA_PPD_FILE.value:
                    self.dont_archive_handler_vis(item)
                else:
                    self.logger.error(
                        f"{item}- checksum_and_db_handler() - not a valid file"
                        f" extension {val.filetype_id} / {val.file_ext}"
                    )
                    return False
        else:
            # The filename was not valid
            self.logger.error(f"{item}- checksum_and_db_handler() {val.validation_message}")
            return False

        self.logger.info(f"{item}- checksum_and_db_handler() Finished")
        return True

    def cal_handler(self, item: str) -> bool:
        self.outgoing_cal_list.append(item)
        return True

    def stats_handler(self, item: str) -> bool:
        """This runs stats against mwax FITS files"""
        self.logger.info(f"{item}- stats_handler() Started...")

        # This is a normal mwax fits file. Run stats on it
        if (
            utils.process_mwax_stats(
                self.logger,
                self.mwax_stats_binary_dir,
                item,
                int(self.archive_command_numa_node),
                self.mwax_stats_timeout_sec,
                self.mwax_stats_dump_dir,
                self.metafits_path,
            )
            is not True
        ):
            self.logger.warning(f"{item}- stats_handler() mwax_stats failed. Skipping.")

        # If observation is a calibrator AND this host is enabled as an archiver then
        # we should put the obs into the cal_outgoing dir so that calvin can
        # pull it in and calibrate it. The calvin server which processed the file(s)
        # will then call our webservice endpoint "release_cal_obs" and then that will
        # move the file into visdata_outgoing so it can be archived.

        # Is this host doing archiving?
        if self.archive_destination_enabled == 1:
            # Validate and get info about the obs
            obs_info: ValidationData = utils.validate_filename(self.logger, item, self.metafits_path)

            # Is the observation a calibrator?
            if obs_info.calibrator:
                # Send to cal_outgoing
                # Take the input filename - strip the path, then append the output path
                outgoing_filename = os.path.join(self.watch_dir_outgoing_cal, os.path.basename(item))
                self.logger.debug(f"{item}- stats_handler() moving file to outgoing cal dir")
                os.rename(item, outgoing_filename)
            else:
                # Should this project be archived?
                if utils.should_project_be_archived(obs_info.project_id):
                    # Send to vis_outgoing
                    # Take the input filename - strip the path, then append the output path
                    outgoing_filename = os.path.join(self.watch_dir_outgoing_vis, os.path.basename(item))
                    self.logger.debug(f"{item}- stats_handler() moving file to outgoing vis dir")
                    os.rename(item, outgoing_filename)
                else:
                    # No this project doesn't get archived
                    self.dont_archive_handler_vis(item)
        else:
            # This host is not doing any archiving
            self.dont_archive_handler_vis(item)

        self.logger.info(f"{item}- stats_handler() Finished")
        return True

    def release_cal_obs(self, obs_id: int):
        try:
            # Release any cal_outgoing files- this is triggered by a calvin server finishing processing
            # and calling the release_cal_obs web service endpoint on this host
            obs_files = glob.glob(os.path.join(self.watch_dir_outgoing_cal, f"{obs_id}*.fits"))

            if len(obs_files) == 0:
                self.logger.debug(f"{obs_id}: release_cal_obs()- no files found for this obs_id")

            # For file in the cal_outgoing dir for this obs_id
            for item in obs_files:
                # Does the file exist?
                if os.path.exists(item):
                    # Is this host doing archiving?
                    if self.archive_destination_enabled == 1:
                        # Validate and get info about the obs
                        obs_info: ValidationData = utils.validate_filename(self.logger, item, self.metafits_path)

                        # Should this project be archived?
                        if utils.should_project_be_archived(obs_info.project_id):
                            # Send to vis_outgoing
                            # Take the input filename - strip the path, then append the output path
                            outgoing_filename = os.path.join(self.watch_dir_outgoing_vis, os.path.basename(item))
                            self.logger.debug(f"{obs_id}- release_cal_obs() moving {item} to outgoing vis dir")
                            os.rename(item, outgoing_filename)
                        else:
                            # No this project doesn't get archived
                            self.dont_archive_handler_vis(item)
                    else:
                        # This host is not doing any archiving
                        self.dont_archive_handler_vis(item)

                    # Remove item from queue
                    self.outgoing_cal_list.remove(item)
                else:
                    self.logger.exception(f"{obs_id}: release_cal_obs()- failed to archive {item}- file does not exist")
        except Exception:
            self.logger.exception(f"{obs_id}: release_cal_obs()- something went wrong when releasing this obs_id")

    def archive_handler(self, item: str) -> bool:
        """This is called whenever a file is moved into the
        outgoing_vis or outgoing_volt directories. For each file attempt to
        send to the mwacache boxes then remove the file"""
        self.logger.info(f"{item}- archive_handler() Started...")

        if (
            mwa_archiver.archive_file_xrootd(
                self.logger,
                item,
                int(self.archive_command_numa_node),
                self.archive_destination_host,
                self.archive_command_timeout_sec,
            )
            is not True
        ):
            return False

        self.logger.debug(f"{item}- archive_handler() Deleting file")
        utils.remove_file(self.logger, item, raise_error=False)

        self.logger.info(f"{item}- archive_handler() Finished")
        return True

    def pause_archiving(self, paused: bool):
        """Pauses archiving"""
        if self.archiving_paused != paused:
            if paused:
                self.logger.info("Pausing archiving")
            else:
                self.logger.info("Resuming archiving")

            for worker in self.workers:
                if worker:
                    worker.pause(paused)

            self.archiving_paused = paused

    def stop(self):
        """Stops the processor"""
        for watcher in self.watchers:
            if watcher:
                watcher.stop()

        for worker in self.workers:
            if worker:
                worker.stop()

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

    def get_status(self) -> dict:
        """Returns a dictionary of status info from all processors"""
        watcher_list = []

        for watcher in self.watchers:
            if watcher:
                status = dict({"name": watcher.name})
                status.update(watcher.get_status())
                watcher_list.append(status)

        worker_list = []

        for worker in self.workers:
            if worker:
                status = dict({"name": worker.name})

                # for the cal_outgoing worker we will do the stats manually
                if worker.name == "outgoing cal vis worker":
                    outgoing_cal_dict = {
                        "Unix timestamp": time.time(),
                        "current item": "",
                        "queue_size": len(self.outgoing_cal_list),
                    }
                    status.update(outgoing_cal_dict)
                else:
                    status.update(worker.get_status())

                worker_list.append(status)

        if self.archiving_paused:
            archiving = "paused"
        else:
            archiving = "running"

        return_status = {
            "Unix timestamp": time.time(),
            "type": type(self).__name__,
            "archiving": archiving,
            "watchers": watcher_list,
            "workers": worker_list,
        }

        return return_status
