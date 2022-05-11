import logging
import logging.handlers
import os
import queue
import threading
import time
from mwax_mover import (
    mwax_mover,
    mwax_db,
    mwax_queue_worker,
    mwax_watcher,
    mwa_archiver,
    utils,
)
from mwax_mover.mwa_archiver import MWADataFileType


class MWAXArchiveProcessor:
    def __init__(
        self,
        context,
        hostname: str,
        archive_destination_enabled: int,
        archive_command_numa_node: int,
        archive_host: str,
        archive_port: str,
        archive_command_timeout_sec: int,
        mwax_stats_executable: str,
        mwax_stats_dump_dir: str,
        mwax_stats_timeout_sec: int,
        db_handler_object,
        voltdata_incoming_path: str,
        voltdata_outgoing_path: str,
        visdata_incoming_path: str,
        visdata_processing_stats_path: str,
        visdata_outgoing_path: str,
        visdata_cal_outgoing_path: str,
        calibrator_destination_host: str,
        calibrator_destination_port: int,
        calibrator_destination_enabled: int,
        metafits_path: str,
        dont_archive_path: str,
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

        self.db_handler_object = db_handler_object

        self.hostname = hostname
        self.archive_destination_enabled = archive_destination_enabled
        self.archive_destination_host = archive_host
        self.archive_destination_port = archive_port
        self.archive_command_numa_node: int = archive_command_numa_node
        self.archive_command_timeout_sec = archive_command_timeout_sec

        # Full path to executable for mwax_stats
        self.mwax_stats_executable = mwax_stats_executable
        # Directory where to dump the stats files
        self.mwax_stats_dump_dir = mwax_stats_dump_dir
        self.mwax_stats_timeout_sec = mwax_stats_timeout_sec

        self.archiving_paused = False

        self.watcher_threads = []
        self.worker_threads = []

        self.dont_archive_path = dont_archive_path
        self.queue_dont_archive = queue.Queue()
        self.queue_worker_dont_archive = None

        self.queue_checksum_and_db = queue.Queue()
        self.queue_worker_checksum_and_db = None

        self.watch_dir_incoming_volt = voltdata_incoming_path
        self.watcher_incoming_volt = None

        self.watch_dir_incoming_vis = visdata_incoming_path
        self.watcher_incoming_vis = None

        self.watch_dir_processing_stats_vis = visdata_processing_stats_path
        self.queue_processing_stats_vis = queue.Queue()
        self.watcher_processing_stats_vis = None
        self.queue_worker_processing_stats_vis = None

        self.watch_dir_outgoing_volt = voltdata_outgoing_path
        self.queue_outgoing_volt = queue.Queue()
        self.watcher_outgoing_volt = None
        self.queue_worker_outgoing_volt = None

        self.watch_dir_outgoing_vis = visdata_outgoing_path
        self.queue_outgoing_vis = queue.Queue()
        self.watcher_outgoing_vis = None
        self.queue_worker_outgoing_vis = None

        self.watch_dir_outgoing_cal = visdata_cal_outgoing_path
        self.queue_outgoing_cal = queue.Queue()
        self.watcher_outgoing_cal = None
        self.queue_worker_outgoing_cal = None

        self.calibrator_destination_enabled = calibrator_destination_enabled
        self.calibrator_destination_host = calibrator_destination_host
        self.calibrator_destination_port = calibrator_destination_port

        self.metafits_path = metafits_path

    def start(self):
        if self.archive_destination_enabled:
            # Create watcher for voltage data -> checksum+db queue
            self.watcher_incoming_volt = mwax_watcher.Watcher(
                path=self.watch_dir_incoming_volt,
                q=self.queue_checksum_and_db,
                pattern=".sub",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
                recursive=False,
            )

            # Create watcher for visibility data -> checksum+db queue
            # This will watch for mwax visibilities being renamed OR
            # fits files being created
            # (e.g. metafits ppd files being copied into /visdata).
            self.watcher_incoming_vis = mwax_watcher.Watcher(
                path=self.watch_dir_incoming_vis,
                q=self.queue_checksum_and_db,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
                recursive=False,
            )

            # Create queueworker for the checksum and db queue
            self.queue_worker_checksum_and_db = mwax_queue_worker.QueueWorker(
                label="checksum and database worker",
                q=self.queue_checksum_and_db,
                executable_path=None,
                event_handler=self.checksum_and_db_handler,
                log=self.logger,
                exit_once_queue_empty=False,
            )

            # Create watcher for visibility processing stats
            self.watcher_processing_stats_vis = mwax_watcher.Watcher(
                path=self.watch_dir_processing_stats_vis,
                q=self.queue_processing_stats_vis,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                recursive=False,
            )

            # worker for visibility processing stats
            self.queue_worker_processing_stats_vis = (
                mwax_queue_worker.QueueWorker(
                    label="processing stats vis worker",
                    q=self.queue_processing_stats_vis,
                    executable_path=None,
                    event_handler=self.stats_handler,
                    log=self.logger,
                    exit_once_queue_empty=False,
                )
            )

            # Create watcher for archiving outgoing voltage data
            self.watcher_outgoing_volt = mwax_watcher.Watcher(
                path=self.watch_dir_outgoing_volt,
                q=self.queue_outgoing_volt,
                pattern=".sub",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                recursive=False,
            )

            # Create queueworker for voltage outgoing queue
            self.queue_worker_outgoing_volt = mwax_queue_worker.QueueWorker(
                label="outgoing volt worker",
                q=self.queue_outgoing_volt,
                executable_path=None,
                event_handler=self.archive_handler,
                log=self.logger,
                exit_once_queue_empty=False,
            )

            # Create watcher for archiving outgoing visibility data
            self.watcher_outgoing_vis = mwax_watcher.Watcher(
                path=self.watch_dir_outgoing_vis,
                q=self.queue_outgoing_vis,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                recursive=False,
            )

            # Create queueworker for visibility outgoing queue
            self.queue_worker_outgoing_vis = mwax_queue_worker.QueueWorker(
                label="outgoing vis worker",
                q=self.queue_outgoing_vis,
                executable_path=None,
                event_handler=self.archive_handler,
                log=self.logger,
                exit_once_queue_empty=False,
            )

            # Create watcher for sending calibration visibility data
            # for processing
            self.watcher_outgoing_cal = mwax_watcher.Watcher(
                path=self.watch_dir_outgoing_cal,
                q=self.queue_outgoing_cal,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                recursive=False,
            )

            # Create queueworker for sending calibration visibility data
            # for processing
            self.queue_worker_outgoing_cal = mwax_queue_worker.QueueWorker(
                label="outgoing cal vis worker",
                q=self.queue_outgoing_cal,
                executable_path=None,
                event_handler=self.cal_handler,
                log=self.logger,
                exit_once_queue_empty=False,
            )

            #
            # Start watcher threads
            #

            # Setup thread for watching incoming filesystem (volt)
            watcher_volt_incoming_thread = threading.Thread(
                name="watch_volt_incoming",
                target=self.watcher_incoming_volt.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_volt_incoming_thread)
            watcher_volt_incoming_thread.start()

            # Setup thread for watching incoming filesystem (vis)
            watcher_vis_incoming_thread = threading.Thread(
                name="watch_vis_incoming",
                target=self.watcher_incoming_vis.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_vis_incoming_thread)
            watcher_vis_incoming_thread.start()

            # Setup thread for watching processing_stats filesystem (vis)
            watcher_vis_processing_stats_thread = threading.Thread(
                name="watch_vis_processing_stats",
                target=self.watcher_processing_stats_vis.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_vis_processing_stats_thread)
            watcher_vis_processing_stats_thread.start()

            # Setup thread for watching outgoing filesystem (volt)
            watcher_volt_outgoing_thread = threading.Thread(
                name="watch_volt_outgoing",
                target=self.watcher_outgoing_volt.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_volt_outgoing_thread)
            watcher_volt_outgoing_thread.start()

            # Setup thread for watching outgoing filesystem (vis)
            watcher_vis_outgoing_thread = threading.Thread(
                name="watch_vis_outgoing",
                target=self.watcher_outgoing_vis.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_vis_outgoing_thread)
            watcher_vis_outgoing_thread.start()

            # Setup thread for watching outgoing filesystem (cal)
            watcher_cal_outgoing_thread = threading.Thread(
                name="watch_cal_outgoing",
                target=self.watcher_outgoing_cal.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_cal_outgoing_thread)
            watcher_cal_outgoing_thread.start()

            #
            # Start queue worker threads
            #
            # Setup thread for processing items on the checksum and db queue
            queue_worker_checksum_and_db_thread = threading.Thread(
                name="work_checksum_and_db",
                target=self.queue_worker_checksum_and_db.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_checksum_and_db_thread)
            queue_worker_checksum_and_db_thread.start()

            # Setup thread for processing items on the
            # processing stats vis queue
            queue_worker_vis_processing_stats_thread = threading.Thread(
                name="work_vis_processing_stats",
                target=self.queue_worker_processing_stats_vis.start,
                daemon=True,
            )
            self.worker_threads.append(
                queue_worker_vis_processing_stats_thread
            )
            queue_worker_vis_processing_stats_thread.start()

            # Setup thread for processing items on the outgoing_volt queue
            queue_worker_volt_outgoing_thread = threading.Thread(
                name="work_volt_outgoing",
                target=self.queue_worker_outgoing_volt.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_volt_outgoing_thread)
            queue_worker_volt_outgoing_thread.start()

            # Setup thread for processing items on the outgoing vis queue
            queue_worker_vis_outgoing_thread = threading.Thread(
                name="work_vis_outgoing",
                target=self.queue_worker_outgoing_vis.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_vis_outgoing_thread)
            queue_worker_vis_outgoing_thread.start()

            # Setup thread for processing items on the outgoing vis queue
            queue_worker_cal_outgoing_thread = threading.Thread(
                name="work_cal_outgoing",
                target=self.queue_worker_outgoing_cal.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_cal_outgoing_thread)
            queue_worker_cal_outgoing_thread.start()
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
                or len(next(os.walk(self.watch_dir_processing_stats_vis))[2])
                > 0
            ):
                self.logger.error(
                    "Error- voltage incoming/outgoing and/or visibility "
                    "incoming/processing/outgoing/cal dirs are not empty! "
                    "Watched paths must be empty before starting with  "
                    "archiving disabled to prevent inadvertent data loss. "
                    "Exiting."
                )
                exit(-2)

            # Create watcher for voltage data -> dont_archive queue
            self.watcher_incoming_volt = mwax_watcher.Watcher(
                path=self.watch_dir_incoming_volt,
                q=self.queue_dont_archive,
                pattern=".sub",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
                recursive=False,
            )

            # Create watcher for visibility data -> dont_archive queue
            # This will watch for mwax visibilities being renamed OR
            # fits files being created (e.g. metafits ppd files being copied
            # into /visdata).
            self.watcher_incoming_vis = mwax_watcher.Watcher(
                path=self.watch_dir_incoming_vis,
                q=self.queue_dont_archive,
                pattern=".fits",
                log=self.logger,
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
                recursive=False,
            )

            # Create queueworker for the don't archive queue
            self.queue_worker_dont_archive = mwax_queue_worker.QueueWorker(
                label="dont archive worker",
                q=self.queue_dont_archive,
                executable_path=None,
                event_handler=self.dont_archive_handler,
                log=self.logger,
                exit_once_queue_empty=False,
            )

            #
            # Start watcher threads
            #

            # Setup thread for watching incoming filesystem (volt)
            watcher_volt_incoming_thread = threading.Thread(
                name="watch_volt_incoming",
                target=self.watcher_incoming_volt.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_volt_incoming_thread)
            watcher_volt_incoming_thread.start()

            # Setup thread for watching incoming filesystem (vis)
            watcher_vis_incoming_thread = threading.Thread(
                name="watch_vis_incoming",
                target=self.watcher_incoming_vis.start,
                daemon=True,
            )
            self.watcher_threads.append(watcher_vis_incoming_thread)
            watcher_vis_incoming_thread.start()

            #
            # Start queue worker threads
            #
            # Setup thread for processing items on the dont archive queue
            queue_worker_dont_archive_thread = threading.Thread(
                name="work_dont_archive",
                target=self.queue_worker_dont_archive.start,
                daemon=True,
            )
            self.worker_threads.append(queue_worker_dont_archive_thread)
            queue_worker_dont_archive_thread.start()

    def dont_archive_handler(self, item: str) -> bool:
        self.logger.info(f"{item}- dont_archive_handler() Started")

        outgoing_filename = os.path.join(
            self.dont_archive_path, os.path.basename(item)
        )

        self.logger.debug(
            f"{item}- dont_archive_handler() moving file to"
            f" {self.dont_archive_path}"
        )
        os.rename(item, outgoing_filename)

        self.logger.info(
            f"{item}- dont_archive_handler() moved file to"
            f" {self.dont_archive_path} dir Queue size:"
            f" {self.queue_dont_archive.qsize()}"
        )

        self.logger.info(f"{item}- dont_archive_handler() Finished")
        return True

    def checksum_and_db_handler(self, item: str) -> bool:
        self.logger.info(f"{item}- checksum_and_db_handler() Started")

        # validate the filename
        (
            valid,
            obs_id,
            filetype,
            file_ext,
            validation_message,
        ) = mwa_archiver.validate_filename(item)

        if valid:
            # checksum then add this file to the db so we insert a record into
            # metadata data_files table
            checksum_type_id: int = 1  # MD5
            checksum: str = utils.do_checksum_md5(
                self.logger, item, int(self.archive_command_numa_node), 180
            )

            # Insert record into metadata database
            if not mwax_db.insert_data_file_row(
                self.db_handler_object,
                obs_id,
                item,
                filetype,
                self.hostname,
                checksum_type_id,
                checksum,
            ):
                # if something went wrong, requeue
                return False

            # immediately add this file (and a ptr to it's queue) to the
            # voltage or vis queue which will deal with archiving
            if filetype == MWADataFileType.MWAX_VOLTAGES.value:
                # move to voltdata/outgoing
                # Take the input filename - strip the path, then append the
                # output path
                outgoing_filename = os.path.join(
                    self.watch_dir_outgoing_volt, os.path.basename(item)
                )

                self.logger.debug(
                    f"{item}- checksum_and_db_handler() moving subfile to volt"
                    " outgoing dir"
                )
                os.rename(item, outgoing_filename)

                self.logger.info(
                    f"{item}- checksum_and_db_handler() moved subfile to volt"
                    " outgoing dir Queue size:"
                    f" {self.queue_checksum_and_db.qsize()}"
                )
            elif filetype == MWADataFileType.MWAX_VISIBILITIES.value:
                # move to visdata/processing_stats
                # Take the input filename - strip the path, then append the
                # output path
                outgoing_filename = os.path.join(
                    self.watch_dir_processing_stats_vis, os.path.basename(item)
                )

                self.logger.debug(
                    f"{item}- checksum_and_db_handler() moving visibility file"
                    " to vis processing stats dir"
                )
                os.rename(item, outgoing_filename)

                self.logger.info(
                    f"{item}- checksum_and_db_handler() moved visibility file"
                    " to vis processing stats dir Queue size:"
                    f" {self.queue_checksum_and_db.qsize()}"
                )

            elif filetype == MWADataFileType.MWA_PPD_FILE.value:
                # move to visdata/outgoing
                # Take the input filename - strip the path, then append the
                # output path
                outgoing_filename = os.path.join(
                    self.watch_dir_outgoing_vis, os.path.basename(item)
                )

                self.logger.debug(
                    f"{item}- checksum_and_db_handler() moving metafits file"
                    " to vis outgoing dir"
                )
                os.rename(item, outgoing_filename)

                self.logger.info(
                    f"{item}- checksum_and_db_handler() moved metafits file to"
                    " vis outgoing dir Queue size:"
                    f" {self.queue_checksum_and_db.qsize()}"
                )
            else:
                self.logger.error(
                    f"{item}- checksum_and_db_handler() - not a valid file"
                    f" extension {filetype}"
                )
                return False
        else:
            # The filename was not valid
            self.logger.error(
                f"{item}- checksum_and_db_handler() {validation_message}"
            )
            return False

        self.logger.info(f"{item}- checksum_and_db_handler() Finished")
        return True

    def stats_handler(self, item: str) -> bool:
        self.logger.info(f"{item}- stats_handler() Started...")

        # This is a normal mwax fits file. Run stats on it
        if (
            utils.process_mwax_stats(
                self.logger,
                self.mwax_stats_executable,
                item,
                int(self.archive_command_numa_node),
                self.mwax_stats_timeout_sec,
                self.mwax_stats_dump_dir,
            )
            is not True
        ):
            return False

        # Take the input filename - strip the path, then append the output path
        outgoing_filename = os.path.join(
            self.watch_dir_outgoing_cal, os.path.basename(item)
        )

        self.logger.debug(
            f"{item}- stats_handler() moving file to outgoing cal dir"
        )
        os.rename(item, outgoing_filename)

        self.logger.info(f"{item}- stats_handler() Finished")
        return True

    def archive_handler(self, item: str) -> bool:
        """This is called whenever a file is moved into the
        outgoing_vis directory. For each file attempt to send to
        the archive then remove the file"""
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
        mwax_mover.remove_file(self.logger, item, raise_error=False)

        self.logger.info(f"{item}- archive_handler() Finished")
        return True

    def cal_handler(self, item: str) -> bool:
        """This is called when a file is detected in the cal watch directory.
        If the observation is a calibrator, then copy this file to the
        designated calibrator destination for processing. Regardless,
        Move the file into the vis_outgoing directory for archiving"""
        self.logger.info(f"{item}- cal_handler() Started...")

        # Determine properties of the file we are dealing with
        (
            valid,
            obs_id,
            filetype,
            file_ext,
            validation_message,
        ) = mwa_archiver.validate_filename(item)

        # Check the filetype is MWAX visibility
        if filetype == MWADataFileType.MWAX_VISIBILITIES.value:
            # Now check that the observation is a calibrator by
            # looking at the associated metafits file
            metafits_filename = os.path.join(
                self.metafits_path, f"{obs_id}_metafits.fits"
            )

            self.logger.debug(
                f"{item}- cal_handler() checking if observation is a "
                f"calibrator by reading {metafits_filename}"
            )

            if (
                utils.is_observation_calibrator(metafits_filename)
                and self.calibrator_destination_enabled == 1
            ):
                # It is an MWAX visibility AND the obs is a calibrator
                # so send it to the calibrator destination
                self.logger.debug(
                    f"{item}- cal_handler() observation IS a calibrator,"
                    " sending to calibration server"
                    f" {self.calibrator_destination_host}"
                )

                if (
                    mwa_archiver.archive_file_xrootd(
                        self.logger,
                        item,
                        int(self.archive_command_numa_node),
                        self.calibrator_destination_host,
                        self.archive_command_timeout_sec,
                    )
                    is not True
                ):
                    return False

        # Take the input filename - strip the path, then append the output path
        outgoing_filename = os.path.join(
            self.watch_dir_outgoing_vis, os.path.basename(item)
        )

        self.logger.debug(
            f"{item}- cal_handler() moving file to vis outgoing dir"
        )
        os.rename(item, outgoing_filename)

        self.logger.info(f"{item}- cal_handler() Finished")
        return True

    def pause_archiving(self, paused: bool):
        if self.archiving_paused != paused:
            if paused:
                self.logger.info("Pausing archiving")
            else:
                self.logger.info("Resuming archiving")

            if self.queue_worker_checksum_and_db:
                self.queue_worker_checksum_and_db.pause(paused)

            if self.queue_worker_processing_stats_vis:
                self.queue_worker_processing_stats_vis.pause(paused)

            if self.queue_worker_outgoing_volt:
                self.queue_worker_outgoing_volt.pause(paused)

            if self.queue_worker_outgoing_vis:
                self.queue_worker_outgoing_vis.pause(paused)

            if self.queue_worker_outgoing_cal:
                self.queue_worker_outgoing_cal.pause(paused)

            self.archiving_paused = paused

    def stop(self):
        if self.watcher_incoming_volt:
            self.watcher_incoming_volt.stop()

        if self.watcher_incoming_vis:
            self.watcher_incoming_vis.stop()

        if self.watcher_processing_stats_vis:
            self.watcher_processing_stats_vis.stop()

        if self.watcher_outgoing_volt:
            self.watcher_outgoing_volt.stop()

        if self.watcher_outgoing_vis:
            self.watcher_outgoing_vis.stop()

        if self.queue_worker_dont_archive:
            self.queue_worker_dont_archive.stop()

        if self.queue_worker_checksum_and_db:
            self.queue_worker_checksum_and_db.stop()

        if self.queue_worker_processing_stats_vis:
            self.queue_worker_processing_stats_vis.stop()

        if self.queue_worker_outgoing_volt:
            self.queue_worker_outgoing_volt.stop()

        if self.queue_worker_outgoing_vis:
            self.queue_worker_outgoing_vis.stop()

        if self.queue_worker_outgoing_cal:
            self.queue_worker_outgoing_cal.stop()

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

    def get_status(self) -> dict:
        watcher_list = []

        if self.watcher_incoming_volt:
            status = dict({"name": "voltdata_incoming_watcher"})
            status.update(self.watcher_incoming_volt.get_status())
            watcher_list.append(status)

        if self.watcher_incoming_vis:
            status = dict({"name": "visdata_incoming_watcher"})
            status.update(self.watcher_incoming_vis.get_status())
            watcher_list.append(status)

        if self.watcher_processing_stats_vis:
            status = dict({"name": "visdata_processing_stats_watcher"})
            status.update(self.watcher_processing_stats_vis.get_status())
            watcher_list.append(status)

        if self.watcher_outgoing_volt:
            status = dict({"name": "voltdata_outgoing_watcher"})
            status.update(self.watcher_outgoing_volt.get_status())
            watcher_list.append(status)

        if self.watcher_outgoing_vis:
            status = dict({"name": "visdata_outgoing_watcher"})
            status.update(self.watcher_outgoing_vis.get_status())
            watcher_list.append(status)

        if self.watcher_outgoing_cal:
            status = dict({"name": "visdata_outgoing_cal_watcher"})
            status.update(self.watcher_outgoing_cal.get_status())
            watcher_list.append(status)

        worker_list = []

        if self.queue_worker_checksum_and_db:
            status = dict({"name": "checksum_and_db_worker"})
            status.update(self.queue_worker_checksum_and_db.get_status())
            worker_list.append(status)

        if self.queue_worker_processing_stats_vis:
            status = dict({"name": "visdata_processing_stats_worker"})
            status.update(self.queue_worker_processing_stats_vis.get_status())
            worker_list.append(status)

        if self.queue_worker_outgoing_volt:
            status = dict({"name": "voltdata_outgoing_worker"})
            status.update(self.queue_worker_outgoing_volt.get_status())
            worker_list.append(status)

        if self.queue_worker_outgoing_vis:
            status = dict({"name": "visdata_outgoing_worker"})
            status.update(self.queue_worker_outgoing_vis.get_status())
            worker_list.append(status)

        if self.queue_worker_outgoing_cal:
            status = dict({"name": "visdata_outgoing_cal_worker"})
            status.update(self.queue_worker_outgoing_cal.get_status())
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
