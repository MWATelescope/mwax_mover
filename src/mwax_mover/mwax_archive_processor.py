"""Module file for MWAXArchiveProcessor"""

import glob
import logging
import os
import sys
import time
from mwax_mover import (
    mwax_db,
    utils,
)
from mwax_mover.mwax_wqw_bf_stitching_processor import BfStitchingProcessor
from mwax_mover.mwax_wqw_checksum_and_db import ChecksumAndDBProcessor
from mwax_mover.mwax_wqw_outgoing import OutgoingProcessor
from mwax_mover.mwax_wqw_vis_stats import VisStatsProcessor
from mwax_mover.mwax_wqw_vis_cal_outgoing import VisCalOutgoingProcessor
from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker, MWAXPriorityWatchQueueWorker
from mwax_mover.utils import ValidationData


class MWAXArchiveProcessor:
    """
    A class representing an instance which sends
    MWAX data products to the mwacache servers.

    visdata/incoming    -> checksum_and_db (or dont_archive_vis) -> processing_stats -> cal_outgoing (if calibrator) -> outgoing
    voltdata/incoming   -> checksum_and_db (or dont_archive_volt) -> outgoing
    bf/incoming         -> stitching-> checksum_and_db (or dont_archive_bf) -> outgoing

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
        bf_incoming_path: str,
        bf_stitching_path: str,
        bf_outgoing_path: str,
        bf_dont_archive_path: str,
        bf_keep_original_files_after_stitching: bool,
    ):
        self.sd_ctx = context

        # Setup logging
        self.logger: logging.Logger = context.logger.getChild("MWAXArchiveProcessor")

        # Database handler object
        self.db_handler_object: mwax_db.MWAXDBHandler = db_handler_object

        # Hostname, metafits, etc
        self.hostname: str = hostname
        self.metafits_path: str = metafits_path
        self.list_of_correlator_high_priority_projects: list[str] = high_priority_correlator_projectids
        self.list_of_vcs_high_priority_projects: list[str] = high_priority_vcs_projectids

        # Archive config
        self.archive_destination_enabled: int = archive_destination_enabled
        self.archive_destination_host: str = archive_host
        self.archive_destination_port: int = archive_port
        self.archive_command_numa_node: int = archive_command_numa_node
        self.archive_command_timeout_sec: int = archive_command_timeout_sec
        self.archiving_paused: bool = False

        # mwax / vis stats
        self.mwax_stats_binary_dir: str = mwax_stats_binary_dir  # Full path to executable for mwax_stats
        self.mwax_stats_dump_dir: str = mwax_stats_dump_dir  # Directory where to dump the stats files
        self.mwax_stats_timeout_sec: int = mwax_stats_timeout_sec

        # Visibility paths
        self.watch_dir_incoming_vis: str = visdata_incoming_path
        self.watch_dir_processing_stats_vis: str = visdata_processing_stats_path
        self.watch_dir_outgoing_cal: str = visdata_cal_outgoing_path
        self.dont_archive_path_vis: str = visdata_dont_archive_path
        self.watch_dir_outgoing_vis: str = visdata_outgoing_path

        # Voltage paths
        self.watch_dir_incoming_volt: str = voltdata_incoming_path
        self.dont_archive_path_volt: str = voltdata_dont_archive_path
        self.watch_dir_outgoing_volt: str = voltdata_outgoing_path

        # Beamformer paths
        self.watch_dir_incoming_bf: str = bf_incoming_path
        self.watch_dir_stitching_bf: str = bf_stitching_path
        self.dont_archive_path_bf: str = bf_dont_archive_path
        self.watch_dir_outgoing_bf: str = bf_outgoing_path
        self.bf_keep_original_files_after_stitching: bool = bf_keep_original_files_after_stitching

        # Since our watcher needs a queue, we'll just get the queue to dump the filenames
        # into this list so we can easily remove them when release_cal_obs is called
        # by a calvin
        self.outgoing_cal_list: list[str] = list()
        self.calibrator_destination_enabled: int = calibrator_destination_enabled

        # This list helps us keep track of all the workers
        self.workers: list[MWAXWatchQueueWorker | MWAXPriorityWatchQueueWorker] = list()

    def start(self):
        """This method is used to start the processor"""
        self.logger.info("Starting ArchiveProcessor...")

        # Watch:
        #   watch_dir_incoming_vis
        #   watch_dir_incoming_volt
        #   watch_dir_stitching_bf
        # Do:
        #   Checksum and insert into database (if archiving)
        # Then:
        #   Move file to outgoing dir (if archiving) or dont_archive dir (if not archiving)
        self.checksum_and_db_processor = ChecksumAndDBProcessor(
            self.logger,
            self.metafits_path,
            self.watch_dir_incoming_vis,
            self.watch_dir_processing_stats_vis,
            self.watch_dir_outgoing_vis,
            self.dont_archive_path_vis,
            self.watch_dir_incoming_volt,
            self.watch_dir_outgoing_volt,
            self.dont_archive_path_volt,
            self.watch_dir_stitching_bf,
            self.watch_dir_outgoing_bf,
            self.dont_archive_path_bf,
            self.list_of_correlator_high_priority_projects,
            self.list_of_vcs_high_priority_projects,
            self.db_handler_object,
            self.archive_destination_enabled == 1,
        )
        self.workers.append(self.checksum_and_db_processor)

        # Watch:
        #   watch_dir_processing_stats_vis
        # Do:
        #   Run mwax_stats
        # Then:
        #   Move file to outgoing cal dir (if archiving & calibrator), outgoing dir (if archiving and not calibrator) or dont_archive dir (if not archiving)
        self.vis_stats_processor = VisStatsProcessor(
            self.logger,
            self.metafits_path,
            self.watch_dir_processing_stats_vis,
            self.mwax_stats_binary_dir,
            self.mwax_stats_timeout_sec,
            self.mwax_stats_dump_dir,
            self.archive_destination_enabled == 1,
            self.watch_dir_outgoing_vis,
            self.watch_dir_outgoing_cal,
            self.dont_archive_path_vis,
        )

        # Watch:
        #   watch_dir_incoming_bf
        # Do:
        #   Stitch the files and save them into watch_dir_stitching_bf
        #   (Optionally copy the pre-stitched files to dont_archive_path_bf if bf_keep_original_files_after_stitching is True)
        # Then:
        #   ChecksumAndDB processor will pick up the new files in watch_dir_stitching_bf
        self.bf_stitching_processor = BfStitchingProcessor(
            self.logger,
            self.metafits_path,
            self.watch_dir_incoming_bf,
            self.watch_dir_stitching_bf,
            self.dont_archive_path_bf,
            self.list_of_correlator_high_priority_projects,
            self.list_of_vcs_high_priority_projects,
            self.archive_destination_enabled == 1,
            self.bf_keep_original_files_after_stitching,
        )

        if self.archive_destination_enabled:
            # Only start these processors if we are archiving

            # Watch:
            #   watch_dir_outgoing_cal
            # Do:
            #   Add to the outgoing cal list so that when release_cal_obs is called by calvin, we can remove the file from the list and archive the file
            #
            self.vis_s_cal_outgoing_processor = VisCalOutgoingProcessor(
                self.logger,
                self.watch_dir_outgoing_cal,
                self.outgoing_cal_list,
            )

            # Watch:
            #   watch_dir_outgoing_vis
            #   watch_dir_outgoing_volt
            #   watch_dir_outgoing_bf
            # Do:
            #   Use xrootd to transfer the files to the mwacache servers
            self.outgoing_processor = OutgoingProcessor(
                self.logger,
                self.metafits_path,
                self.watch_dir_outgoing_vis,
                self.watch_dir_outgoing_volt,
                self.watch_dir_outgoing_bf,
                self.list_of_correlator_high_priority_projects,
                self.list_of_vcs_high_priority_projects,
                self.archive_command_numa_node,
                self.archive_destination_host,
                self.archive_command_timeout_sec,
            )
        else:
            # We have disabled archiving, so use a different
            # handler for incoming data
            # which just moves the files elsewhere

            # First check to ensure there are no existing unarchived files on
            # our watching dirs
            if utils.running_under_pytest:
                # Ignore this check if we're testing
                pass
            else:
                if (
                    len(next(os.walk(self.watch_dir_incoming_volt))[2]) > 0
                    or len(next(os.walk(self.watch_dir_incoming_vis))[2]) > 0
                    or len(next(os.walk(self.watch_dir_outgoing_volt))[2]) > 0
                    or len(next(os.walk(self.watch_dir_outgoing_vis))[2]) > 0
                    or len(next(os.walk(self.watch_dir_outgoing_cal))[2]) > 0
                    or len(next(os.walk(self.watch_dir_processing_stats_vis))[2]) > 0
                    or len(next(os.walk(self.watch_dir_incoming_bf))[2]) > 0
                    or len(next(os.walk(self.watch_dir_outgoing_bf))[2]) > 0
                ):
                    self.logger.error(
                        "Error- voltage incoming/outgoing and/or visibility "
                        "incoming/processing/outgoing/cal/bf dirs are not empty! "
                        "Watched paths must be empty before starting with  "
                        "archiving disabled to prevent inadvertent data loss. "
                        "Exiting."
                    )
                    sys.exit(-2)

        #
        # Start processors
        #
        self.logger.info("Waiting for all workers to finish scanning....")
        workers_still_scanning = len(self.workers)
        while workers_still_scanning > 0:
            workers_still_scanning = 0
            for w in self.workers:
                if not w.scan_completed:
                    self.logger.debug(f"{w.name} still scanning!")
                    workers_still_scanning += 1
            time.sleep(1)  # hold off for another second
        self.logger.info("Workers are finished scanning.")

        #
        # Start workers
        #
        for w in self.workers:
            w.start()

        self.logger.info("ArchiveProcessor started.")

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
                            outgoing_filename = os.path.join(self.dont_archive_path_vis, os.path.basename(item))
                            self.logger.debug(f"{item}- release_cal_obs() moving file to {self.dont_archive_path_vis}")
                            os.rename(item, outgoing_filename)
                    else:
                        # This host is not doing any archiving
                        outgoing_filename = os.path.join(self.dont_archive_path_vis, os.path.basename(item))
                        self.logger.debug(f"{item}- release_cal_obs() moving file to {self.dont_archive_path_vis}")
                        os.rename(item, outgoing_filename)
                else:
                    self.logger.exception(f"{obs_id}: release_cal_obs()- failed to archive {item}- file does not exist")

                # Remove item from queue
                try:
                    self.outgoing_cal_list.remove(item)
                except Exception:
                    # Don't want an exception if file is already gone from list
                    pass
        except Exception:
            self.logger.exception(f"{obs_id}: release_cal_obs()- something went wrong when releasing this obs_id")

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
        for w in self.workers:
            w.stop()

    def get_status(self) -> dict:
        """Returns a dictionary of status info from all processors"""
        worker_list = []

        for w in self.workers:
            status = dict({"name": w.name})
            status.update(w.get_status())
            worker_list.append(status)

        if self.archiving_paused:
            archiving = "paused"
        else:
            archiving = "running"

        return_status = {
            "Unix timestamp": time.time(),
            "type": type(self).__name__,
            "archiving": archiving,
            "workers": worker_list,
        }

        return return_status
