"""
Module hosting the MWAXCalvinProcessor for near-realtime
calibration
"""

import argparse
from configparser import ConfigParser
import datetime
import glob
import json
import logging
import logging.handlers
import os
import queue
import shutil
import signal
import sys
import threading
import time
from astropy import time as astrotime
from mwax_mover import (
    utils,
    version,
    mwax_watcher,
    mwax_queue_worker,
    mwax_calvin_utils,
    mwax_db,
)
import numpy as np
from pandas import DataFrame
import traceback
import coloredlogs

from mwax_mover.mwax_db import insert_calibration_fits_row, insert_calibration_solutions_row, update_calsolution_request
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW
from mwax_mover.mwax_calvin_utils import (
    HyperfitsSolution,
    HyperfitsSolutionGroup,
    Metafits,
    debug_phase_fits,
    fit_phase_line,
    fit_gain,
    PhaseFitInfo,
    GainFitInfo,
    write_readme_file,
)

CALIBRATION_REQUEST_ERROR_STATUS = -1
CALIBRATION_REQUEST_NOT_STARTED = 0
CALIBRATION_REQUEST_PROCESSING = 1
CALIBRATION_REQUEST_SUCCESS = 2


class MWAXCalvinProcessor:
    """The main class processing calibration solutions"""

    def __init__(
        self,
    ):
        # General
        self.logger = logging.getLogger(__name__)
        self.log_path = None
        self.hostname = None
        self.db_handler_object = None

        # health
        self.health_multicast_interface_ip = None
        self.health_multicast_interface_name = None
        self.health_multicast_ip = None
        self.health_multicast_port = None
        self.health_multicast_hops = None
        self.processing_error_count: int = 0
        self.upload_error_count: int = 0
        self.completed_count: int = 0

        self.metadata_webservice_url = None

        self.running = False
        self.ready_to_exit = False

        self.watchers = []
        self.queue_workers = []
        self.watcher_threads = []
        self.worker_threads = []

        # assembly
        self.incoming_realtime_watch_path = None
        self.remove_partial_files_check_seconds: int = 60 * 60 * 4
        self.incoming_asvo_watch_path = None
        self.assembly_realtime_watch_queue = queue.Queue()
        self.assembly_asvo_watch_queue = queue.Queue()
        self.assemble_path = None
        self.assemble_check_seconds = None
        self.obsid_check_assembled_thread = None
        self.check_and_remove_partial_files_thread = None

        # processing
        self.processing_path = None
        self.processing_error_path = None
        self.processing_queue = queue.Queue()
        self.source_list_filename = None
        self.source_list_type = None
        self.phase_fit_niter = None

        # Upload
        self.produce_debug_plots = True  # default to true- for now only off if running via pytest
        self.upload_path = None
        self.upload_error_path = None
        self.upload_queue = queue.Queue()

        # Complete
        self.complete_path = None
        self.keep_completed_visibility_files = None

        # birli
        self.birli_timeout = None
        self.birli_binary_path = None
        self.birli_popen_process = None

        # hyperdrive
        self.hyperdrive_timeout = None
        self.hyperdrive_binary_path = None
        self.hyperdrive_popen_process = None

    def start(self):
        """Start the processor"""
        self.running = True

        # creating database connection pool(s)
        self.logger.info("Starting database connection pool...")
        self.db_handler_object.start_database_pool()

        # create a health thread
        self.logger.info("Starting health_thread...")
        health_thread = threading.Thread(name="health_thread", target=self.health_loop, daemon=True)
        health_thread.start()

        #
        # Do initial scan for directories to add to the processing
        # queue (in the processing_path)
        #
        scanned_dirs = utils.scan_directory(self.logger, self.processing_path, "", False, None)
        for item in scanned_dirs:
            if os.path.isdir(item):
                self.add_to_processing_queue(item)

        #
        # Do initial scan for directories to add to the upload
        # queue (in the processing_upload_path)
        #
        scanned_dirs = utils.scan_directory(self.logger, self.upload_path, "", False, None)
        for item in scanned_dirs:
            if os.path.isdir(item):
                self.add_to_upload_queue(item)

        #
        # Create watchers
        #
        self.logger.info("Creating watchers...")
        # Create watcher for the incoming realtime watch path queue
        new_watcher = mwax_watcher.Watcher(
            name="incoming_realtime_watcher",
            path=self.incoming_realtime_watch_path,
            dest_queue=self.assembly_realtime_watch_queue,
            pattern=".fits",
            log=self.logger,
            mode=MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
            recursive=False,
            exclude_pattern=None,
        )
        self.watchers.append(new_watcher)

        # Create watcher for the incoming asvo watch path queue
        new_watcher = mwax_watcher.Watcher(
            name="incoming_asvo_watcher",
            path=self.incoming_asvo_watch_path,
            dest_queue=self.assembly_asvo_watch_queue,
            pattern=".*",
            log=self.logger,
            mode=MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
            recursive=False,
            exclude_pattern=None,
        )
        self.watchers.append(new_watcher)

        #
        # Create queue workers
        #

        # Create queueworker for assembly queue
        self.logger.info("Creating workers...")
        new_worker = mwax_queue_worker.QueueWorker(
            name="incoming_realtime_worker",
            source_queue=self.assembly_realtime_watch_queue,
            executable_path=None,
            event_handler=self.incoming_realtime_handler,
            log=self.logger,
            requeue_to_eoq_on_failure=True,
            exit_once_queue_empty=False,
            requeue_on_error=True,
        )
        self.queue_workers.append(new_worker)

        # For the ASVO queue, don't requeue to the back of queue. Keep it where it is! Order matters!
        new_worker = mwax_queue_worker.QueueWorker(
            name="incoming_asvo_worker",
            source_queue=self.assembly_asvo_watch_queue,
            executable_path=None,
            event_handler=self.incoming_asvo_handler,
            log=self.logger,
            requeue_to_eoq_on_failure=False,
            exit_once_queue_empty=False,
            requeue_on_error=True,
        )
        self.queue_workers.append(new_worker)

        # Create queueworker for processing queue
        # if we fail, do not requeue, move to processing_error dir
        new_worker = mwax_queue_worker.QueueWorker(
            name="processing_worker",
            source_queue=self.processing_queue,
            executable_path=None,
            event_handler=self.processing_handler,
            log=self.logger,
            requeue_to_eoq_on_failure=False,
            exit_once_queue_empty=False,
            requeue_on_error=False,
        )
        self.queue_workers.append(new_worker)

        # Create queueworker for upload queue
        # if we fail, DO requeue as it is likely the db is down
        # as opposed to a permafail
        new_worker = mwax_queue_worker.QueueWorker(
            name="upload_worker",
            source_queue=self.upload_queue,
            executable_path=None,
            event_handler=self.upload_handler,
            log=self.logger,
            requeue_to_eoq_on_failure=False,
            exit_once_queue_empty=False,
            requeue_on_error=False,
        )
        self.queue_workers.append(new_worker)

        self.logger.info("Starting watchers...")
        # Setup threads for watching filesystem
        for i, watcher in enumerate(self.watchers):
            watcher_thread = threading.Thread(name=f"watch_thread{i}", target=watcher.start, daemon=True)
            self.watcher_threads.append(watcher_thread)
            watcher_thread.start()
            # Wait 2 seconds- this ensures the watchers start in the correct order
            time.sleep(2)

        self.logger.info("Waiting for all watchers to finish scanning....")
        count_of_watchers_still_scanning = len(self.watchers)
        while count_of_watchers_still_scanning > 0:
            count_of_watchers_still_scanning = 0
            for watcher in self.watchers:
                if not watcher.scan_completed:
                    count_of_watchers_still_scanning += 1
            time.sleep(1)  # hold off for another second
        self.logger.info("Watchers are finished scanning.")

        self.logger.info("Starting workers...")
        # Setup threads for processing items
        for i, worker in enumerate(self.queue_workers):
            queue_worker_thread = threading.Thread(name=f"worker_thread{i}", target=worker.start, daemon=True)
            self.worker_threads.append(queue_worker_thread)
            queue_worker_thread.start()

        # Start obs_id_check_thread
        self.obsid_check_assembled_thread = threading.Thread(
            name="obsid_assemble_thread",
            target=self.obsid_check_assembled_handler,
            daemon=True,
        )
        self.obsid_check_assembled_thread.start()
        self.worker_threads.append(self.obsid_check_assembled_thread)

        # Start check_and_remove_partial_files_thread

        self.check_and_remove_partial_files_thread = threading.Thread(
            name="check_and_remove_partial_files_thread",
            target=self.check_and_remove_partial_files_handler,
            daemon=True,
        )
        self.check_and_remove_partial_files_thread.start()
        self.worker_threads.append(self.check_and_remove_partial_files_thread)

        self.logger.info("Started...")

        while self.running:
            for worker_thread in self.worker_threads:
                if worker_thread:
                    if worker_thread.is_alive():
                        time.sleep(0.2)
                    else:
                        self.logger.debug(f"Worker {worker_thread.name} has died unexpectedly! Exiting!")
                        self.running = False
                        self.stop()
                        break

        #
        # Finished- do some clean up
        #
        while not self.ready_to_exit:
            time.sleep(1)

        # Final log message
        self.logger.info("Completed Successfully")

    def incoming_realtime_handler(self, item) -> bool:
        """This is triggered each time a new fits file
        appears in the incoming_realtime_path

        Realtime requests have higher priority than MWA ASVO"""
        self.logger.info(f"Handling... incoming realtime FITS file {item}...")
        filename = os.path.basename(item)
        obs_id: int = int(filename[0:10])

        obsid_assembly_dir = os.path.join(self.assemble_path, str(obs_id))
        if not os.path.exists(obsid_assembly_dir):
            self.logger.info(f"{item} creating obs_id's assembly dir" f" {obsid_assembly_dir}...")
            # This is the first file of this obs_id to be seen
            os.mkdir(obsid_assembly_dir)

        # Relocate this file to the obs_id_work_dir
        new_filename = os.path.join(obsid_assembly_dir, filename)
        self.logger.info(f"{item} moving file into obs_id's assembly dir {new_filename}...")
        shutil.move(item, new_filename)
        return True

    def incoming_asvo_handler(self, item) -> bool:
        """This is triggered each time a new file
        appears in the incoming_asvo_path

        NOTE: when we download from MWA ASVO we will also get the following files which should be deleted:
        * <OBSID>.metafits
        * <OBSID>_metafits_ppds.fits
        * MWA_ASVO_README.md

        MWA ASVO requests have lower priority than realtime"""

        # Purge the files we don't care about!
        if (
            str(item).endswith(".metafits")
            or str(item).endswith("_metafits_ppds.fits")
            or str(item).endswith("MWA_ASVO_README.md")
        ):
            try:
                os.remove(item)
                return True
            except FileNotFoundError:
                # It was already removed?
                pass

        # Only handle MWA ASVO if the incoming_realtime_queue is empty
        if self.assembly_realtime_watch_queue.qsize() == 0:
            self.logger.info(f"Handling... incoming MWA ASVO FITS file {item}...")
            filename = os.path.basename(item)
            obs_id: int = int(filename[0:10])

            obsid_assembly_dir = os.path.join(self.assemble_path, str(obs_id))
            if not os.path.exists(obsid_assembly_dir):
                self.logger.info(f"{item} creating obs_id's assembly dir" f" {obsid_assembly_dir}...")
                # This is the first file of this obs_id to be seen
                os.mkdir(obsid_assembly_dir)

            # Relocate this file to the obs_id_work_dir
            new_filename = os.path.join(obsid_assembly_dir, filename)
            self.logger.info(f"{item} moving file into obs_id's assembly dir {new_filename}...")
            shutil.move(item, new_filename)
            return True
        else:
            # Sleep this thread for a minute so we don't go into
            # a tight loop
            # Returning false will cause this item to just be put back on the queue
            time.sleep(60)
            return False

    def check_obs_is_ready_to_process(self, obs_id, obsid_assembly_dir) -> bool:
        """This routine checks to see if an observation is ready to be processed
        by hyperdrive"""
        #
        # Check we have a metafits
        #
        metafits_filename = f"{obs_id}_metafits.fits"
        metafits_assembly_filename = os.path.join(obsid_assembly_dir, metafits_filename)

        if not os.path.exists(metafits_assembly_filename):
            # download the metafits file to the assembly dir for the obs
            try:
                self.logger.info(f"{obs_id} Downloading metafits file...")
                utils.download_metafits_file(obs_id, obsid_assembly_dir)
                self.logger.info(f"{obs_id} metafits downloaded successfully")
            except Exception as catch_all_exception:
                self.logger.error(
                    f"Metafits file {metafits_assembly_filename} did not exist"
                    " and could not download one from web"
                    f" service. {catch_all_exception}"
                )
                return False

        # Get the duration of the obs from the metafits and only proceed
        # if the current gps time is > the obs_id + duration + a constant
        exp_time = int(utils.get_metafits_value(metafits_assembly_filename, "EXPOSURE"))
        current_gpstime = astrotime.Time(datetime.datetime.utcnow(), scale="utc").gps

        if current_gpstime > (int(obs_id) + exp_time):
            #
            # perform web service call to get list of data files from obsid
            #
            web_service_filenames = utils.get_data_files_for_obsid_from_webservice(
                self.logger, obs_id, self.metadata_webservice_url
            )

            if web_service_filenames:
                # we need a list of files from the work dir
                # this first list has the full path
                # put a basic UNIX pattern so we don't pick up the metafits

                # Check for gpubox files (mwax OR legacy)
                glob_spec = "*.fits"
                assembly_dir_full_path_files = glob.glob(os.path.join(obsid_assembly_dir, glob_spec))
                assembly_dir_filenames = [os.path.basename(i) for i in assembly_dir_full_path_files]
                assembly_dir_filenames.sort()
                # Remove the metafits file
                if metafits_filename in assembly_dir_filenames:
                    assembly_dir_filenames.remove(metafits_filename)

                # How does what we need compare to what we have?
                return_value = set(assembly_dir_filenames).issuperset(web_service_filenames)

                self.logger.debug(
                    f"{obs_id} check_obs_is_ready_to_process() =="
                    f" {return_value} (WS: {len(web_service_filenames)},"
                    f" assembly_dir: {len(assembly_dir_filenames)})"
                )
                return return_value
            else:
                # Web service didn't return any files
                # This is usually because there ARE no files in the database
                # Best to fail
                self.logger.error(
                    f"utils.get_data_files_for_obsid_from_webservice({obs_id}, "
                    f"{self.metadata_webservice_url}) did not return any files."
                )
                return False
        else:
            self.logger.info(
                f"{obs_id} Observation is still in progress:"
                f" {current_gpstime} < ({obs_id} - {int(obs_id)+exp_time})"
            )
            return False

    def check_and_remove_partial_files_handler(self):
        """This thread sleeps most of the time, but wakes up
        to check if there are any orphaned realtime incoming
        visibility files orphaned. This can happen if the mwax
        boxes are interrupted when xrdcp'ing files to calvin"""
        while self.running:
            self.logger.debug(f"sleeping for {self.remove_partial_files_check_seconds} secs")
            time.sleep(self.remove_partial_files_check_seconds)

            if self.running:
                self.logger.debug("Waking up and checking for and removing orphaned partial realtime files...")

                #
                # Remove any partial files first if they are old
                #
                partial_files = glob.glob(os.path.join(self.incoming_realtime_watch_path, "*.part*"))
                for partial_file in partial_files:
                    # Ensure now minus the last mod time of the partial file
                    # is > 4 hours, it is definitely safe to delete
                    # In theory we could be starting up as mwax is sending
                    # us a new file and we don't want to delete an real
                    # in progress file.
                    min_partial_purge_age_secs = 60 * 60 * 4  # 4 hours

                    # Get the last modified time of the partial file
                    try:
                        mod_time = os.path.getmtime(partial_file)
                    except FileNotFoundError:
                        # this file got renamed or removed, so ignore!
                        continue

                    if time.time() - mod_time > min_partial_purge_age_secs:
                        self.logger.warning(
                            f"Partial file {partial_file} is older than"
                            f" {min_partial_purge_age_secs} seconds and will be"
                            " removed..."
                        )

                        try:
                            os.remove(partial_file)
                            self.logger.warning(f"Partial file {partial_file} deleted")
                        except FileNotFoundError:
                            # this file got renamed or removed, so ignore!
                            continue
        return True

    def obsid_check_assembled_handler(self):
        """This thread sleeps most of the time, but wakes up
        to check if there are any completely assembled sets of
        gpubox files which we should process"""
        while self.running:
            self.logger.debug(f"sleeping for {self.assemble_check_seconds} secs")
            time.sleep(self.assemble_check_seconds)

            if self.running:
                self.logger.debug("Waking up and checking un-assembled observations...")

                obs_id_list = []

                # make a list of all obs_ids in the work path
                for filename in os.listdir(self.assemble_path):
                    full_filename = os.path.join(self.assemble_path, filename)
                    if os.path.isdir(full_filename):
                        obs_id_list.append(filename)

                # sort it
                obs_id_list.sort()

                # Check each one
                for obs_id in obs_id_list:
                    obs_assemble_path = os.path.join(self.assemble_path, obs_id)
                    if self.check_obs_is_ready_to_process(obs_id, obs_assemble_path):
                        # do processing
                        obs_processing_path = os.path.join(self.processing_path, obs_id)

                        self.logger.info(
                            f"{obs_id} is ready for processing. Moving" f" {obs_assemble_path} to {obs_processing_path}"
                        )

                        # Move the directory to the processing path
                        shutil.move(obs_assemble_path, obs_processing_path)
                        self.add_to_processing_queue(obs_processing_path)
        return True

    def add_to_processing_queue(self, item):
        """Adds a dir containing all the files for an obsid
        to the processing queue"""
        self.processing_queue.put(item)
        self.logger.info(f"{item} added to processing_queue." f" ({self.processing_queue.qsize()}) in queue.")

    def processing_handler(self, item) -> bool:
        """This is triggered when an obsid dir is moved into
        the processing directory, indicating it is ready to
        have birli then hyperdrive run on it.
        item is the processing directory"""
        birli_success: bool = False

        file_no_path = item.split("/")
        obs_id = file_no_path[-1][0:10]
        metafits_filename = os.path.join(item, str(obs_id) + "_metafits.fits")
        uvfits_filename = os.path.join(item, str(obs_id) + ".uvfits")

        # Run Birli
        birli_success = mwax_calvin_utils.run_birli(
            self,
            metafits_filename,
            uvfits_filename,
            obs_id,
            item,
        )

        if birli_success:
            # If all good run hyperdrive- once per uvfits file created
            # N (where N>1) uvfits are generated if Birli sees the obs is picket fence
            # Therefore we need to run hyperdrive N times too
            #
            # get a list of the uvfits files
            uvfits_files = glob.glob(os.path.join(item, "*.uvfits"))

            # Run hyperdrive
            hyperdrive_success: bool = mwax_calvin_utils.run_hyperdrive(
                self, uvfits_files, metafits_filename, obs_id, item
            )

            # Did we have N number of successful runs?
            if hyperdrive_success:
                # Run hyperdrive and get plots and stats
                mwax_calvin_utils.run_hyperdrive_stats(self, uvfits_files, metafits_filename, obs_id, item)

                # now move the whole dir
                # to the upload_path
                upload_path = os.path.join(self.upload_path, obs_id)
                self.logger.info(f"{obs_id}: moving successfull files to" f" {upload_path} for upload")
                shutil.move(item, upload_path)
                # Now add to queue
                self.add_to_upload_queue(upload_path)

        if not (birli_success and hyperdrive_success):
            self.processing_error_count += 1

        return True

    def add_to_upload_queue(self, item):
        """Adds a dir containing all the files for an obsid
        to the upload queue"""
        self.upload_queue.put(item)
        self.logger.info(f"{item} added to upload_queue." f" ({self.upload_queue.qsize()}) in queue.")

    def process_phase_fits(
        self, item, tiles, chanblocks_hz, all_xx_solns, all_yy_solns, weights, soln_tile_ids, phase_fit_niter
    ):
        """
        Fit a line to each tile phase solution, return a dataframe of phase fit parameters for each
        tile and pol
        """
        fits = []
        phase_diff_path = os.path.join(item, "phase_diff.txt")
        # by default we don't want to apply any phase rotation.
        phase_diff = np.full((len(chanblocks_hz),), 1.0, dtype=np.complex128)
        if os.path.exists(phase_diff_path):
            # phase_diff_raw is an array, first column is frequency, second column is phase difference
            phase_diff_raw = np.loadtxt(phase_diff_path)
            for i, chanblock_hz in enumerate(chanblocks_hz):
                # find the closest frequency in phase_diff_raw
                idx = np.abs(phase_diff_raw[:, 0] - chanblock_hz).argmin()
                diff = phase_diff_raw[idx, 1]
                phase_diff[i] = np.exp(-1j * diff)

        for soln_idx, (tile_id, xx_solns, yy_solns) in enumerate(zip(soln_tile_ids, all_xx_solns[0], all_yy_solns[0])):
            for pol, solns in [("XX", xx_solns), ("YY", yy_solns)]:
                id_matches = tiles[tiles.id == tile_id]
                if len(id_matches) != 1:
                    continue
                tile = id_matches.iloc[0]
                if tile.flag:
                    continue
                name = tile.name
                if tile.flavor.endswith("-NI"):
                    solns *= phase_diff
                # else:
                #     continue
                try:
                    fit = fit_phase_line(chanblocks_hz, solns, weights, niter=phase_fit_niter)
                except Exception as exc:
                    self.logger.error(f"{item} - {tile_id=:4} {pol} ({name}) {exc}")
                    continue
                self.logger.debug(f"{item} - {tile_id=:4} {pol} ({name}) {fit=}")
                fits.append([tile_id, soln_idx, pol, *fit])

        return DataFrame(fits, columns=["tile_id", "soln_idx", "pol", *PhaseFitInfo._fields])

    def process_gain_fits(
        self, item, tiles, chanblocks_hz, all_xx_solns, all_yy_solns, weights, soln_tile_ids, chanblocks_per_coarse
    ):
        """
        for each tile, pol, fit a GainFitInfo to the gains
        """
        fits = []
        for soln_idx, (tile_id, xx_solns, yy_solns) in enumerate(zip(soln_tile_ids, all_xx_solns[0], all_yy_solns[0])):
            for pol, solns in [("XX", xx_solns), ("YY", yy_solns)]:
                id_matches = tiles[tiles.id == tile_id]
                if len(id_matches) != 1:
                    continue
                tile = id_matches.iloc[0]
                if tile.flag:
                    continue
                name = tile.name
                try:
                    fit = fit_gain(chanblocks_hz, solns, weights, chanblocks_per_coarse)
                except Exception as exc:
                    self.logger.error(f"{item} - {tile_id=:4} {pol} ({name}) {exc}")
                    continue
                self.logger.debug(f"{item} - {tile_id=:4} {pol} ({name}) {fit=}")
                fits.append([tile_id, soln_idx, pol, *fit])
        self.logger.warning("TODO: fake gain fits!")

        return DataFrame(fits, columns=["tile_id", "soln_idx", "pol", *GainFitInfo._fields])

    def upload_handler(self, item: str) -> bool:
        """Will deal with completed hyperdrive solutions
        by getting them into a format we can insert into
        the calibration database

        item is a fully qualified directory which MUST
        end in the obsid"""

        conn = None
        try:
            # get obs_id
            file_no_path = item.split("/")
            obs_id = int(file_no_path[-1][0:10])

            metafits_files = glob.glob(os.path.join(item, "*_metafits.fits"))
            # if len(metafits_files) > 1:
            #     self.logger.warning(f"{item} - more than one metafits file found.")

            self.logger.debug(f"{item} - {metafits_files=}")
            fits_solution_files = sorted(glob.glob(os.path.join(item, "*_solutions.fits")))
            # _bin_solution_files = glob.glob(os.path.join(item, "*_solutions.bin"))
            self.logger.debug(f"{item} - uploading {fits_solution_files=}")

            soln_group = HyperfitsSolutionGroup(
                [Metafits(f) for f in metafits_files], [HyperfitsSolution(f) for f in fits_solution_files]
            )

            # get tiles
            tiles = soln_group.metafits_tiles_df
            self.logger.debug(f"{item} - metafits tiles:\n{tiles.to_string(max_rows=999)}")

            # determine refant
            unflagged_tiles = tiles[tiles.flag == 0]
            if not len(unflagged_tiles):
                raise ValueError("No unflagged tiles found")
            refant = unflagged_tiles.sort_values(by=["id"]).iloc[0]
            self.logger.debug(f"{item} - {refant['name']=} ({refant['id']})")

            # get channel info
            chaninfo = soln_group.metafits_chan_info
            self.logger.debug(f"{item} - {chaninfo=}")
            all_coarse_chan_ranges = chaninfo.coarse_chan_ranges

            if len(fits_solution_files) != len(all_coarse_chan_ranges):
                raise RuntimeError(
                    f"{item} - number of solution files ({len(fits_solution_files)})"
                    f" does not match number of coarse chan ranges in metafits {len(all_coarse_chan_ranges)}"
                )

            chanblocks_per_coarse = soln_group.chanblocks_per_coarse
            # all_chanblocks_hz = soln_group.all_chanblocks_hz
            all_chanblocks_hz = np.concatenate(soln_group.all_chanblocks_hz)
            self.logger.debug(f"{item} - {chanblocks_per_coarse=}, {all_chanblocks_hz=}")

            soln_tile_ids, all_xx_solns, all_yy_solns = soln_group.get_solns(refant["name"])

            weights = soln_group.weights

            phase_fits = self.process_phase_fits(
                item,
                unflagged_tiles,
                all_chanblocks_hz,
                all_xx_solns,
                all_yy_solns,
                weights,
                soln_tile_ids,
                self.phase_fit_niter,
            )
            gain_fits = self.process_gain_fits(
                item,
                unflagged_tiles,
                all_chanblocks_hz,
                all_xx_solns,
                all_yy_solns,
                weights,
                soln_tile_ids,
                chanblocks_per_coarse,
            )

            # if ~np.any(np.isfinite(phase_fits["length"])):
            #     self.logger.warning(f"{item} - no valid phase fits found, continuing anyway")

            # Matplotlib stuff seems to break pytest so we will
            # pass false in for pytest stuff (or if we don't want debug)
            if self.produce_debug_plots:
                phase_fits_pivot = debug_phase_fits(
                    phase_fits,
                    tiles,
                    all_chanblocks_hz,
                    all_xx_solns[0],
                    all_yy_solns[0],
                    weights,
                    prefix=f"{item}/",
                    plot_residual=True,
                )
            # phase_fits_pivot = pivot_phase_fits(phase_fits, tiles)
            # self.logger.debug(f"{item} - fits:\n{phase_fits_pivot.to_string(max_rows=512)}")
            success = True

            # get a database connection, unless we are using dummy connection (for testing)
            transaction_cursor = None
            if not self.db_handler_object.dummy:
                conn = self.db_handler_object.pool.getconn()

                # Create a cursor
                transaction_cursor = conn.cursor()

            (success, fit_id) = insert_calibration_fits_row(
                self.db_handler_object,
                transaction_cursor,
                obs_id=obs_id,
                code_version=version.get_mwax_mover_version_string(),
                creator="calvin",
                fit_niter=self.phase_fit_niter,
                fit_limit=20,
            )

            if not success:
                self.logger.error(f"{item} - failed to insert calibration fit")

                if transaction_cursor:
                    # Rollback the calibration_fit row
                    conn.rollback()
                    # close the cursor
                    transaction_cursor.close()
                raise Exception(f"{item} - failed to insert calibration fit")

            for tile_id in soln_tile_ids:
                some_fits = False
                try:
                    x_gains = gain_fits[(gain_fits.tile_id == tile_id) & (gain_fits.pol == "XX")].iloc[0]
                    some_fits = True
                except IndexError:
                    x_gains = GainFitInfo.nan()

                try:
                    y_gains = gain_fits[(gain_fits.tile_id == tile_id) & (gain_fits.pol == "YY")].iloc[0]
                    some_fits = True
                except IndexError:
                    y_gains = GainFitInfo.nan()

                try:
                    x_phase = phase_fits[(phase_fits.tile_id == tile_id) & (phase_fits.pol == "XX")].iloc[0]
                    some_fits = True
                except IndexError:
                    x_phase = PhaseFitInfo.nan()

                try:
                    y_phase = phase_fits[(phase_fits.tile_id == tile_id) & (phase_fits.pol == "YY")].iloc[0]
                    some_fits = True
                except IndexError:
                    y_phase = PhaseFitInfo.nan()

                if not some_fits:
                    # we could `continue` here, which avoids inserting an empty row in the
                    # database, however we want to stick to the old behaviour for now.
                    # continue
                    pass

                success = insert_calibration_solutions_row(
                    self.db_handler_object,
                    transaction_cursor,
                    int(fit_id),
                    int(obs_id),
                    int(tile_id),
                    x_phase.length,
                    x_phase.intercept,
                    x_gains.gains,
                    y_phase.length,
                    y_phase.intercept,
                    y_gains.gains,
                    x_gains.pol1,
                    y_gains.pol1,
                    x_phase.sigma_resid,
                    x_phase.chi2dof,
                    x_phase.quality,
                    y_phase.sigma_resid,
                    y_phase.chi2dof,
                    y_phase.quality,
                    x_gains.quality,
                    y_gains.quality,
                    x_gains.sigma_resid,
                    y_gains.sigma_resid,
                    x_gains.pol0,
                    y_gains.pol0,
                )

                if not success:
                    self.logger.error(f"{item} - failed to insert calibration solution for tile {tile_id}")
                    if transaction_cursor:
                        # Rollback the calibration_fit row and any solutions rows already inserted
                        transaction_cursor.connection.rollback()
                        # close the cursor
                        transaction_cursor.close()

                    raise Exception(f"{item} - failed to insert calibration solution for tile {tile_id}")

            # on success move to complete
            if success:
                # The whole calibration solution was inserted ok. Commit the transation
                # unless we are a dummy db_handler
                if transaction_cursor:
                    transaction_cursor.connection.commit()
                    transaction_cursor.close()

                #
                # If this cal solution was a requested one, update it to completed
                #
                update_calsolution_request(
                    self.db_handler_object, obs_id, CALIBRATION_REQUEST_SUCCESS, "", self.hostname
                )

                # now move the whole dir
                # to the complete path
                if not self.complete_path:
                    raise ValueError("No complete path specified")
                complete_path = os.path.join(self.complete_path, str(obs_id))
                self.logger.info(f"{obs_id}: moving successfull files to" f" {complete_path} for review.")
                shutil.move(item, complete_path)

                if not self.keep_completed_visibility_files:
                    # Remove visibilitiy files
                    visibility_files = glob.glob(os.path.join(complete_path, f"{obs_id}_*_*_*.fits"))
                    for file_to_delete in visibility_files:
                        os.remove(file_to_delete)

                    # Now remove uvfits too
                    uvfits_files = glob.glob(os.path.join(complete_path, f"{obs_id}_*.uvfits"))
                    for file_to_delete in uvfits_files:
                        os.remove(file_to_delete)

                self.completed_count += 1

                return True
        except Exception:
            error_text = f"{item} - Error in upload_handler:\n{traceback.format_exc()}"
            self.logger.exception(error_text)

            # Write an error readme
            write_readme_file(
                self.logger,
                os.path.join(item, "readme_error.txt"),
                f"upload_handler({item})",
                -999,
                "",
                error_text,
            )

            # move all the files into upload_error directory
            upload_error_path = os.path.join(self.upload_error_path, str(obs_id))
            self.logger.info(f"{obs_id}: moving failed files to" f" {upload_error_path} for review.")
            shutil.move(item, upload_error_path)

            #
            # If this cal solution was a requested one, update it to failed
            #
            update_calsolution_request(
                self.db_handler_object,
                obs_id,
                CALIBRATION_REQUEST_ERROR_STATUS,
                error_text.replace("\n", ""),
                self.hostname,
            )

            self.upload_error_count += 1

            return False
        finally:
            if not self.db_handler_object.dummy:
                if conn:
                    self.db_handler_object.pool.putconn(conn)

    def stop(self):
        """Shutsdown all processes"""
        for watcher in self.watchers:
            watcher.stop()

        for queue_worker in self.queue_workers:
            queue_worker.stop()

        # check for a hyperdrive process and kill it
        self.logger.debug("Checking for running hyperdrive process...")
        if self.hyperdrive_popen_process:
            if not self.hyperdrive_popen_process.poll():
                self.logger.debug("Running hyperdrive process found. Sending it SIGINT...")
                self.hyperdrive_popen_process.send_signal(signal.SIGINT)
                self.logger.debug("SIGINT sent to Hyperdrive")

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
                    # Short timeout- everything other than a running hyperdrive
                    # instance should have joined by now.
                    worker_thread.join(timeout=10)
                self.logger.debug(f"QueueWorker {thread_name} Stopped")

        # Close all database connections
        if not self.db_handler_object.dummy:
            if self.db_handler_object.pool:
                self.db_handler_object.pool.closeall()

        self.ready_to_exit = True

    def health_loop(self):
        """Send health information via UDP multicast"""
        while self.running:
            # Code to run by the health thread
            status_dict = self.get_status()

            # Convert the status to bytes
            status_bytes = json.dumps(status_dict).encode("utf-8")

            # Send the bytes
            try:
                utils.send_multicast(
                    self.health_multicast_interface_ip,
                    self.health_multicast_ip,
                    self.health_multicast_port,
                    status_bytes,
                    self.health_multicast_hops,
                )
            except Exception as catch_all_exception:  # pylint: disable=broad-except
                self.logger.warning("health_handler: Failed to send health information." f" {catch_all_exception}")

            # Sleep for a second
            time.sleep(1)

    def get_status(self) -> dict:
        """Returns status of all process as a dictionary"""
        main_status = {
            "Unix timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
            "incoming_realtime_queue": self.assembly_realtime_watch_queue.qsize(),
            "incoming_asvo_queue": self.assembly_asvo_watch_queue.qsize(),
            "processing_queue": self.processing_queue.qsize(),
            "processing_errors": self.processing_error_count,
            "upload_queue": self.upload_queue.qsize(),
            "upload_errors": self.upload_error_count,
            "completed:": self.completed_count,
        }

        status = {"main": main_status}

        return status

    def signal_handler(self, _signum, _frame):
        """Handles SIGINT and SIGTERM"""
        self.logger.warning("Interrupted. Shutting down processor...")
        self.running = False

        # Stop any Processors
        self.stop()

    def initialise(self, config_filename):
        """Initialise the processor from the command line"""
        # Get this hosts hostname
        self.hostname = utils.get_hostname()

        if not os.path.exists(config_filename):
            print(f"Configuration file location {config_filename} does not" " exist. Quitting.")
            sys.exit(1)

        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)

        # Parse config file
        config = ConfigParser()
        config.read_file(open(config_filename, "r", encoding="utf-8"))

        # read from config file
        self.log_path = config.get("mwax mover", "log_path")

        if not os.path.exists(self.log_path):
            print(f"log_path {self.log_path} does not exist. Quiting.")
            sys.exit(1)

        # It's now safe to start logging
        # start logging
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.DEBUG)
        console_log = logging.StreamHandler()
        console_log.setLevel(logging.DEBUG)
        console_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        self.logger.addHandler(console_log)

        if config.getboolean("mwax mover", "coloredlogs", fallback=False):
            coloredlogs.install(level="DEBUG", logger=self.logger)

        # Removing file logging for now
        # file_log = logging.FileHandler(filename=os.path.join(self.log_path, "calvin_processor_main.log"))
        # file_log.setLevel(logging.DEBUG)
        # file_log.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
        # self.logger.addHandler(file_log)

        self.logger.info("Starting mwax_calvin_processor" f" processor...v{version.get_mwax_mover_version_string()}")
        self.logger.info(f"Reading config file: {config_filename}")

        # health
        self.health_multicast_ip = utils.read_config(self.logger, config, "mwax mover", "health_multicast_ip")
        self.health_multicast_port = int(utils.read_config(self.logger, config, "mwax mover", "health_multicast_port"))
        self.health_multicast_hops = int(utils.read_config(self.logger, config, "mwax mover", "health_multicast_hops"))
        self.health_multicast_interface_name = utils.read_config(
            self.logger,
            config,
            "mwax mover",
            "health_multicast_interface_name",
        )

        # get this hosts primary network interface ip
        self.health_multicast_interface_ip = utils.get_ip_address(self.health_multicast_interface_name)
        self.logger.info(f"IP for sending multicast: {self.health_multicast_interface_ip}")

        #
        # MRO database
        #
        self.mro_metadatadb_host = utils.read_config(self.logger, config, "mro metadata database", "host")

        if self.mro_metadatadb_host != mwax_db.DUMMY_DB:
            self.mro_metadatadb_db = utils.read_config(self.logger, config, "mro metadata database", "db")
            self.mro_metadatadb_user = utils.read_config(self.logger, config, "mro metadata database", "user")
            self.mro_metadatadb_pass = utils.read_config(self.logger, config, "mro metadata database", "pass", True)
            self.mro_metadatadb_port = utils.read_config(self.logger, config, "mro metadata database", "port")
        else:
            self.mro_metadatadb_db = None
            self.mro_metadatadb_user = None
            self.mro_metadatadb_pass = None
            self.mro_metadatadb_port = None

        # Initiate database connection for rmo metadata db
        self.db_handler_object = mwax_db.MWAXDBHandler(
            logger=self.logger,
            host=self.mro_metadatadb_host,
            port=self.mro_metadatadb_port,
            db_name=self.mro_metadatadb_db,
            user=self.mro_metadatadb_user,
            password=self.mro_metadatadb_pass,
        )

        # metadata web service URL
        self.metadata_webservice_url = utils.read_config(
            self.logger,
            config,
            "mwax mover",
            "metadata_webservice_url",
        )

        #
        # Assembly config
        #

        # Get the watch dirs
        self.incoming_realtime_watch_path = utils.read_config(
            self.logger,
            config,
            "assembly",
            "incoming_realtime_watch_path",
        )

        if not os.path.exists(self.incoming_realtime_watch_path):
            self.logger.error(
                "incoming_realtime_watch_path location "
                f" {self.incoming_realtime_watch_path} does not exist. Quitting."
            )
            sys.exit(1)

        self.remove_partial_files_check_seconds = int(
            utils.read_config(self.logger, config, "assembly", "remove_partial_files_check_seconds")
        )

        self.incoming_asvo_watch_path = utils.read_config(
            self.logger,
            config,
            "assembly",
            "incoming_asvo_watch_path",
        )

        if not os.path.exists(self.incoming_realtime_watch_path):
            self.logger.error(
                "incoming_realtime_watch_path location "
                f" {self.incoming_realtime_watch_path} does not exist. Quitting."
            )
            sys.exit(1)

        # Get the assemble dir
        self.assemble_path = utils.read_config(
            self.logger,
            config,
            "assembly",
            "assemble_path",
        )

        if not os.path.exists(self.assemble_path):
            self.logger.error("assemble_path location " f" {self.assemble_path} does not exist. Quitting.")
            sys.exit(1)

        # Set assemble_check_seconds
        # How many secs between waiting for all gpubox files to arrive?
        self.assemble_check_seconds = int(utils.read_config(self.logger, config, "assembly", "assemble_check_seconds"))

        #
        # processing config
        #

        # Get the processing dir
        self.processing_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "processing_path",
        )

        if not os.path.exists(self.processing_path):
            self.logger.error("processing_path location " f" {self.processing_path} does not exist. Quitting.")
            sys.exit(1)

        # Get the processing error dir
        self.processing_error_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "processing_error_path",
        )

        if not os.path.exists(self.processing_error_path):
            self.logger.error(
                "processing_error_path location " f" {self.processing_error_path} does not exist. Quitting."
            )
            sys.exit(1)

        self.phase_fit_niter = int(
            utils.read_config(
                self.logger,
                config,
                "processing",
                "phase_fit_niter",
            )
        )

        #
        # Hyperdrive config
        #
        self.source_list_filename = utils.read_config(
            self.logger,
            config,
            "processing",
            "source_list_filename",
        )

        if not os.path.exists(self.source_list_filename):
            self.logger.error(
                "source_list_filename location " f" {self.source_list_filename} does not exist. Quitting."
            )
            sys.exit(1)

        self.source_list_type = utils.read_config(
            self.logger,
            config,
            "processing",
            "source_list_type",
        )

        # hyperdrive timeout
        self.hyperdrive_timeout = int(
            utils.read_config(
                self.logger,
                config,
                "processing",
                "hyperdrive_timeout",
            )
        )

        # Get the hyperdrive binary
        self.hyperdrive_binary_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "hyperdrive_binary_path",
        )

        if not os.path.exists(self.hyperdrive_binary_path):
            self.logger.error(
                "hyperdrive_binary_path location " f" {self.hyperdrive_binary_path} does not exist. Quitting."
            )
            sys.exit(1)

        # Birli timeout
        self.birli_timeout = int(
            utils.read_config(
                self.logger,
                config,
                "processing",
                "birli_timeout",
            )
        )

        # Get the Birli binary
        self.birli_binary_path = utils.read_config(
            self.logger,
            config,
            "processing",
            "birli_binary_path",
        )

        if not os.path.exists(self.birli_binary_path):
            self.logger.error("birli_binary_path location " f" {self.birli_binary_path} does not exist. Quitting.")
            sys.exit(1)

        # upload path
        self.upload_path = utils.read_config(
            self.logger,
            config,
            "upload",
            "upload_path",
        )

        if not os.path.exists(self.upload_path):
            self.logger.error("processing_upload_path location " f" {self.upload_path} does not exist. Quitting.")
            sys.exit(1)

        # upload error path
        self.upload_error_path = utils.read_config(
            self.logger,
            config,
            "upload",
            "upload_error_path",
        )

        if not os.path.exists(self.upload_error_path):
            self.logger.error(
                "processing_upload_error_path location " f" {self.upload_error_path} does not exist. Quitting."
            )
            sys.exit(1)

        # complete path
        self.complete_path = utils.read_config(
            self.logger,
            config,
            "complete",
            "complete_path",
        )

        if not os.path.exists(self.complete_path):
            self.logger.error("complete_path location " f" {self.complete_path} does not exist. Quitting.")
            sys.exit(1)

        self.keep_completed_visibility_files = utils.read_config_bool(
            self.logger,
            config,
            "complete",
            "keep_completed_visibility_files",
        )

    def initialise_from_command_line(self):
        """Initialise if initiated from command line"""

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_calvin_processor: a command line tool which is part of the"
            " MWA correlator for the MWA. It will monitor directories on each"
            " mwax server and, upon detecting a calibrator observation, will"
            " execute birli then hyperdrive to generate calibration"
            " solutions. (mwax_mover"
            f" v{version.get_mwax_mover_version_string()})\n"
        )

        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        self.initialise(config_filename)


def main():
    """Mainline function"""
    processor = MWAXCalvinProcessor()

    try:
        processor.initialise_from_command_line()
        processor.start()
        sys.exit(0)
    except Exception as catch_all_exception:  # pylint: disable=broad-except
        if processor.logger:
            processor.logger.exception(str(catch_all_exception))
        else:
            print(str(catch_all_exception))


if __name__ == "__main__":
    main()
