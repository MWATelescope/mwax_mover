"""
Module managing the MWAXCalvinProcessor for near-realtime
calibration form the calvin HPC cluster

* Realtime Calibration Jobs
1. Detect new calibration observation completed
2. Create SBATCH script:
   * Copy data from all MWAX boxes for all files of the observation
   * Process and upload the solution
   * Let MWAX boxes know they can release the files
   * Clean up

* MWA ASVO Calibration Jobs
1. Detect new calibration requested by MWA ASVO
2. Create SBATCH script:
   * Submit MWA ASVO job
   * Download files
   * Process and upload the solution
   * Clean up
"""

import argparse
import json
import logging
import os
import signal
import subprocess
import sys
import threading
import time
from configparser import ConfigParser
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

from mwax_mover import mwax_asvo_helper, utils, version
from mwax_mover.mwax_calvin_utils import (
    CalvinJobType,
    count_slurm_asvo_jobs,
    create_sbatch_script,
    submit_sbatch,
)
from mwax_mover.mwax_db import (
    MWAXDBHandler,
    get_unattempted_calibration_requests,
    get_unattempted_unrequested_cal_obsids,
    insert_calibration_request_row,
    update_calibration_request_slurm_status,
    update_calsolution_request_submit_mwa_asvo_job_status,
)

# Setup root logger
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(name)s.%(funcName)s, %(message)s"))
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# Default number of published fit directories uploaded per pass over a single
# plot upload path. Uploads run newest-first, and the directory is re-scanned at
# the start of every pass, so a bounded batch is what allows a fit published
# moments ago to overtake a backlog rather than queue behind it. With thousands
# of old fits pending, an unbounded pass would upload all of them before it ever
# looked at the directory again.
DEFAULT_PLOT_UPLOAD_MAX_FITS_PER_PASS = 100


def fit_dir_sort_key(fit_dir: Path) -> tuple[int, int, str]:
    """Build a sort key ordering fit directories newest (largest fitid) first.

    Fit directories are named for their fitid, which is a microsecond epoch
    value (see ``mwax_db.insert_calibration_fits_row``), so a larger fitid is
    always a more recent fit. Sorting numerically rather than lexicographically
    means the ordering does not silently depend on every fitid happening to have
    the same number of digits.

    Any directory whose name is not an integer is not a fit directory. Those
    sort last, alphabetically, so an unexpected entry can never displace real
    fits from the head of the batch.

    Args:
        fit_dir: The fit directory to derive a sort key for. Only its name is
            used.

    Returns:
        A tuple suitable for ``list.sort(key=...)``: ``(0, -fitid, "")`` for
        numerically named directories and ``(1, 0, name)`` for anything else.
    """
    try:
        return (0, -int(fit_dir.name), "")
    except ValueError:
        return (1, 0, fit_dir.name)


class CalibrationRequest:
    """One row of work read from the calibration_request table."""

    def __init__(self):
        """Initialize a CalibrationRequest instance.

        Stores information about a calibration request including observation ID,
        request ID, and job type and if it is a bulk request.
        """
        self.obs_id: int = 0
        self.request_id: int = 0
        self.type: CalvinJobType
        self.bulk_request: bool = False


class MWAXCalvinController:
    """The main class managing calvin processes:

    realtime: Get new realtime calibration requests, copies data from mwax
    hosts, then submits job to process them and upload solutions to the db.

    mwa_asvo: Gets new MWA ASVO calibration requests, submits MWA ASVO jobs to download
    data, then submits job to process them and upload solutions to the db.
    NOTE: no downloading is done in this code. It is handled by CalvinProcessor as it
    will be running on the calvin HPC nodes"""

    def __init__(
        self,
    ):
        """Initialize MWAXCalvinController with default values.

        Sets up instance variables for managing realtime and MWA ASVO calibration
        requests, database connections, and health monitoring.
        """
        # General
        self.log_path: str = ""
        self.hostname: str = ""
        self.db_handler: MWAXDBHandler
        self.config_filename: str = ""
        self.worker_config_filename: str = ""
        self.running: bool = False
        self.ready_to_exit: bool = False

        # health
        self.health_multicast_interface_ip: str = ""
        self.health_multicast_interface_name: str = ""
        self.health_multicast_ip: str = ""
        self.health_multicast_port: int = 0
        self.health_multicast_hops: int = 0
        # We update these two only every 10 seconds (in the health thread) to not hammer the server(s)
        self.slurm_queue_size: int = 0
        self.mwa_asvo_vis_jobs_in_progress: int = 0

        self.realtime_slurm_jobs_submitted: int = 0
        self.mwa_asvo_slurm_jobs_submitted: int = 0
        self.giant_squid_errors: int = 0
        self.database_errors: int = 0
        self.slurm_errors: int = 0
        self.mwa_asvo_errors: int = 0

        # calvin settings
        self.check_interval_seconds: int = 0
        self.script_path = ""
        self.oldest_cal_obs_id: int = 0
        self.max_in_progress_asvo_jobs: int = 999
        self.mwa_asvo_calibration_requests_queued = 0

        # giant-squid
        self.mwa_asvo_outage_check_seconds: int = 0
        self.mwa_asvo_longest_wait_time_seconds: int = 0
        self.giant_squid_binary_path: str = ""
        self.giant_squid_list_timeout_seconds: int = 0
        self.giant_squid_submitvis_timeout_seconds: int = 0

        # S3 upload settings
        self.s3_profile: str = ""
        self.s3_bucket: str = ""
        self.plot_upload_interval_secs = 120
        self.plot_upload_max_fits_per_pass: int = DEFAULT_PLOT_UPLOAD_MAX_FITS_PER_PASS

        # Create a stop event handler for the plot uploader thread
        self.plot_uploader_stop_event = threading.Event()

        # Helper for MWA ASVO interactions and job record keeping
        self.mwax_asvo_helper: mwax_asvo_helper.MWAASVOHelper = mwax_asvo_helper.MWAASVOHelper()

    def start(self):
        """Start the controller and main event loop.

        Initializes database connection pool, starts health monitoring thread,
        and enters main loop to process calibration requests.
        """
        self.running = True

        # creating database connection pool(s)
        logger.info("Starting database connection pool...")
        self.db_handler.start_database_pool()

        # create a health thread
        logger.info("Starting health_thread...")
        health_thread = threading.Thread(name="health_thread", target=self.health_loop, daemon=True)
        health_thread.start()

        # Start plot upload thread
        # Create a thread to handle uploading plots to S3
        self.plot_upload_thread = threading.Thread(
            target=self.plot_upload_handler,
            args=[self.plot_uploader_stop_event],
            name="plot-upload",
            daemon=True,
        )
        logger.info("Starting plot_upload_thread...")
        self.plot_upload_thread.start()

        logger.info("Started...")

        # Main loop
        while self.running:
            self.main_loop_handler()

            tracked_asvo_job_count: int = 0
            with self.mwax_asvo_helper.current_asvo_jobs_lock:
                for job in self.mwax_asvo_helper.current_asvo_jobs:
                    if job.download_slurm_job_submitted is False and job.download_error_datetime is None:
                        tracked_asvo_job_count += 1
                        logger.debug(
                            f"{job} {job.job_state}; "
                            f"elapsed: {job.elapsed_time_seconds()} s; "
                            f"last_seen={job.last_seen_datetime}"
                        )

            # Dump out the counters
            logger.debug(
                "Counters:\n"
                f"Tracked MWA ASVO jobs        : {tracked_asvo_job_count}\n"
                f"realtime_slurm_jobs_submitted: {self.realtime_slurm_jobs_submitted}\n"
                f"mwa_asvo_slurm_jobs_submitted: {self.mwa_asvo_slurm_jobs_submitted}\n"
                f"giant_squid_errors           : {self.giant_squid_errors}\n"
                f"mwa_asvo_errors              : {self.mwa_asvo_errors}\n"
                f"database_errors              : {self.database_errors}\n"
                f"slurm_errors                 : {self.slurm_errors}"
            )

            # If we're still running, wait before we do the next loop
            if self.running:
                logger.debug(f"Sleeping for {self.check_interval_seconds} seconds")
                self.sleep(self.check_interval_seconds)
        #
        # Finished- do some clean up
        #
        while not self.ready_to_exit:
            self.sleep(1)

        # Final log message
        logger.info("Completed Successfully")

    def plot_upload_handler(self, stop_event: threading.Event) -> None:
        """Main loop for the background upload thread.

        Every ``self.plot_upload_interval_secs`` seconds, walks each path in
        ``self.plot_upload_paths`` looking for published fit directories, uploads
        the newest ``self.plot_upload_max_fits_per_pass`` of them to S3, and
        removes each directory once it is empty. Per-path exponential backoff is
        applied when a pass uploads nothing at all.

        A directory is "published" if its name does not start with a dot.
        ``mwax_calvin_utils.upload_plot_files`` assembles each fit in a
        ``.staging-<fit_id>`` directory and publishes it with a single atomic
        rename, so any directory we can see here is complete. That is what makes
        it safe to delete: this thread runs on a different host to the processor
        that writes these files, so there is no reliable way to *infer*
        completion from emptiness or from mtimes set by another machine's clock.
        Do not reintroduce such a check here.

        Args:
            stop_event: Threading event that signals the loop to exit cleanly.
        """

        # we keep track of failures for each path
        @dataclass
        class UploadPathTracker:
            """Per-path upload state, so one failing path does not stall the others."""

            plot_upload_path: str
            consecutive_failures: int = 0
            next_attempt_time: float = 0.0

            def get_backoff_delay(self) -> float:
                """Calculate exponential backoff delay for a given failure count.

                Returns:
                    Backoff delay in seconds, capped at BACKOFF_MAX_SECS.
                """
                BACKOFF_BASE_SECS = 10
                BACKOFF_MAX_SECS = 3600  # 1 hour

                delay = BACKOFF_BASE_SECS * (2 ** (self.consecutive_failures - 1))
                return min(delay, BACKOFF_MAX_SECS)

        # initialise the upload trackers
        upload_trackers: list[UploadPathTracker] = [
            UploadPathTracker(plot_upload_path=t, next_attempt_time=time.monotonic()) for t in self.plot_upload_paths
        ]

        while not stop_event.is_set():
            for tracker in upload_trackers:
                now: float = time.monotonic()

                if now < tracker.next_attempt_time:
                    remaining = tracker.next_attempt_time - now
                    logger.debug(f"Skipping {tracker.plot_upload_path} — backoff active, {remaining:.1f}s remaining")
                    continue

                try:
                    self.upload_published_fit_dirs(tracker.plot_upload_path)

                    if tracker.consecutive_failures > 0:
                        logger.info(
                            f"Upload succeeded for {tracker.plot_upload_path} "
                            f"after {tracker.consecutive_failures} failure(s)",
                        )
                    tracker.consecutive_failures = 0
                    tracker.next_attempt_time = time.monotonic()

                except subprocess.CalledProcessError as e:
                    tracker.consecutive_failures += 1
                    delay = tracker.get_backoff_delay()
                    tracker.next_attempt_time = time.monotonic() + delay

                    logger.warning(
                        f"rclone move failed for {tracker.plot_upload_path} "
                        f"(failure #{tracker.consecutive_failures}, retrying in {delay:.0f}s): "
                        f"{e.stderr.strip() if e.stderr else str(e)}"
                    )
                except Exception as e:
                    tracker.consecutive_failures += 1
                    delay = tracker.get_backoff_delay()
                    tracker.next_attempt_time = time.monotonic() + delay
                    logger.warning(
                        f"Unexpected error uploading {tracker.plot_upload_path} "
                        f"(failure #{tracker.consecutive_failures}, retrying in {delay:.0f}s): {e}",
                    )

            stop_event.wait(timeout=self.plot_upload_interval_secs)

        logger.debug("Plot upload thread completed successfully.")

    def upload_published_fit_dirs(self, plot_upload_path: str) -> None:
        """Upload a batch of the newest published fit directories, then remove them.

        Each fit directory's contents are moved into ``<bucket>/<fit_dir_name>``
        so the resulting object keys match the URLs written into that fit's
        index.json by ``populate_index_json_entry``.

        Directories are uploaded newest fitid first, and at most
        ``self.plot_upload_max_fits_per_pass`` of them per call. Both matter when
        a backlog builds up: fitids are monotonically increasing, so newest-first
        gets the freshest solutions into S3 without waiting on older ones, and
        the batch limit bounds how long a pass takes so that a fit published
        during it is picked up by the next re-scan rather than after the entire
        backlog has drained.

        A failing fit directory does not abandon the rest of the batch -- with
        newest-first ordering, one permanently broken directory would otherwise
        starve every older one indefinitely. The first error is re-raised at the
        end only if *nothing* in the batch uploaded, which keeps the caller's
        backoff for a genuine S3 or network outage while stopping a single bad
        directory from throttling a path that is otherwise working.

        A directory is only removed after rclone reports success for it *and*
        the directory is confirmed empty, so a partial upload leaves the
        remaining files in place to be retried on the next pass.

        Args:
            plot_upload_path: Base directory containing published fit
                directories, e.g. ``/shared/data/calvin11/calvin/plots``.

        Raises:
            subprocess.CalledProcessError: If rclone exits non-zero and no fit
                directory in the batch uploaded successfully. Propagated so the
                caller can apply backoff to this whole path.
        """
        base = Path(plot_upload_path)
        if not base.is_dir():
            logger.warning(f"Plot upload path {plot_upload_path} does not exist or is not a directory. Skipping.")
            return

        candidates: list[Path] = []

        for fit_dir in base.iterdir():
            # Skip staging dirs (and anything else hidden): these are still
            # being written to by a calvin_processor. See upload_plot_files.
            if fit_dir.name.startswith("."):
                logger.debug(f"Skipping in-progress staging dir {fit_dir}")
                continue

            if not fit_dir.is_dir():
                logger.warning(f"Unexpected file (not a directory) in {plot_upload_path}: {fit_dir}. Skipping.")
                continue

            candidates.append(fit_dir)

        # Newest fitid first, then take only this pass's batch.
        candidates.sort(key=fit_dir_sort_key)
        batch = candidates[: self.plot_upload_max_fits_per_pass]

        if not batch:
            logger.debug(f"No published fit dirs to upload in {plot_upload_path}")
            return

        if len(candidates) > len(batch):
            logger.info(
                f"{len(candidates)} fit dir(s) pending in {plot_upload_path}."
                f" Uploading the {len(batch)} newest this pass; the remainder"
                " follow on subsequent passes."
            )

        first_error: Exception | None = None
        uploaded_count = 0

        for fit_dir in batch:
            try:
                transfers, bytes_moved = utils.rclone_move(
                    str(fit_dir),
                    self.s3_profile,
                    self.s3_bucket,
                    dest_subpath=fit_dir.name,
                )
            except Exception as e:
                # Keep the first error so the caller can still see a real
                # failure, but carry on: the next dir may well succeed.
                if first_error is None:
                    first_error = e

                logger.warning(
                    f"Upload failed for {fit_dir}: "
                    f"{e.stderr.strip() if isinstance(e, subprocess.CalledProcessError) and e.stderr else e}."
                    " Continuing with the remaining fit dirs in this batch."
                )
                continue

            uploaded_count += 1
            logger.info(f"Uploaded {transfers} file(s) ({bytes_moved / 1000.0:.1f} KB) from {fit_dir}")

            # rclone move leaves the (now empty) source dir behind. Only remove
            # it if it really is empty- if rclone skipped anything, we want to
            # keep the dir so the next pass picks up the remainder.
            try:
                fit_dir.rmdir()
                logger.debug(f"Removed uploaded fit dir {fit_dir}")
            except OSError as e:
                logger.warning(f"Not removing {fit_dir}: {e}. Will retry on the next pass.")

        if first_error is not None:
            if uploaded_count == 0:
                # Nothing worked, so this looks like a problem with the path or
                # the S3 endpoint rather than one bad fit dir. Let the caller
                # back this path off.
                raise first_error

            logger.warning(
                f"{len(batch) - uploaded_count} of {len(batch)} fit dir(s) failed to upload from"
                f" {plot_upload_path}, but {uploaded_count} succeeded, so this path is not being"
                " backed off. Failed dirs will be retried on the next pass."
            )

    def main_loop_handler(self):
        """Handle a single iteration of the main control loop.

        Creates calibration requests for unattempted observations, retrieves new
        requests, and submits them to SLURM. Also updates MWA ASVO job statuses.
        """
        # Look at the schedule and create cal requests for any unattempted calibrator observations
        try:
            self.realtime_create_requests_for_unattempted_cal_obs()
        except Exception:
            logger.exception("Error creating requests for unattempted cal obs")
            self.database_errors += 1

        # get new requests
        realtime_requests_list: list[CalibrationRequest] = []
        asvo_requests_list: list[CalibrationRequest] = []

        try:
            realtime_requests_list, asvo_requests_list = self.get_new_calibration_requests()
        except Exception:
            logger.exception("Error retrieving new calibration requests")
            self.database_errors += 1

        logger.info(
            f"Found {len(realtime_requests_list)} realtime,"
            f" {sum(1 for r in asvo_requests_list if not r.bulk_request)} MWA ASVO and"
            f" {sum(1 for r in asvo_requests_list if r.bulk_request)} bulk requests to add."
        )

        # Handle realtime requests first!
        for cal_request in realtime_requests_list:
            self.realtime_submit_to_slurm(cal_request)

        # now handle ASVO if we don't have an outage
        if self.mwax_asvo_helper.mwa_asvo_outage_datetime is not None:
            # There was an outage at some point.
            # If it's been long enough reset the outage and retry
            elapsed: timedelta = datetime.now() - self.mwax_asvo_helper.mwa_asvo_outage_datetime
            if elapsed.total_seconds() >= self.mwa_asvo_outage_check_seconds:
                # Reset the MWA ASVO outage so we retry
                self.mwax_asvo_helper.mwa_asvo_outage_datetime = None

        # We repeat this check here since above we might have reset the outage
        # GO and add jobs up to the limit
        if self.mwax_asvo_helper.mwa_asvo_outage_datetime is None:
            requests_queued = 0
            # we keep our own local var here so we don't go over the limit
            vis_jobs_in_progress = self.mwa_asvo_vis_jobs_in_progress

            # Do one loop through bulk requests=false (i.e. ASVO requests)
            for cal_request in (r for r in asvo_requests_list if not r.bulk_request):
                # For mwa_asvo, if we are not already dealing with this obsid,
                # AND we are below our asvo job limit add it!
                # This prevents us pulling in dupes
                if vis_jobs_in_progress < self.max_in_progress_asvo_jobs:
                    try:
                        if self.mwa_asvo_add_new_asvo_job(
                            cal_request.request_id,
                            cal_request.obs_id,
                            bulk_request=False,
                        ):
                            vis_jobs_in_progress += 1
                    except Exception:
                        logger.exception("Error submitting asvo slurm job")
                        self.slurm_errors += 1
                else:
                    # This job was passed over because we have too many in progress
                    requests_queued += 1

            # Do another loop through bulk_requests=true (i.e. background bulk calibration tasks by Ops team)
            for cal_request in (r for r in asvo_requests_list if r.bulk_request):
                # For mwa_asvo, if we are not already dealing with this obsid,
                # AND we are below our asvo job limit add it!
                # This prevents us pulling in dupes
                if vis_jobs_in_progress < self.max_in_progress_asvo_jobs:
                    try:
                        if self.mwa_asvo_add_new_asvo_job(
                            cal_request.request_id,
                            cal_request.obs_id,
                            bulk_request=True,
                        ):
                            vis_jobs_in_progress += 1
                    except Exception:
                        logger.exception("Error submitting asvo slurm job")
                        self.slurm_errors += 1
                else:
                    # This job was passed over because we have too many in progress
                    requests_queued += 1

            # Now update the value in self- the health loop will report this number
            self.mwa_asvo_calibration_requests_queued = requests_queued

            # Put in a sleep to allow newly created jobs to be shown in giant squid
            # when we run mwa_asvo_update_tracked_jobs()
            self.sleep(5)

            # For mwa_asvo requests, if we're not in an MWA ASVO outage, update jobs check for ready ones
            self.mwa_asvo_update_tracked_jobs()

            self.mwa_asvo_submit_ready_asvo_jobs_to_slurm()

    def realtime_create_requests_for_unattempted_cal_obs(self):
        """Create calibration requests for unattempted calibrator observations.

        Queries the database for calibrator observations that don't have requests
        yet and creates requests for them.
        """
        logger.debug("Querying mwa database for uncalibrated realtime observations ...")
        # First get the obs's which need requests created
        obs_ids_to_request: list[int] | None = get_unattempted_unrequested_cal_obsids(
            self.db_handler, self.oldest_cal_obs_id
        )

        if obs_ids_to_request:
            # Insert them all as requests
            for obs_id in obs_ids_to_request:
                insert_calibration_request_row(self.db_handler, obs_id, True, bulk_request=False)

    def realtime_submit_to_slurm(self, realtime_request: CalibrationRequest):
        """Submit a realtime calibration request to SLURM.

        Creates an SBATCH script for the request and submits it to SLURM for
        processing. Updates the database with submission status.

        Args:
            realtime_request: CalibrationRequest object containing the observation ID
                and request ID to submit.
        """
        logger.debug(f"Attempting to submit {realtime_request.request_id} ({realtime_request.obs_id}) to SLURM...")

        # Create a sbatch script
        script = create_sbatch_script(
            self.worker_config_filename,
            realtime_request.obs_id,
            CalvinJobType.realtime,
            self.log_path,
            [realtime_request.request_id],
            False,
            "",
        )

        success: bool = False
        slurm_job_id: int | None = None

        # submit sbatch script
        try:
            (success, slurm_job_id) = submit_sbatch(
                self.script_path,
                script,
                realtime_request.obs_id,
                [
                    realtime_request.request_id,
                ],
            )

            if success:
                self.realtime_slurm_jobs_submitted += 1
            else:
                self.slurm_errors += 1

        except Exception:
            logger.exception(
                f"{realtime_request.obs_id!s}: Unable to submit a realtime calibration sbatch job. Will retry next loop"
            )
            self.slurm_errors += 1
            return

        if success and slurm_job_id is not None:
            # Success- update request to ensure it does not get picked up next loop
            # i.e. we have already submitted it, so don't do it again!
            try:
                update_calibration_request_slurm_status(
                    self.db_handler,
                    [
                        realtime_request.request_id,
                    ],
                    slurm_job_id,
                    datetime.now().astimezone(),
                    None,
                    None,
                )
            except Exception:
                logger.exception("Unable to update calibration_request table")
                self.database_errors += 1
        else:
            error_message = (
                f"Unable to submit {realtime_request.obs_id} to SLURM for realtime calibration. Will retry next loop"
            )
            logger.error(error_message)

    def mwa_asvo_submit_ready_asvo_jobs_to_slurm(self):
        """Submit ready MWA ASVO jobs to SLURM for download and processing.

        Checks for any MWA ASVO jobs that are in Ready state and submits SBATCH
        scripts to SLURM for them to be downloaded and processed by the calvin
        processor. Handles error states appropriately.
        """

        with self.mwax_asvo_helper.current_asvo_jobs_lock:
            for job in self.mwax_asvo_helper.current_asvo_jobs:
                if not job.download_slurm_job_submitted:
                    if job.job_state == mwax_asvo_helper.MWAASVOJobState.Error:
                        # MWA ASVO completed this job with error
                        error_message = "MWA ASVO completed this job with an Error state"
                        logger.warning(f"{job}: {error_message}")

                        self.mwa_asvo_errors += 1

                        job.download_error_datetime = datetime.now().astimezone()
                        job.download_error_message = error_message

                        # Update database
                        try:
                            update_calsolution_request_submit_mwa_asvo_job_status(
                                self.db_handler,
                                job.request_ids,
                                job.job_id,
                                None,
                                job.download_error_datetime,
                                job.download_error_message,
                            )

                            # now remove the job from our list
                            job.remove_from_list = True

                        except Exception:
                            logger.exception("Unable to update calibration_request table")
                            self.database_errors += 1

                    elif job.job_state == mwax_asvo_helper.MWAASVOJobState.Ready:
                        try:
                            logger.debug(f"{job}: Submitting slurm job")

                            script = create_sbatch_script(
                                self.worker_config_filename,
                                job.obs_id,
                                CalvinJobType.mwa_asvo,
                                self.log_path,
                                job.request_ids,
                                job.bulk_request,
                                f'--mwa-asvo-download-url="{job.download_url}" --asvo-job-id={job.job_id}',
                            )

                            # submit sbatch script
                            success = False
                            slurm_job_id = None
                            try:
                                (success, slurm_job_id) = submit_sbatch(
                                    self.script_path,
                                    script,
                                    job.obs_id,
                                    job.request_ids,
                                )

                                if success:
                                    self.mwa_asvo_slurm_jobs_submitted += 1
                                else:
                                    self.slurm_errors += 1
                            except Exception:
                                logger.exception("Unable to submit MWA ASVO slurm job")
                                self.slurm_errors += 1

                            # all is good
                            if success and slurm_job_id is not None:
                                job.download_slurm_job_submitted = True
                                job.download_slurm_job_id = slurm_job_id
                                job.download_slurm_job_submitted_datetime = datetime.now().astimezone()

                                # Now update the database with the jobid
                                try:
                                    update_calibration_request_slurm_status(
                                        self.db_handler,
                                        job.request_ids,
                                        slurm_job_id,
                                        job.download_slurm_job_submitted_datetime,
                                        None,
                                        None,
                                    )
                                except Exception:
                                    logger.exception("Unable to update calibration_request table")
                                    self.database_errors += 1

                        except Exception:
                            # Something went wrong!
                            logger.exception(
                                f"{job}: Exception submitting sbtach to SLURM, will try again in next loop"
                            )
                            self.slurm_errors += 1

    def stop(self):
        """Shutdown the controller and close all connections.

        Sets running flag to false and closes database connections to initiate
        graceful shutdown.
        """

        self.running = False
        self.plot_uploader_stop_event.set()

        # Close all database connections
        if self.db_handler:
            self.db_handler.close()

        self.ready_to_exit = True

    def health_loop(self):
        """Periodically send health status via UDP multicast.

        Runs in a separate thread and sends status information every second while
        the controller is running.
        """

        while self.running:
            # Update the jobs in progress
            self.mwa_asvo_vis_jobs_in_progress = self.mwax_asvo_helper.get_in_progress_asvo_job_count()

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
                logger.warning(f"health_handler: Failed to send health information. {catch_all_exception}")

            # Sleep for a second
            self.sleep(1)

    def get_status(self) -> dict:
        """Return status of all processes as a dictionary.

        Returns:
            A dictionary containing process status information including running
            state, job counters, and error counts.
        """
        main_status = {
            "unix_timestamp": time.time(),
            "process": type(self).__name__,
            "version": version.get_mwax_mover_version_string(),
            "host": self.hostname,
            "running": self.running,
            "slurm_queue": self.slurm_queue_size,
            "mwa_asvo_calibration_requests_queued": self.mwa_asvo_calibration_requests_queued,
            "mwa_asvo_vis_jobs_in_progress": self.mwa_asvo_vis_jobs_in_progress,
            "realtime_slurm_jobs_submitted": self.realtime_slurm_jobs_submitted,
            "mwa_asvo_slurm_jobs_submitted": self.mwa_asvo_slurm_jobs_submitted,
            "giant_squid_errors": self.giant_squid_errors,
            "mwa_asvo_errors": self.mwa_asvo_errors,
            "database_errors": self.database_errors,
            "slurm_errors": self.slurm_errors,
        }

        return {"main": main_status}

    def signal_handler(self, _signum, _frame):
        """Handle SIGINT and SIGTERM signals for graceful shutdown.

        Args:
            _signum: Signal number (unused).
            _frame: Stack frame (unused).
        """
        logger.warning("Interrupted. Shutting down processor...")

        # Stop any Processors
        self.stop()

    def get_new_calibration_requests(
        self,
    ) -> tuple[list[CalibrationRequest], list[CalibrationRequest]]:
        """Retrieve new calibration requests from database and separate by type.

        Checks for unassigned calibration requests from the database and assigns
        them to this host. MWA ASVO jobs are also added to tracked jobs.

        Returns:
            A tuple of (realtime_requests, asvo_requests), each a list of
            CalibrationRequest objects.
        """

        return_list_realtime: list[CalibrationRequest] = []
        return_list_asvo: list[CalibrationRequest] = []

        if self.running:
            logger.debug("Querying database for unattempted calsolution_requests...")

            # Get the outstanding calibration_requests from the db
            #
            # returned fields: list[Tuple[requestid, calid, realtime, bulk_request]] or None
            results = get_unattempted_calibration_requests(self.db_handler)
            if results is not None:
                logger.debug(f"...Query returned {len(results)} rows")
            else:
                logger.debug("...Query returned 0 rows")

            if results:
                for result in results:
                    new_request = CalibrationRequest()

                    # get the id of the request
                    new_request.request_id = result[0]

                    # Get the obs_id
                    new_request.obs_id = result[1]

                    # Get the type
                    request_type_is_realtime: bool = result[2]
                    if request_type_is_realtime:
                        new_request.type = CalvinJobType.realtime
                        return_list_realtime.append(new_request)
                    else:
                        new_request.bulk_request = result[3]
                        new_request.type = CalvinJobType.mwa_asvo
                        return_list_asvo.append(new_request)

        return return_list_realtime, return_list_asvo

    def mwa_asvo_add_new_asvo_job(self, request_id: int, obs_id: int, bulk_request: bool) -> bool:
        """Add and track a new MWA ASVO job, submitting if not already submitted.

        Args:
            request_id: The request ID for this job.
            obs_id: The observation ID for this job.
            bulk_request: Is this request a bulk request?

        Returns:
            bool: True on successful adding of a job, False if any issue occurred (or job not needed to be added)

        Raises:
            GiantSquidMWAASVOOutageException: If MWA ASVO is experiencing an outage.
            Exception: For other errors during job submission.
        """

        if not self.mwax_asvo_helper.does_request_exist(request_id):
            logger.debug(
                f"Attempting to submit visibility download job for {obs_id} (request: {request_id}) to MWA ASVO..."
            )

            try:
                # Submit job and add to the ones we are tracking
                new_job = self.mwax_asvo_helper.submit_download_job(request_id, obs_id, bulk_request)

                # We submitted a new MWA ASVO job, update the request table so we know we're on it!
                # Update database
                try:
                    update_calsolution_request_submit_mwa_asvo_job_status(
                        self.db_handler,
                        new_job.request_ids,
                        new_job.job_id,
                        new_job.submitted_datetime,
                        None,
                        None,
                    )
                    return True

                except Exception:
                    logger.exception("Unable to update calibration_request table")
                    self.database_errors += 1
                    return False

            except mwax_asvo_helper.GiantSquidMWAASVOOutageException:
                # Handle me!
                logger.warning(
                    f"RequestID: {request_id} ObsID: {obs_id} Cannot submit new download job: MWA ASVO has an outage"
                )
                return False

            except Exception as e:
                # Some other fatal error occurred, let's log it and update the db
                error_message = f"Error submitting job for ObsID {obs_id} RequestID {request_id}."
                logger.exception(error_message)
                error_message = error_message + f" {e!s}"
                update_calsolution_request_submit_mwa_asvo_job_status(
                    self.db_handler,
                    [
                        request_id,
                    ],
                    None,
                    None,
                    datetime.now().astimezone(),
                    error_message,
                )
                self.giant_squid_errors += 1
                return False
        else:
            # This job didn't need to be added
            return False

    def mwa_asvo_update_tracked_jobs(self):
        """Update the status of all tracked MWA ASVO jobs.

        Queries MWA ASVO via giant-squid to get the latest job statuses and
        populates current_asvo_jobs with results.
        """
        # Find out the status of all this user's jobs in MWA ASVO
        # Get the job list from giant-squid, populating current_asvo_jobs
        # If we find a job in giant-squid which we don't know about,
        # DON'T include it in the list we track
        if self.running:
            logger.debug("Getting latest MWA ASVO job statuses...")
            try:
                self.mwax_asvo_helper.update_all_job_status()

            except mwax_asvo_helper.GiantSquidMWAASVOOutageException:
                logger.warning("Cannot update MWA ASVO job states: MWA ASVO has an outage")

            except Exception:
                logger.exception("Error in update_all_job_status. Will retry next loop")
                self.giant_squid_errors += 1

    def initialise(
        self,
        config_filename: str,
        override_db_handler: MWAXDBHandler | None = None,
    ):
        """Initialize the controller from a configuration file.

        Args:
            config_filename: Path to the configuration file.
            override_db_handler: If present, this will override the default
                MWAXDBHandler (this is used for testing via tests/tests_fakedb.py
                FakeMWAXDBHandler). Defaults to None.
        """
        self.config_filename = config_filename
        self.worker_config_filename = config_filename.replace("calvin_controller", "calvin_processor")

        # Get this hosts hostname
        self.hostname = utils.get_hostname()

        if not os.path.exists(config_filename):
            print(f"Configuration file location {config_filename} does not exist. Quitting.")
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

        # Read log level
        config_file_log_level: str | None = utils.read_optional_config(config, "mwax mover", "log_level")
        if config_file_log_level:
            logger.setLevel(config_file_log_level)

        logger.info(f"Starting mwax_calvin_controller...v{version.get_mwax_mover_version_string()}")
        logger.info(f"Reading config file: {config_filename}")

        # health
        self.health_multicast_ip = utils.read_config(config, "mwax mover", "health_multicast_ip")
        self.health_multicast_port = int(utils.read_config(config, "mwax mover", "health_multicast_port"))
        self.health_multicast_hops = int(utils.read_config(config, "mwax mover", "health_multicast_hops"))
        self.health_multicast_interface_name = utils.read_config(
            config,
            "mwax mover",
            "health_multicast_interface_name",
        )

        # get this hosts primary network interface ip
        self.health_multicast_interface_ip = utils.get_ip_address(self.health_multicast_interface_name)
        logger.info(f"IP for sending multicast: {self.health_multicast_interface_ip}")

        #
        # MRO database
        #
        self.mro_metadatadb_host = utils.read_config(config, "mro metadata database", "host")
        self.mro_metadatadb_db = utils.read_config(config, "mro metadata database", "db")
        self.mro_metadatadb_user = utils.read_config(config, "mro metadata database", "user")
        self.mro_metadatadb_pass = utils.read_config(
            config, "mro metadata database", "pass", not utils.running_under_pytest()
        )
        self.mro_metadatadb_port = int(utils.read_config(config, "mro metadata database", "port"))

        # Initiate database connection for mro metadata db
        if override_db_handler:
            self.db_handler = override_db_handler
        else:
            self.db_handler = MWAXDBHandler(
                host=self.mro_metadatadb_host,
                port=self.mro_metadatadb_port,
                db_name=self.mro_metadatadb_db,
                user=self.mro_metadatadb_user,
                password=self.mro_metadatadb_pass,
            )

        #
        # calvin config
        #
        # How long between iterations of the main loop (in seconds)
        self.check_interval_seconds = int(utils.read_config(config, "calvin", "check_interval_seconds"))

        # script path (path for keeping all sbatch scripts)
        self.script_path = config.get("calvin", "script_path")

        if not os.path.exists(self.script_path):
            print(f"script_path {self.script_path} does not exist. Quiting.")
            sys.exit(1)

        # oldest calvin obsid (when looking for new calibrator obs in the schedule, don't
        # look before this obsid)
        self.oldest_cal_obs_id = int(config.get("calvin", "oldest_calibrator_obs_id"))

        self.max_in_progress_asvo_jobs = int(config.get("calvin", "max_in_progress_asvo_jobs"))

        #
        # giant-squid config
        #
        # How many seconds do we wait before rechecking when giant squid says
        # MWA ASVO has an outage?
        self.mwa_asvo_outage_check_seconds = int(
            utils.read_config(config, "giant squid", "mwa_asvo_outage_check_seconds")
        )

        # How many secs do we wait for MWA ASVO to get us a completed job??
        self.mwa_asvo_longest_wait_time_seconds = int(
            utils.read_config(config, "giant squid", "mwa_asvo_longest_wait_time_seconds")
        )

        # Get the giant squid binary
        self.giant_squid_binary_path = utils.read_config(
            config,
            "giant squid",
            "giant_squid_binary_path",
        )

        if not os.path.exists(self.giant_squid_binary_path):
            logger.error(f"giant_squid_binary_path location  {self.giant_squid_binary_path} does not exist. Quitting.")
            sys.exit(1)

        # How long do we wait for giant-squid to execute a list subcommand
        self.giant_squid_list_timeout_seconds = int(
            utils.read_config(config, "giant squid", "giant_squid_list_timeout_seconds")
        )

        # How long do we wait for giant-squid to execute a submit-vis subcommand
        self.giant_squid_submitvis_timeout_seconds = int(
            utils.read_config(config, "giant squid", "giant_squid_submitvis_timeout_seconds")
        )

        #
        # plots upload section
        #
        self.s3_profile = str(utils.read_config(config, "plots upload", "s3_profile"))
        self.s3_bucket = str(utils.read_config(config, "plots upload", "s3_bucket"))
        self.plot_upload_paths: list[str] = utils.read_config_list(config, "plots upload", "plot_upload_paths")
        for p in self.plot_upload_paths:
            if not os.path.exists(p):
                logger.error(f"plot_upload_path: {p} does not exist. Quitting.")
                sys.exit(1)
        self.plot_upload_interval_secs: int = int(
            utils.read_config(config, "plots upload", "plot_upload_interval_secs")
        )

        # Optional: how many fit dirs to upload per pass over each plot upload
        # path. Left optional so existing config files keep working unchanged.
        if config.has_option("plots upload", "plot_upload_max_fits_per_pass"):
            self.plot_upload_max_fits_per_pass = int(
                utils.read_config(config, "plots upload", "plot_upload_max_fits_per_pass")
            )

            if self.plot_upload_max_fits_per_pass < 1:
                logger.error(
                    "plot_upload_max_fits_per_pass must be at least 1, got"
                    f" {self.plot_upload_max_fits_per_pass}. Quitting."
                )
                sys.exit(1)
        else:
            self.plot_upload_max_fits_per_pass = DEFAULT_PLOT_UPLOAD_MAX_FITS_PER_PASS

        logger.info(f"Uploading up to {self.plot_upload_max_fits_per_pass} fit dir(s) per pass, newest fitid first.")

        # Setup the MWA ASVO Helper
        self.mwax_asvo_helper.initialise(
            self.giant_squid_binary_path,
            self.giant_squid_list_timeout_seconds,
            self.giant_squid_submitvis_timeout_seconds,
        )

    def initialise_from_command_line(self):
        """Initialize the controller from command-line arguments.

        Parses command-line arguments and calls initialise() with the configuration
        file path.
        """

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_calvin_controller: a command line tool which is part of the "
            "MWA correlator for the MWA. It will submit SBATCH jobs as needed "
            "to process real time or MWA ASVO calibration jobs."
        )

        parser.add_argument("-c", "--cfg", required=True, help="Configuration file location.\n")

        args = vars(parser.parse_args())

        # Check that config file exists
        config_filename = args["cfg"]

        self.initialise(config_filename)

    def sleep(self, seconds):
        """Sleep for a specified duration while remaining responsive to shutdown.

        Breaks long sleeps into intervals to remain responsive to the running
        flag and shutdown directives.

        Args:
            seconds: Duration to sleep in seconds.
        """
        SECS_PER_INTERVAL: int = 5
        SLURM_REFRESH_INTERVAL: int = 10

        if self.running:
            last_slurm_queue_update = 0
            if seconds <= SECS_PER_INTERVAL:
                time.sleep(seconds)
            else:
                integer_intervals, remainder_secs = divmod(seconds, SECS_PER_INTERVAL)

                while self.running and integer_intervals > 0:
                    time.sleep(SECS_PER_INTERVAL)
                    integer_intervals -= 1

                    if time.time() - last_slurm_queue_update > SLURM_REFRESH_INTERVAL:
                        # Only update the slurm jobs every 10 seconds so we don't kill the server
                        self.slurm_queue_size = count_slurm_asvo_jobs()
                        last_slurm_queue_update = time.time()

                if self.running and remainder_secs > 0:
                    time.sleep(remainder_secs)


def main():
    """Main entry point for the MWA Calvin controller process."""
    processor = MWAXCalvinController()

    try:
        processor.initialise_from_command_line()
        processor.start()
        sys.exit(0)
    except Exception:
        logger.exception("Exited with error")


if __name__ == "__main__":
    main()
