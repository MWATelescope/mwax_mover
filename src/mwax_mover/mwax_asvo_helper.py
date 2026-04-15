"""Helper classes for managing MWA ASVO download jobs via the giant-squid CLI.

MWAASVOHelper maintains a list of in-flight MWA ASVO jobs, submitting new download
requests via giant-squid submitvis and polling their status via giant-squid list.
MWAASVOJob tracks a single job including its state, request IDs, submission
timestamp, and download URL. MWAASVOJobState enumerates the possible ASVO job
states. Typed exceptions are raised for outages and duplicate submissions.
"""

from datetime import datetime, timezone
from enum import Enum
import json
import logging
import re
import time
from typing import List, Optional
from mwax_mover.mwax_command import run_command_ext

logger = logging.getLogger(__name__)


class GiantSquidException(Exception):
    """Raised when an unknown exception is thrown when running giant-squid"""


class GiantSquidMWAASVOOutageException(Exception):
    """Raised when giant-squid reports that MWA ASVO is in an outage"""


class GiantSquidJobAlreadyExistsException(Exception):
    """Raised when giant-squid reports that an obs_id already exists in
    the MWA ASVO queue in queued, processing or ready state"""

    def __init__(self, message, job_id: int):
        """Initialize the exception with a message and job ID.

        Args:
            message: Error message describing the exception.
            job_id: The ID of the existing ASVO job.
        """
        # Call the base class constructor with the parameters it needs, but add job id
        # for us to use!
        super().__init__(message)
        self.job_id: int = job_id


class MWAASVOJobState(Enum):
    Queued = "Queued"
    Waitcal = "WaitCal"
    Staging = "Staging"
    Staged = "Staged"
    Downloading = "Downloading"
    Preprocessing = "Processing"
    Imaging = "Imaging"
    Delivering = "Delivering"
    Ready = "Ready"
    Error = "Error"
    Expired = "Expired"
    Cancelled = "Cancelled"
    Unknown = "Unknown"  # not a real ASVO status but a good default


class MWAASVOJob:
    """
    This class represents a single MWA ASVO job. We use this
    to track its progress from submission to completion
    """

    def __init__(self, request_id: int, obs_id: int, job_id: int):
        """Initialize an MWA ASVO job instance.

        Args:
            request_id: The calibration solution request ID.
            obs_id: The observation ID.
            job_id: The MWA ASVO job ID.
        """
        self.request_ids: list[int] = []
        self.request_ids.append(request_id)

        self.obs_id = obs_id
        self.job_id = job_id
        self.job_state = MWAASVOJobState.Unknown
        self.submitted_datetime: datetime
        self.download_error_datetime: Optional[datetime] = None
        self.download_error_message: Optional[str] = None
        self.last_seen_datetime: Optional[datetime] = None
        self.download_url: Optional[str] = None
        self.download_slurm_job_submitted: bool = False
        self.download_slurm_job_id: Optional[int] = None
        self.download_slurm_job_submitted_datetime: Optional[datetime] = None

    def __str__(self):
        return f"JobID: {self.job_id}; ObsID: {self.obs_id}"

    def __repr__(self):
        return f"{self.get_status()}"

    def elapsed_time_seconds(self) -> int:
        """Get the number of seconds between now and the submission time.

        Returns:
            The elapsed time in seconds, or 0 if not yet submitted.
        """
        if self.submitted_datetime is not None:
            return int((datetime.now(timezone.utc) - self.submitted_datetime).total_seconds())
        else:
            return 0

    def is_in_progress(self) -> bool:
        """Determine if a tracked MWA ASVO job is still in progress

        Returns:
            A bool indicating if the job is in progress (i.e. not complete or failed)
        """
        return (
            self.job_state != MWAASVOJobState.Cancelled
            and self.job_state != MWAASVOJobState.Error
            and self.job_state != MWAASVOJobState.Ready
        )

    def get_status(self) -> dict:
        """Get the current status of the job as a dictionary.

        Returns:
            A dictionary containing job ID, observation ID, state, timestamps,
            and error information.
        """
        return {
            "job_id": str(self.job_id),
            "obs_id": str(self.obs_id),
            "state": str(self.job_state.value),
            "MWA ASVO Job submitted": (
                self.submitted_datetime.strftime("%Y-%m-%d %H:%M:%S") if self.submitted_datetime else ""
            ),
            "last_seen": self.last_seen_datetime.strftime("%Y-%m-%d %H:%M:%S") if self.last_seen_datetime else "",
            "download_slurm_job_submitted_datetime": (
                self.download_slurm_job_submitted_datetime if self.download_slurm_job_submitted else ""
            ),
            "download_error_datetime": (self.download_error_datetime if self.download_error_datetime else ""),
            "download_error_message": (self.download_error_message if self.download_error_message else ""),
            "request_ids": " ,".join(str(r) for r in self.request_ids),
        }


class MWAASVOHelper:
    """
    This class is the main helper to allow the CalvinProcessor to interact with MWA ASVO
    via the giant-squid CLI
    """

    def __init__(self):
        """Initialize the MWA ASVO helper instance.

        Sets up configuration variables for giant-squid execution and initializes
        the job tracking list.
        """
        # Where is giant-squid binary?
        self.path_to_giant_squid_binary: str = ""

        # How many seconds do we wait when executing giant-squid list
        self.giant_squid_list_timeout_seconds: int = 0

        # How many seconds do we wait when executing giant-squid submit-vis
        self.giant_squid_submitvis_timeout_seconds: int = 0

        # List of Jobs and obs_ids the helper is keeping track of
        self.current_asvo_jobs: List[MWAASVOJob] = []

        self.mwa_asvo_outage_datetime: Optional[datetime] = None

    def initialise(
        self,
        path_to_giant_squid_binary: str,
        giant_squid_list_timeout_seconds: int,
        giant_squid_submitvis_timeout_seconds: int,
    ):
        """Initialize the helper with configuration parameters.

        Args:
            path_to_giant_squid_binary: Path to the giant-squid executable.
            giant_squid_list_timeout_seconds: Timeout for giant-squid list commands.
            giant_squid_submitvis_timeout_seconds: Timeout for giant-squid submit-vis commands.
        """
        # Set class variables
        self.path_to_giant_squid_binary = path_to_giant_squid_binary
        self.giant_squid_list_timeout_seconds = giant_squid_list_timeout_seconds
        self.giant_squid_submitvis_timeout_seconds = giant_squid_submitvis_timeout_seconds

    def does_request_exist(self, request_id: int) -> bool:
        """Check if a request ID is already being handled.

        Args:
            request_id: The request ID to search for.

        Returns:
            True if the request is being tracked, False otherwise.
        """
        for job in self.current_asvo_jobs:
            if request_id in job.request_ids:
                return True

        # not found
        return False

    def get_in_progress_asvo_job_count(self) -> int:
        """A helper function to get the count of in progress ASVO jobs

        Returns:
            the number of ASVO jobs which are in progress"""
        try:
            return sum(1 for item in self.current_asvo_jobs if item.is_in_progress())
        except Exception:
            logger.exception("get_in_progress_asvo_job_count() failed")
            return -1

    def submit_download_job(self, request_id: int, obs_id: int) -> MWAASVOJob:
        """Submit an MWA ASVO download job and track it internally.

        Args:
            request_id: The calibration solution request ID.
            obs_id: The observation ID for which to download files.

        Returns:
            A new MWAASVOJob instance with submission details.

        Raises:
            GiantSquidMWAASVOOutageException: If MWA ASVO is in an outage.
            GiantSquidException: If an error occurs during job submission.
        """
        logger.info(f"{obs_id}: Submitting MWA ASVO job to dowload for request {request_id}")

        try:
            stdout = self._run_giant_squid(
                "submit-vis", f"--delivery acacia {obs_id}", self.giant_squid_submitvis_timeout_seconds
            )

            # If submitted successfully, get the new job id from stdout
            job_id: int = get_job_id_from_giant_squid_stdout(stdout)

            logger.info(f"{obs_id}: MWA ASVO job {job_id} submitted successfully")

        except GiantSquidJobAlreadyExistsException as already_exists_exception:
            # Job already exists in queued, processing or ready state, get the job id
            job_id: int = already_exists_exception.job_id

            logger.info(f"{obs_id}: MWA ASVO job {job_id} already exists.")

        except GiantSquidMWAASVOOutageException:
            self.mwa_asvo_outage_datetime = datetime.now()
            # Re-raise this error
            raise

        except Exception:
            # Some other error happened- update database as an error
            raise

            #
            # GJS commenting out so we always add a job
            #
            # create, populate and add the MWAASVOJob if we don't already have it
            # job = self.get_first_job_for_obs_id(obs_id)
            # if job:
            #     # Only add this request if it is not already in the list
            #     if request_id not in job.request_ids:
            #         job.request_ids.append(request_id)

            #     if job.submitted_datetime is None:
            #         job.submitted_datetime = datetime.now(timezone.utc)

            #     logger.info(
            #         f"{obs_id}: Added RequestID {request_id} to JobID {job_id} as this ObsID is already tracked."
            #         f"Tracking {len(self.current_asvo_jobs)} MWA ASVO jobs"
            #     )
            # else:
        # add a new job to be tracked
        job = MWAASVOJob(request_id=request_id, obs_id=obs_id, job_id=job_id)
        job.submitted_datetime = datetime.now(timezone.utc)
        self.current_asvo_jobs.append(job)
        logger.info(f"{obs_id}: Added JobID {job_id}. Now tracking {len(self.current_asvo_jobs)} MWA ASVO jobs")

        return job

    def update_all_job_status(self):
        """Update the status of all tracked jobs using giant-squid list.

        Queries the MWA ASVO service for current job statuses and updates
        internal state accordingly. Removes jobs no longer reported by the service.

        Raises:
            GiantSquidMWAASVOOutageException: If MWA ASVO is in an outage.
            GiantSquidException: If an error occurs during status update.
        """
        # Get list of jobs with status info
        try:
            stdout = self._run_giant_squid("list", "--json", self.giant_squid_list_timeout_seconds)
        except GiantSquidMWAASVOOutageException:
            self.mwa_asvo_outage_datetime = datetime.now()
            # Re-raise this error
            raise

        # Convert stdout into json
        json_stdout = json.loads(stdout)

        logger.debug(f"giant-squid list returned {len(json_stdout)} jobs")

        # We'll set all the jobs we see to this exact datetime so
        # we can figure out if one of our in memory jobs is no longer
        # reported by giant-squid
        update_datetime = datetime.now().astimezone()

        # Iterate through each job
        for json_one_job in json_stdout:
            # Extract the job_id, state and a download url (if status is Ready)
            obs_id, job_id, job_state, download_url = get_job_info_from_giant_squid_json(json_stdout, json_one_job)

            # Find the giant squid job in our in memory list
            for job in self.current_asvo_jobs:
                if job.job_id == job_id:
                    changed: bool = False

                    if job.job_state != job_state:
                        job.job_state = job_state
                        changed = True

                    if download_url is not None:
                        if job.download_url != download_url:
                            job.download_url = download_url
                            changed = True

                    job.last_seen_datetime = update_datetime

                    if changed:
                        logger.info(f"{job}: updated - {job.job_state.value} {job.download_url}")
                    break

        # Finally, we need to check for any jobs in memory which were not seen anymore
        # in giant squid
        #
        # TODO: hmm we may want to update the database to say it's failed?
        #
        jobs_to_delete: list[MWAASVOJob] = []

        for job in self.current_asvo_jobs:
            if job.last_seen_datetime != update_datetime:
                # We didn't see this job
                # We should log it and remove it
                logger.warning(
                    f"{job}: removed - {job.job_state.value} {job.download_url} as it was no longer "
                    f"seen by giant-squid-list. {update_datetime} vs {job.last_seen_datetime}"
                )
                jobs_to_delete.append(job)

        # Now do the delete/removal
        for job_to_delete in jobs_to_delete:
            # We didn't see this job
            # We should log it and remove it
            logger.warning(
                f"{job}: removed - {job.job_state.value} {job.download_url} as it was no longer "
                f"seen by giant-squid-list. {update_datetime} vs {job.last_seen_datetime}"
            )
            self.current_asvo_jobs.remove(job_to_delete)

    def _run_giant_squid(self, subcommand: str, args: str, timeout_seconds: int) -> str:
        """Execute a giant-squid command and return its output.

        Args:
            subcommand: The giant-squid subcommand (e.g., 'submit-vis', 'list').
            args: Arguments to pass to the giant-squid command.
            timeout_seconds: Maximum time in seconds to wait for command completion.

        Returns:
            The stdout output from the giant-squid command.

        Raises:
            GiantSquidMWAASVOOutageException: If the ASVO service is down.
            GiantSquidException: If the command fails or returns an error code.
        """
        cmdline: str = f"{self.path_to_giant_squid_binary} {subcommand} {args}"

        start_time = time.time()

        # run giant-squid. We don't care about running on a specific numa
        # node so we pass -1 for that
        success, stdout = run_command_ext(cmdline, None, timeout_seconds, True)

        elapsed = time.time() - start_time

        logger.debug(f"_run_giant_squid: completed in {elapsed:.3f} seconds [Success={success}]")

        if success:
            return stdout
        else:
            # Bad return code, failure!

            # Known errors have error codes:
            # These exist in manta-ray/asvo_server/views_dispatch.py
            # 0 = Invalid input, outage_in_progress
            # 1 = job_limit_reached
            # 2 = job_running_or_complete
            # 3 = job_not_found
            # Error code looks like this in StdOut:  "error_code": 2
            regex_match = re.search(r'"error_code": (\d+)', stdout)

            if regex_match:
                # We found an error code in the stdout
                error_code_str = regex_match.group(1)

                raise GiantSquidException(
                    f"_run_giant_squid: Error running {cmdline} in {elapsed:.3f} seconds. "
                    f"Error code: {error_code_str} {stdout}"
                )
            elif "Your job cannot be submitted as the archive location of the observation is down" in stdout:
                raise GiantSquidMWAASVOOutageException(
                    "Unable to communicate with MWA ASVO- the archive location is down"
                )

            elif "outage" in stdout:
                # Outage message looks like this:
                # 'Error: The server responded with status code 0, message:
                # Your job cannot be submitted as there is a full outage in progress.'"
                raise GiantSquidMWAASVOOutageException("Unable to communicate with MWA ASVO- an outage is in progress")
            else:
                # There was no "error_code" in stdout, so just report the whole stdout error message
                raise GiantSquidException(
                    f"_run_giant_squid: Error running {cmdline} in {elapsed:.3f} seconds. Error: {stdout}"
                )


def get_job_id_from_giant_squid_stdout(stdout: str) -> int:
    """Extract the job ID from giant-squid submit-vis output.

    Parses the stdout from a giant-squid submit-vis command to retrieve
    the job ID, handling both new submissions and existing job scenarios.

    Args:
        stdout: The stdout output from giant-squid submit-vis.

    Returns:
        The newly created or existing job ID.

    Raises:
        Exception: If no job ID can be found in the output.
    """

    # Output of successful submission is:
    # 17:19:03 [INFO] Submitted obs_id as MWA ASVO job ID job_id
    # 17:19:03 [INFO] Submitted 1 obs_ids for visibility download.
    lines = stdout.splitlines()
    for line in lines:
        regex_match = re.search(r"as MWA ASVO job ID (\d+)", line)

        if regex_match:
            job_id_str = regex_match.group(1)

            return int(job_id_str)

    # Output of successful, but already existing job id is:
    # 14:24:17 [WARN] Job already queued, processing or complete. Job Id: 10001610
    regex_match = re.search(r"Job already queued, processing or complete. Job Id: (\d+)", stdout, re.M)

    if regex_match:
        job_id_str = regex_match.group(1)

        return int(job_id_str)

    # Try this too
    # 12:08:08 [WARN] Job already running or complete. Job Id: 878060 ObsID: 1427464352
    regex_match = re.search(r"Job already running or complete. Job Id: (\d+) ObsID:", stdout, re.M)

    if regex_match:
        job_id_str = regex_match.group(1)

        return int(job_id_str)

    # No job_id was found, raise exception
    raise Exception(f"No Job Id could be found in the output from giant-squid: {stdout}")


def get_job_info_from_giant_squid_json(stdout_json, json_for_one_job) -> tuple[int, int, MWAASVOJobState, str | None]:
    """Extract job information from giant-squid list JSON output.

    Parses a single job entry from the giant-squid list output. Note: MWA ASVO
    returns inconsistent JSON formats where job state can be a string or dict.

    Args:
        stdout_json: The full JSON object output from giant-squid list.
        json_for_one_job: The JSON object for the current job to parse.

    Returns:
        A tuple containing:
        - obs_id (int): The observation ID.
        - job_id (int): The job ID.
        - job_state (MWAASVOJobState): The current job state.
        - download_url (str|None): The download URL if state is Ready, None otherwise.

    Raises:
        Exception: If the job status code is unrecognized.
    """

    job_id: int = int(json_for_one_job)
    obs_id: int = int(stdout_json[json_for_one_job]["obsid"])
    job_state: str = "Unknown"
    job_state_json = stdout_json[json_for_one_job]["jobState"]

    # Some job_state_json values are just strings, others are dicts!
    if isinstance(job_state_json, str):
        job_state = job_state_json
    else:
        # If it is a dict, get the first key- that will be the state
        for key, value in job_state_json.items():
            job_state = key
            break

    # See if the job status returned matches our enum
    for state in MWAASVOJobState:
        if job_state == state.value:
            # We have a match on state
            url = None

            # Now get the download url if Ready
            if state == MWAASVOJobState.Ready:
                files_json = stdout_json[json_for_one_job]["files"]
                url = files_json[0]["fileUrl"]

            return obs_id, job_id, state, url

    # Nothing matched
    raise Exception(f"{job_id}: giant-squid unknown job status code {job_state}.")
