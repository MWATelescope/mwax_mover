"""This class and functions automates the downloading of data
from MWA ASVO. It calls the MWATelescope/giant-squid CLI"""

from datetime import datetime, timezone
from enum import Enum
import json
import logging
import re
import time
from typing import List, Optional
from mwax_mover.mwax_command import run_command_ext


class GiantSquidException(Exception):
    """Raised when an unknown exception is thrown when running giant-squid"""


class GiantSquidMWAASVOOutageException(Exception):
    """Raised when giant-squid reports that MWA ASVO is in an outage"""


class GiantSquidJobAlreadyExistsException(Exception):
    """Raised when giant-squid reports that an obs_id already exists in
    the MWA ASVO queue in queued, processing or ready state"""

    def __init__(self, message, job_id: int):
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
        """Returns the number of seconds between Now and the submitted datetime"""
        if self.submitted_datetime is not None:
            return int((datetime.now(timezone.utc) - self.submitted_datetime).total_seconds())
        else:
            return 0

    def get_status(self) -> dict:
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
        # Logger object
        self.logger: logging.Logger

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
        logger: logging.Logger,
        path_to_giant_squid_binary: str,
        giant_squid_list_timeout_seconds: int,
        giant_squid_submitvis_timeout_seconds: int,
    ):
        # Set class variables
        self.logger = logger
        self.path_to_giant_squid_binary = path_to_giant_squid_binary
        self.giant_squid_list_timeout_seconds = giant_squid_list_timeout_seconds
        self.giant_squid_submitvis_timeout_seconds = giant_squid_submitvis_timeout_seconds

    def get_first_job_for_obs_id(self, obs_id: int) -> MWAASVOJob | None:
        """Get the first MWAASVOJob object found in current_asvo_jobs, matching on obs_id.

        Parameters:
            obs_id (int): the obs_id we want to find the first job for

        Returns:
            The MWAASVOJob object found, or None if none are found
        """
        for job in self.current_asvo_jobs:
            if job.obs_id == obs_id:
                return job

        # not found
        return None

    def submit_download_job(self, request_id: int, obs_id: int) -> MWAASVOJob:
        """Submits an MWA ASVO Download Job by executing giant-squid
        and adds the details to our internal list

            Parameters:
                request_id (int): The id of the calibration_solution_request row this is for
                obs_id (int): the obs_id we want MWA ASVO to get us files

            Returns:
                New populated MWAASVO job class: raises exceptions on error (from called functions)
        """
        self.logger.info(f"{obs_id}: Submitting MWA ASVO job to dowload for request {request_id}")

        try:
            stdout = self._run_giant_squid(
                "submit-vis", f"--delivery acacia {obs_id}", self.giant_squid_submitvis_timeout_seconds
            )

            # If submitted successfully, get the new job id from stdout
            job_id: int = get_job_id_from_giant_squid_stdout(stdout)

            self.logger.info(f"{obs_id}: MWA ASVO job {job_id} submitted successfully")

        except GiantSquidJobAlreadyExistsException as already_exists_exception:
            # Job already exists in queued, processing or ready state, get the job id
            job_id: int = already_exists_exception.job_id

            self.logger.info(f"{obs_id}: MWA ASVO job {job_id} already exists.")

        except GiantSquidMWAASVOOutageException:
            self.mwa_asvo_outage_datetime = datetime.now()
            # Re-raise this error
            raise

        except Exception:
            # Some other error happened- update database as an error
            raise

        # create, populate and add the MWAASVOJob if we don't already have it
        job = self.get_first_job_for_obs_id(obs_id)

        if job:
            job.request_ids.append(request_id)

            if job.submitted_datetime is None:
                job.submitted_datetime = datetime.now(timezone.utc)

            self.logger.info(
                f"{obs_id}: Added RequestID {request_id} to JobID {job_id} as this ObsID is already tracked."
                f"Tracking {len(self.current_asvo_jobs)} MWA ASVO jobs"
            )
        else:
            # add a new job to be tracked
            job = MWAASVOJob(request_id=request_id, obs_id=obs_id, job_id=job_id)
            job.submitted_datetime = datetime.now(timezone.utc)
            self.current_asvo_jobs.append(job)
            self.logger.info(
                f"{obs_id}: Added JobID {job_id}. Now tracking {len(self.current_asvo_jobs)} MWA ASVO jobs"
            )

        return job

    def update_all_job_status(self):
        """Updates the status of all our jobs using giant-squid list

        Parameters:
            Nothing

        Returns:
            Nothing: raises exceptions on error (from called functions)

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

        self.logger.debug(f"giant-squid list returned {len(json_stdout)} jobs")

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
                        self.logger.info(f"{job}: updated - {job.job_state.value} {job.download_url}")
                    break

        # Finally, we need to check for any jobs in memory which were not seen anymore
        # in giant squid
        #
        # TODO: hmm we may want to update the database to say it's failed?
        #
        for job in self.current_asvo_jobs:
            if job.last_seen_datetime != update_datetime:
                # We didn't see this job
                # We should log it and remove it
                self.logger.warning(
                    f"{job}: removed - {job.job_state.value} {job.download_url} as it was no longer "
                    f"seen by giant-squid-list. {update_datetime} vs {job.last_seen_datetime}"
                )
                self.current_asvo_jobs.remove(job)

    def _run_giant_squid(self, subcommand: str, args: str, timeout_seconds: int) -> str:
        """Runs giant-squid and returns stdout output if successful or
        raises an exception if there was an error"""
        cmdline: str = f"{self.path_to_giant_squid_binary} {subcommand} {args}"

        start_time = time.time()

        # run giant-squid. We don't care about running on a specific numa
        # node so we pass -1 for that
        success, stdout = run_command_ext(self.logger, cmdline, None, timeout_seconds, True)

        elapsed = time.time() - start_time

        self.logger.debug(f"_run_giant_squid: completed in {elapsed:.3f} seconds [Success={success}]")

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
    """Given stdout from giant-squid after submitting a vis job,
    return the provided job_id or raise exception if not found

    Parameters:
        stdout (str): The stdout output from giant_squid submit-vis

    Returns:
        job_id (int): returns the newly created job_id

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
    """Returns an obs_id, Job_id and MWAASVOJobState Enum and URL from the json for one job
    NOTE: MWA ASVO returns funky json! E.g. some are just string values.
    Others are a dictionary. Very annoying!
    "Ready"
    {"Error": "No files found."}

    Parameters:
        stdout_json: the full json object output from giant-squid list
        json_for_one_job: the json object from the current job

    Returns:
        tuple[
            job_id (int): The job_id of this job,
            job_state (MWAASVOJobState): Enum instance of the job state for this job,
            download_url (str|None): If state is Ready then this will contain the download url
        ]

        Raises exceptions on error
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
