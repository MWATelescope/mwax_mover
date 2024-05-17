"""This class and functions automates the downloading of data
from MWA ASVO. It calls the MWATelescope/giant-squid CLI"""

import datetime
from enum import Enum
import json
import logging
import os
import re
import time
from typing import List
from mwax_mover.mwax_command import run_command_ext


class GiantSquidException(Exception):
    """Raised when an unknow exception is thrown when running giant-squid"""


class GiantSquidMWAASVOOutageException(Exception):
    """Raised when giant-squid reports that MWA ASVO is in an outage"""


class GiantSquidJobAlreadyExistsException(Exception):
    """Raised when giant-squid reports that an obs_id already exists in
    the MWA ASVO queue in queued, processing or ready state"""

    def __init__(self, message, job_id: int):
        # Call the base class constructor with the parameters it needs, but add job id
        # for us to use!
        super().__init__(message)
        self.job_id = job_id


class MWAASVOJobState(Enum):
    Cancelled = "Cancelled"
    Error = "Error"
    Expired = "Expired"
    Ready = "Ready"
    Queued = "Queued"
    Processing = "Processing"


"""
This class represents a single MWA ASVO job. We use this
to track its progress from submission to completion
"""


class MWAASVOJob:

    def __init__(self, obs_id: int, job_id: int):
        self.obs_id = obs_id
        self.job_id = job_id
        self.job_state = None
        self.submitted_datetime = datetime.datetime.now()
        self.last_seen_datetime = self.submitted_datetime
        self.download_url = None

    def elapsed_time_seconds(self) -> int:
        """Returns the number of seconds between Now and the submitted datetime"""
        return (datetime.datetime.now() - self.submitted_datetime).total_seconds()

    def get_status(self) -> dict:
        return {
            "job_id": str(self.job_id),
            "obs_id": str(self.obs_id),
            "state": str(self.job_state.value),
            "submitted": (self.submitted_datetime.strftime("%Y-%m-%d %H:%M:%S") if self.submitted_datetime else ""),
            "last_seen": (self.last_seen_datetime.strftime("%Y-%m-%d %H:%M:%S") if self.last_seen_datetime else ""),
        }


"""
This class is the main helper to allow the CalvinProcessor to interact with MWA ASVO
via the giant-squid CLI
"""


class MWAASVOHelper:
    def __init__(self):
        # Logger object
        self.logger = None

        # Where is giant-squid binary?
        self.path_to_giant_squid_binary = None

        # How many seconds do we wait when executing giant-squid list
        self.giant_squid_list_timeout_seconds = None

        # How many seconds do we wait when executing giant-squid submit-vis
        self.giant_squid_submitvis_timeout_seconds = None

        # How many seconds do we wait when executing giant-squid download
        self.giant_squid_download_timeout_seconds = None

        # Where do we tell giant-squid to download data to?
        # Probably the incoming directory of the calvin_processor
        self.download_path = None

        # List of Jobs and obs_ids the helper is keeping track of
        self.current_asvo_jobs = None

    def initialise(
        self,
        logger: logging.Logger,
        path_to_giant_squid_binary: str,
        giant_squid_list_timeout_seconds: int,
        giant_squid_submitvis_timeout_seconds: int,
        giant_squid_download_timeout_seconds: int,
        download_path: str,
    ):
        # Set class variables
        self.logger = logger
        self.path_to_giant_squid_binary = path_to_giant_squid_binary
        self.giant_squid_list_timeout_seconds = giant_squid_list_timeout_seconds
        self.giant_squid_submitvis_timeout_seconds = giant_squid_submitvis_timeout_seconds
        self.giant_squid_download_timeout_seconds = giant_squid_download_timeout_seconds
        self.download_path = download_path
        self.current_asvo_jobs: List[MWAASVOJob] = []

    def get_first_job_for_obs_id(self, obs_id) -> MWAASVOJob | None:
        for job in self.current_asvo_jobs:
            if job.obs_id == obs_id:
                return job

        # not found
        return None

    def submit_download_job(self, obs_id: int):
        """Submits an MWA ASVO Download Job by executing giant-squid
        and adds the details to our internal list

            Parameters:
                obs_id (int): the obs_id we want MWA ASVO to get us files

            Returns:
                Nothing: raises exceptions on error (from called functions)
        """
        self.logger.info(f"{obs_id}: Submitting MWA ASVO job to dowload")

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

        # create, populate and add the MWAASVOJob
        self.current_asvo_jobs.append(MWAASVOJob(obs_id=obs_id, job_id=job_id))
        self.logger.info(f"{obs_id}: Added job {job_id}. Now tracking {len(self.current_asvo_jobs)} MWA ASVO jobs")

    def update_all_job_status(self, add_missing_jobs_to_current_jobs: bool):
        """Updates the status of all our jobs using giant-squid list

        Parameters:
            Nothing

        Returns:
            Nothing: raises exceptions on error (from called functions)

        """
        self.logger.info("Checking all jobs status")

        # Get list of jobs with status info
        stdout = self._run_giant_squid("list", "--json", self.giant_squid_list_timeout_seconds)

        # Convert stdout into json
        json_stdout = json.loads(stdout)

        self.logger.debug(f"giant-squid list returned {len(json_stdout)} jobs")

        # We'll set all the jobs we see to this exact datetime so
        # we can figure out if one of our in memory jobs is no longer
        # reported by giant-squid
        update_datetime = datetime.datetime.now()

        # Iterate through each job
        for json_one_job in json_stdout:
            job_found: bool = False

            # Extract the job_id, state and a download url (if status is Ready)
            obs_id, job_id, job_state, download_url = get_job_info_from_giant_squid_json(json_stdout, json_one_job)

            # Find the giant squid job in our in memory list
            for job in self.current_asvo_jobs:
                if job.job_id == job_id:
                    job_found = True
                    changed: bool = False

                    if job.job_state != job_state:
                        job.job_state = job_state
                        changed = True

                    if job.download_url != download_url:
                        job.download_url = download_url
                        changed = True

                    job.last_seen_datetime = update_datetime

                    if changed:
                        self.logger.debug(
                            f"{job.obs_id}: updated - {job.job_id} {job.job_state.value} {job.download_url}"
                        )
                    break

            # Job was in the giant squid list but not in my in memory list
            # Maybe this code was run earlier, stopped and restarted?
            # Better add the exitsing giant-squid jobs to my interal list
            # if add_missing_jobs_to_current_jobs is True
            if add_missing_jobs_to_current_jobs and not job_found:
                new_job = MWAASVOJob(obs_id, job_id)
                new_job.job_state = job_state
                new_job.download_url = download_url
                new_job.last_seen_datetime = update_datetime
                new_job.submitted_datetime = new_job.last_seen_datetime

                self.current_asvo_jobs.append(new_job)
                self.logger.debug(
                    f"{new_job.obs_id}: added job from giant-squid: {new_job.job_id} "
                    f"{new_job.job_state.value} {new_job.download_url}"
                )

        # Finally, we need to check for any jobs in memory which were not seen anymore
        # in giant squid
        for job in self.current_asvo_jobs:
            if job.last_seen_datetime != update_datetime:
                # We didn't see this job
                # We should log it and remove it
                self.logger.debug(
                    f"{job.obs_id}: removed - {job.job_id} {job.job_state.value} {job.download_url} as it was not "
                    f"seen by giant-squid-list. {update_datetime} vs {job.last_seen_datetime}"
                )
                self.current_asvo_jobs.remove(job)

    def _run_giant_squid(self, subcommand: str, args: str, timeout_seconds: int) -> str:
        """Runs giant-squid and returns stdout output if successful or
        raises an exception if there was an error"""
        cmdline: str = f"{self.path_to_giant_squid_binary} {subcommand} {args}"
        self.logger.debug(f"_run_giant_squid: Executing {cmdline}...")

        start_time = time.time()

        # run giant-squid. We don't care about running on a specific numa
        # node so we pass -1 for that
        success, stdout = run_command_ext(self.logger, cmdline, -1, timeout_seconds, True)

        elapsed = time.time() - start_time

        if success:
            self.logger.debug(f"_run_giant_squid: successful in {elapsed:.3f} seconds")
            return stdout
        else:
            # Bad return code, failure!
            if ("outage" in stdout) or ("archive location of the observation is down" in stdout):
                raise GiantSquidMWAASVOOutageException("Unable to communicate with MWA ASVO- an outage is in progress")

            # Special case if subcommand is submit-vis- check for existing job and raise different exception
            # In this case, we don't need to submit the job
            if subcommand == "submit-vis":
                # Get job_id
                job_id = get_existing_job_id_from_giant_squid_stdout(stdout)

                if job_id:
                    raise GiantSquidJobAlreadyExistsException("Job already exists", job_id)

            raise GiantSquidException(
                f"_run_giant_squid: Error running {cmdline} in {elapsed:.3f} seconds. Error: {stdout}"
            )

    def download_asvo_job(self, job: MWAASVOJob):
        """Download the data product for the given job.
        This method checks the job is in the right state
        and that the download path exists and raises exceptions
        on error"""
        if job.job_state == MWAASVOJobState.Ready:
            if os.path.exists(self.download_path):
                stdout = self._run_giant_squid(
                    "download",
                    f"--download-dir={self.download_path} {job.job_id}",
                    self.giant_squid_download_timeout_seconds,
                )
                stdout = stdout.replace("\n", " ")
                self.logger.debug(f"{job.obs_id} Successfully downloaded: {stdout}")
            else:
                raise Exception(f"{job.obs_id} Error: Download path {self.download_path} does not exist")
        else:
            raise Exception(f"{job.obs_id} Error: job {job.job_id} is not ready for download State={job.job_state}")


def get_job_id_from_giant_squid_stdout(stdout: str) -> int:
    """Given stdout from giant-squid after submitting a vis job,
    return the provided job_id or raise exception if not found

    Parameters:
        stdout (str): The stdout output from giant_squid submit-vis

    Returns:
        job_id (int): returns the newly created job_id

    """

    # Output of successful submission is:
    # 17:19:03 [INFO] Submitted obs_id as ASVO job ID job_id
    # 17:19:03 [INFO] Submitted 1 obs_ids for visibility download.
    lines = stdout.splitlines()
    for line in lines:
        regex_match = re.search(r"as ASVO job ID (\d+)", line)

        if regex_match:
            job_id_str = regex_match.group(1)

            return int(job_id_str)

    # No job_id was found, raise exception
    raise Exception(f"No job_id could be found in the output from giant-squid '{stdout}'")


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


def get_existing_job_id_from_giant_squid_stdout(stdout: str) -> int | None:
    """
    If running giant-squid with 'submit-vis' check for
    a specific error which corresponds to the job already existing
    for that obs id.
    Example stdout output in this case:
    {"error": "Job already queued, processing or complete", "error_code": 2, "job_id": 10001610}
    """
    regex_match = re.search(
        r'{"error": "Job already queued, processing or complete", "error_code": 2, "job_id": (\d+)}', stdout
    )

    if regex_match:
        job_id_str = regex_match.group(1)

        return int(job_id_str)
    else:
        return None
