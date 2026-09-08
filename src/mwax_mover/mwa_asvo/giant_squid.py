"""Submitting and polling MWA ASVO jobs via the giant-squid CLI tool.

run_giant_squid() shells out to the giant-squid binary, retrying transient
failures with backoff and raising a specific exception for a known ASVO
outage or a definitive server-side error code.
extract_filename_from_mwa_asvo_signed_url() pulls the filename out of an
ASVO presigned download URL.

Moved here from net/asvo.py, alongside mwa_asvo/jobs.py (the job-tracking
layer built on top of this), consolidating all MWA-ASVO-related code into
one package.
"""

import logging
import os
import random
import re
import time
from urllib.parse import urlparse

from mwax_mover.core.command import run_command_ext
from mwax_mover.core.env import running_under_pytest

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


def run_giant_squid(
    path_to_giant_squid_binary: str,
    subcommand: str,
    args: str,
    timeout_seconds: int,
    max_retries: int = 5,
    retry_delay_seconds: float = 10.0,
    env_args: dict[str, str] | None = None,
) -> str:
    """Execute a giant-squid command and return its output.

    Retries on transient failures (no known error code) with exponential
    backoff and jitter. Does not retry on known error codes or ASVO outages.

    Args:
        path_to_giant_squid_binary: The fully qualified path to giant-squid binary.
        subcommand: The giant-squid subcommand (e.g., 'submit-vis', 'list').
        args: Arguments to pass to the giant-squid command.
        timeout_seconds: Maximum time in seconds to wait for command completion.
        max_retries: Maximum number of retry attempts after the initial try.
        retry_delay_seconds: Base delay in seconds between retries; doubles each
            attempt with added jitter.
        env_args: Optionally specify extra env args in a dict- e.g. a https proxy to use. E.g. {"HTTPS_PROXY": "http://127.0.0.1:3128"}

    Returns:
        The stdout output from the giant-squid command.

    Raises:
        GiantSquidMWAASVOOutageException: If the ASVO service is down. Not retried.
        GiantSquidException: If the command fails with a known error code, or if
            all retry attempts are exhausted on a transient failure.
    """
    cmdline: str = f"{path_to_giant_squid_binary} {subcommand} {args}"
    last_exception: GiantSquidException = GiantSquidException("Unknown Error")

    for attempt in range(max_retries + 1):
        if attempt > 0:
            delay = retry_delay_seconds * (2 ** (attempt - 1)) + random.uniform(0, 1)
            # Under pytest, collapse the wait. With the defaults this loop
            # otherwise sleeps 10+20+40+80+160 = 310 real seconds before giving
            # up, which made a single unit test take over five minutes whenever
            # the giant-squid binary was absent. The retry *logic* is still
            # exercised; only the wall-clock wait is skipped.
            if running_under_pytest():
                delay = 0.01
            logger.warning(
                f"run_giant_squid: retry {attempt}/{max_retries} after {delay:.1f}s (last error: {last_exception})"
            )
            time.sleep(delay)

        start_time = time.time()

        success, stdout = run_command_ext(
            command=cmdline,
            numa_node=None,
            timeout=timeout_seconds,
            use_shell=True,
            copy_user_env=True,
            extra_env_vars=env_args,
        )

        elapsed = time.time() - start_time
        logger.debug(
            f"run_giant_squid: attempt {attempt + 1}/{max_retries} completed in "
            f"{elapsed:.3f} seconds [Success={success}]"
        )

        if success:
            return stdout

        # Bad return code — classify the failure before deciding whether to retry.
        regex_match = re.search(r'"error_code": (\d+)', stdout)

        if regex_match:
            # Known server-side error code — definitive, do not retry.
            error_code_str = regex_match.group(1)

            raise GiantSquidException(
                f"run_giant_squid: Error running {cmdline} in {elapsed:.3f} seconds. "
                f"Error code: {error_code_str} {stdout}"
            )

        elif (
            "Your job cannot be submitted as the archive location of the observation is down" in stdout
            or "No obs locations found" in stdout
        ):
            # ASVO outage — raise immediately, caller handles this.
            raise GiantSquidMWAASVOOutageException("Unable to communicate with MWA ASVO- the archive location is down")

        elif "outage" in stdout:
            # ASVO outage — raise immediately, caller handles this.
            raise GiantSquidMWAASVOOutageException("Unable to communicate with MWA ASVO- an outage is in progress")

        else:
            # Transient / unknown failure — eligible for retry.
            last_exception = GiantSquidException(
                f"run_giant_squid: Error running {cmdline} in {elapsed:.3f} seconds. Error: {stdout}"
            )

    logger.error(f"run_giant_squid: all {max_retries + 1} attempts failed for {cmdline}. Last error: {last_exception}")
    raise last_exception  # type: ignore[misc]  # always set if we reach here


def extract_filename_from_mwa_asvo_signed_url(url: str) -> str:
    """Extract the filename from an MWA ASVO presigned URL.

    Parses the URL path component and returns the final path segment,
    ignoring any query string parameters (e.g. AWSAccessKeyId, Signature,
    Expires).

    Args:
        url: A presigned URL in the format:
            https://projects.pawsey.org.au/mwa-asvo/<filename>?<query>

    Returns:
        The filename portion of the URL path, e.g. '1066827928_1045138_vis.tar'.

    Raises:
        ValueError: If the URL path contains no filename component.
    """
    path = urlparse(url).path  # '/mwa-asvo/1066827928_1045138_vis.tar'
    filename = os.path.basename(path)  # '1066827928_1045138_vis.tar'
    if not filename:
        raise ValueError(f"Could not extract filename from URL: {url}")
    return filename
