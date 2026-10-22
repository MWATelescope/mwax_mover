"""Reading and downloading an MWA ASVO signed URL.

parse_signed_url() reads the observation ID, the job ID and the kind of download from the tar file
name in the URL (``<obs_id>_<job_id>_vis.tar`` for raw visibilities, ``<obs_id>_<job_id>_ms.tar`` for
CASA measurement sets). download_signed_url() downloads the URL over HTTP, with retries. A signed URL
needs no MWA ASVO login, so giant-squid (which downloads by job ID with an API key) is not used.
"""

import logging
import os
import re
import time
from dataclasses import dataclass
from enum import Enum
from urllib.parse import urlparse

import requests

from mwax_mover.mwa_asvo.giant_squid import extract_filename_from_mwa_asvo_signed_url

logger = logging.getLogger(__name__)

# Seconds to wait to connect, and to wait for the next data once connected. These are not a limit on
# the whole download. That is download_signed_url()'s timeout_secs.
DOWNLOAD_CONNECT_TIMEOUT_SECS = 30
DOWNLOAD_READ_TIMEOUT_SECS = 300

# How much to read from the connection at a time.
DOWNLOAD_CHUNK_BYTES = 8 * 1024 * 1024

# <obs_id>_<job_id>_<kind>.tar. The obs ID has 10 digits.
SIGNED_URL_TAR_PATTERN = re.compile(r"^(?P<obs_id>\d{10})_(?P<job_id>\d+)_(?P<kind>vis|ms)\.tar$")


class SignedUrlKind(Enum):
    """What an MWA ASVO download holds."""

    vis = "vis"  # raw visibilities (gpubox FITS files), which Birli converts
    ms = "ms"  # CASA measurement sets, so Birli is not needed


@dataclass
class SignedUrlInfo:
    """What the tar file name in a signed URL says.

    Attributes:
        obs_id: The observation ID.
        job_id: The MWA ASVO job ID.
        kind: Raw visibilities or measurement sets.
        tar_filename: The tar file name, e.g. 1444927824_1021186_vis.tar.
    """

    obs_id: int
    job_id: int
    kind: SignedUrlKind
    tar_filename: str


class DownloadError(Exception):
    """A download failed. It can be tried again."""


class NonRetryableDownloadError(DownloadError):
    """A download failed, and trying again will not help."""


class SignedUrlExpiredError(NonRetryableDownloadError):
    """The server refused the signed URL (HTTP 403). It has expired, or it is not valid."""


class DownloadNotFoundError(NonRetryableDownloadError):
    """The file is not there (HTTP 404)."""


def parse_signed_url(url: str) -> SignedUrlInfo:
    """Read the observation ID, job ID and kind from a signed URL.

    Args:
        url: An MWA ASVO signed URL, e.g.
            https://projects.pawsey.org.au/mwa-asvo/1444927824_1021186_vis.tar?<query>

    Returns:
        What the tar file name says.

    Raises:
        ValueError: If the URL has no file name, or the name is not
            ``<obs_id>_<job_id>_vis.tar`` or ``<obs_id>_<job_id>_ms.tar``.
    """
    tar_filename = extract_filename_from_mwa_asvo_signed_url(url)

    match = SIGNED_URL_TAR_PATTERN.match(tar_filename)
    if match is None:
        raise ValueError(
            f"Cannot use the file name '{tar_filename}' in the URL. It must be <obs_id>_<job_id>_vis.tar"
            " (raw visibilities) or <obs_id>_<job_id>_ms.tar (measurement sets), with a 10 digit obs ID."
        )

    return SignedUrlInfo(
        obs_id=int(match.group("obs_id")),
        job_id=int(match.group("job_id")),
        kind=SignedUrlKind(match.group("kind")),
        tar_filename=tar_filename,
    )


def _proxies_for(url: str, https_proxy: str | None, no_proxy: str | None) -> dict[str, str] | None:
    """Work out the proxies to give requests.

    Args:
        url: The URL to download.
        https_proxy: The proxy to use, or None to leave it to requests (which reads HTTPS_PROXY).
        no_proxy: Comma separated host names that must not use the proxy, or None.

    Returns:
        A proxies dict, or None.
    """
    if https_proxy is None:
        return None

    host = urlparse(url).hostname or ""
    skip_hosts = [h.strip() for h in (no_proxy or "").split(",") if h.strip()]
    if any(host == h or host.endswith(f".{h.lstrip('.')}") for h in skip_hosts):
        return None

    return {"https": https_proxy, "http": https_proxy}


def _download_once(url: str, dest_filename: str, timeout_secs: int, proxies: dict[str, str] | None) -> int:
    """Download the URL to a file, once.

    Args:
        url: The signed URL.
        dest_filename: The file to write.
        timeout_secs: How long the whole download may take.
        proxies: The proxies for requests, or None.

    Returns:
        The number of bytes written.

    Raises:
        NonRetryableDownloadError: For HTTP 403 (expired) and 404 (not found).
        DownloadError: For any other failure, including a download that is too long or too short.
    """
    start_time = time.monotonic()

    try:
        with requests.get(
            url,
            stream=True,
            timeout=(DOWNLOAD_CONNECT_TIMEOUT_SECS, DOWNLOAD_READ_TIMEOUT_SECS),
            proxies=proxies,
        ) as response:
            if response.status_code == requests.codes.forbidden:
                raise SignedUrlExpiredError("The server refused the URL (HTTP 403). It has expired or is not valid.")
            if response.status_code == requests.codes.not_found:
                raise DownloadNotFoundError("The server has no such file (HTTP 404).")
            response.raise_for_status()

            expected_header = response.headers.get("Content-Length")
            bytes_written = 0

            with open(dest_filename, "wb") as dest_file:
                for chunk in response.iter_content(chunk_size=DOWNLOAD_CHUNK_BYTES):
                    if time.monotonic() - start_time > timeout_secs:
                        raise DownloadError(f"The download took longer than {timeout_secs} seconds.")
                    dest_file.write(chunk)
                    bytes_written += len(chunk)
    except requests.RequestException as request_error:
        raise DownloadError(f"{type(request_error).__name__}: {request_error}") from request_error

    if expected_header is not None and bytes_written != int(expected_header):
        raise DownloadError(f"Got {bytes_written} bytes, but the server said {expected_header}.")

    return bytes_written


def download_signed_url(
    url: str,
    dest_filename: str,
    attempts: int,
    retry_wait_secs: int,
    timeout_secs: int,
    https_proxy: str | None = None,
    no_proxy: str | None = None,
) -> int:
    """Download an MWA ASVO signed URL to a file, trying again if it fails.

    An expired URL (HTTP 403) or a missing file (HTTP 404) is not tried again. Any other failure is.
    A partly written file is removed after a failure.

    Args:
        url: The signed URL.
        dest_filename: The file to write.
        attempts: How many times to try (at least 1).
        retry_wait_secs: Seconds to wait between attempts.
        timeout_secs: How long each attempt may take.
        https_proxy: The proxy to use, or None to leave it to requests.
        no_proxy: Comma separated host names that must not use the proxy.

    Returns:
        The number of bytes downloaded.

    Raises:
        DownloadError: If every attempt failed, or the failure cannot be retried. The cause is in the message.
    """
    proxies = _proxies_for(url, https_proxy, no_proxy)
    total_attempts = max(1, attempts)
    last_error: DownloadError | None = None

    for attempt in range(1, total_attempts + 1):
        try:
            bytes_downloaded = _download_once(url, dest_filename, timeout_secs, proxies)
            logger.info(f"Downloaded {bytes_downloaded} bytes to {dest_filename}")
            return bytes_downloaded
        except NonRetryableDownloadError:
            _remove_partial_file(dest_filename)
            raise
        except DownloadError as download_error:
            last_error = download_error
            _remove_partial_file(dest_filename)
            logger.warning(f"Download attempt {attempt} of {total_attempts} failed: {download_error}")
            if attempt < total_attempts:
                time.sleep(retry_wait_secs)

    raise DownloadError(f"The download failed after {total_attempts} attempt(s). Last error: {last_error}")


def _remove_partial_file(filename: str) -> None:
    """Remove a partly downloaded file. If that fails, log and carry on.

    Args:
        filename: The file to remove.
    """
    try:
        if os.path.exists(filename):
            os.remove(filename)
    except OSError:
        logger.warning(f"Could not remove the partial download {filename}. Ignoring.")
