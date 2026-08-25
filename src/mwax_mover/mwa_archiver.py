"""Stateless file transfer utilities for archiving MWA data to remote storage.

Provides three transfer backends:
- copy_file_rsync(): copies a file between hosts over SSH/rsync (AES128-CTR).
- archive_file_xrootd(): uploads to an xrootd server with atomic temp-file rename.
- archive_file_rclone_haproxy(): uploads to Pawsey S3 (Acacia/Banksia) via
  rclone through a local HAProxy instance, which handles endpoint selection,
  health checking and failover. Checksum verified afterwards with rclone check.
"""

import logging
import os
import time
import uuid

from mwax_mover.mwax_command import run_command_ext
from mwax_mover.utils import bytes_to_gigabytes, get_gbps, running_under_pytest

logger = logging.getLogger(__name__)


def copy_file_rsync(
    source_filename: str,
    destination_dir: str,
    timeout: int,
):
    """Copy a file to a remote host using rsync over SSH.

    Uses rsync with AES128-CTR encryption and compression disabled.
    Logs transfer speed and file size on success.

    Args:
        source_filename: Path to the source file to copy.
        destination_dir: Remote destination directory (host:/path format).
        timeout: Maximum time in seconds to wait for the transfer.

    Returns:
        True if transfer succeeded, False otherwise.
    """
    logger.debug(f"{source_filename}: attempting copy_file_rsync...")

    # Build final command line
    # --no-compress ensures we don't try to compress (it's going to be quite
    # uncompressible)
    cmdline = (
        "rsync --no-compress -e 'ssh -T -c aes128-ctr -o"
        " StrictHostKeyChecking=no -o Compression=no -x' "
        f"{source_filename} {destination_dir}"
    )

    start_time = time.time()

    # run rsync
    return_val, stdout = run_command_ext(cmdline, None, timeout, False)

    if return_val:
        try:
            file_size = os.path.getsize(
                os.path.join(destination_dir, os.path.basename(source_filename))
            )
        except Exception:
            logger.exception(
                f"{source_filename}: Error determining destination file size."
            )
            return False

        elapsed = time.time() - start_time

        size_gigabytes = float(file_size) / (1000.0 * 1000.0 * 1000.0)
        gbps_per_sec = (size_gigabytes * 8) / elapsed

        logger.info(
            f"{source_filename}: copy_file_rsync success ({size_gigabytes:.3f}GB in {elapsed:.3f} seconds at {gbps_per_sec:.3f} Gbps)"
        )
        return True
    else:
        logger.error(f"{source_filename}: copy_file_rsync failed. Error {stdout}")
        return False


def archive_file_xrootd(
    full_filename: str,
    archive_numa_node: int,
    archive_destination_host: str,
    timeout: int,
):
    """Upload a file to an xrootd server with atomic temp-file rename.

    Uploads to a temporary file first, then renames it atomically on the
    remote host. Logs transfer speed and validates checksums.

    Args:
        full_filename: Path to the file to archive.
        archive_numa_node: NUMA node for command execution.
        archive_destination_host: Destination in format "host://path".
        timeout: Maximum time in seconds to wait for the transfer.

    Returns:
        True if transfer and rename succeeded, False otherwise.
    """
    logger.debug(f"{full_filename}: attempting archive_file_xrootd...")

    # get file size
    try:
        file_size = os.path.getsize(full_filename)
    except Exception:
        logger.exception(f"{full_filename}: Error determining file size.")
        return False

    # Gather some info for later
    filename = os.path.basename(full_filename)
    temp_filename = f"{filename}.part{uuid.uuid4()}"
    # Archive destination host looks like: "192.168.120.110://volume2/incoming"
    # so just get the bit before the ":" for the host and the bit after for
    # the path
    destination_host = archive_destination_host.split(":")[0]
    destination_path = archive_destination_host.split(":")[1]
    full_destination_temp_filename = os.path.join(destination_path, temp_filename)
    full_destination_final_filename = os.path.join(destination_path, filename)

    # Build final command line
    #
    # --posc         = persist on successful copy. If copy fails either remove
    #                  the file or set it to 0 bytes. Setting to 0 bytes is
    #                  weird, but I'll take it
    # --rm-bad-cksum = Delete dest file if checksums do not match
    #
    cmdline = (
        "/usr/local/bin/xrdcp --cksum adler32 --posc --rm-bad-cksum --silent"
        " --streams 2 --tlsnodata"
        f" {full_filename} xroot://{archive_destination_host}/{temp_filename}"
    )

    start_time = time.time()

    # run xrdcp
    return_val, stdout = run_command_ext(cmdline, archive_numa_node, timeout, False)

    if return_val:
        elapsed = time.time() - start_time

        size_gigabytes = float(file_size) / (1000.0 * 1000.0 * 1000.0)
        gbps_per_sec = (size_gigabytes * 8) / elapsed

        logger.info(
            f"{full_filename}: archive_file_xrootd success"
            f" ({size_gigabytes:.3f}GB in {elapsed:.3f} seconds at"
            f" {gbps_per_sec:.3f} Gbps)"
        )

        cmdline = (
            f"ssh -o StrictHostKeyChecking=no mwa@{destination_host} 'mv"
            f" {full_destination_temp_filename}"
            f" {full_destination_final_filename}'"
        )

        # run the mv command to rename the temp file to the final file
        # If this works, then mwacache will actually do its thing
        return_val, stdout = run_command_ext(cmdline, archive_numa_node, timeout, False)

        if return_val:
            logger.info(
                f"{full_filename}: archive_file_xrootd successfully renamed"
                f" {full_destination_temp_filename} to"
                f" {full_destination_final_filename} on the remote host"
                f" {destination_host}"
            )
            return True
        else:
            logger.error(
                f"{full_filename}: archive_file_xrootd rename failed. Error {stdout}"
            )
            return False
    else:
        logger.error(f"{full_filename}: archive_file_xrootd failed. Error {stdout}")
        return False


# HAProxy client/server timeouts are configured at 60 minutes in haproxy.cfg.
# rclone's effective maximum time per attempt is timeout * retries, which must
# not exceed this limit or HAProxy may kill the connection before rclone finishes.
_HAPROXY_MAX_TIMEOUT_MINS = 60

# Number of times to retry rclone check if it fails. Retries back off
# exponentially (see _RCLONE_CHECK_BACKOFF_BASE_SECS below) rather than waiting
# a fixed amount, since with balance uri routing in haproxy.cfg (Aug 2026) a
# given object's requests all land on the same VSS node, so most files are
# already consistent by the time the first check runs and pay no wait at all;
# only the rare lagging case pays an escalating, still-small cost.
_RCLONE_CHECK_RETRIES = 3

# Starting point for exponential backoff between failed rclone check retries:
# 1s, then 2s, then 4s, ... doubling each time, capped at rclone_check_wait_secs.
_RCLONE_CHECK_BACKOFF_BASE_SECS = 1


def archive_file_rclone_haproxy(
    rclone_profile: str,
    full_filename: str,
    bucket_name: str,
    md5hash: str,
    rclone_timeout_mins: int = 20,
    rclone_retries: int = 3,
    rclone_check_wait_secs: int = 15,
) -> bool:
    """Upload a file to Pawsey S3 (Acacia/Banksia) via rclone with HAProxy load balancing.

    Uploads via a local HAProxy instance which handles endpoint selection,
    health checking, and failover across all available S3 nodes transparently.
    Checksum verification is performed after a successful upload.

    If running under pytest, will automatically return True.

    Note:
        rclone_timeout_mins * rclone_retries must not exceed the HAProxy client/server
        timeout (currently 60 minutes, configured in haproxy.cfg). A warning is emitted
        if this limit is approached or exceeded.

    Args:
        rclone_profile: The rclone profile/remote name to use.
        full_filename: Path to the file to archive.
        bucket_name: S3 bucket name for the upload.
        md5hash: MD5 checksum hash for verification.
        rclone_timeout_mins: Inactivity timeout in minutes for rclone operations.
            Resets whenever data is flowing; only triggers if the connection goes
            silent for this duration. Defaults to 20 minutes.
        rclone_retries: Number of times rclone will retry a failed transfer or
            check before giving up. Handles transient errors on live endpoints.
            Defaults to 3.
        rclone_check_wait_secs: Maximum backoff cap, in seconds, for retrying a
            failed rclone check. No wait occurs before the first check attempt
            (immediately after copyto succeeds); subsequent retries back off
            exponentially starting at 1s, doubling each time, capped at this
            value. Defaults to 15.

    Returns:
        True if upload succeeded and checksum verified.
        False if all endpoints are down or upload failed after rclone retries.

    Raises:
        Exception: If an unexpected error occurs (not endpoint unavailability).
    """
    logger.debug(f"{full_filename}: attempting archive_file_rclone_haproxy...")

    # Warn if rclone's worst-case duration could exceed HAProxy's timeout.
    # Each retry can take up to rclone_timeout_mins before rclone gives up on it.
    effective_max_mins = rclone_timeout_mins * rclone_retries
    if effective_max_mins > _HAPROXY_MAX_TIMEOUT_MINS:
        logger.warning(
            f"rclone_timeout_mins ({rclone_timeout_mins}) * rclone_retries ({rclone_retries})"
            f" = {effective_max_mins} minutes, which exceeds the HAProxy client/server timeout"
            f" of {_HAPROXY_MAX_TIMEOUT_MINS} minutes. HAProxy may kill the connection before"
            f" rclone finishes. To fix, increase 'timeout client', 'timeout server', and"
            f" 'timeout server' in the backend block of /etc/haproxy/haproxy.cfg."
        )

    # Get just the filename
    filename = os.path.basename(full_filename)

    # Get file size
    try:
        file_size = os.path.getsize(full_filename)
    except Exception:
        logger.exception(f"{full_filename}: Error determining file size.")
        return False
    size_gigabytes = bytes_to_gigabytes(file_size)

    start_time = time.time()
    rclone_timeout = f"{rclone_timeout_mins}m"
    # Subprocess wall-clock limit accounts for full retry cycle:
    # each retry can take up to rclone_timeout_mins, plus a small buffer.
    subprocess_timeout_secs = rclone_timeout_mins * rclone_retries * 60

    #
    # TODO: Ugly solution here for testing - should replace this with a Mock pattern
    #
    if running_under_pytest():
        elapsed = 1.0
        check_elapsed = 1.0
        logger.info(
            f"{full_filename}: archive_file_rclone_haproxy success."
            f" Copied ({size_gigabytes:.3f}GB in {elapsed:.3f} seconds at"
            f" {get_gbps(size_gigabytes, start_time):.3f} Gbps)."
            f" Check took {check_elapsed:.3f} seconds."
        )
        return True

    # HAProxy listens on localhost:8080 and routes to all configured S3 endpoints.
    # No --s3-endpoint flag needed; the rclone profile's endpoint is set to
    # http://127.0.0.1:8080 in rclone.conf.
    # NOTE: --ignore-checksum skips the post-copy checksum verification in rclone
    # itself, which is safe here because we run a separate rclone check afterwards.
    # NOTE: --low-level-retries governs retries of individual HTTP calls (e.g. a
    # single part upload) and is set explicitly rather than relying on rclone's
    # default of 10, since transient errors against a load-balanced HAProxy
    # backend are expected and cheap to retry at this level.
    # NOTE: --retries-sleep is deliberately left at rclone's default (0, i.e.
    # immediate retry) rather than backing off. HAProxy transparently reroutes
    # each new attempt to a (possibly different) live backend node, so there is
    # no value in waiting before retrying - unlike the check step below, where
    # the delay exists specifically to wait out VSS replication lag rather than
    # to back off from a busy endpoint.
    # NOTE: --s3-chunk-size 512M (up from 128M) and --s3-upload-concurrency 16
    # (down from 32) - Aug 2026 testing against Versity/Banksia via HAProxy on
    # mwacache20 found the server-side multipart completion tail scales with
    # part count: ~73s at 16M chunks (640 parts), ~22s at 128M (80 parts),
    # ~1-6s at 512M (20 parts) for a 10GB test file. Concurrency above the
    # resulting part count for a file is wasted (a 13.5GB file has ~27 parts
    # at 512M chunks), and 32 concurrency at 512M chunks would mean up to 16GiB
    # of chunk buffers per worker in the worst case across 6 workers. The
    # concurrency comparison itself (8 vs 16) was inconclusive - runs showed
    # stalled individual chunks consistent with contention from the other 8
    # mwacache servers sharing the same 100Gbps trunk, not a concurrency
    # effect - so 16 is a reasonable default given the part-count ceiling
    # rather than a value pinned by a clean throughput measurement.
    try:
        cmdline = (
            f'/usr/bin/rclone copyto -M --metadata-set "md5={md5hash}"'
            f" --retries {rclone_retries}"
            f" --low-level-retries 20"
            f" --s3-upload-concurrency 16"
            f" --s3-chunk-size 512M"
            f" --ignore-checksum"
            f" --timeout {rclone_timeout}"
            f" --contimeout 30s"
            f" {full_filename} {rclone_profile}:/{bucket_name}/{filename}"
        )

        return_val, stdout = run_command_ext(
            cmdline, None, subprocess_timeout_secs, False
        )

        if return_val:
            elapsed = time.time() - start_time

            # Check immediately - no blind pre-sleep. With balance uri routing
            # in haproxy.cfg, this file's copyto and the check below hash to
            # the same VSS node, so there is no cross-node replication lag to
            # wait out. If that node's own write-to-read consistency has a
            # small lag, the retry loop below (exponential backoff) absorbs
            # it without penalising every file with an up-front wait.

            # Verify the file at the remote, retrying with exponential backoff
            # to absorb any small node-local write-to-read lag (see comment
            # above - cross-node lag is no longer a factor once balance uri
            # is in use in haproxy.cfg).
            #
            # --no-traverse: without this, rclone check lists the entire destination
            # bucket (paginated) to find the single matching file, even though a HEAD
            # for the exact object would suffice. With it, rclone does a single HEAD
            # per side and skips the listing entirely. Confirmed via -vv --dump=headers
            # against Versity Gateway (Aug 2026) - only HEAD calls are made, no
            # ListObjects/list-type requests.
            cmdline = (
                f"/usr/bin/rclone check"
                f" --no-traverse"
                f" --retries {rclone_retries}"
                f" --low-level-retries 20"
                f" --timeout {rclone_timeout}"
                f" --contimeout 30s"
                f" {full_filename} {rclone_profile}:/{bucket_name}"
            )

            check_start = time.time()
            check_attempt = 0
            return_val = False
            backoff_secs = _RCLONE_CHECK_BACKOFF_BASE_SECS

            while check_attempt < _RCLONE_CHECK_RETRIES and not return_val:
                check_attempt += 1
                logger.debug(
                    f"{full_filename}: rclone check attempt {check_attempt}"
                    f" of {_RCLONE_CHECK_RETRIES} against {rclone_profile}"
                    f" bucket {bucket_name} via HAProxy..."
                )
                return_val, stdout = run_command_ext(
                    cmdline, None, subprocess_timeout_secs, False
                )

                if not return_val and check_attempt < _RCLONE_CHECK_RETRIES:
                    logger.warning(
                        f"{full_filename}: rclone check attempt {check_attempt}"
                        f" of {_RCLONE_CHECK_RETRIES} failed, retrying in"
                        f" {backoff_secs} seconds. Output: {stdout}"
                    )
                    time.sleep(backoff_secs)
                    backoff_secs = min(backoff_secs * 2, rclone_check_wait_secs)

            if return_val:
                check_elapsed = time.time() - check_start
                logger.info(
                    f"{full_filename}: archive_file_rclone_haproxy success."
                    f" Copied ({size_gigabytes:.3f}GB in {elapsed:.3f} seconds at"
                    f" {get_gbps(size_gigabytes, start_time):.3f} Gbps)."
                    f" Check took {check_elapsed:.3f} seconds."
                )
                return True
            else:
                # All check attempts exhausted - treat as failure
                logger.error(
                    f"{full_filename}: rclone check failed after {_RCLONE_CHECK_RETRIES}"
                    f" attempts. Last output: {stdout}"
                )
                return False
        else:
            # rclone exhausted its retries - all endpoints likely down
            logger.warning(
                f"{full_filename}: could not be archived via rclone_haproxy."
                f" All HAProxy backends may be down or transfer failed. rclone copy output: {stdout}"
            )
            return False

    except Exception:
        logger.exception(
            f"{full_filename}: Error uploading to bucket {bucket_name} via rclone_haproxy."
        )
        return False
