"""rclone wrappers for moving, deleting, and checking files on S3-compatible
remotes (Acacia, Banksia).

rclone_move() moves a local directory's contents to a bucket, parsing
rclone's JSON stats log via parse_rclone_stats() to report transfer counts.
rclone_delete_file() and check_remote_file_exists() perform single-file
operations on a remote.
"""

import json
import logging
import subprocess

logger = logging.getLogger(__name__)


def parse_rclone_stats(stderr: str) -> dict:
    """Extract the final stats block from rclone's JSON log output.

    rclone emits one JSON object per line to stderr when --use-json-log is set.
    The last line containing a 'stats' key is the end-of-run summary.

    Args:
        stderr: Raw stderr output from the rclone process.

    Returns:
        A dict of rclone stats, or an empty dict if none could be parsed.
        Useful keys: 'transfers' (files moved), 'bytes', 'errors', 'elapsedTime'.
    """
    last_stats: dict = {}
    for line in stderr.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
            if "stats" in entry:
                last_stats = entry["stats"]
        except json.JSONDecodeError:
            continue
    return last_stats


def rclone_move(
    path: str,
    profile: str,
    bucket: str,
    dest_subpath: str | None = None,
    min_file_age_secs: int = 0,
) -> tuple[int, int]:
    """Run rclone move for files in a directory to the S3 destination.

    Uses --no-traverse for efficiency on large buckets.

    Args:
        path: Local directory path to move files from.
        profile: The rclone profile to use (see rclone.conf).
        bucket: Destination bucket name.
        dest_subpath: Optional path within the bucket to move into. Use this to
            preserve a directory name that would otherwise be lost by moving the
            directory's *contents* rather than the directory itself -- e.g. pass
            the fit_id when moving ``<base>/<fit_id>`` so the objects land under
            ``<bucket>/<fit_id>/`` and match the URLs written into index.json.
        min_file_age_secs: Do not attempt to move any file which is newer than
            this many seconds. Defaults to 0 (no age filter).

            NOTE: this is not a safe way to detect "the writer has finished".
            A file moved into ``path`` with os.rename keeps its original mtime,
            so it can arrive already older than any threshold set here. Callers
            that need completion detection should have the writer publish a
            fully-populated directory atomically instead -- see
            mwax_calvin_utils.upload_plot_files.

    Returns:
        tuple of transfers and bytes_transferred

    Raises:
        subprocess.CalledProcessError: If rclone exits with a non-zero return code.
    """

    dest = f"{profile}:{bucket}"
    if dest_subpath:
        dest = f"{dest}/{dest_subpath}"

    cmd = [
        "rclone",
        "move",
        "-v",  # This is needed to get any json output
        "--min-age",
        f"{min_file_age_secs}s",
        "--no-traverse",
        "--use-json-log",  # structured JSON lines on stderr
        "--stats",
        "1h",  # suppress periodic stats, only emit at end
        path,
        dest,
    ]
    logger.debug(f"Running rclone: {' '.join(cmd)}")

    result = subprocess.run(cmd, capture_output=True, text=True, check=False)

    #  get rclone stats - skip if we hit an error
    try:
        stats = parse_rclone_stats(result.stderr)
        transfers = stats.get("transfers", 0)
        bytes_moved = 0
        if transfers > 0:
            bytes_moved = stats.get("bytes", 0)
            elapsed = stats.get("elapsedTime", 0.0)
            logger.info(
                f"rclone moved {transfers} file(s) ({bytes_moved / 1000.0:.1f} KB) from {path} in {elapsed:.1f}s",
            )
        else:
            logger.debug(f"rclone: nothing to move from {path}")
    except Exception:
        bytes_moved = 0
        transfers = 0
        logger.exception("Error getting stats from rclone. Skipping.")

    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd, output=result.stdout, stderr=result.stderr)

    if result.stdout:
        logger.debug(f"rclone stdout: {result.stdout.strip()}")

    # pass back transfers, transferred_bytes
    return transfers, bytes_moved


def rclone_delete_file(rclone_profile: str, bucket: str, filename: str) -> None:
    """Delete a file from a remote bucket using rclone.

    Args:
        rclone_profile: The rclone remote profile name (as configured in rclone.conf).
        bucket: The name of the bucket to delete the file from.
        filename: The name of the file to delete within the bucket.

    Raises:
        subprocess.CalledProcessError: If rclone exits with a non-zero return
            code, with stderr included in the exception message.
        FileNotFoundError: If the rclone binary is not found on PATH.
    """
    remote_path = f"{rclone_profile}:{bucket}/{filename}"
    result = subprocess.run(
        ["rclone", "deletefile", remote_path],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise subprocess.CalledProcessError(
            result.returncode,
            result.args,
            output=result.stdout,
            stderr=result.stderr,
        )


def check_remote_file_exists(rclone_profile: str, bucket_name: str, filename: str) -> bool:
    """Check whether a file exists on an rclone remote.

    Args:
        rclone_profile: The rclone remote name (e.g. ``"acacia"``).
        bucket_name: The bucket or top-level path on the remote.
        filename: The filename or relative path within the bucket.

    Returns:
        True if the file exists, False if it does not.

    Raises:
        subprocess.CalledProcessError: If rclone exits with an unexpected
            error (i.e. not a simple "not found" result).
        FileNotFoundError: If the rclone binary cannot be found.
    """
    remote_path = f"{rclone_profile}:{bucket_name}/{filename}"

    result = subprocess.run(["rclone", "lsf", remote_path], capture_output=True, text=True, check=False)

    if result.returncode == 0:
        return True
    elif result.returncode == 3:
        # Exit code 3: directory/file not found
        return False
    else:
        raise subprocess.CalledProcessError(result.returncode, result.args, result.stdout, result.stderr)
