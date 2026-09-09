"""Generic file-level operations: delete, checksum, extract, inspect.

remove_file() and delete_files_older_than() manage cleanup; do_checksum_md5()
runs the system md5sum binary; extract_tar() safely extracts a tar archive;
get_png_dimensions() reads a PNG's width/height directly from its header
bytes. Unlike fits.subfile's external-tool wrappers, none of these are
subfile-specific -- they operate on any file.
"""

import logging
import os
import struct
import tarfile
import time
from pathlib import Path

from tenacity import retry, stop_after_attempt, wait_fixed

from mwax_mover.core.command import run_command_ext

logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
def remove_file(filename: str, raise_error: bool) -> bool:
    """
    Delete a file from the filesystem, with up to 3 automatic attempts.

    Retries are handled by tenacity with a 10-second fixed wait between
    attempts. Retries only occur when ``raise_error`` is True and the deletion
    raises an exception; when ``raise_error`` is False a failed deletion is
    logged as a warning and True is returned without retrying.

    Note: When ``raise_error`` is False this function returns True even if the
    file was not successfully deleted (e.g. because it had already been moved
    or removed by another process). Callers that require confirmation of
    deletion should pass ``raise_error=True``.

    Args:
        filename: Full path to the file to delete.
        raise_error: If True, log an error and re-raise the exception on
            failure (triggering a tenacity retry). If False, log a warning
            and return True without raising.

    Returns:
        True if the file was deleted, or if ``raise_error`` is False and the
        deletion failed (assumed already gone).

    Raises:
        Exception: The underlying OS exception, if ``raise_error`` is True and
            all retry attempts are exhausted.
    """
    try:
        os.remove(filename)
        logger.info(f"{filename}- file deleted")
        return True

    except Exception as delete_exception:
        if raise_error:
            logger.error(f"{filename}- Error deleting: {delete_exception}. Retrying up to 3 times.")
            raise
        else:
            logger.warning(f"{filename}- Error deleting: {delete_exception}. File may have been moved or removed.")
            return True


def delete_files_older_than(path: str, older_than_seconds: int, extensions: list[str]) -> list[str]:
    """
    Delete files in a directory that are older than a threshold and match given extensions.

    Scans ``path`` non-recursively and deletes any regular file whose
    modification time is at least ``older_than_seconds`` seconds in the past
    and whose extension (case-insensitive) is in ``extensions``. Files that
    cannot be stat-ed or deleted (e.g. due to permissions) are silently skipped.

    Args:
        path: Directory to scan. Must exist and be a directory.
        older_than_seconds: Age threshold in seconds (based on ``mtime``).
            Files with ``(now - mtime) >= older_than_seconds`` are candidates
            for deletion. Must be non-negative.
        extensions: List of file extensions to match, with or without a
            leading dot, case-insensitive (e.g. ``['log', '.tmp', '.TXT']``).
            An empty list matches nothing and no files will be deleted.

    Returns:
        A list of absolute path strings for every file successfully deleted.

    Raises:
        ValueError: If ``path`` is not an existing directory, or if
            ``older_than_seconds`` is negative.
    """
    # --- Validate inputs ---
    p = Path(path)
    if not p.exists() or not p.is_dir():
        raise ValueError(f"Path is not a directory or does not exist: {path}")

    if older_than_seconds < 0:
        raise ValueError("older_than_seconds must be non-negative")

    # Normalize extensions: make them lower-case and ensure they start with a dot.
    norm_exts = {("." + ext.lower().lstrip(".")) for ext in extensions}

    now = time.time()
    deleted: list[str] = []

    # Iterate non-recursively over files in the directory
    for entry in p.iterdir():
        # Only operate on files (skip directories, symlinks-to-directories, etc.)
        try:
            is_file = entry.is_file()
        except OSError:
            # Some entries may be inaccessible; skip them
            continue

        if not is_file:
            continue

        # Check extension match (case-insensitive)
        suffix = entry.suffix.lower()  # includes leading dot if any
        if norm_exts and suffix not in norm_exts:
            continue
        # If norm_exts is empty, treat as "match none" (requires explicit extensions)
        if not norm_exts:
            continue

        # Check age based on modification time (mtime)
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            # Could be permission issues; skip
            continue

        file_age = now - mtime
        if file_age >= older_than_seconds:
            # Attempt deletion
            try:
                os.remove(entry)  # pathlib's unlink() also works; os.remove is fine for files
                deleted.append(str(entry.resolve()))
            except OSError:
                # If deletion fails (permissions, locked files), skip silently or log if desired
                # You could collect failures separately if you want to report them.
                continue

    return deleted


def do_checksum_md5(full_filename: str, numa_node: int | None, timeout: int) -> str:
    """
    Compute the MD5 checksum of a file by running the system ``md5sum`` command.

    Args:
        full_filename: Full path to the file to checksum.
        numa_node: NUMA node to pin the subprocess to, or None for no pinning.
        timeout: Maximum number of seconds to wait for the command to complete.

    Returns:
        The 32-character lowercase hexadecimal MD5 digest string.

    Raises:
        Exception: If ``md5sum`` returns a non-zero exit code, or if the parsed
            checksum is not exactly 32 characters.
    """

    # default output of md5 hash command is:
    # "5ce49e5ebd72c41a1d70802340613757
    # /visdata/incoming/1320133480_20211105074422_ch055_000.fits"
    md5output = ""
    checksum = ""

    logger.debug(f"{full_filename}- running md5sum...")

    cmdline = f"md5sum {full_filename}"

    size = os.path.getsize(full_filename)

    start_time = time.time()
    return_value, md5output = run_command_ext(cmdline, numa_node, timeout, False)
    elapsed = time.time() - start_time

    size_megabytes = size / (1000 * 1000)
    mb_per_sec = size_megabytes / elapsed

    if return_value:
        # md5sum output format is: "<hash>  <filename>"
        # Split on whitespace and take the first token to avoid any risk of
        # the filename appearing in the hash field if it contains unusual characters.
        checksum = md5output.split()[0]

        # MD5 hash is ALWAYS 32 characters
        if len(checksum) == 32:
            logger.info(
                f"{full_filename} md5sum success"
                f" {checksum} ({size_megabytes:.3f}MB in {elapsed:.3f} secs at"
                f" {mb_per_sec:.3f} MB/s)"
            )
            return checksum
        else:
            raise Exception(f"Calculated MD5 checksum is not valid: md5 output {md5output}")
    else:
        raise Exception(f"md5sum returned an unexpected return code {return_value}")


def extract_tar(tar_filename: str, dest_path: str) -> None:
    """Extract a tar archive to the specified destination.

    Opens the given tar archive and extracts all members to dest_path,
    using the 'data' filter to reject unsafe paths (absolute paths,
    '../' traversal, symlinks pointing outside the destination).

    NOTE: tar.extractall() silently overwrites existing files.

    For our purposes this is fine so I don't care, but be warned!

    Args:
        tar_filename: Path to the tar archive to extract.
        dest_path: Directory to extract the archive contents into.
            Must already exist.

    Raises:
        FileNotFoundError: If tar_filename does not exist.
        NotADirectoryError: If dest_path does not exist or is not a directory.
        RuntimeError: If extraction fails due to a malformed, truncated,
            or otherwise invalid archive.
    """
    if not os.path.isfile(tar_filename):
        raise FileNotFoundError(f"tar archive not found: {tar_filename}")

    if not os.path.isdir(dest_path):
        raise NotADirectoryError(f"Destination directory does not exist: {dest_path}")

    try:
        with tarfile.open(tar_filename) as tf:
            members = tf.getmembers()
            total = len(members)
            for i, member in enumerate(members, start=1):
                tf.extract(member, path=dest_path, filter="data")
                logger.debug(f"Extracted ({i}/{total}): {os.path.join(dest_path, member.name)}")
    except tarfile.TarError as e:
        raise RuntimeError(f"Failed to extract {tar_filename}: {e}") from e


def get_png_dimensions(path: str) -> tuple[int, int]:
    """Get the width and height of a PNG image file.

    Args:
        path: Path to the PNG file.

    Returns:
        A tuple of (width, height) in pixels.

    Raises:
        ValueError: If the file does not appear to be a valid PNG.
    """
    with open(path, "rb") as f:
        sig = f.read(8)
        if sig != b"\x89PNG\r\n\x1a\n":
            raise ValueError(f"Not a valid PNG file: {path}")
        f.read(4)  # IHDR chunk length
        f.read(4)  # "IHDR"
        width, height = struct.unpack(">II", f.read(8))
    return width, height
