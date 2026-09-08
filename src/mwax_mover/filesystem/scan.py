"""Directory scanning for files matching a glob pattern.

scan_directory() returns matches as a list; scan_for_existing_files_and_add_to_queue()
scans and enqueues them onto a regular queue.Queue in sorted order. (The
priority-queue equivalent, scan_for_existing_files_and_add_to_priority_queue(),
lives in queues.priority_queue_data -- see docs/RESTRUCTURE.md Phase 1.)
"""

import glob
import logging
import os
import queue

logger = logging.getLogger(__name__)


def scan_directory(watch_dir: str, pattern: str, recursive: bool, exclude_pattern) -> list:
    """
    Scan a directory for files matching a glob pattern and return them as a list.

    Args:
        watch_dir: Root directory to scan. Resolved to an absolute path internally.
        pattern: Glob suffix pattern to match (e.g. ``'.fits'``). Prepended
            with ``'*'`` (and ``'**/'`` for recursive scans) internally.
        recursive: If True, search all subdirectories using ``**`` glob syntax.
        exclude_pattern: Glob suffix pattern for files to exclude. Files whose
            paths match ``<watch_dir>/*<exclude_pattern>`` are removed from
            the results. Pass None (or a falsy value) to skip exclusion.

    Returns:
        A list of absolute path strings for all matched (and non-excluded) files.
    """
    # Watch dir must end in a slash for the iglob to work
    # Just loop through all files and add them to the queue
    if recursive:
        find_pattern = os.path.join(os.path.abspath(watch_dir), "**/*" + pattern)
        logger.info(f"{watch_dir}: Scanning recursively for files matching {find_pattern}...")
    else:
        find_pattern = os.path.join(os.path.abspath(watch_dir), "*" + pattern)
        logger.info(f"{watch_dir}: Scanning for files matching *{pattern}...")

    files = glob.glob(find_pattern, recursive=recursive)

    # Now exclude files if they match the exclude pattern
    if exclude_pattern:
        exclude_glob = os.path.join(os.path.abspath(watch_dir), "*" + exclude_pattern)
        logger.info(f"{watch_dir}: Excluding files *{exclude_pattern}...")
        return [fn for fn in files if fn not in glob.glob(exclude_glob)]
    else:
        return files


def scan_for_existing_files_and_add_to_queue(
    watch_dir: str,
    pattern: str,
    recursive: bool,
    queue_target: queue.Queue,
    exclude_pattern=None,
):
    """
    Scan a directory for files matching a pattern and add them to a regular Queue.

    Files are sorted before being enqueued to provide a deterministic ordering.

    Args:
        watch_dir: Root directory to scan.
        pattern: Glob suffix pattern to match (e.g. ``'.fits'``). Prepended
            with ``'*'`` internally.
        recursive: If True, scan all subdirectories recursively.
        queue_target: The ``queue.Queue`` instance to add matched filenames to.
        exclude_pattern: Optional glob suffix pattern. Files matching this
            pattern are excluded from the results. Defaults to None (no exclusion).
    """
    files = scan_directory(watch_dir, pattern, recursive, exclude_pattern)
    files = sorted(files)
    logger.info(f"{watch_dir}: Found {len(files)} files")

    for filename in files:
        queue_target.put(filename)
        logger.info(f"{watch_dir}: {os.path.basename(filename)} added to queue")
