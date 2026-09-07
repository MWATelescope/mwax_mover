"""
Class representing the data item within a PriorityQueue.
The comparison operators ignore the path and just compare the filename.
This allows us to have a PriorityQueue which sorts by the filename not
the full filepath.
E.g.
without this class:
q = PriorityQueue()
q.put((1, 'path1/file1.dat'))
q.put((1, 'path2/file2.dat'))
q.put((1, 'path1/file3.dat'))
print(q.get()[1])
    path1/file1.dat
print(q.get()[1])
    path1/file3.dat
print(q.get()[1])
    path2/file2.dat

But correct order should be:
print(q.get()[1])
    path1/file1.dat
print(q.get()[1])
    path2/file2.dat
print(q.get()[1])
    path1/file3.dat
"""

import logging
import os
import queue

from mwax_mover import utils

logger = logging.getLogger(__name__)


class MWAXPriorityQueueData:
    """
    Use an instance of this class where you normally specify
    data when interacting with a PriorityQueue
    """

    # Unhashable by design: __eq__ is filename-only so instances must not be
    # used as dict keys or set members (Python 3 would set this implicitly, but
    # we make it explicit for clarity).
    __hash__ = None

    def __init__(self, full_filename: str):
        """Initialize a priority queue data item with a full file path.

        Args:
            full_filename: The full path and filename of the file.
        """
        self.value: str = full_filename

    def __repr__(self):
        """Return the official string representation of the object.

        Returns:
            The full filename/path.
        """
        return self.value

    def __str__(self):
        """Return the string representation of the object.

        Returns:
            The full filename/path.
        """
        return self.value

    def _sort_key(self, full_path: str) -> str:
        """Return the sort key for a full path (filename only).

        Args:
            full_path: A full file path.

        Returns:
            The filename component of the path, without any directory prefix.
        """
        return os.path.split(full_path)[1]

    def __lt__(self, obj):
        """Check if this object is less than another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if this object's filename is less than obj's filename.
        """
        return self._sort_key(self.value) < self._sort_key(obj.value)

    def __le__(self, obj):
        """Check if this object is less than or equal to another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if this object's filename is less than or equal to obj's filename.
        """
        return self._sort_key(self.value) <= self._sort_key(obj.value)

    def __eq__(self, obj):
        """Check if this object is equal to another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if both objects have the same filename.
        """
        return self._sort_key(self.value) == self._sort_key(obj.value)

    def __ne__(self, obj):
        """Check if this object is not equal to another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if the objects have different filenames.
        """
        return self._sort_key(self.value) != self._sort_key(obj.value)

    def __gt__(self, obj):
        """Check if this object is greater than another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if this object's filename is greater than obj's filename.
        """
        return self._sort_key(self.value) > self._sort_key(obj.value)

    def __ge__(self, obj):
        """Check if this object is greater than or equal to another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if this object's filename is greater than or equal to obj's filename.
        """
        return self._sort_key(self.value) >= self._sort_key(obj.value)


def scan_for_existing_files_and_add_to_priority_queue(
    metafits_path: str,
    watch_dir: str,
    pattern: str,
    recursive: bool,
    queue_target: queue.PriorityQueue,
    list_of_correlator_high_priority_projects: list,
    list_of_vcs_high_priority_projects: list,
    exclude_pattern=None,
):
    """
    Scan a directory for files matching a pattern and add them to a PriorityQueue.

    Each file's priority is determined by ``get_priority()``. Files are sorted
    before priority assignment to provide a deterministic ordering when multiple
    files share the same priority.

    Args:
        metafits_path: Directory containing metafits files, passed through to
            ``get_priority()`` for file type and project ID resolution.
        watch_dir: Root directory to scan.
        pattern: Glob suffix pattern to match (e.g. ``'.fits'``). Prepended
            with ``'*'`` internally.
        recursive: If True, scan all subdirectories recursively.
        queue_target: The ``queue.PriorityQueue`` instance to add
            ``(priority, MWAXPriorityQueueData)`` tuples to.
        list_of_correlator_high_priority_projects: Project IDs that should
            receive elevated priority for correlator observations.
        list_of_vcs_high_priority_projects: Project IDs that should receive
            elevated priority for VCS observations.
        exclude_pattern: Optional glob suffix pattern. Files matching this
            pattern are excluded from the results. Defaults to None (no exclusion).
    """
    files = utils.scan_directory(watch_dir, pattern, recursive, exclude_pattern)
    files = sorted(files)
    logger.info(f"{watch_dir}: Found {len(files)} files")

    for filename in files:
        priority = utils.get_priority(
            filename,
            metafits_path,
            list_of_correlator_high_priority_projects,
            list_of_vcs_high_priority_projects,
        )
        queue_target.put((priority, MWAXPriorityQueueData(filename)))
        logger.info(f"{watch_dir}: {os.path.basename(filename)} added to queue with priority {priority}")
