"""Module to watch a folder for file events and add file to a queue"""
import os
import queue
import time
import inotify.constants
import inotify.adapters
from mwax_mover import mwax_mover, utils


class Watcher(object):
    """Class that watches a directory and adds files to a queue"""

    def __init__(
        self,
        name: str,
        path: str,
        dest_queue: queue.Queue,
        pattern: str,
        log,
        mode,
        recursive,
        exclude_pattern=None,
    ):
        self.logger = log
        self.name = name
        self.inotify_tree = None
        self.recursive = recursive
        self.mode = mode
        self.path = path
        self.watching = False
        self.dest_queue = dest_queue
        self.pattern = pattern  # must be ".ext" or ".*"
        self.exclude_pattern = exclude_pattern  # Can be None or ".ext"
        self.scan_completed = False

        if self.mode == mwax_mover.MODE_WATCH_DIR_FOR_NEW:
            self.mask = inotify.constants.IN_CLOSE_WRITE
        elif self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME:
            self.mask = inotify.constants.IN_MOVED_TO
        elif self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW:
            self.mask = (
                inotify.constants.IN_MOVED_TO
                | inotify.constants.IN_CLOSE_WRITE
            )

        # Check that the path to watch exists
        if not os.path.exists(self.path):
            raise FileNotFoundError(self.path)

    def start(self):
        """Begins watching the directory"""
        if self.recursive:
            self.logger.info(
                f"Watcher starting on {self.path}/*{self.pattern} and all"
                " subdirectories..."
            )
            self.inotify_tree = inotify.adapters.InotifyTree(
                self.path, mask=self.mask
            )
        else:
            self.logger.info(
                f"Watcher starting on {self.path}/*{self.pattern}..."
            )
            self.inotify_tree = inotify.adapters.Inotify()
            self.inotify_tree.add_watch(self.path, mask=self.mask)

        if self.exclude_pattern:
            self.logger.info(
                f"Watcher on {self.path}/*{self.pattern} is excluding"
                f" *{self.exclude_pattern}"
            )

        self.watching = True
        self.do_watch_loop()

    def stop(self):
        """Stop watching the directory"""
        self.logger.info(f"Watcher stopping on {self.path}/*{self.pattern}...")

        self.watching = False

        if self.recursive:
            self.inotify_tree = None
        else:
            self.inotify_tree.remove_watch(self.path)

    def do_watch_loop(self):
        """ "Initiate watching"""
        # If we're in NEW or RENAME mode, then scan the folder once we have
        # enqueued any waiting items
        if (
            self.mode == mwax_mover.MODE_WATCH_DIR_FOR_NEW
            or self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME
            or self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW
        ):
            utils.scan_for_existing_files_and_add_to_queue(
                self.logger,
                self.path,
                self.pattern,
                self.recursive,
                self.dest_queue,
                self.exclude_pattern,
            )
        self.scan_completed = True

        while self.watching:
            for event in self.inotify_tree.event_gen(
                timeout_s=0.1, yield_nones=False
            ):
                (header, _, path, filename) = event

                # check event is one we care about
                if header.mask | self.mask == self.mask:
                    # Check file extension is one we care about
                    if (
                        os.path.splitext(filename)[1] == self.pattern
                        or self.pattern == ".*"
                    ) and os.path.splitext(filename)[
                        1
                    ] != self.exclude_pattern:
                        dest_filename = os.path.join(path, filename)
                        self.dest_queue.put(dest_filename)
                        self.logger.info(
                            f"{dest_filename} added to queue"
                            f" ({self.dest_queue.qsize()})"
                        )

    def get_status(self) -> dict:
        """Returns a dictionary describing status of this watcher"""
        _, used_bytes, free_bytes = utils.get_disk_space_bytes(self.path)

        return {
            "Unix timestamp": time.time(),
            "watching": self.watching,
            "mode": self.mode,
            "watch_path": self.path,
            "watch_pattern": self.pattern,
            "watch_used_bytes": used_bytes,
            "watch_free_bytes": free_bytes,
        }
