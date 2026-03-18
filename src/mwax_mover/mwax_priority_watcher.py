"""
Module to watch a folder for file events and add file to a
priority queue
"""

import logging
import os
import queue
import inotify.constants
import inotify.adapters
from typing import Optional
from mwax_mover import mwax_mover, utils
from mwax_mover.mwax_priority_queue_data import MWAXPriorityQueueData

logger = logging.getLogger(__name__)


class PriorityWatcher(object):
    """Class that watches a directory and adds files to a priority queue"""

    def __init__(
        self,
        name: str,
        path: str,
        dest_queue: queue.PriorityQueue,
        pattern: str,
        mode,
        recursive,
        metafits_path,
        list_of_correlator_high_priority_projects: list[str],
        list_of_vcs_high_priority_projects: list[str],
        exclude_pattern: Optional[str] = None,
    ):
        self.name = name
        self.inotify_tree: Optional[inotify.adapters.InotifyTree | inotify.adapters.Inotify] = None
        self.recursive = recursive
        self.mode = mode
        self.path = path
        self.watching = False
        self.dest_queue = dest_queue
        self.pattern = pattern  # must be ".ext" or ".*"
        self.exclude_pattern = exclude_pattern  # Can be None or ".ext"
        self.metafits_path = metafits_path
        self.list_of_correlator_high_priority_projects: list = list_of_correlator_high_priority_projects
        self.list_of_vcs_high_priority_projects: list = list_of_vcs_high_priority_projects
        # This is a flag used so callers can know when,
        # on startup that the scan for existing files
        # has completed. This is useful for the workers
        # who will want to wait until the scan is done
        # before starting to process the queue (so that
        # all the priorities are taken into account)
        self.scan_completed = False

        if self.mode == mwax_mover.MODE_WATCH_DIR_FOR_NEW:
            self.mask = inotify.constants.IN_CLOSE_WRITE
        elif self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME:
            self.mask = inotify.constants.IN_MOVED_TO
        elif self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW:
            self.mask = inotify.constants.IN_MOVED_TO | inotify.constants.IN_CLOSE_WRITE

        # Check that the path to watch exists
        if not os.path.exists(self.path):
            raise FileNotFoundError(self.path)

    def start(self):
        """Begins watching the directory"""
        # supress all but most critical inotify logs
        logging.getLogger("inotify.adapters").setLevel(logging.CRITICAL)

        if self.recursive:
            logger.info(f"PriorityWatcher starting on {self.path}/*{self.pattern} and all subdirectories...")
            self.inotify_tree = inotify.adapters.InotifyTree(self.path, mask=self.mask)
        else:
            logger.info(f"PriorityWatcher starting on {self.path}/*{self.pattern}...")
            self.inotify_tree = inotify.adapters.Inotify()
            self.inotify_tree.add_watch(self.path, mask=self.mask)

        if self.exclude_pattern:
            logger.info(f"Watcher on {self.path}/*{self.pattern} is excluding *{self.exclude_pattern}")

        self.watching = True
        self.do_watch_loop()

    def stop(self):
        """Stop watching the directory"""
        logger.info(f"PriorityWatcher stopping on {self.path}/*{self.pattern}...")

        self.watching = False

        if self.recursive:
            self.inotify_tree = None
        else:
            if isinstance(self.inotify_tree, inotify.adapters.Inotify):
                self.inotify_tree.remove_watch(self.path)

        # Destroy the inotify adpater
        try:
            del self.inotify_tree
        except Exception:
            pass

    def do_watch_loop(self):
        """ "Initiate watching"""
        # If we're in NEW or RENAME mode, then scan the folder once we have
        # enqueued any waiting items
        if (
            self.mode == mwax_mover.MODE_WATCH_DIR_FOR_NEW
            or self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME
            or self.mode == mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW
        ):
            utils.scan_for_existing_files_and_add_to_priority_queue(
                self.metafits_path,
                self.path,
                self.pattern,
                self.recursive,
                self.dest_queue,
                self.list_of_correlator_high_priority_projects,
                self.list_of_vcs_high_priority_projects,
                self.exclude_pattern,
            )
        self.scan_completed = True

        while self.watching:
            if self.inotify_tree:
                for event in self.inotify_tree.event_gen(timeout_s=0.1, yield_nones=False):
                    # This if is a bit redundant as above we specify we don't yield nones.
                    if event:
                        (header, _, path, filename) = event

                        # debug (uncomment if needed)
                        # logger.debug(f"Event {path} {filename}")

                        # check event is one we care about
                        if header.mask | self.mask == self.mask:
                            # Check file extension is one we care about
                            if (
                                os.path.splitext(filename)[1] == self.pattern or self.pattern == ".*"
                            ) and os.path.splitext(filename)[1] != self.exclude_pattern:
                                dest_filename = os.path.join(path, filename)

                                # We need to determine the priority
                                priority = utils.get_priority(
                                    dest_filename,
                                    self.metafits_path,
                                    self.list_of_correlator_high_priority_projects,
                                    self.list_of_vcs_high_priority_projects,
                                )

                                new_queue_item = (
                                    priority,
                                    MWAXPriorityQueueData(dest_filename),
                                )

                                self.dest_queue.put(new_queue_item)
                                logger.info(
                                    f"{dest_filename} added to queue with priority {priority} ({self.dest_queue.qsize()})"
                                )

    def get_status(self) -> dict:
        """Returns a dictionary describing status of this watcher"""
        return {
            "name": self.name,
            "watch_path": self.path,
        }
