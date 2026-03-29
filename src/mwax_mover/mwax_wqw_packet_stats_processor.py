"""Watch-queue-worker that copies packet statistics dump files to a remote destination host.

Monitors a local packet statistics dump directory for new files and copies each
one to a configured remote destination (e.g. vulcan) using shutil.copy2(), then
deletes the local source file on success.
"""

from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_NEW
import os
import shutil

import logging

logger = logging.getLogger(__name__)


class PacketStatsProcessor(MWAXWatchQueueWorker):
    def __init__(self, packet_stats_dump_dir: str, packet_stats_file_ext: str, packet_stats_destination_dir: str):
        """Initialize the PacketStatsProcessor.

        Args:
            packet_stats_dump_dir: Directory where packet statistics dump files are created.
            packet_stats_file_ext: File extension to monitor (e.g., '.dump').
            packet_stats_destination_dir: Remote destination directory to copy files to.
        """
        super().__init__(
            "PacketStatsProcessor",
            [(packet_stats_dump_dir, packet_stats_file_ext)],
            mode=MODE_WATCH_DIR_FOR_NEW,
            requeue_to_eoq_on_failure=False,
        )
        self.packet_stats_dump_dir = packet_stats_dump_dir
        self.packet_stats_destination_dir = packet_stats_destination_dir

    def handler(self, item: str) -> bool:
        """Copy a packet statistics dump file to the remote destination and delete the local source.

        Processes packet statistics dump files by copying them to the configured remote
        destination directory and deleting the local source file on success.

        Args:
            item: Full path to the packet statistics dump file.

        Returns:
            True if the file was successfully copied and deleted, False otherwise.
        """
        # This gets called by the queueworker who had put an "item"
        # on the queue from a watcher of the filesystem.
        # The "item" is the full path and filename of a packet stats data file
        # which now needs to be copied to the destination path (e.g. vulcan)
        # and the deleted once successful
        destination_filename: str = os.path.join(self.packet_stats_destination_dir, os.path.basename(item))

        try:
            logger.debug(f"{item}: Attempting to copy local packet stats file {item} to{destination_filename}")
            shutil.copy2(item, destination_filename)

            logger.debug(f"{item}: Copy success. Deleting local packet stats file {item}")

            # Success- now delete the file
            os.remove(item)

            logger.debug(f"{item}: Deleted local packet stats file {item}")

            return True
        except Exception:
            # Something went wrong- log it and requeue
            logger.exception(f"{item}: Unable to copy/delete to {destination_filename}")
            return False
