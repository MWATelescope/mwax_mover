from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_NEW
from logging import Logger
import os
import shutil


class PacketStatsProcessor(MWAXWatchQueueWorker):
    def __init__(
        self, logger: Logger, packet_stats_dump_dir: str, packet_stats_file_ext: str, packet_stats_destination_dir: str
    ):
        super().__init__(
            "PacketStatsProcessor",
            logger,
            [(packet_stats_dump_dir, packet_stats_file_ext)],
            mode=MODE_WATCH_DIR_FOR_NEW,
            requeue_to_eoq_on_failure=False,
        )
        self.packet_stats_dump_dir = packet_stats_dump_dir
        self.packet_stats_destination_dir = packet_stats_destination_dir

    def handler(self, item: str) -> bool:
        # This gets called by the queueworker who had put an "item"
        # on the queue from a watcher of the filesystem.
        # The "item" is the full path and filename of a packet stats data file
        # which now needs to be copied to the destination path (e.g. vulcan)
        # and the deleted once successful
        destination_filename: str = os.path.join(self.packet_stats_destination_dir, os.path.basename(item))

        try:
            self.logger.debug(f"{item}: Attempting to copy local packet stats file {item} to{destination_filename}")
            shutil.copy2(item, destination_filename)

            self.logger.debug(f"{item}: Copy success. Deleting local packet stats file {item}")

            # Success- now delete the file
            os.remove(item)

            self.logger.debug(f"{item}: Deleted local packet stats file {item}")

            return True
        except Exception:
            # Something went wrong- log it and requeue
            self.logger.exception(f"Unable to copy/delete {item} to {destination_filename}")
            return False
