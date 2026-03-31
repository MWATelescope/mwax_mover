"""Watch-queue-worker that collects outgoing calibrator visibility files for the Calvin pipeline.

Monitors the vis cal_outgoing directory for renamed .fits files. Each file path is
appended to a shared, thread-safe list consumed by MWAXSubfileDistributor to track
which calibrator observations are ready for submission to the Calvin calibration
pipeline.
"""

from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME
import logging
import threading

logger = logging.getLogger(__name__)


class VisCalOutgoingProcessor(MWAXWatchQueueWorker):
    def __init__(
        self,
        visdata_outgoing_cal_path: str,
        outgoing_cal_list: list[str],
        outgoing_cal_list_lock: threading.Lock,
    ):
        """Initialize the VisCalOutgoingProcessor.

        Args:
            visdata_outgoing_cal_path: Path to the visibility calibrator outgoing directory.
            outgoing_cal_list: Shared list to store outgoing calibrator file paths.
            outgoing_cal_list_lock: Thread-safe lock for accessing the outgoing_cal_list.
        """
        super().__init__(
            "VisCalOutgoingProcessor",
            [(visdata_outgoing_cal_path, ".fits")],
            mode=MODE_WATCH_DIR_FOR_RENAME,
            requeue_to_eoq_on_failure=False,
        )

        self.visdata_outgoing_cal_path = visdata_outgoing_cal_path
        self.outgoing_cal_list = outgoing_cal_list
        self.outgoing_cal_list_lock = outgoing_cal_list_lock

    def handler(self, item: str) -> bool:
        """Process a renamed .fits calibrator visibility file.

        Appends the file path to the shared outgoing calibrator list for consumption
        by the Calvin calibration pipeline.

        Args:
            item: Path to the renamed .fits file.

        Returns:
            True if the file was successfully added to the list.
        """
        logger.info(f"{item}: Started")

        with self.outgoing_cal_list_lock:
            self.outgoing_cal_list.append(item)

        logger.info(f"{item}: Finished")
        return True
