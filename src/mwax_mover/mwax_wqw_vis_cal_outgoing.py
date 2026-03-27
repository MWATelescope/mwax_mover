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
        logger.info(f"{item}- Started")

        with self.outgoing_cal_list_lock:
            self.outgoing_cal_list.append(item)

        logger.info(f"{item}- Finished")
        return True
