#
# Tests for the watch_queue_worker abstract base class (ABC)
#

from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker, MWAXPriorityWatchQueueWorker
import time
import logging
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW

logger = logging.getLogger(__name__)


class MyWatchQueueWorker(MWAXWatchQueueWorker):
    def handler(self, item: str) -> bool:
        logger.info(f"Handling item: {item}")
        return True


def test_wqw():
    wqw = MyWatchQueueWorker(
        "test_wqw",
        [
            ("/tmp", ".*"),
        ],
        MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
    )

    wqw.start()

    time.sleep(4)

    wqw.stop()


class MyPriorityWatchQueueWorker(MWAXPriorityWatchQueueWorker):
    def handler(self, item: str) -> bool:
        logger.info(f"Handling item: {item}")
        return True


def test_priority_wqw():
    metafits_path = "tests/test_data/1328239120/1328239120.metafits"
    wqw = MyPriorityWatchQueueWorker(
        "test_priority_wqw",
        metafits_path,
        [
            ("/tmp", ".fits"),
        ],
        MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
        [],
        [],
    )

    wqw.start()

    time.sleep(4)

    wqw.stop()
