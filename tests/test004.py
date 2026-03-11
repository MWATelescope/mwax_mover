#
# Tests for the watch_queue_worker abstract base class (ABC)
#
from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker, MWAXPriorityWatchQueueWorker
import time
import logging
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW


class MyWatchQueueWorker(MWAXWatchQueueWorker):
    def handler(self, item: str) -> bool:
        self.logger.info(f"Handling item: {item}")
        return True


def test_wqw():
    logger = logging.getLogger(__name__)
    logger.level = logging.DEBUG

    wqw = MyWatchQueueWorker(
        "test_wqw",
        logger,
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
        self.logger.info(f"Handling item: {item}")
        return True


def test_priority_wqw():
    logger = logging.getLogger(__name__)
    logger.level = logging.DEBUG

    metafits_path = "tests/test_data/metafits_ppd/1328239120.metafits"
    wqw = MyPriorityWatchQueueWorker(
        "test_priority_wqw",
        logger,
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
