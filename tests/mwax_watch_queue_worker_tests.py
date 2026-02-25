from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker, MWAXPriorityWatchQueueWorker
import time
import logging
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW
import pytest


logger = logging.getLogger(__name__)


@pytest.fixture
def debug_logger():
    """Returns a logger configured to DEBUG level for use in tests."""
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    return logging.getLogger("myapp")


class MyWatchQueueWorker(MWAXWatchQueueWorker):
    def handler(self, item: str) -> bool:
        self.logger.info(f"Handling item: {item}")
        return True


def test_wqw(debug_logger):
    wqw = MyWatchQueueWorker(
        "test_wqw",
        debug_logger,
        [
            ("/tmp", ".*"),
        ],
        MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
    )

    wqw.start()

    time.sleep(15)

    wqw.stop()


class MyPriorityWatchQueueWorker(MWAXPriorityWatchQueueWorker):
    def handler(self, item: str) -> bool:
        self.logger.info(f"Handling item: {item}")
        return True


def test_priority_wqw(debug_logger):
    metafits_path = "tests/test_data/metafits_ppd/1328239120.metafits"
    wqw = MyPriorityWatchQueueWorker(
        "test_priority_wqw",
        debug_logger,
        metafits_path,
        [
            ("/tmp", ".fits"),
        ],
        MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
        [],
        [],
    )

    wqw.start()

    time.sleep(15)

    wqw.stop()
