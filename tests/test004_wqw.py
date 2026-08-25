#
# Tests for the watch_queue_worker abstract base class (ABC)
#

import logging
import os
import queue
import shutil
import time

from tests_common import setup_test_directories

from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW
from mwax_mover.mwax_queue_worker import QueueWorker, calculate_backoff_seconds
from mwax_mover.mwax_watch_queue_worker import (
    MWAXPriorityWatchQueueWorker,
    MWAXWatchQueueWorker,
)

# Setup root logger
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"))
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)


class MyWatchQueueWorker(MWAXWatchQueueWorker):
    def handler(self, item) -> bool:
        logger.info(f"Handling item: {item}")
        return True


def test_wqw():
    base_dir = setup_test_directories("test004")

    metafits_path = "tests/data/1369821496/1369821496_metafits.fits"
    assert os.path.exists(metafits_path)

    incoming_dir = os.path.join(base_dir, "visdata/incoming")

    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1369821482_metafits.fits"))
    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1369821616_metafits.fits"))
    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1369821736_metafits.fits"))

    wqw = MyWatchQueueWorker(
        "test_wqw",
        [
            (incoming_dir, ".fits"),
        ],
        MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
    )

    wqw.start()

    time.sleep(4)

    wqw.stop()


class MyPriorityWatchQueueWorker(MWAXPriorityWatchQueueWorker):
    def handler(self, item) -> bool:
        logger.info(f"Handling item: {item}")
        return True


def test_priority_wqw():
    base_dir = setup_test_directories("test004")

    metafits_path = "tests/data/1451758560/1451758560_metafits.fits"
    assert os.path.exists(metafits_path)

    incoming_dir = os.path.join(base_dir, "visdata/incoming")

    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1451758560_metafits.fits"))
    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1451768432_metafits.fits"))
    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1451768488_metafits.fits"))

    wqw = MyPriorityWatchQueueWorker(
        "test_priority_wqw",
        metafits_path,
        [
            (incoming_dir, ".fits"),
        ],
        MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
        [],
        [],
    )

    wqw.start()

    time.sleep(4)

    wqw.stop()


#
# Backoff calculation
#


def test_calculate_backoff_seconds_is_exponential():
    """Backoff must grow geometrically, not linearly.

    Regression test: this used to be computed as
    initial * factor * consecutive_error_count, giving 2, 4, 6, 8, ... which is
    linear despite every docstring describing it as exponential.
    """
    delays = [calculate_backoff_seconds(n, 1, 2, 60) for n in range(1, 9)]

    assert delays == [1, 2, 4, 8, 16, 32, 60, 60]
    # The old linear formula would have produced this instead
    assert delays != [2, 4, 6, 8, 10, 12, 14, 16]


def test_calculate_backoff_seconds_respects_limit_and_zero():
    """The delay is capped, and no failures means no wait."""
    assert calculate_backoff_seconds(0, 1, 2, 60) == 0
    assert calculate_backoff_seconds(-1, 1, 2, 60) == 0
    assert calculate_backoff_seconds(20, 1, 2, 60) == 60
    assert calculate_backoff_seconds(1, 5, 3, 1000) == 5


def test_queue_worker_start_clears_backoff_event(tmp_path):
    """start() must reset the event stop() sets.

    Regression test: nothing ever cleared this event, so after the first stop()
    every subsequent event.wait(backoff) returned immediately and backoff was
    silently disabled for the rest of the process's life.
    """
    tmp_file = tmp_path / "item.dat"
    tmp_file.write_text("x")

    worker = QueueWorker(
        name="test_event_reset",
        source_queue=queue.Queue(),
        executable_path=None,
        event_handler=lambda _item: True,
        exit_once_queue_empty=True,
    )

    worker.stop()
    assert worker.event.is_set(), "stop() should set the event to break an in-flight wait"

    # Observe the event's state from inside the processing loop, which is where
    # a backoff wait would actually happen. Without the clear() in start(), the
    # event is still set here and event.wait(backoff) would return instantly.
    observed = []

    def handler(_item):
        observed.append(worker.event.is_set())
        return True

    worker._event_handler = handler
    worker.source_queue.put(str(tmp_file))
    worker.start()

    assert observed == [False], f"event should be cleared by start(), saw is_set()={observed}"
