#
# Tests for the watch_queue_worker abstract base class (ABC)
#

import os
import shutil

from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker, MWAXPriorityWatchQueueWorker
import time
import logging
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW
from tests_common import setup_test_directories

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
    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1369821488_metafits.fits"))
    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1369821496_metafits.fits"))

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
    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1451758568_metafits.fits"))
    shutil.copyfile(metafits_path, os.path.join(incoming_dir, "1451758576_metafits.fits"))

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
