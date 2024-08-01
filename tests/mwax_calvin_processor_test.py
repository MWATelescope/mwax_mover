import logging
import os
import shutil
from mwax_mover.mwax_calvin_processor import MWAXCalvinProcessor
from mwax_mover.mwax_db import MWAXDBHandler, DUMMY_DB


def upload_handler_helper(base_dir: str, obsid: int):
    # Base dir should be for example "tests/data/solution_handler_no_low_picket"
    # Obsid is the obsid being tested

    processing_dir = os.path.join(base_dir, str(obsid))

    # Setup logger
    logger = logging.getLogger(__name__)
    # Set up MWAXCalvinProcessor instance
    processor = MWAXCalvinProcessor()
    # Configure a dummy db handler
    db_handler = MWAXDBHandler(logger, DUMMY_DB, 5432, DUMMY_DB, DUMMY_DB, DUMMY_DB)
    processor.db_handler_object = db_handler
    processor.complete_path = os.path.join(base_dir, "complete")

    # Call upload_handler method with test path from mock_mwax_calvin_test03
    success = processor.upload_handler(processing_dir)
    # Assert that the success flag is True
    assert success is True
    # We need to move the files back (maybe!)
    if not os.path.exists(processing_dir):
        # It got moved! Move it back
        shutil.move(os.path.join(processor.complete_path, str(obsid)), processing_dir)


def test_upload_handler_no_low_picket():
    upload_handler_helper(
        "tests/data/solution_handler_no_low_picket",
        1361707216,
    )


def test_upload_handler_lightning():
    upload_handler_helper("tests/data/solution_handler_lightning", 1365977896)


def test_upload_handler_compact_hyda_picket():
    upload_handler_helper("tests/data/solution_handler_hyda_picket3", 1369821496)


def test_upload_handler_1094488624_50():
    upload_handler_helper("tests/data/solution_handler_1094488624.50-Tile073", 1094488624)
