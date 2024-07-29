import logging
import os
from mwax_mover.mwax_calvin_processor import MWAXCalvinProcessor
from mwax_mover.mwax_db import MWAXDBHandler, DUMMY_DB


def upload_handler_helper(upload):
    # Setup logger
    logger = logging.getLogger(__name__)
    # Set up MWAXCalvinProcessor instance
    processor = MWAXCalvinProcessor()
    # Configure a dummy db handler
    db_handler = MWAXDBHandler(logger, DUMMY_DB, 5432, DUMMY_DB, DUMMY_DB, DUMMY_DB)
    processor.db_handler_object = db_handler
    # prevent upload handler from moving files somewhere else
    processor.complete_path = upload
    # Call upload_handler method with test path from mock_mwax_calvin_test03
    success = processor.upload_handler(upload)
    # Assert that the success flag is True
    assert success is True


def test_upload_handler_no_low_picket():
    testdir = os.path.dirname(os.path.abspath(__file__))
    upload = os.path.join(testdir, "data/solution_handler_no_low_picket/1361707216")
    upload_handler_helper(upload)


def test_upload_handler_lightning():
    testdir = os.path.dirname(os.path.abspath(__file__))
    upload = os.path.join(testdir, "data/solution_handler_lightning/1365977896")
    upload_handler_helper(upload)


def test_upload_handler_compact_hyda_picket():
    testdir = os.path.dirname(os.path.abspath(__file__))
    upload = os.path.join(testdir, "data/solution_handler_hyda_picket3/1369821496")
    upload_handler_helper(upload)


def test_upload_handler_1094488624_50():
    testdir = os.path.dirname(os.path.abspath(__file__))
    upload = os.path.join(testdir, "data/solution_handler_1094488624.50-Tile073/1094488624")
    upload_handler_helper(upload)
