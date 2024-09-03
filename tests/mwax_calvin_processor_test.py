import logging
import os
import shutil
from mwax_mover.mwax_calvin_processor import MWAXCalvinProcessor
from mwax_mover.mwax_db import MWAXDBHandler
import tests_common
import pytest


@pytest.fixture
def setup_calvin_processor_tables():
    logger = logging.getLogger(__name__)

    # Configure a db handler
    db_handler = MWAXDBHandler(logger, "localhost", 5432, "mwax_mover_test", "postgres", "postgres")
    db_handler.start_database_pool()

    # Setup database tables
    tests_common.create_test_database(db_handler, "tests/mwax_calvin_processor.sql")

    return db_handler


def upload_handler_helper(setup_calvin_processor_tables, base_dir: str, obsid: int):
    # Base dir should be for example "tests/data/solution_handler_no_low_picket"
    # Obsid is the obsid being tested

    processing_dir = os.path.join(base_dir, str(obsid))

    # Set up MWAXCalvinProcessor instance
    processor = MWAXCalvinProcessor()
    # Configure a db handler
    processor.db_handler_object = setup_calvin_processor_tables

    processor.complete_path = os.path.join(base_dir, "complete")

    if not os.path.exists(processor.complete_path):
        os.mkdir(processor.complete_path)

    # Call upload_handler method with test path from mock_mwax_calvin_test03
    success = processor.upload_handler(processing_dir)
    # Assert that the success flag is True
    assert success is True
    # We need to move the files back from processing_dir/complete/date_time/. to processing_dir
    if not os.path.exists(processing_dir):
        complete_path = os.path.join(processor.complete_path, str(obsid))

        # Determine the dir name- it will be a date!
        dirs = os.listdir(complete_path)

        for d in dirs:
            if not os.path.isdir(os.path.join(complete_path, d)):
                dirs.remove(d)

        assert len(dirs) == 1
        dated_dir = os.path.join(complete_path, dirs[0])

        # It got moved! Move it back
        shutil.move(dated_dir, processing_dir)


def test_upload_handler_no_low_picket(setup_calvin_processor_tables):
    upload_handler_helper(
        setup_calvin_processor_tables,
        "tests/data/solution_handler_no_low_picket",
        1361707216,
    )


def test_upload_handler_lightning(setup_calvin_processor_tables):
    upload_handler_helper(setup_calvin_processor_tables, "tests/data/solution_handler_lightning", 1365977896)


def test_upload_handler_compact_hyda_picket(setup_calvin_processor_tables):
    upload_handler_helper(setup_calvin_processor_tables, "tests/data/solution_handler_hyda_picket3", 1369821496)


def test_upload_handler_1094488624_50(setup_calvin_processor_tables):
    upload_handler_helper(
        setup_calvin_processor_tables, "tests/data/solution_handler_1094488624.50-Tile073", 1094488624
    )
