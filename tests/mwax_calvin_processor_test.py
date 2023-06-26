import os
from unittest.mock import patch, MagicMock

from mwax_mover.mwax_calvin_processor import MWAXCalvinProcessor
import tempfile

def upload_handler_helper(upload):
    # Set up MWAXCalvinProcessor instance
    processor = MWAXCalvinProcessor()
    # prevent upload handler from moving files somewhere else
    processor.complete_path = upload
    # Call upload_handler method with test path from mock_mwax_calvin_test03
    success = processor.upload_handler(upload)
    # Assert that the success flag is True
    assert success is True

def test_upload_handler_no_low_picket():
    testdir = os.path.dirname(os.path.abspath(__file__))
    upload = os.path.join(testdir, 'data/solution_handler_no_low_picket')
    upload_handler_helper(upload)

def test_upload_handler_lightning():
    testdir = os.path.dirname(os.path.abspath(__file__))
    upload = os.path.join(testdir, 'data/solution_handler_lightning')
    upload_handler_helper(upload)
