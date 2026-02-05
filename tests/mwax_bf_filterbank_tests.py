import logging
from mwax_mover.mwax_bf_filterbank_utils import stitch_filterbank_files, get_stitched_filename
import pytest
import os


def test_get_stitched_filename():
    filename = "/test/1234567890_1234567898_ch123_beam00.fil"
    assert "/test/1234567890_ch123_beam00.fil" == get_stitched_filename(filename)

    filename = "/test/test2/1234567890_1234567898_ch123_beam10.fil"
    assert "/test/test2/1234567890_ch123_beam10.fil" == get_stitched_filename(filename)

    filename = "/test/test2/1234567890_1234567898_ch001_beam01.fil"
    assert "/test/test2/1234567890_ch001_beam01.fil" == get_stitched_filename(filename)


def test_stitch_zero_files():
    logger = logging.getLogger()
    filenames = []

    with pytest.raises(Exception):
        stitch_filterbank_files(logger, filenames)


def test_stitch_one_file():
    logger = logging.getLogger()
    filenames = [
        "tests/data/filterbank/1451758560_1451758560_ch109_beam00.fil",
    ]

    output_filterbank_filename = stitch_filterbank_files(logger, filenames)

    assert output_filterbank_filename == "tests/data/filterbank/1451758560_ch109_beam00.fil"

    assert os.path.exists(output_filterbank_filename)


def test_stitch_many_files():
    logger = logging.getLogger()
    filenames = [
        "tests/data/filterbank/1451758560_1451758560_ch109_beam00.fil",
        "tests/data/filterbank/1451758560_1451758568_ch109_beam00.fil",
    ]

    output_filterbank_filename = stitch_filterbank_files(logger, filenames)

    assert output_filterbank_filename == "tests/data/filterbank/1451758560_ch109_beam00.fil"

    assert os.path.exists(output_filterbank_filename)
