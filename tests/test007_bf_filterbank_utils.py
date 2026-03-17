from mwax_mover.mwax_bf_filterbank_utils import (
    stitch_filterbank_files,
    get_stitched_filename,
    get_filterbank_components,
)
import pytest
import os

output_dir = "tests/data/test007"


def test_get_stitched_filename():
    filename = "/test/1234567890_1234567898_ch123_beam00.fil"
    assert "/test/1234567890_ch123_beam00.fil" == get_stitched_filename(filename)

    filename = "/test/test2/1234567890_1234567898_ch123_beam10.fil"
    assert "/test/test2/1234567890_ch123_beam10.fil" == get_stitched_filename(filename)

    filename = "/test/test2/1234567890_1234567898_ch001_beam01.fil"
    assert "/test/test2/1234567890_ch001_beam01.fil" == get_stitched_filename(filename)


def test_stitch_zero_files():

    filenames = []

    with pytest.raises(Exception):
        stitch_filterbank_files(filenames, output_dir)


def test_stitch_one_file():

    filenames = [
        "tests/data/1451758560/1451758560_1451758560_ch109_beam00.fil",
    ]

    output_filterbank_filename = stitch_filterbank_files(filenames, output_dir)

    assert output_filterbank_filename == "tests/data/test007/1451758560_ch109_beam00.fil"

    assert os.path.exists(output_filterbank_filename)


def test_stitch_many_files():

    filenames = [
        "tests/data/1451758560/1451758560_1451758560_ch109_beam00.fil",
        "tests/data/1451758560/1451758560_1451758568_ch109_beam00.fil",
    ]

    _, data1_start_index = get_filterbank_components(filenames[0])
    file1_datalen = os.path.getsize(filenames[0]) - data1_start_index

    _, data2_start_index = get_filterbank_components(filenames[1])
    file2_datalen = os.path.getsize(filenames[1]) - data2_start_index

    output_filterbank_filename = stitch_filterbank_files(filenames, output_dir)

    assert output_filterbank_filename == "tests/data/test007/1451758560_ch109_beam00.fil"
    assert os.path.exists(output_filterbank_filename)

    # Get new data len- check it is the same as 1+2
    _, data3_start_index = get_filterbank_components(output_filterbank_filename)
    file3_datalen = os.path.getsize(output_filterbank_filename) - data3_start_index
    assert file3_datalen == file1_datalen + file2_datalen
