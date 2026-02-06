import logging
from mwax_mover.mwax_bf_vdif_utils import stitch_vdif_files_and_write_hdr
import pytest
import os


def test_stitch_zero_files():
    logger = logging.getLogger()
    filenames = []
    metafits_filename = ""
    output_vdif_filename = ""
    output_hdr_filename = ""

    with pytest.raises(Exception):
        output_vdif_filename, output_hdr_filename = stitch_vdif_files_and_write_hdr(
            logger, metafits_filename, filenames
        )


def test_stitch_one_file():
    logger = logging.getLogger()
    filenames = [
        "tests/data/vdif/1454361952_1454361952_ch109_beam00.vdif",
    ]

    metafits_filename = "tests/data/vdif/1454361952_metafits.fits"
    output_vdif_filename = ""
    output_hdr_filename = ""

    output_vdif_filename, output_hdr_filename = stitch_vdif_files_and_write_hdr(logger, metafits_filename, filenames)

    assert output_vdif_filename == "tests/data/vdif/1454361952_ch109_beam00.vdif"
    assert output_hdr_filename == "tests/data/vdif/1454361952_ch109_beam00.hdr"

    assert os.path.exists(output_vdif_filename)
    assert os.path.exists(output_hdr_filename)


def test_stitch_many_files():
    logger = logging.getLogger()
    filenames = [
        "tests/data/vdif/1454361952_1454361952_ch109_beam00.vdif",
        "tests/data/vdif/1454361952_1454361960_ch109_beam00.vdif",
    ]
    metafits_filename = "tests/data/vdif/1454361952_metafits.fits"
    output_vdif_filename = ""
    output_hdr_filename = ""

    output_vdif_filename, output_hdr_filename = stitch_vdif_files_and_write_hdr(logger, metafits_filename, filenames)

    assert output_vdif_filename == "tests/data/vdif/1454361952_ch109_beam00.vdif"
    assert output_hdr_filename == "tests/data/vdif/1454361952_ch109_beam00.hdr"

    assert os.path.exists(output_vdif_filename)
    assert os.path.exists(output_hdr_filename)
