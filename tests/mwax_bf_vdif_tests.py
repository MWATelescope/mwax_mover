import logging
from mwax_mover.mwax_bf_vdif_utils import stitch_vdif_files_and_write_hdr
import pytest


def test_stitch_zero_files():
    logger = logging.getLogger()
    filenames = []
    metafits_filename = ""
    output_vdif_filename = ""
    output_hdr_filename = ""

    with pytest.raises(Exception):
        stitch_vdif_files_and_write_hdr(logger, metafits_filename, filenames, output_vdif_filename, output_hdr_filename)
