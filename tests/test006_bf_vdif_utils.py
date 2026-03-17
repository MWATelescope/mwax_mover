from mwax_mover.mwax_bf_vdif_utils import stitch_vdif_files_and_write_hdr
import pytest
import os

output_dir = "tests/data/test006"


def test_stitch_zero_files():

    filenames = []
    metafits_filename = ""

    with pytest.raises(Exception):
        _, _ = stitch_vdif_files_and_write_hdr(metafits_filename, filenames, output_dir)


def test_stitch_one_file():

    filenames = [
        "tests/data/1454343736/1454343736_1454343736_ch109_beam00.vdif",
    ]

    obs_id = os.path.basename(filenames[0])[0:10]

    metafits_filename = f"tests/data/{obs_id}/{obs_id}_metafits.fits"
    output_vdif_filename = ""
    output_hdr_filename = ""

    output_vdif_filename, output_hdr_filename = stitch_vdif_files_and_write_hdr(
        metafits_filename, filenames, output_dir
    )

    assert output_vdif_filename == f"{output_dir}/1454343736_ch109_beam00.vdif"
    assert output_hdr_filename == f"{output_dir}/1454343736_ch109_beam00.hdr"

    assert os.path.exists(output_vdif_filename)
    assert os.path.exists(output_hdr_filename)


def test_stitch_many_files2():

    filenames = [
        "tests/data/1454343736/1454343736_1454343736_ch109_beam01.vdif",
        "tests/data/1454343736/1454343736_1454343744_ch109_beam01.vdif",
    ]

    obs_id = os.path.basename(filenames[0])[0:10]

    metafits_filename = f"tests/data/{obs_id}/{obs_id}_metafits.fits"
    output_vdif_filename = ""
    output_hdr_filename = ""

    output_vdif_filename, output_hdr_filename = stitch_vdif_files_and_write_hdr(
        metafits_filename, filenames, output_dir
    )

    assert output_vdif_filename == f"{output_dir}/{obs_id}_ch109_beam01.vdif"
    assert output_hdr_filename == f"{output_dir}/{obs_id}_ch109_beam01.hdr"

    assert os.path.exists(output_vdif_filename)
    assert os.path.exists(output_hdr_filename)
    file_size = 0
    for f in filenames:
        file_size += os.path.getsize(f)

    assert os.path.getsize(output_vdif_filename) == file_size
