import shutil
from mwax_mover.mwax_bf_vdif_utils import stitch_vdif_files_and_write_hdr
from tests_common import setup_test_directories
import pytest
import os


def test_stitch_zero_files():
    output_dir = setup_test_directories("test006")

    filenames = []
    metafits_filename = ""

    with pytest.raises(Exception):
        _, _ = stitch_vdif_files_and_write_hdr(metafits_filename, filenames, output_dir)


def test_stitch_one_file():
    output_dir = setup_test_directories("test006")

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

    assert output_vdif_filename == os.path.join(output_dir, "1454343736_ch109_beam00.vdif")
    assert output_hdr_filename == os.path.join(output_dir, "1454343736_ch109_beam00.hdr")

    assert os.path.exists(output_vdif_filename)
    assert os.path.exists(output_hdr_filename)


def test_stitch_many_files2():
    output_dir = setup_test_directories("test006")

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

    assert output_vdif_filename == os.path.join(output_dir, f"{obs_id}_ch109_beam01.vdif")
    assert output_hdr_filename == os.path.join(output_dir, f"{obs_id}_ch109_beam01.hdr")

    assert os.path.exists(output_vdif_filename)
    assert os.path.exists(output_hdr_filename)
    file_size = 0
    for f in filenames:
        file_size += os.path.getsize(f)

    assert os.path.getsize(output_vdif_filename) == file_size


def test_stitch_with_target_name_sra_sdec():
    output_dir = setup_test_directories("test006")
    #
    # Note to save space in github we will use an existing vdif file in the repo (not the real one)
    #
    metafits_filename = "tests/data/1457904016/1457904016_metafits.fits"
    original_vdif_filename1 = "tests/data/1454343736/1454343736_1454343736_ch109_beam01.vdif"
    original_vdif_filename2 = "tests/data/1454343736/1454343736_1454343744_ch109_beam01.vdif"

    new_vdif_filename1 = os.path.join(output_dir, "1457904016_1457904016_ch120_beam01.vdif")
    new_vdif_filename2 = os.path.join(output_dir, "1457904016_1457904024_ch120_beam01.vdif")

    # copy the data files into test dir and rename so the code sees the correct filename
    shutil.copyfile(original_vdif_filename1, new_vdif_filename1)
    shutil.copyfile(original_vdif_filename2, new_vdif_filename2)

    # Stitch
    filenames = [new_vdif_filename1, new_vdif_filename2]
    output_vdif_filename = ""
    output_hdr_filename = ""

    output_vdif_filename, output_hdr_filename = stitch_vdif_files_and_write_hdr(
        metafits_filename, filenames, output_dir
    )

    assert os.path.exists(output_hdr_filename)
    assert os.path.exists(output_vdif_filename)

    # Now check for target name
    with open(output_hdr_filename) as hdr:
        lines = hdr.readlines()

        # First pass get the keys
        for line in lines:
            if line.startswith("SOURCE"):
                source_line = line
            if line.startswith("RA"):
                ra_line = line
            if line.startswith("DEC"):
                dec_line = line

    assert source_line != ""
    assert ra_line != ""
    assert dec_line != ""

    # Now we check the contents
    assert source_line == "SOURCE       PSR J1453-6413 # name of the astronomical source\n", f"{source_line}"
    assert ra_line == "RA           223.38606     # Right Ascension of the source\n", f"{ra_line}"
    assert dec_line == "DEC          -64.22112    # Declination of the source\n", f"{dec_line}"
