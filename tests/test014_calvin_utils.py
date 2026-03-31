"""
Tests for the mwax_calvin_utils.py module
"""

import struct
import os
import math
import mwalib
import mwax_mover
import mwax_mover.mwax_calvin_utils
from mwax_mover.mwax_calvin_utils import (
    AOCAL_FILE_TYPE,
    AOCAL_HEADER_FORMAT,
    AOCAL_INTERVAL_COUNT,
    AOCAL_INTRO,
    AOCAL_POLS,
    AOCAL_STRUCTURE_TYPE,
    AOCAL_VALUES,
    get_solution_fits_filename,
)
import numpy as np
from tests_common import setup_test_directories


def test_estimate_birli_output_bytes():
    test_metafits = "tests/data/1244973688/1244973688_metafits.fits"

    metafits_context = mwalib.MetafitsContext(test_metafits, None)

    calc_bytes: float = mwax_mover.mwax_calvin_utils.estimate_birli_output_bytes(metafits_context, 40, 2.0)

    # Manually calculate the gigabytes
    # manual = timesteps * baselines * coarse_channels * fine_channels * pols * bytes_per_r_i (from Birli)
    manual_bytes: float = 60 * 8256 * 24 * 32 * 4 * 13

    assert calc_bytes == manual_bytes


def test_split_aocal_file_into_coarse_channels_24_per_file():
    in_filename = "tests/data/1451758560/1451758560_solutions.bin"
    base_dir = setup_test_directories("test0014")
    out_dir = os.path.join(base_dir, "data/calvin/out_jobs")

    chans = [
        109,
        110,
        111,
        112,
        113,
        114,
        115,
        116,
        117,
        118,
        119,
        120,
        121,
        122,
        123,
        124,
        125,
        126,
        127,
        128,
        129,
        130,
        131,
        132,
    ]

    out_files = mwax_mover.mwax_calvin_utils.split_aocal_file_into_coarse_channels(
        1451758560, in_filename, chans, out_dir
    )

    for c_idx, c in enumerate(chans):
        assert out_files[c_idx] == os.path.join(out_dir, f"1451758560_256_0032_{chans[c_idx]}_calfile.bin")


def test_split_aocal_file_into_coarse_channels_1_per_file():
    base_dir = setup_test_directories("test0014")
    out_dir = os.path.join(base_dir, "data/calvin/out_jobs")

    in_filenames = [
        "tests/data/1450212840/1450212840_ch101_solutions.bin",
        "tests/data/1450212840/1450212840_ch107_solutions.bin",
        "tests/data/1450212840/1450212840_ch113_solutions.bin",
        "tests/data/1450212840/1450212840_ch120_solutions.bin",
        "tests/data/1450212840/1450212840_ch127_solutions.bin",
        "tests/data/1450212840/1450212840_ch134_solutions.bin",
        "tests/data/1450212840/1450212840_ch142_solutions.bin",
        "tests/data/1450212840/1450212840_ch150_solutions.bin",
        "tests/data/1450212840/1450212840_ch158_solutions.bin",
        "tests/data/1450212840/1450212840_ch167_solutions.bin",
        "tests/data/1450212840/1450212840_ch177_solutions.bin",
        "tests/data/1450212840/1450212840_ch187_solutions.bin",
        "tests/data/1450212840/1450212840_ch210_solutions.bin",
        "tests/data/1450212840/1450212840_ch226_solutions.bin",
        "tests/data/1450212840/1450212840_ch58_solutions.bin",
        "tests/data/1450212840/1450212840_ch61_solutions.bin",
        "tests/data/1450212840/1450212840_ch65_solutions.bin",
        "tests/data/1450212840/1450212840_ch69_solutions.bin",
        "tests/data/1450212840/1450212840_ch73_solutions.bin",
        "tests/data/1450212840/1450212840_ch77_solutions.bin",
        "tests/data/1450212840/1450212840_ch81_solutions.bin",
        "tests/data/1450212840/1450212840_ch86_solutions.bin",
        "tests/data/1450212840/1450212840_ch91_solutions.bin",
        "tests/data/1450212840/1450212840_ch96_solutions.bin",
    ]

    chans = [
        58,
        61,
        65,
        69,
        73,
        77,
        81,
        86,
        91,
        96,
        101,
        107,
        113,
        120,
        127,
        134,
        142,
        150,
        158,
        167,
        177,
        187,
        210,
        226,
    ]

    all_files = []

    for f_idx, f in enumerate(in_filenames):
        out_files = mwax_mover.mwax_calvin_utils.split_aocal_file_into_coarse_channels(
            1450212840,
            f,
            [
                chans[f_idx],
            ],
            out_dir,
        )

        for of in out_files:
            all_files.append(of)

    for c_idx, c in enumerate(chans):
        assert os.path.join(out_dir, f"1450212840_256_0032_{chans[c_idx]:03}_calfile.bin") in all_files[c_idx]


def test_split_aocal_file_into_coarse_channels_data():
    base_dir = setup_test_directories("test0014")

    # In this test we ensure that actual data is good
    obs_id = 1234567890
    num_ants = 2
    num_fine_chans = 4
    coarse_chans = [
        109,
        110,
        111,
        112,
        113,
        114,
        115,
        116,
        117,
        118,
        119,
        120,
        121,
        122,
        123,
        124,
        125,
        126,
        127,
        128,
        129,
        130,
        131,
        132,
    ]
    num_coarse_chans = len(coarse_chans)
    start_time = 0
    end_time = 0

    out_dir = os.path.join(base_dir, "data/calvin/out_jobs")

    test_filename = os.path.join(out_dir, f"{obs_id}_aocal.bin")

    # Create a test aocal file containing 24 channels worth of data
    with open(test_filename, "wb") as out_file:
        out_file.write(
            struct.pack(
                AOCAL_HEADER_FORMAT,
                AOCAL_INTRO,
                AOCAL_FILE_TYPE,
                AOCAL_STRUCTURE_TYPE,
                AOCAL_INTERVAL_COUNT,
                num_ants,
                num_fine_chans * num_coarse_chans,
                AOCAL_POLS,
                start_time,
                end_time,
            )
        )

        # Write data
        data = np.empty((1, num_ants, num_coarse_chans * num_fine_chans, AOCAL_POLS, AOCAL_VALUES), dtype=np.float64)
        counter: float = 0
        for ant in range(0, num_ants):
            for f_chan in range(0, num_coarse_chans * num_fine_chans):
                for pol in range(0, AOCAL_POLS):
                    for value in range(0, AOCAL_VALUES):
                        data[0, ant, f_chan, pol, value] = counter
                        counter += 1

        # Write data out
        out_data = np.asarray(data[:, :, :, :, :], dtype="<f8")
        bytes_written = out_file.write(out_data.tobytes(order="C"))

        # each double is 8 bytes
        assert bytes_written / 8 == counter

    # Ok, so we've produced our 24 channel test file. Lets split it and check we get the right numbers!
    out_files = mwax_mover.mwax_calvin_utils.split_aocal_file_into_coarse_channels(
        obs_id, test_filename, coarse_chans, out_dir
    )

    assert out_files[0] == os.path.join(
        out_dir,
        mwax_mover.mwax_calvin_utils.get_aocal_filename(obs_id, num_ants, num_fine_chans, coarse_chans[0]),
    )

    # read and test each file
    for coarse_chan_idx, f in enumerate(out_files):
        with open(f, "rb") as in_file:
            # Read header
            header = in_file.read(struct.calcsize(AOCAL_HEADER_FORMAT))

            assert len(header) == 8 + (4 * 6) + (2 * 8)

            # unpack header
            (r_intro, r_ft, r_st, r_int, r_ants, r_fch, r_pols, r_startt, r_endt) = struct.unpack(
                AOCAL_HEADER_FORMAT, header
            )
            assert r_intro == AOCAL_INTRO
            assert r_ft == AOCAL_FILE_TYPE
            assert r_st == AOCAL_STRUCTURE_TYPE
            assert r_int == AOCAL_INTERVAL_COUNT
            assert r_ants == num_ants
            assert r_fch == num_fine_chans
            assert r_pols == AOCAL_POLS
            assert r_startt == start_time
            assert r_endt == end_time

            # Read data
            input_data = in_file.read()

            assert len(input_data) == num_ants * num_fine_chans * AOCAL_POLS * AOCAL_VALUES * 8

            np_data = np.frombuffer(input_data, dtype=np.float64)
            np_data = np.reshape(
                np_data,
                (
                    AOCAL_INTERVAL_COUNT,
                    num_ants,
                    num_fine_chans,
                    AOCAL_POLS,
                    AOCAL_VALUES,
                ),
            )

            # Check some known precalculated values
            # interval (0), ant (0-1), fchan (0-3), pol (0-3), value (0-1)
            if coarse_chan_idx == 0:
                assert np_data[0, 0, 0, 0, 0] == 0
                assert np_data[0, 0, 0, 0, 1] == 1
                assert np_data[0, 0, 0, 1, 0] == 2
                assert np_data[0, 0, 0, 1, 1] == 3
                assert np_data[0, 0, 1, 1, 1] == 11
                assert np_data[0, 1, 0, 0, 0] == 768
                assert np_data[0, 1, 0, 0, 1] == 769
                assert np_data[0, 1, 0, 1, 0] == 770
                assert np_data[0, 1, 0, 1, 1] == 771
                assert np_data[0, 1, 1, 1, 1] == 779

            if coarse_chan_idx == 1:
                assert np_data[0, 0, 0, 0, 0] == 32
                assert np_data[0, 0, 0, 0, 1] == 33
                assert np_data[0, 0, 0, 1, 0] == 34
                assert np_data[0, 0, 0, 1, 1] == 35
                assert np_data[0, 0, 1, 1, 1] == 43
                assert np_data[0, 1, 0, 0, 0] == 800
                assert np_data[0, 1, 0, 0, 1] == 801
                assert np_data[0, 1, 0, 1, 0] == 802
                assert np_data[0, 1, 0, 1, 1] == 803
                assert np_data[0, 1, 1, 1, 1] == 811

            counter = 0

            for ant in range(0, num_ants):
                for f_chan in range(0, num_fine_chans * num_coarse_chans):
                    this_coarse_chan = math.floor(f_chan / num_fine_chans)

                    for pol in range(0, AOCAL_POLS):
                        for value in range(0, AOCAL_VALUES):
                            # only check if we're looking at the correct coarse chan
                            if this_coarse_chan == coarse_chan_idx:
                                assert np_data[0, ant, f_chan % num_fine_chans, pol, value] == float(counter), (
                                    f"f: {f} this_coarse_chan {this_coarse_chan}, a:{ant}, f:{f_chan} % {num_fine_chans}: {f_chan % num_fine_chans}, p:{pol}, v:{value}; counter = {counter}"
                                )
                            counter += 1


def test_get_aocal_filename():
    # typical
    assert (
        mwax_mover.mwax_calvin_utils.get_aocal_filename(1234567890, 128, 768, 123)
        == "1234567890_128_0768_123_calfile.bin"
    )

    # single digit num_ants
    assert (
        mwax_mover.mwax_calvin_utils.get_aocal_filename(1234567890, 8, 768, 123)
        == "1234567890_008_0768_123_calfile.bin"
    )

    # single digit fine chans
    assert (
        mwax_mover.mwax_calvin_utils.get_aocal_filename(1234567890, 128, 4, 123)
        == "1234567890_128_0004_123_calfile.bin"
    )

    # single digit rec chan no
    assert (
        mwax_mover.mwax_calvin_utils.get_aocal_filename(1234567890, 128, 768, 9)
        == "1234567890_128_0768_009_calfile.bin"
    )

    # double digit rec chan no
    assert (
        mwax_mover.mwax_calvin_utils.get_aocal_filename(1234567890, 128, 768, 89)
        == "1234567890_128_0768_089_calfile.bin"
    )

    # double digit fine chans
    assert (
        mwax_mover.mwax_calvin_utils.get_aocal_filename(1234567890, 128, 64, 123)
        == "1234567890_128_0064_123_calfile.bin"
    )

    # four digit fine chans
    assert (
        mwax_mover.mwax_calvin_utils.get_aocal_filename(1234567890, 128, 6400, 123)
        == "1234567890_128_6400_123_calfile.bin"
    )

    # two digit num ants
    assert (
        mwax_mover.mwax_calvin_utils.get_aocal_filename(1234567890, 64, 6400, 123)
        == "1234567890_064_6400_123_calfile.bin"
    )


def test_get_solution_fits_filename_flavour1_all_channels(tmp_path):
    """Flavour 1: obsid_solutions.fits matches any rec_chan"""
    fits = tmp_path / "1234567890_solutions.fits"
    fits.touch()

    for chan in [1, 12, 24]:
        result = get_solution_fits_filename(str(tmp_path), 1234567890, chan)
        assert result == str(fits)


def test_get_solution_fits_filename_flavour2_single_channel(tmp_path):
    """Flavour 2: obsid_chN_solutions.fits matches exact channel"""
    fits = tmp_path / "1234567890_ch5_solutions.fits"
    fits.touch()

    result = get_solution_fits_filename(str(tmp_path), 1234567890, 5)
    assert result == str(fits)


def test_get_solution_fits_filename_flavour2_single_channel_no_match(tmp_path):
    """Flavour 2: obsid_chN_solutions.fits does not match a different channel"""
    fits = tmp_path / "1234567890_ch5_solutions.fits"
    fits.touch()

    result = get_solution_fits_filename(str(tmp_path), 1234567890, 6)
    assert result is None


def test_get_solution_fits_filename_flavour2_no_zero_padding(tmp_path):
    """Flavour 2: channel numbers are not zero-padded"""
    fits = tmp_path / "1234567890_ch007_solutions.fits"
    fits.touch()

    # ch007 should NOT match rec_chan=7 (not zero-padded)
    result = get_solution_fits_filename(str(tmp_path), 1234567890, 7)
    assert result is None


def test_get_solution_fits_filename_flavour3_range_channel_within(tmp_path):
    """Flavour 3: rec_chan within range [N, M] matches"""
    fits = tmp_path / "1234567890_ch1-12_solutions.fits"
    fits.touch()

    for chan in [1, 6, 12]:
        result = get_solution_fits_filename(str(tmp_path), 1234567890, chan)
        assert result == str(fits)


def test_get_solution_fits_filename_flavour3_range_channel_outside(tmp_path):
    """Flavour 3: rec_chan outside range [N, M] does not match"""
    fits = tmp_path / "1234567890_ch1-12_solutions.fits"
    fits.touch()

    for chan in [0, 13, 24]:
        result = get_solution_fits_filename(str(tmp_path), 1234567890, chan)
        assert result is None


def test_get_solution_fits_filename_flavour3_range_boundary_values(tmp_path):
    """Flavour 3: rec_chan at exact boundaries matches"""
    fits = tmp_path / "1234567890_ch5-10_solutions.fits"
    fits.touch()

    assert get_solution_fits_filename(str(tmp_path), 1234567890, 5) == str(fits)
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 10) == str(fits)
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 4) is None
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 11) is None


def test_get_solution_fits_filename_flavour3_multi_digit_channels(tmp_path):
    """Flavour 3: multi-digit channel numbers (e.g. ch13-24) work correctly"""
    fits = tmp_path / "1234567890_ch13-24_solutions.fits"
    fits.touch()

    assert get_solution_fits_filename(str(tmp_path), 1234567890, 13) == str(fits)
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 24) == str(fits)
    assert get_solution_fits_filename(str(tmp_path), 1234567890, 12) is None


def test_get_solution_fits_filename_flavour1_takes_priority_over_flavour2(tmp_path):
    """Flavour 1 is returned first if both flavour 1 and 2 exist"""
    f1 = tmp_path / "1234567890_solutions.fits"
    f1.touch()
    f2 = tmp_path / "1234567890_ch5_solutions.fits"
    f2.touch()

    result = get_solution_fits_filename(str(tmp_path), 1234567890, 5)
    assert result == str(f1)


def test_get_solution_fits_filename_flavour1_takes_priority_over_flavour3(tmp_path):
    """Flavour 1 is returned first if both flavour 1 and 3 exist"""
    f1 = tmp_path / "1234567890_solutions.fits"
    f1.touch()
    f3 = tmp_path / "1234567890_ch1-12_solutions.fits"
    f3.touch()

    result = get_solution_fits_filename(str(tmp_path), 1234567890, 6)
    assert result == str(f1)


def test_get_solution_fits_filename_no_files_returns_none(tmp_path):
    """Empty directory returns None"""
    result = get_solution_fits_filename(str(tmp_path), 1234567890, 5)
    assert result is None


def test_get_solution_fits_filename_wrong_obsid_ignored(tmp_path):
    """Files for a different obs_id are not matched"""
    fits = tmp_path / "9999999999_solutions.fits"
    fits.touch()

    result = get_solution_fits_filename(str(tmp_path), 1234567890, 1)
    assert result is None


def test_get_solution_fits_filename_directory_does_not_exist():
    """Non-existent directory returns None without raising"""
    result = get_solution_fits_filename("/nonexistent/path", 1234567890, 5)
    assert result is None
