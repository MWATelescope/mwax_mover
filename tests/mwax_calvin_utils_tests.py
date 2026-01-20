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
)
import numpy as np


def test_estimate_birli_output_GB():
    test_metafits = "tests/data/correlator_C001/1244973688_metafits.fits"

    metafits_context = mwalib.MetafitsContext(test_metafits, None)

    calc_bytes: float = mwax_mover.mwax_calvin_utils.estimate_birli_output_bytes(metafits_context, 40, 2.0)

    # Manually calculate the gigabytes
    # manual = baselines * coarse_channels * fine_channels * pols * values * bytes_per_value * timesteps
    manual_bytes: float = 8256 * 24 * 32 * 4 * 2 * 4 * 60

    assert calc_bytes == manual_bytes


def test_split_aocal_file_into_coarse_channels_24_per_file():
    in_filename = "tests/data/aocal_split/1451758560_solutions.bin"

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
        1451758560,
        in_filename,
        chans,
    )

    for c_idx, c in enumerate(chans):
        assert out_files[c_idx] == f"tests/data/aocal_split/1451758560_ch{chans[c_idx]}_aocal.bin"


def test_split_aocal_file_into_coarse_channels_1_per_file():
    in_filenames = [
        "tests/data/aocal_split/1450212840_ch101_solutions.bin",
        "tests/data/aocal_split/1450212840_ch107_solutions.bin",
        "tests/data/aocal_split/1450212840_ch113_solutions.bin",
        "tests/data/aocal_split/1450212840_ch120_solutions.bin",
        "tests/data/aocal_split/1450212840_ch127_solutions.bin",
        "tests/data/aocal_split/1450212840_ch134_solutions.bin",
        "tests/data/aocal_split/1450212840_ch142_solutions.bin",
        "tests/data/aocal_split/1450212840_ch150_solutions.bin",
        "tests/data/aocal_split/1450212840_ch158_solutions.bin",
        "tests/data/aocal_split/1450212840_ch167_solutions.bin",
        "tests/data/aocal_split/1450212840_ch177_solutions.bin",
        "tests/data/aocal_split/1450212840_ch187_solutions.bin",
        "tests/data/aocal_split/1450212840_ch210_solutions.bin",
        "tests/data/aocal_split/1450212840_ch226_solutions.bin",
        "tests/data/aocal_split/1450212840_ch58_solutions.bin",
        "tests/data/aocal_split/1450212840_ch61_solutions.bin",
        "tests/data/aocal_split/1450212840_ch65_solutions.bin",
        "tests/data/aocal_split/1450212840_ch69_solutions.bin",
        "tests/data/aocal_split/1450212840_ch73_solutions.bin",
        "tests/data/aocal_split/1450212840_ch77_solutions.bin",
        "tests/data/aocal_split/1450212840_ch81_solutions.bin",
        "tests/data/aocal_split/1450212840_ch86_solutions.bin",
        "tests/data/aocal_split/1450212840_ch91_solutions.bin",
        "tests/data/aocal_split/1450212840_ch96_solutions.bin",
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
        )

        for of in out_files:
            all_files.append(of)

    for c_idx, c in enumerate(chans):
        assert f"tests/data/aocal_split/1450212840_ch{chans[c_idx]}_aocal.bin" in all_files[c_idx]


def test_split_aocal_file_into_coarse_channels_data():
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
    test_filename = os.path.join("tests/data/aocal_split", f"{obs_id}_aocal.bin")

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
        obs_id,
        test_filename,
        coarse_chans,
    )

    assert out_files[0] == os.path.join(
        "tests/data/aocal_split/",
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


def test_fit_gains_good1_no_nan():
    start_freq_hz = 100000000.0  # 100 MHz
    n_coarse = 2
    n_fch = 4
    fch_width_hz = 40000.0  # 40 kHz
    chan_array = np.array(np.arange(start_freq_hz, start_freq_hz + (n_coarse * n_fch * fch_width_hz), fch_width_hz))
    print(f"chan_array: {chan_array}")
    assert len(chan_array) == 8

    fit_gains = mwax_mover.mwax_calvin_utils.fit_gain(
        # 100 kHz
        chanblocks_hz=chan_array,
        solns=np.array([1.4, 1.3, 1.2, 1.1, 1.0, 0.9, 0.8, 0.7]),
        weights=np.array([1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
        chanblocks_per_coarse=n_fch,
    )

    assert len(fit_gains.gains) == n_coarse
    assert len(fit_gains.pol0) == n_coarse
    assert len(fit_gains.pol1) == n_coarse
    assert len(fit_gains.sigma_resid) == n_coarse

    # cc 1
    assert np.isclose(fit_gains.gains[0], 0.80649)
    assert np.isclose(fit_gains.pol0[0], -161.420330)  # intercept
    assert np.isclose(fit_gains.pol1[0], 0.00000162)  # slope
    assert np.isclose(fit_gains.sigma_resid[0], 0.005210617)

    # cc 2
    assert np.isclose(fit_gains.gains[1], 1.19742)
    assert np.isclose(fit_gains.pol0[1], -355.736905)  # intercept
    assert np.isclose(fit_gains.pol1[1], 0.00000356)  # slope
    assert np.isclose(fit_gains.sigma_resid[1], 0.016917519)

    # overall
    assert np.isclose(fit_gains.quality, 1.0)


def test_fit_gains_good2_some_nan():
    start_freq_hz = 100000000.0  # 100 MHz
    n_coarse = 2
    n_fch = 4
    fch_width_hz = 40000.0  # 40 kHz
    chan_array = np.array(np.arange(start_freq_hz, start_freq_hz + (n_coarse * n_fch * fch_width_hz), fch_width_hz))
    print(f"chan_array: {chan_array}")
    assert len(chan_array) == 8

    fit_gains = mwax_mover.mwax_calvin_utils.fit_gain(
        # 100 kHz
        chanblocks_hz=chan_array,
        solns=np.array([1.4, 1.3, 1.2, np.nan, 1.0, 0.9, 0.8, np.nan]),
        weights=np.array([1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
        chanblocks_per_coarse=n_fch,
    )

    assert len(fit_gains.gains) == n_coarse
    assert len(fit_gains.pol0) == n_coarse
    assert len(fit_gains.pol1) == n_coarse
    assert len(fit_gains.sigma_resid) == n_coarse

    # cc 1
    assert np.isclose(fit_gains.gains[0], 0.77228)
    assert np.isclose(fit_gains.pol0[0], -148.096764)  # intercept
    assert np.isclose(fit_gains.pol1[0], 0.00000149)  # slope
    assert np.isclose(fit_gains.sigma_resid[0], 0.002158446)

    # cc 2
    assert np.isclose(fit_gains.gains[1], 1.12037)
    assert np.isclose(fit_gains.pol0[1], -312.004630)  # intercept
    assert np.isclose(fit_gains.pol1[1], 0.00000313)  # slope
    assert np.isclose(fit_gains.sigma_resid[1], 0.006547285)

    # overall
    assert np.isclose(fit_gains.quality, 1)


def test_fit_gains_good3_outlier():
    start_freq_hz = 100000000.0  # 100 MHz
    n_coarse = 2
    n_fch = 4
    fch_width_hz = 40000.0  # 40 kHz
    chan_array = np.array(np.arange(start_freq_hz, start_freq_hz + (n_coarse * n_fch * fch_width_hz), fch_width_hz))
    print(f"chan_array: {chan_array}")
    assert len(chan_array) == 8

    fit_gains = mwax_mover.mwax_calvin_utils.fit_gain(
        # 100 kHz
        chanblocks_hz=chan_array,
        solns=np.array([1.4, 1.3, 1.2, 9, 1.0, 0.9, 0.8, 9]),
        weights=np.array([1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0]),
        chanblocks_per_coarse=n_fch,
    )

    assert len(fit_gains.gains) == n_coarse
    assert len(fit_gains.pol0) == n_coarse
    assert len(fit_gains.pol1) == n_coarse
    assert len(fit_gains.sigma_resid) == n_coarse

    # cc 1
    assert np.isclose(fit_gains.gains[0], 0.6069902)
    assert np.isclose(fit_gains.pol0[0], 437.224115)  # intercept
    assert np.isclose(fit_gains.pol1[0], -0.00000436)  # slope
    assert np.isclose(fit_gains.sigma_resid[0], 0.213680163)

    # cc 2
    assert np.isclose(fit_gains.gains[1], 0.8680556)
    assert np.isclose(fit_gains.pol0[1], 634.202778)  # intercept
    assert np.isclose(fit_gains.pol1[1], -0.00000632)  # slope
    assert np.isclose(fit_gains.sigma_resid[1], 0.344908961)

    # overall
    assert np.isclose(fit_gains.quality, 1)
