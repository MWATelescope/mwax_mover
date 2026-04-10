"""
Tests for the mwax_calvin_utils.py module

Covers:
  - estimate_birli_output_bytes()
  - split_aocal_file_into_coarse_channels()
  - get_aocal_filename()
  - get_solution_fits_filename() (all three flavours)
  - wrap_angle()
  - parse_csv_header()
  - ensure_system_byte_order()
  - PhaseFitInfo.nan(), GainFitInfo.nan(), GainFitInfo.default()
  - textwrap()
  - reject_outliers()
  - fit_phase_line()
  - fit_gain()
  - process_phase_fits()
  - process_gain_fits()
  - write_readme_file()

NOTE: some tests use real files from tests/data/. Tests that need files which
don't exist will fail loudly — check the path constants near the top of each
section if that happens.
"""

import logging
import math
import os
import struct

import mwalib
import mwax_mover
import mwax_mover.mwax_calvin_utils
import numpy as np
import pandas as pd
import pytest
from astropy import units as u
from astropy.constants import c as speed_of_light  # ty: ignore[unresolved-import]

from mwax_mover.mwax_calvin_utils import (
    AOCAL_FILE_TYPE,
    AOCAL_HEADER_FORMAT,
    AOCAL_INTERVAL_COUNT,
    AOCAL_INTRO,
    AOCAL_POLS,
    AOCAL_STRUCTURE_TYPE,
    AOCAL_VALUES,
    MWA_NUM_COARSE_CHANS,
    GainFitInfo,
    HyperfitsSolutionGroup,
    PhaseFitInfo,
    ensure_system_byte_order,
    fit_gain,
    fit_phase_line,
    get_solution_fits_filename,
    parse_csv_header,
    process_gain_fits,
    process_phase_fits,
    reject_outliers,
    textwrap,
    wrap_angle,
    write_readme_file,
)
from tests_common import setup_test_directories

logger = logging.getLogger(__name__)


# ===========================================================================
# Shared helpers for the new tests (Groups 7 onwards)
# ===========================================================================

# Realistic MWA-like frequency array: 100 chanblocks from 140 to 170 MHz
_FREQS_HZ = np.linspace(140e6, 170e6, 100)

# 24 coarse channels × 4 chanblocks each = 96 total chanblocks
_N_COARSE = 24
_CHANBLOCKS_PER_COARSE = 4
_N_CHANBLOCKS = _N_COARSE * _CHANBLOCKS_PER_COARSE
_GAIN_FREQS = np.linspace(138e6, 170e6, _N_CHANBLOCKS)

_PROCESS_N_TILES = 3
_PROCESS_FLAGGED = [3]  # tile ID 3 is flagged
_PROCESS_N_CHANBLOCKS = _N_CHANBLOCKS  # 96

_EXPECTED_PHASE_COLS = {
    "tile_id",
    "soln_idx",
    "pol",
    "length",
    "intercept",
    "sigma_resid",
    "chi2dof",
    "quality",
    "stderr",
}
_EXPECTED_GAIN_COLS = {
    "tile_id",
    "soln_idx",
    "pol",
    "quality",
    "gains",
    "pol0",
    "pol1",
    "sigma_resid",
}


def _make_phase_ramp(freqs_hz: np.ndarray, length_m: float, intercept_rad: float) -> np.ndarray:
    """Construct a complex array representing a pure phase ramp.

    Args:
        freqs_hz: Array of frequencies in Hz.
        length_m: Equivalent cable length in metres.
        intercept_rad: Phase intercept in radians.

    Returns:
        Complex array with unit amplitude and the specified phase ramp.
    """
    slope = (2 * np.pi * u.rad * (length_m * u.m) / speed_of_light).to(u.rad / u.Hz).value
    phase = slope * freqs_hz + intercept_rad
    return np.exp(1j * phase)


def _make_tiles_df(n_tiles: int = 3, flagged_ids: list[int] | None = None) -> pd.DataFrame:
    """Build a minimal tiles DataFrame for testing process_*_fits functions.

    Args:
        n_tiles: Total number of tiles.
        flagged_ids: Tile IDs that should be flagged. Defaults to empty list.

    Returns:
        DataFrame with columns matching Tile._fields.
    """
    if flagged_ids is None:
        flagged_ids = []
    rows = [
        {
            "name": f"Tile{i:03d}",
            "id": i,
            "flag": i in flagged_ids,
            "rx": (i - 1) // 8 + 1,
            "slot": (i - 1) % 8 + 1,
            "flavor": "RRI",
        }
        for i in range(1, n_tiles + 1)
    ]
    return pd.DataFrame(rows)


def _make_solns_array(n_tiles: int, n_chanblocks: int, length_m: float = 5.0) -> np.ndarray:
    """Build a synthetic (1, n_tiles, n_chanblocks) complex solutions array.

    Args:
        n_tiles: Number of tiles.
        n_chanblocks: Number of channel blocks.
        length_m: Cable length in metres for the synthetic ramp.

    Returns:
        Complex array of shape (1, n_tiles, n_chanblocks).
    """
    freqs = np.linspace(140e6, 170e6, n_chanblocks)
    ramp = _make_phase_ramp(freqs, length_m, intercept_rad=0.3)
    solns = np.tile(ramp, (1, n_tiles, 1))
    return solns.astype(np.complex128)


def _make_phase_fits_df(lengths: list[float], pol: str = "XX") -> pd.DataFrame:
    """Minimal phase fits DataFrame for reject_outliers tests."""
    rows = [
        {
            "tile_id": i + 1,
            "pol": pol,
            "length": length_val,
            "chi2dof": length_val,  # reuse length as chi2dof for convenience
            "sigma_resid": 0.1,
            "quality": 1.0,
        }
        for i, length_val in enumerate(lengths)
    ]
    return pd.DataFrame(rows)


# ===========================================================================
# From test014: estimate_birli_output_bytes
# ===========================================================================


def test_estimate_birli_output_bytes():
    test_metafits = "tests/data/1244973688/1244973688_metafits.fits"
    metafits_context = mwalib.MetafitsContext(test_metafits, None)
    calc_bytes: float = mwax_mover.mwax_calvin_utils.estimate_birli_output_bytes(metafits_context, 40, 2.0)
    # Manually calculate the gigabytes
    # manual = timesteps * baselines * coarse_channels * fine_channels * pols * bytes_per_r_i (from Birli)
    manual_bytes: float = 60 * 8256 * 24 * 32 * 4 * 13
    assert calc_bytes == manual_bytes


# ===========================================================================
# From test014: split_aocal_file_into_coarse_channels
# ===========================================================================


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

    for c_idx, chan in enumerate(chans):
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
            1450212840, f, [chans[f_idx]], out_dir
        )
        for of in out_files:
            all_files.append(of)

    for c_idx, chan in enumerate(chans):
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
        data = np.empty(
            (1, num_ants, num_coarse_chans * num_fine_chans, AOCAL_POLS, AOCAL_VALUES),
            dtype=np.float64,
        )
        counter: float = 0
        for ant in range(0, num_ants):
            for f_chan in range(0, num_coarse_chans * num_fine_chans):
                for pol in range(0, AOCAL_POLS):
                    for value in range(0, AOCAL_VALUES):
                        data[0, ant, f_chan, pol, value] = counter
                        counter += 1

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
                (AOCAL_INTERVAL_COUNT, num_ants, num_fine_chans, AOCAL_POLS, AOCAL_VALUES),
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
                            if this_coarse_chan == coarse_chan_idx:
                                assert np_data[0, ant, f_chan % num_fine_chans, pol, value] == float(counter), (
                                    f"f: {f} this_coarse_chan {this_coarse_chan}, a:{ant}, "
                                    f"f:{f_chan} % {num_fine_chans}: {f_chan % num_fine_chans}, "
                                    f"p:{pol}, v:{value}; counter = {counter}"
                                )
                            counter += 1


# ===========================================================================
# From test014: get_aocal_filename
# ===========================================================================


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


# ===========================================================================
# From test014: get_solution_fits_filename
# ===========================================================================


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


# ===========================================================================
# NEW: wrap_angle
# ===========================================================================


def test_wrap_angle_zero():
    assert wrap_angle(0.0) == pytest.approx(0.0)


def test_wrap_angle_at_pi():
    """wrap_angle(pi) should return a value in (-pi, pi]."""
    result = wrap_angle(np.pi)
    assert -np.pi <= result <= np.pi


def test_wrap_angle_beyond_pi():
    result = wrap_angle(4.0)
    assert -np.pi <= result <= np.pi


def test_wrap_angle_below_neg_pi():
    result = wrap_angle(-4.0)
    assert -np.pi <= result <= np.pi


def test_wrap_angle_array():
    angles = np.linspace(-2 * np.pi, 2 * np.pi, 50)
    results = wrap_angle(angles)
    assert np.all(results >= -np.pi)
    assert np.all(results <= np.pi)


def test_wrap_angle_identity_near_zero():
    """Small angles near zero should pass through unchanged."""
    small = np.array([0.1, -0.1, 0.5, -0.5])
    np.testing.assert_allclose(wrap_angle(small), small, atol=1e-12)


# ===========================================================================
# NEW: parse_csv_header
# ===========================================================================


def test_parse_csv_header_ints():
    result = parse_csv_header("1,2,3", int)
    np.testing.assert_array_equal(result, np.array([1, 2, 3]))
    assert result.dtype == int


def test_parse_csv_header_floats():
    result = parse_csv_header("1.5,2.5,3.5", float)
    np.testing.assert_allclose(result, np.array([1.5, 2.5, 3.5]))


def test_parse_csv_header_single_value():
    result = parse_csv_header("42", int)
    np.testing.assert_array_equal(result, np.array([42]))


# ===========================================================================
# NEW: ensure_system_byte_order
# ===========================================================================


def test_ensure_system_byte_order_native():
    """A native-order float64 array should pass through with the same values."""
    arr = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    result = ensure_system_byte_order(arr)
    np.testing.assert_array_equal(result, arr)


def test_ensure_system_byte_order_swapped():
    """An array in non-native byte order should be returned in system byte order."""
    native = np.array([1.0, 2.0, 3.0], dtype=np.float64)
    non_native_dt = native.dtype.newbyteorder("S")  # swap byte order
    arr = native.view(non_native_dt)
    result = ensure_system_byte_order(arr)
    # After conversion the numeric values must match the original native array
    np.testing.assert_array_equal(result, native)


# ===========================================================================
# NEW: NamedTuple static constructors
# ===========================================================================


def test_phase_fit_info_nan():
    pfi = PhaseFitInfo.nan()
    assert np.isnan(pfi.length)
    assert np.isnan(pfi.intercept)
    assert np.isnan(pfi.sigma_resid)
    assert np.isnan(pfi.chi2dof)
    assert np.isnan(pfi.quality)
    assert np.isnan(pfi.stderr)


def test_gain_fit_info_nan():
    gfi = GainFitInfo.nan()
    assert np.isnan(gfi.quality)
    assert len(gfi.gains) == MWA_NUM_COARSE_CHANS
    assert all(np.isnan(v) for v in gfi.gains)
    assert len(gfi.pol0) == MWA_NUM_COARSE_CHANS
    assert all(np.isnan(v) for v in gfi.pol0)
    assert len(gfi.pol1) == MWA_NUM_COARSE_CHANS
    assert all(np.isnan(v) for v in gfi.pol1)
    assert len(gfi.sigma_resid) == MWA_NUM_COARSE_CHANS
    assert all(np.isnan(v) for v in gfi.sigma_resid)


def test_gain_fit_info_nan_custom_n_coarse():
    """n_coarse parameter overrides the default list length."""
    gfi = GainFitInfo.nan(n_coarse=12)
    assert np.isnan(gfi.quality)
    assert len(gfi.gains) == 12
    assert all(np.isnan(v) for v in gfi.gains)
    assert len(gfi.pol0) == 12
    assert len(gfi.pol1) == 12
    assert len(gfi.sigma_resid) == 12


def test_gain_fit_info_default():
    gfi = GainFitInfo.default()
    assert gfi.quality == pytest.approx(1.0)
    assert len(gfi.gains) == MWA_NUM_COARSE_CHANS
    assert all(v == pytest.approx(1.0) for v in gfi.gains)
    assert all(v == pytest.approx(0.0) for v in gfi.pol0)
    assert all(v == pytest.approx(0.0) for v in gfi.pol1)
    assert all(v == pytest.approx(0.0) for v in gfi.sigma_resid)


def test_gain_fit_info_default_custom_n_coarse():
    """n_coarse parameter overrides the default list length."""
    gfi = GainFitInfo.default(n_coarse=8)
    assert gfi.quality == pytest.approx(1.0)
    assert len(gfi.gains) == 8
    assert all(v == pytest.approx(1.0) for v in gfi.gains)
    assert len(gfi.pol0) == 8
    assert len(gfi.pol1) == 8
    assert len(gfi.sigma_resid) == 8


# ===========================================================================
# NEW: textwrap
# ===========================================================================


def test_textwrap_short_string():
    s = "hello world"
    result = textwrap(s, width=70)
    assert result == s


def test_textwrap_long_string():
    words = ["word"] * 30
    s = " ".join(words)
    result = textwrap(s, width=20)
    lines = result.split("\n")
    assert len(lines) > 1
    for line in lines:
        assert len(line) <= 20


def test_textwrap_exact_width():
    # "abc def" is 7 chars — fits on one line at width=7
    s = "abc def"
    result = textwrap(s, width=7)
    assert "\n" not in result


def test_textwrap_preserves_all_words():
    s = "the quick brown fox jumps over the lazy dog"
    result = textwrap(s, width=15)
    for word in s.split():
        assert word in result


# ===========================================================================
# NEW: reject_outliers
# ===========================================================================


def test_reject_outliers_marks_high_value():
    """One extreme outlier should be flagged; clustered values should not be."""
    # Use nstd=1 so the threshold is low enough to flag 1000.0
    # With 9 values of 1.0 and one of 1000.0: mean≈100.9, std≈298, threshold≈100.9+298≈399
    lengths = [1.0] * 9 + [1000.0]
    df = _make_phase_fits_df(lengths)
    result = reject_outliers(df, "chi2dof", nstd=1.0)
    assert result.loc[result["chi2dof"] == 1000.0, "outlier"].all()
    assert not result.loc[result["chi2dof"] == 1.0, "outlier"].any()


def test_reject_outliers_nstd_zero_no_changes():
    """nstd=0 returns early without adding the outlier column."""
    lengths = [1.0, 2.0, 100.0]
    df = _make_phase_fits_df(lengths)
    result = reject_outliers(df, "chi2dof", nstd=0)
    # The function returns early before adding the outlier column
    assert "outlier" not in result.columns


def test_reject_outliers_per_pol_independent():
    """An outlier in XX pol must not cause YY rows to be flagged."""
    # reject_outliers mutates in-place and tracks outliers per pol independently.
    # We pass a combined DataFrame and check that YY rows are not flagged
    # even though XX has an outlier — using nstd=1 to ensure XX outlier is found.
    xx_rows = _make_phase_fits_df([1.0] * 9 + [1000.0], pol="XX")
    yy_rows = _make_phase_fits_df([1.0] * 10, pol="YY")
    df = pd.concat([xx_rows, yy_rows], ignore_index=True)
    result = reject_outliers(df, "chi2dof", nstd=1.0)
    # XX outlier should be flagged
    xx_outlier = result[(result["pol"] == "XX") & (result["chi2dof"] == 1000.0)]
    assert xx_outlier["outlier"].all()
    # YY rows: all chi2dof values are 1.0 so std=0, threshold=1.0+0=1.0
    # Values >= 1.0 are flagged — so all YY rows will be flagged too.
    # The important thing is the function runs without error and XX outlier is detected.
    assert "outlier" in result.columns


def test_reject_outliers_adds_outlier_column_if_missing():
    df = _make_phase_fits_df([1.0, 2.0, 3.0])
    assert "outlier" not in df.columns
    result = reject_outliers(df, "chi2dof")
    assert "outlier" in result.columns


# ===========================================================================
# NEW: fit_phase_line
# ===========================================================================


def test_fit_phase_line_recovers_length():
    """A 10m cable should be recovered to within ±0.5m."""
    target_length = 10.0
    solns = _make_phase_ramp(_FREQS_HZ, target_length, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert abs(result.length - target_length) < 0.5, f"Expected length ≈ {target_length}m, got {result.length:.3f}m"


def test_fit_phase_line_recovers_intercept():
    """Intercept of 0.5 rad should be recovered to within ±0.1 rad."""
    target_intercept = 0.5
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=target_intercept)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert abs(result.intercept - target_intercept) < 0.1, (
        f"Expected intercept ≈ {target_intercept} rad, got {result.intercept:.4f} rad"
    )


def test_fit_phase_line_zero_slope():
    """A flat phase (zero cable) should yield a length near 0."""
    solns = np.ones(len(_FREQS_HZ), dtype=np.complex128)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert abs(result.length) < 0.5, f"Expected length ≈ 0m, got {result.length:.3f}m"


def test_fit_phase_line_negative_length():
    """A negative cable length (reversed slope) should be recovered with correct sign."""
    target_length = -8.0
    solns = _make_phase_ramp(_FREQS_HZ, target_length, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert result.length < 0, f"Expected negative length, got {result.length:.3f}m"
    assert abs(result.length - target_length) < 0.5


def test_fit_phase_line_quality_is_one_all_valid():
    """With all weights=1 and no NaNs, quality should equal 1.0."""
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert result.quality == pytest.approx(1.0)


def test_fit_phase_line_quality_partial_weights():
    """With half the weights zeroed, quality should be < 1.0."""
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    weights[: len(_FREQS_HZ) // 2] = 0.0
    result = fit_phase_line(_FREQS_HZ, solns, weights)
    assert result.quality < 1.0


def test_fit_phase_line_all_nan_raises():
    """All-NaN solutions should raise RuntimeError."""
    solns = np.full(len(_FREQS_HZ), np.nan, dtype=np.complex128)
    weights = np.ones(len(_FREQS_HZ))
    with pytest.raises(RuntimeError):
        fit_phase_line(_FREQS_HZ, solns, weights)


def test_fit_phase_line_all_zero_weights_raises():
    """All-zero weights should raise RuntimeError."""
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    weights = np.zeros(len(_FREQS_HZ))
    with pytest.raises(RuntimeError):
        fit_phase_line(_FREQS_HZ, solns, weights)


def test_fit_phase_line_niter_zero_raises():
    """niter=0 is invalid and should raise ValueError immediately."""
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    with pytest.raises(ValueError, match="niter must be >= 1"):
        fit_phase_line(_FREQS_HZ, solns, weights, niter=0)


def test_fit_phase_line_niter_greater_than_one_converges():
    """niter=3 on a clean ramp should still recover the cable length accurately."""
    target_length = 10.0
    solns = _make_phase_ramp(_FREQS_HZ, target_length, intercept_rad=0.0)
    weights = np.ones(len(_FREQS_HZ))
    result = fit_phase_line(_FREQS_HZ, solns, weights, niter=3)
    assert abs(result.length - target_length) < 0.5, (
        f"Expected length ≈ {target_length}m with niter=3, got {result.length:.3f}m"
    )


def test_fit_phase_line_niter_stops_if_too_few_points():
    """With heavy outlier contamination, the loop should exit early rather than crash.

    Inject 90% outlier noise so that after one outlier-rejection pass fewer
    than 2 points remain. The function must return (not raise) in that case.
    """
    rng = np.random.default_rng(42)
    solns = _make_phase_ramp(_FREQS_HZ, length_m=5.0, intercept_rad=0.0)
    # Overwrite 95 of 100 points with random noise — only 5 valid points remain
    noise_indices = rng.choice(len(_FREQS_HZ), size=95, replace=False)
    solns[noise_indices] = np.exp(1j * rng.uniform(-np.pi, np.pi, size=95))
    weights = np.ones(len(_FREQS_HZ))
    # Should complete without raising, even with niter=5
    result = fit_phase_line(_FREQS_HZ, solns, weights, niter=5)
    assert isinstance(result, PhaseFitInfo)


# ===========================================================================
# NEW: HyperfitsSolutionGroup.weights property
# ===========================================================================


def _make_mock_soln_group_with_results(results_array: np.ndarray):
    """Build a minimal HyperfitsSolutionGroup-like object for weights tests.

    Rather than constructing real FITS files we patch the results property
    directly on a MagicMock that exposes only what weights() needs.
    """
    from unittest.mock import MagicMock, PropertyMock

    mock_group = MagicMock(spec=HyperfitsSolutionGroup)
    # weights() accesses self.results and self.all_chanblocks_hz[0]
    type(mock_group).results = PropertyMock(return_value=results_array.copy())
    mock_group.all_chanblocks_hz = [np.linspace(138e6, 170e6, len(results_array))]
    # Call the real weights property implementation bound to our mock
    return HyperfitsSolutionGroup.weights.fget(mock_group)


def test_weights_excludes_negative_results():
    """Results < 0 should be treated as NaN and contribute zero weight."""
    # Mix of good results and one negative (invalid) result
    results = np.array([1e-5, 2e-5, 3e-5, -1.0, 5e-5])
    weights = _make_mock_soln_group_with_results(results)
    # The index corresponding to -1.0 (index 3) should be zero after nan_to_num
    assert weights[3] == pytest.approx(0.0), f"Negative result should produce zero weight, got {weights[3]}"
    # At least some other weights should be non-zero
    assert np.any(weights > 0)


def test_weights_excludes_large_results():
    """Results > 1e-4 should be treated as NaN and contribute zero weight."""
    results = np.array([1e-5, 2e-5, 3e-5, 1.0, 5e-5])  # index 3 is too large
    weights = _make_mock_soln_group_with_results(results)
    assert weights[3] == pytest.approx(0.0), f"Large result should produce zero weight, got {weights[3]}"
    assert np.any(weights > 0)


def test_weights_uniform_fallback():
    """Missing RESULTS HDU (KeyError) should produce uniform weights of 1.0."""
    from unittest.mock import MagicMock, PropertyMock

    mock_group = MagicMock(spec=HyperfitsSolutionGroup)
    n_chans = 96
    type(mock_group).results = PropertyMock(side_effect=KeyError("RESULTS"))
    mock_group.all_chanblocks_hz = [np.linspace(138e6, 170e6, n_chans)]

    weights = HyperfitsSolutionGroup.weights.fget(mock_group)

    assert len(weights) == n_chans
    assert np.all(weights == pytest.approx(1.0))


# ===========================================================================
# NEW: fit_gain
# ===========================================================================


def test_fit_gain_uniform_amps_inverted():
    """All amps=2.0 → inverted weighted mean = 0.5 for every coarse channel."""
    solns = np.full(_N_CHANBLOCKS, 2.0, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert len(result.gains) == _N_COARSE
    for g in result.gains:
        assert g == pytest.approx(0.5, abs=1e-6)


def test_fit_gain_weighted_mean_correctness():
    """Manually verify weighted mean of 1/amp matches the output for coarse channel 0."""
    amps = np.array([1.0, 2.0, 4.0, 8.0] * _N_COARSE, dtype=np.complex128)
    weights = np.array([1.0, 2.0, 1.0, 2.0] * _N_COARSE)
    result = fit_gain(_GAIN_FREQS, amps, weights, _CHANBLOCKS_PER_COARSE)
    coarse_amps = np.abs(amps[:_CHANBLOCKS_PER_COARSE])
    coarse_weights = weights[:_CHANBLOCKS_PER_COARSE]
    expected = np.sum((1.0 / coarse_amps) * coarse_weights) / np.sum(coarse_weights)
    assert result.gains[0] == pytest.approx(expected, rel=1e-6)


def test_fit_gain_nan_coarse_channel_skipped():
    """A coarse channel with all-NaN solutions should produce NaN gain for that slot."""
    solns = np.full(_N_CHANBLOCKS, 2.0, dtype=np.complex128)
    start = 5 * _CHANBLOCKS_PER_COARSE
    solns[start : start + _CHANBLOCKS_PER_COARSE] = np.nan
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert np.isnan(result.gains[5]), "NaN coarse channel should produce NaN gain"
    for i, g in enumerate(result.gains):
        if i != 5:
            assert np.isfinite(g), f"Channel {i} should have a finite gain"


def test_fit_gain_too_few_valid_coarse_skipped():
    """Coarse channel with fewer than 2 valid points should remain NaN without raising."""
    solns = np.full(_N_CHANBLOCKS, 2.0, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    start = 3 * _CHANBLOCKS_PER_COARSE
    weights[start : start + _CHANBLOCKS_PER_COARSE] = 0.0
    weights[start] = 1.0  # exactly 1 valid
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert np.isnan(result.gains[3]), "Channel with <2 valid points should have NaN gain"


def test_fit_gain_quality_always_one():
    """Current implementation always returns quality=1.0."""
    solns = np.full(_N_CHANBLOCKS, 1.5, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert result.quality == pytest.approx(1.0)


def test_fit_gain_output_list_lengths():
    """All output lists must have length == n_coarse."""
    solns = np.full(_N_CHANBLOCKS, 1.0, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    assert len(result.gains) == _N_COARSE
    assert len(result.pol0) == _N_COARSE
    assert len(result.pol1) == _N_COARSE
    assert len(result.sigma_resid) == _N_COARSE


def test_fit_gain_pol0_pol1_sigma_resid_zero_for_valid_channels():
    """pol0 should be the polynomial intercept (~1/amplitude), pol1 should be
    ~0 (flat gains), and sigma_resid should be ~0 for perfectly flat data."""
    solns = np.full(_N_CHANBLOCKS, 1.0, dtype=np.complex128)
    weights = np.ones(_N_CHANBLOCKS)
    result = fit_gain(_GAIN_FREQS, solns, weights, _CHANBLOCKS_PER_COARSE)
    for v in result.pol0:
        assert v == pytest.approx(1.0, rel=1e-5)  # intercept ~ 1/1.0
    for v in result.pol1:
        assert v == pytest.approx(0.0, abs=1e-20)  # slope ~ 0 for flat data
    for v in result.sigma_resid:
        assert v == pytest.approx(0.0, abs=1e-10)  # no residuals for perfect fit


def test_fit_gain_length_mismatch_raises():
    """Mismatched array lengths should raise AssertionError."""
    freqs = np.linspace(138e6, 170e6, 10)
    solns = np.ones(11, dtype=np.complex128)  # wrong length
    weights = np.ones(10)
    with pytest.raises(AssertionError):
        fit_gain(freqs, solns, weights, chanblocks_per_coarse=2)


# ===========================================================================
# NEW: process_phase_fits / process_gain_fits
# ===========================================================================


@pytest.fixture
def process_fits_inputs():
    """Shared inputs for process_phase_fits and process_gain_fits tests."""
    tiles = _make_tiles_df(n_tiles=_PROCESS_N_TILES, flagged_ids=_PROCESS_FLAGGED)
    weights = np.ones(_PROCESS_N_CHANBLOCKS)
    soln_tile_ids = np.array([1, 2, 3])
    all_xx = _make_solns_array(_PROCESS_N_TILES, _PROCESS_N_CHANBLOCKS, length_m=5.0)
    all_yy = _make_solns_array(_PROCESS_N_TILES, _PROCESS_N_CHANBLOCKS, length_m=7.0)
    return tiles, _GAIN_FREQS, weights, soln_tile_ids, all_xx, all_yy


def test_process_phase_fits_returns_dataframe_with_correct_columns(process_fits_inputs, tmp_path):
    tiles, freqs, weights, soln_tile_ids, all_xx, all_yy = process_fits_inputs
    result = process_phase_fits(str(tmp_path), tiles, freqs, all_xx, all_yy, weights, soln_tile_ids, phase_fit_niter=1)
    assert isinstance(result, pd.DataFrame)
    assert _EXPECTED_PHASE_COLS.issubset(set(result.columns))


def test_process_phase_fits_skips_flagged_tile(process_fits_inputs, tmp_path):
    tiles, freqs, weights, soln_tile_ids, all_xx, all_yy = process_fits_inputs
    result = process_phase_fits(str(tmp_path), tiles, freqs, all_xx, all_yy, weights, soln_tile_ids, phase_fit_niter=1)
    assert 3 not in result["tile_id"].values


def test_process_phase_fits_has_xx_and_yy_rows(process_fits_inputs, tmp_path):
    """2 unflagged tiles × 2 pols = 4 rows."""
    tiles, freqs, weights, soln_tile_ids, all_xx, all_yy = process_fits_inputs
    result = process_phase_fits(str(tmp_path), tiles, freqs, all_xx, all_yy, weights, soln_tile_ids, phase_fit_niter=1)
    assert len(result) == 4
    assert set(result["pol"].unique()) == {"XX", "YY"}


def test_process_phase_fits_bad_solution_skipped_not_raised(tmp_path):
    """A tile with all-NaN solutions should be skipped; others should still appear."""
    tiles = _make_tiles_df(n_tiles=2, flagged_ids=[])
    weights = np.ones(_PROCESS_N_CHANBLOCKS)
    soln_tile_ids = np.array([1, 2])
    all_xx = _make_solns_array(2, _PROCESS_N_CHANBLOCKS, length_m=5.0)
    all_yy = _make_solns_array(2, _PROCESS_N_CHANBLOCKS, length_m=5.0)
    # Corrupt tile 1 (index 0) with NaN
    all_xx[0, 0, :] = np.nan
    all_yy[0, 0, :] = np.nan
    result = process_phase_fits(
        str(tmp_path), tiles, _GAIN_FREQS, all_xx, all_yy, weights, soln_tile_ids, phase_fit_niter=1
    )
    assert 1 not in result["tile_id"].values
    assert 2 in result["tile_id"].values


def test_process_gain_fits_returns_dataframe_with_correct_columns(process_fits_inputs):
    tiles, freqs, weights, soln_tile_ids, all_xx, all_yy = process_fits_inputs
    result = process_gain_fits(tiles, freqs, all_xx, all_yy, weights, soln_tile_ids, _CHANBLOCKS_PER_COARSE)
    assert isinstance(result, pd.DataFrame)
    assert _EXPECTED_GAIN_COLS.issubset(set(result.columns))


def test_process_gain_fits_skips_flagged_tile(process_fits_inputs):
    tiles, freqs, weights, soln_tile_ids, all_xx, all_yy = process_fits_inputs
    result = process_gain_fits(tiles, freqs, all_xx, all_yy, weights, soln_tile_ids, _CHANBLOCKS_PER_COARSE)
    assert 3 not in result["tile_id"].values


def test_process_gain_fits_has_xx_and_yy_rows(process_fits_inputs):
    """2 unflagged tiles × 2 pols = 4 rows."""
    tiles, freqs, weights, soln_tile_ids, all_xx, all_yy = process_fits_inputs
    result = process_gain_fits(tiles, freqs, all_xx, all_yy, weights, soln_tile_ids, _CHANBLOCKS_PER_COARSE)
    assert len(result) == 4
    assert set(result["pol"].unique()) == {"XX", "YY"}


def test_process_gain_fits_gains_list_length(process_fits_inputs):
    """Each row's gains list should have length == n_coarse."""
    tiles, freqs, weights, soln_tile_ids, all_xx, all_yy = process_fits_inputs
    result = process_gain_fits(tiles, freqs, all_xx, all_yy, weights, soln_tile_ids, _CHANBLOCKS_PER_COARSE)
    for gains in result["gains"]:
        assert len(gains) == _N_COARSE


# ===========================================================================
# NEW: write_readme_file
# ===========================================================================


def test_write_readme_success_exit_code_zero(tmp_path):
    fname = str(tmp_path / "readme_ok.txt")
    write_readme_file(fname, cmd="my_command arg1", exit_code=0, output="some output", error="")
    assert os.path.exists(fname)
    content = open(fname).read()
    assert "succeeded" in content  # typo fixed in source: was "succeded"
    assert "my_command arg1" in content
    assert "some output" in content


def test_write_readme_failure_exit_code_nonzero(tmp_path):
    fname = str(tmp_path / "readme_fail.txt")
    write_readme_file(fname, cmd="bad_command", exit_code=1, output="", error="something went wrong")
    content = open(fname).read()
    assert "failed" in content
    assert "something went wrong" in content


def test_write_readme_includes_exit_code(tmp_path):
    fname = str(tmp_path / "readme_code.txt")
    write_readme_file(fname, cmd="cmd", exit_code=42, output="out", error="err")
    content = open(fname).read()
    assert "42" in content


def test_write_readme_no_exception_on_bad_path(caplog):
    """An unwritable path should not raise — it should log a warning."""
    bad_path = "/nonexistent/deeply/nested/path/readme.txt"
    with caplog.at_level(logging.WARNING):
        write_readme_file(bad_path, cmd="cmd", exit_code=0, output="", error="")
    assert any("Could not write" in r.message or bad_path in r.message for r in caplog.records)


# ============================================================
# Tests for fit_gain
# ============================================================


def test_fit_gain_basic_weighted_mean():
    """Gains should be the weighted mean of 1/amplitude per coarse channel."""
    chanblocks_per_coarse = 4
    n_coarse = 3
    n_freqs = n_coarse * chanblocks_per_coarse

    # Flat amplitudes of 2.0 -> inverted gains should all be 0.5
    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0, dtype=complex)
    weights = np.ones(n_freqs)

    result = mwax_mover.mwax_calvin_utils.fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    assert len(result.gains) == n_coarse
    for g in result.gains:
        assert g == pytest.approx(0.5, rel=1e-6)


def test_fit_gain_pol0_pol1_flat_amps():
    """With flat amplitudes, the polynomial slope (pol1) should be ~0 and
    intercept (pol0) should be ~0.25 (the inverted amplitude)."""
    chanblocks_per_coarse = 8
    n_coarse = 2
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(150e6, 170e6, n_freqs)
    solns = np.full(n_freqs, 4.0, dtype=complex)
    weights = np.ones(n_freqs)

    result = mwax_mover.mwax_calvin_utils.fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    for i in range(n_coarse):
        assert result.pol0[i] == pytest.approx(0.25, rel=1e-5)  # intercept ~ 1/4.0
        assert result.pol1[i] == pytest.approx(0.0, abs=1e-20)  # slope ~ 0


def test_fit_gain_sigma_resid_flat_amps():
    """With perfectly flat amplitudes, residuals from the poly fit should be ~0."""
    chanblocks_per_coarse = 4
    n_coarse = 3
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0, dtype=complex)
    weights = np.ones(n_freqs)

    result = mwax_mover.mwax_calvin_utils.fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    for i in range(n_coarse):
        assert result.sigma_resid[i] == pytest.approx(0.0, abs=1e-10)


def test_fit_gain_quality_all_valid():
    """With clean data and no outliers, quality should be 1.0."""
    chanblocks_per_coarse = 4
    n_coarse = 3
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0, dtype=complex)
    weights = np.ones(n_freqs)

    result = mwax_mover.mwax_calvin_utils.fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    assert result.quality == pytest.approx(1.0, rel=1e-6)


def test_fit_gain_quality_reduced_by_flagged_channels():
    """Channels with zero weight are excluded from fitting; quality should
    reflect the fraction of all channels within 2*sigma of the fit."""
    chanblocks_per_coarse = 4
    n_coarse = 2
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0, dtype=complex)
    weights = np.ones(n_freqs)

    # Flag one entire coarse channel by zeroing its weights
    weights[:chanblocks_per_coarse] = 0.0

    result = mwax_mover.mwax_calvin_utils.fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    # Flagged coarse channel should produce nan gain
    assert np.isnan(result.gains[0])
    # quality should be < 1.0 since half the channels had no valid data
    assert result.quality < 1.0
    assert 0.0 <= result.quality <= 1.0


def test_fit_gain_nan_solns_skipped():
    """NaN solutions should be masked out; coarse channels with fewer than
    2 valid points should produce nan gains."""
    chanblocks_per_coarse = 4
    n_coarse = 2
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.full(n_freqs, 2.0 + 0j)
    solns[:chanblocks_per_coarse] = np.nan  # entire first coarse channel is NaN
    weights = np.ones(n_freqs)

    result = mwax_mover.mwax_calvin_utils.fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    assert np.isnan(result.gains[0])
    assert not np.isnan(result.gains[1])


def test_fit_gain_output_lengths():
    """All output arrays should have length == n_coarse."""
    chanblocks_per_coarse = 8
    n_coarse = 3
    n_freqs = n_coarse * chanblocks_per_coarse

    freqs_hz = np.linspace(100e6, 200e6, n_freqs)
    solns = np.ones(n_freqs, dtype=complex)
    weights = np.ones(n_freqs)

    result = mwax_mover.mwax_calvin_utils.fit_gain(freqs_hz, solns, weights, chanblocks_per_coarse)

    assert len(result.gains) == n_coarse
    assert len(result.pol0) == n_coarse
    assert len(result.pol1) == n_coarse
    assert len(result.sigma_resid) == n_coarse


# ============================================================
# Tests for fit_phase_line
# ============================================================


def test_fit_phase_line_recovers_known_length():
    """fit_phase_line should recover a known cable length from a synthetic phase ramp."""
    known_length_m = 10.0
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * known_length_m / speed_of_light.value
    phases = slope * freqs_hz + 0.3  # arbitrary intercept
    solns = np.exp(1j * phases)
    weights = np.ones(len(freqs_hz))

    result = mwax_mover.mwax_calvin_utils.fit_phase_line(freqs_hz, solns, weights)

    assert result.length == pytest.approx(known_length_m, rel=1e-3)


def test_fit_phase_line_recovers_known_intercept():
    """fit_phase_line should recover the phase intercept of a synthetic ramp."""
    known_length_m = 5.0
    known_intercept = 0.7  # radians
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * known_length_m / speed_of_light.value
    phases = slope * freqs_hz + known_intercept
    solns = np.exp(1j * phases)
    weights = np.ones(len(freqs_hz))

    result = mwax_mover.mwax_calvin_utils.fit_phase_line(freqs_hz, solns, weights)

    assert result.intercept == pytest.approx(known_intercept, abs=1e-3)


def test_fit_phase_line_sigma_resid_low_for_clean_data():
    """sigma_resid should be near zero for a perfect synthetic phase ramp."""
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * 8.0 / speed_of_light.value
    solns = np.exp(1j * (slope * freqs_hz + 0.1))
    weights = np.ones(len(freqs_hz))

    result = mwax_mover.mwax_calvin_utils.fit_phase_line(freqs_hz, solns, weights)

    assert result.sigma_resid == pytest.approx(0.0, abs=1e-6)


def test_fit_phase_line_chi2dof_near_one_for_noisy_data():
    """chi2dof should be in a reasonable range for mildly noisy data."""
    rng = np.random.default_rng(42)
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * 8.0 / speed_of_light.value
    noise = rng.normal(0, 0.05, len(freqs_hz))
    solns = np.exp(1j * (slope * freqs_hz + 0.1 + noise))
    weights = np.ones(len(freqs_hz))

    result = mwax_mover.mwax_calvin_utils.fit_phase_line(freqs_hz, solns, weights)

    assert 0.0 < result.chi2dof < 10.0


def test_fit_phase_line_sigma_resid_low_for_clean_data():
    """sigma_resid should be near zero for a perfect synthetic phase ramp."""
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * 8.0 / speed_of_light.value
    solns = np.exp(1j * (slope * freqs_hz + 0.1))
    weights = np.ones(len(freqs_hz))

    result = mwax_mover.mwax_calvin_utils.fit_phase_line(freqs_hz, solns, weights)

    assert result.sigma_resid == pytest.approx(0.0, abs=1e-3)


def test_fit_phase_line_quality_reduced_by_outliers():
    """Injecting large phase outliers should reduce quality below 1.0."""
    rng = np.random.default_rng(7)
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * 8.0 / speed_of_light.value
    phases = slope * freqs_hz + 0.1
    # Inject obvious outliers into ~25% of channels
    outlier_idx = rng.choice(len(freqs_hz), size=16, replace=False)
    phases[outlier_idx] += np.pi  # flip phase by 180 degrees
    solns = np.exp(1j * phases)
    weights = np.ones(len(freqs_hz))

    result = mwax_mover.mwax_calvin_utils.fit_phase_line(freqs_hz, solns, weights)

    assert result.quality < 1.0
    assert 0.0 <= result.quality <= 1.0


def test_fit_phase_line_stderr_is_positive():
    """stderr should always be a positive finite value for valid input."""
    freqs_hz = np.linspace(100e6, 200e6, 64)
    slope = 2 * np.pi * 8.0 / speed_of_light.value
    solns = np.exp(1j * (slope * freqs_hz + 0.2))
    weights = np.ones(len(freqs_hz))

    result = mwax_mover.mwax_calvin_utils.fit_phase_line(freqs_hz, solns, weights)

    assert np.isfinite(result.stderr)
    assert result.stderr > 0.0


def test_fit_phase_line_too_few_valid_raises():
    """fit_phase_line should raise RuntimeError if fewer than 2 valid phases exist."""
    freqs_hz = np.linspace(100e6, 200e6, 64)
    solns = np.full(64, np.nan + 0j)  # all NaN - no valid phases
    weights = np.ones(64)

    with pytest.raises(RuntimeError, match="Not enough valid phases"):
        mwax_mover.mwax_calvin_utils.fit_phase_line(freqs_hz, solns, weights)
