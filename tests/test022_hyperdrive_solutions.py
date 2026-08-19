"""
Tests for the mwax_hyperdrive_solutions.py module.

Covers:
  - HyperfitsSolution.get_jones / chanblock_converged / baseline_tile_flags / write_jones
  - HyperfitsSolutionGroup.load / combined_tile_flags / apply_tile_flags /
    enforce_whole_jones_nan / weights / process_phase_fits / process_gain_fits_for_db

NOTE: HyperfitsSolution/HyperfitsSolutionGroup were moved out of
mwax_calvin_utils.py into this module's source file
(mwax_hyperdrive_solutions.py). Several tests below moved with them from
test014_calvin_utils.py, where they previously lived (the weights tests,
and process_phase_fits/process_gain_fits_for_db -- the latter now methods on
HyperfitsSolutionGroup rather than free functions, so their tests were
rewritten rather than moved verbatim).

TestSharedHduHelpersAgreeAcrossCallers (which cross-checked HyperfitsSolution
against mwax_calvin_quality.CalSolutionQuality) has since been removed from
test014_calvin_utils.py entirely, now that mwax_calvin_quality.py has been
deleted -- there's no longer a second independent reader to cross-check
against.
"""

import os
from unittest.mock import MagicMock, PropertyMock, patch

import numpy as np
import pandas as pd
import pytest
from astropy import units as u
from astropy.constants import c as speed_of_light  # ty: ignore[unresolved-import]

from mwax_mover.mwax_calvin_utils import Metafits, reject_outliers
from mwax_mover.mwax_hyperdrive_solutions import (
    ChannelFlagReason,
    HyperfitsSolution,
    HyperfitsSolutionGroup,
    TileFlagReason,
)

# A real fixture whose filename parse_solution_channels() can parse -- same
# file used by TestSharedHduHelpersAgreeAcrossCallers in test014.
SOLUTIONS_PATH = "tests/data/1391522232/1391522232_ch89_solutions.fits"
METAFITS_PATH = "tests/data/1391522232/1391522232_metafits.fits"


# ===========================================================================
# HyperfitsSolution.get_jones / chanblock_converged / baseline_tile_flags
# ===========================================================================


def test_get_jones_matches_get_solutions():
    """get_jones()'s 4 Jones terms match get_solutions()'s 4 flat arrays."""
    hs = HyperfitsSolution(SOLUTIONS_PATH)
    jones = hs.get_jones()
    xx, xy, yx, yy = hs.get_solutions()

    assert jones.shape == (xx.shape[1], xx.shape[2], 2, 2)
    assert np.allclose(jones[..., 0, 0], xx[0], equal_nan=True)
    assert np.allclose(jones[..., 0, 1], xy[0], equal_nan=True)
    assert np.allclose(jones[..., 1, 0], yx[0], equal_nan=True)
    assert np.allclose(jones[..., 1, 1], yy[0], equal_nan=True)


def test_chanblock_converged_matches_results_nan_pattern():
    """chanblock_converged is exactly ~isnan(results)."""
    hs = HyperfitsSolution(SOLUTIONS_PATH)
    assert np.array_equal(hs.chanblock_converged, ~np.isnan(hs.results))


def test_baseline_tile_flags_length_matches_tile_flags():
    """baseline_tile_flags and tile_flags (TILES HDU) have the same length."""
    hs = HyperfitsSolution(SOLUTIONS_PATH)
    assert len(hs.baseline_tile_flags) == len(hs.tile_flags)


def test_baseline_tile_flags_all_false_when_no_flagged_baselines():
    """No NaN baseline weights in this fixture -> no inferred tile flags."""
    hs = HyperfitsSolution(SOLUTIONS_PATH)
    assert not hs.baseline_tile_flags.any()


def test_baseline_tile_flags_all_false_when_baselines_hdu_absent(tmp_path):
    """A file with no BASELINES HDU at all degrades to all-False rather than raising.

    Regression test: older/synthetic solution files (e.g. those built by
    tests/test020_calvin_solutions.py's _make_synthetic_solution, which
    omits BASELINES entirely) must not crash the pipeline just because
    this third, supplementary flag source has nothing to report.
    """
    from astropy.io import fits as astropy_fits

    solutions_path = str(tmp_path / "no_baselines.fits")
    with astropy_fits.open(SOLUTIONS_PATH) as hdul:
        hdus_without_baselines = [hdu for hdu in hdul if hdu.name != "BASELINES"]
        astropy_fits.HDUList(hdus_without_baselines).writeto(solutions_path)

    hs = HyperfitsSolution(solutions_path)
    flags = hs.baseline_tile_flags
    assert not flags.any()
    assert len(flags) == len(hs.tile_flags)


# ===========================================================================
# HyperfitsSolution.write_jones
# ===========================================================================


def test_write_jones_round_trip_nans_correct_entry(tmp_path):
    """Writing a modified Jones array persists exactly the entries changed."""
    import shutil

    solutions_path = str(tmp_path / "solutions.fits")
    shutil.copy2(SOLUTIONS_PATH, solutions_path)

    hs = HyperfitsSolution(solutions_path)
    original_jones = hs.get_jones()

    modified = original_jones.copy()
    modified[3, 5, :, :] = np.nan + 1j * np.nan

    backup_path = hs.write_jones(modified)

    assert backup_path is not None
    assert os.path.exists(backup_path)

    # Backup preserves the pristine original.
    backup_jones = HyperfitsSolution(backup_path).get_jones()
    assert not np.any(np.isnan(backup_jones[3, 5]))

    # Re-reading the (now overwritten) original path reflects the change.
    reread_jones = HyperfitsSolution(solutions_path).get_jones()
    assert np.all(np.isnan(reread_jones[3, 5]))
    # Everything else is untouched.
    unaffected_mask = np.ones(reread_jones.shape[:2], dtype=bool)
    unaffected_mask[3, 5] = False
    assert np.allclose(
        reread_jones[unaffected_mask],
        original_jones[unaffected_mask],
        equal_nan=True,
    )


def test_write_jones_backup_false_skips_backup(tmp_path):
    """backup=False does not create a .original.fits copy."""
    import shutil

    solutions_path = str(tmp_path / "solutions.fits")
    shutil.copy2(SOLUTIONS_PATH, solutions_path)

    hs = HyperfitsSolution(solutions_path)
    jones = hs.get_jones()

    result = hs.write_jones(jones, backup=False)

    assert result is None
    assert not os.path.exists(solutions_path.replace(".fits", ".original.fits"))


def test_write_jones_wrong_shape_raises(tmp_path):
    """A jones array with the wrong shape raises before backing up or writing."""
    import shutil

    solutions_path = str(tmp_path / "solutions.fits")
    shutil.copy2(SOLUTIONS_PATH, solutions_path)

    hs = HyperfitsSolution(solutions_path)
    wrong_shape = np.zeros((3, 3, 2, 2), dtype=np.complex128)

    with pytest.raises(RuntimeError):
        hs.write_jones(wrong_shape)

    # Shape is validated before any backup is made.
    assert not os.path.exists(solutions_path.replace(".fits", ".original.fits"))


# ===========================================================================
# HyperfitsSolutionGroup.combined_tile_flags
# ===========================================================================


def test_refant_is_unflagged_lowest_id():
    """refant returns the lowest-ID tile not flagged by any of the three sources."""
    metafits = Metafits(METAFITS_PATH)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    refant = group.refant
    assert not group.combined_tile_flags[refant.name]  # .name is the DataFrame index here
    candidate_ids = group.metafits_tiles_df["id"].to_numpy()
    unflagged_ids = candidate_ids[~group.combined_tile_flags]
    assert refant["id"] == unflagged_ids.min()


def test_refant_excludes_baseline_only_flagged_tile():
    """A tile flagged only via BASELINES inference (not metafits/TILES) is never chosen as refant."""
    metafits = Metafits(METAFITS_PATH)
    mock_soln = MagicMock(spec=HyperfitsSolution)
    n_tiles = len(metafits.tiles_df)
    type(mock_soln).tile_flags = PropertyMock(return_value=np.zeros(n_tiles, dtype=bool))
    baseline_flags = np.zeros(n_tiles, dtype=bool)

    real_group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    lowest_unflagged_idx = np.where(~real_group.combined_tile_flags)[0][
        np.argmin(real_group.metafits_tiles_df["id"].to_numpy()[~real_group.combined_tile_flags])
    ]
    baseline_flags[lowest_unflagged_idx] = True  # flag what would otherwise be chosen
    type(mock_soln).baseline_tile_flags = PropertyMock(return_value=baseline_flags)

    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    group.solns = [mock_soln]

    assert group.refant.name != lowest_unflagged_idx


def test_combined_tile_flags_matches_metafits_when_no_other_flags():
    """With no TILES/BASELINES flags in this fixture, combined equals metafits alone."""
    metafits = Metafits(METAFITS_PATH)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])

    expected = metafits.tiles_df["flag"].to_numpy(dtype=bool)
    assert np.array_equal(group.combined_tile_flags, expected)


def test_combined_tile_flags_ors_in_tiles_hdu_flag():
    """A TILES-HDU-only flag (not in metafits) is still caught by combined_tile_flags."""
    metafits = Metafits(METAFITS_PATH)
    mock_soln = MagicMock(spec=HyperfitsSolution)
    n_tiles = len(metafits.tiles_df)
    tiles_hdu_flags = np.zeros(n_tiles, dtype=bool)
    tiles_hdu_flags[7] = True
    type(mock_soln).tile_flags = PropertyMock(return_value=tiles_hdu_flags)
    type(mock_soln).baseline_tile_flags = PropertyMock(return_value=np.zeros(n_tiles, dtype=bool))

    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    group.solns = [mock_soln]

    assert group.combined_tile_flags[7]


def test_combined_tile_flags_ors_in_baseline_inferred_flag():
    """A BASELINES-HDU-inferred-only flag is still caught by combined_tile_flags."""
    metafits = Metafits(METAFITS_PATH)
    mock_soln = MagicMock(spec=HyperfitsSolution)
    n_tiles = len(metafits.tiles_df)
    baseline_flags = np.zeros(n_tiles, dtype=bool)
    baseline_flags[11] = True
    type(mock_soln).tile_flags = PropertyMock(return_value=np.zeros(n_tiles, dtype=bool))
    type(mock_soln).baseline_tile_flags = PropertyMock(return_value=baseline_flags)

    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    group.solns = [mock_soln]

    assert group.combined_tile_flags[11]


# ===========================================================================
# HyperfitsSolutionGroup.weights property
# ===========================================================================


def _make_mock_soln_group_with_results(results_array: np.ndarray):
    """Build a minimal HyperfitsSolutionGroup-like object for weights tests.

    Rather than constructing real FITS files we patch the results property
    directly on a MagicMock that exposes only what weights() needs.
    """
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
    mock_group = MagicMock(spec=HyperfitsSolutionGroup)
    n_chans = 96
    type(mock_group).results = PropertyMock(side_effect=KeyError("RESULTS"))
    mock_group.all_chanblocks_hz = [np.linspace(138e6, 170e6, n_chans)]

    weights = HyperfitsSolutionGroup.weights.fget(mock_group)

    assert len(weights) == n_chans
    assert np.all(weights == pytest.approx(1.0))


# ===========================================================================
# HyperfitsSolutionGroup.load
# ===========================================================================


def test_load_populates_jones_and_reason_arrays():
    """load() populates jones (one array per file) and zeroed reason arrays."""
    metafits = Metafits(METAFITS_PATH)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    group.load()
    assert group.jones is not None
    assert group.tile_flag_reasons is not None
    assert group.channel_flag_reasons is not None

    assert len(group.jones) == 1
    n_tiles, n_chanblocks = group.jones[0].shape[:2]
    assert group.tile_flag_reasons.shape == (n_tiles,)
    assert len(group.channel_flag_reasons) == 1
    assert group.channel_flag_reasons[0].shape == (n_tiles, n_chanblocks)


def test_load_marks_pre_existing_nan():
    """A whole-Jones-NaN entry in the raw file is marked PRE_EXISTING_NAN at load time."""
    metafits = Metafits(METAFITS_PATH)
    hs = HyperfitsSolution(SOLUTIONS_PATH)
    group = HyperfitsSolutionGroup(metafits, [hs])
    group.load()
    assert group.channel_flag_reasons is not None

    raw_jones = hs.get_jones()
    pre_existing = np.any(np.isnan(raw_jones), axis=(-2, -1))
    if not pre_existing.any():
        pytest.skip("fixture has no pre-existing NaN entries to check against")
    assert np.all(group.channel_flag_reasons[0][pre_existing] & ChannelFlagReason.PRE_EXISTING_NAN)


def test_methods_raise_before_load():
    """Calling a jones-dependent method before load() raises a clear error."""
    metafits = Metafits(METAFITS_PATH)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    with pytest.raises(RuntimeError, match="load\\(\\)"):
        group.apply_tile_flags()


# ===========================================================================
# HyperfitsSolutionGroup.apply_tile_flags / enforce_whole_jones_nan
# ===========================================================================


def test_apply_tile_flags_nans_out_flagged_tile_and_records_reason():
    """A metafits-flagged tile gets fully NaN'd and tagged METAFITS."""
    metafits = Metafits(METAFITS_PATH)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    group.load()
    assert group.jones is not None
    assert group.tile_flag_reasons is not None

    flagged_idx = np.where(group.metafits_tiles_df["flag"].to_numpy())[0]
    if len(flagged_idx) == 0:
        pytest.skip("fixture has no metafits-flagged tiles to check against")
    idx = flagged_idx[0]

    group.apply_tile_flags()

    assert np.all(np.isnan(group.jones[0][idx]))
    assert group.tile_flag_reasons[idx] & TileFlagReason.METAFITS


def test_apply_tile_flags_leaves_unflagged_tile_untouched():
    """A tile flagged nowhere keeps its original (non-NaN) data."""
    metafits = Metafits(METAFITS_PATH)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    group.load()
    assert group.jones is not None
    assert group.tile_flag_reasons is not None
    unflagged_idx = np.where(~group.combined_tile_flags)[0]
    assert len(unflagged_idx) > 0
    idx = unflagged_idx[0]
    original = group.jones[0][idx].copy()

    group.apply_tile_flags()

    assert np.array_equal(group.jones[0][idx], original, equal_nan=True)
    assert group.tile_flag_reasons[idx] == TileFlagReason.NONE


def test_enforce_whole_jones_nan_promotes_partial_entry():
    """An entry with only one Jones term NaN gets promoted to fully NaN and tagged."""
    metafits = Metafits(METAFITS_PATH)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    group.load()
    assert group.jones is not None
    assert group.channel_flag_reasons is not None

    # Pick an entry that starts fully finite, then corrupt just one term.
    finite_mask = ~np.any(np.isnan(group.jones[0]), axis=(-2, -1))
    tile_idx, chan_idx = next(zip(*np.where(finite_mask)))
    group.jones[0][tile_idx, chan_idx, 0, 1] = np.nan + 1j * np.nan  # Dx only

    group.enforce_whole_jones_nan()

    assert np.all(np.isnan(group.jones[0][tile_idx, chan_idx]))
    assert group.channel_flag_reasons[0][tile_idx, chan_idx] & ChannelFlagReason.PARTIAL_JONES


def test_enforce_whole_jones_nan_leaves_fully_finite_entry_untouched():
    """An entry with no NaN terms at all is not marked or modified."""
    metafits = Metafits(METAFITS_PATH)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(SOLUTIONS_PATH)])
    group.load()
    assert group.jones is not None
    assert group.channel_flag_reasons is not None
    finite_mask = ~np.any(np.isnan(group.jones[0]), axis=(-2, -1))
    tile_idx, chan_idx = next(zip(*np.where(finite_mask)))
    original = group.jones[0][tile_idx, chan_idx].copy()

    group.enforce_whole_jones_nan()

    assert np.array_equal(group.jones[0][tile_idx, chan_idx], original)
    assert group.channel_flag_reasons[0][tile_idx, chan_idx] == ChannelFlagReason.NONE


# ===========================================================================
# HyperfitsSolutionGroup.process_phase_fits / process_gain_fits_for_db
# ===========================================================================

_FIT_N_CHANBLOCKS = 96
_FIT_CHANBLOCKS_PER_COARSE = 4

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

    Duplicated from test014_calvin_utils.py's helper of the same name,
    rather than imported across test files.
    """
    slope = (2 * np.pi * u.rad * (length_m * u.m) / speed_of_light).to(u.rad / u.Hz).value
    phase = slope * freqs_hz + intercept_rad
    return np.exp(1j * phase)


def _make_fake_group(n_tiles, n_chanblocks, flagged_ids=None, xx_length_m=5.0, yy_length_m=7.0, flavors=None):
    """Build a minimal HyperfitsSolutionGroup for process_phase_fits/process_gain_fits_for_db tests.

    Bypasses __init__/load() (no real FITS files); sets exactly the
    attributes those methods and their dependencies (get_solns_both,
    combined_tile_flags, _find_ref_tile_idx) need.

    Tile ID 1 (index 0) is always the reference tile and is given an
    identity Jones matrix (gx=gy=1, Dx=Dy=0), so get_solns_both's
    reference normalisation is a mathematical no-op and the synthetic
    ramps given to other tiles pass through get_solns_both unchanged --
    this is what lets these tests construct "already reference-normalised"
    data directly, matching how the pre-refactor tests fed such arrays
    straight into the (now-removed) free process_phase_fits/
    process_gain_fits_for_db functions.

    Args:
        flavors: Optional per-tile receiver flavour, as a list of length
            n_tiles (index 0 = tile ID 1). Defaults to "RRI" for every
            tile, matching every test written before flavour-scoped
            outlier rejection existed.
    """
    if flagged_ids is None:
        flagged_ids = []
    if flavors is None:
        flavors = ["RRI"] * n_tiles
    group = HyperfitsSolutionGroup.__new__(HyperfitsSolutionGroup)
    tile_ids = np.arange(1, n_tiles + 1)
    group.metafits_tiles_df = pd.DataFrame(
        {
            "name": [f"Tile{i:03d}" for i in tile_ids],
            "id": tile_ids,
            "flag": [i in flagged_ids for i in tile_ids],
            "rx": [(i - 1) // 8 + 1 for i in tile_ids],
            "slot": [(i - 1) % 8 + 1 for i in tile_ids],
            "flavor": flavors,
        }
    )
    group.solns = []  # combined_tile_flags then reduces to just the metafits flag column

    freqs = np.linspace(140e6, 170e6, n_chanblocks).astype(np.int_)
    xx_ramp = _make_phase_ramp(freqs, xx_length_m, intercept_rad=0.3)
    yy_ramp = _make_phase_ramp(freqs, yy_length_m, intercept_rad=0.3)

    jones = np.zeros((n_tiles, n_chanblocks, 2, 2), dtype=np.complex128)
    jones[:, :, 0, 0] = xx_ramp
    jones[:, :, 1, 1] = yy_ramp
    jones[0, :, 0, 0] = 1.0 + 0j  # reference tile: identity Jones
    jones[0, :, 1, 1] = 1.0 + 0j

    group.jones = [jones]
    group.all_chanblocks_hz = [freqs]
    group.chanblocks_per_coarse = _FIT_CHANBLOCKS_PER_COARSE

    return group


def _patched_uniform_weights(n_chanblocks):
    """Context manager patching HyperfitsSolutionGroup.weights to return all-1.0.

    process_phase_fits/process_gain_fits_for_db read self.weights internally;
    the fake group above has no real solns to derive it from, so this
    patches the property directly for the duration of a test.
    """
    return patch.object(
        HyperfitsSolutionGroup,
        "weights",
        new_callable=PropertyMock,
        return_value=np.ones(n_chanblocks),
    )


def test_process_phase_fits_returns_dataframe_with_correct_columns():
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[3])
    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        result = group.process_phase_fits(refant_name="Tile001", phase_fit_niter=1)
    assert isinstance(result, pd.DataFrame)
    assert _EXPECTED_PHASE_COLS.issubset(set(result.columns))


def test_process_phase_fits_skips_flagged_tile():
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[3])
    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        result = group.process_phase_fits(refant_name="Tile001", phase_fit_niter=1)
    assert 3 not in result["tile_id"].values


def test_process_phase_fits_has_xx_and_yy_rows():
    """2 unflagged tiles (1 and 2) x 2 pols = 4 rows."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[3])
    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        result = group.process_phase_fits(refant_name="Tile001", phase_fit_niter=1)
    assert len(result) == 4
    assert set(result["pol"].unique()) == {"XX", "YY"}


def test_process_phase_fits_bad_solution_skipped_not_raised():
    """A tile with all-NaN solutions should be skipped; others should still appear."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[])
    # Corrupt tile ID 2 (index 1, not the reference tile) with NaN.
    group.jones[0][1, :, :, :] = np.nan + 1j * np.nan
    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        result = group.process_phase_fits(refant_name="Tile001", phase_fit_niter=1)
    assert 2 not in result["tile_id"].values
    assert 1 in result["tile_id"].values
    assert 3 in result["tile_id"].values


def test_process_gain_fits_for_db_returns_dataframe_with_correct_columns():
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[3])
    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        result = group.process_gain_fits_for_db(refant_name="Tile001")
    assert isinstance(result, pd.DataFrame)
    assert _EXPECTED_GAIN_COLS.issubset(set(result.columns))


def test_process_gain_fits_for_db_skips_flagged_tile():
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[3])
    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        result = group.process_gain_fits_for_db(refant_name="Tile001")
    assert 3 not in result["tile_id"].values


def test_process_gain_fits_for_db_has_xx_and_yy_rows():
    """2 unflagged tiles (1 and 2) x 2 pols = 4 rows."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[3])
    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        result = group.process_gain_fits_for_db(refant_name="Tile001")
    assert len(result) == 4
    assert set(result["pol"].unique()) == {"XX", "YY"}


def test_process_gain_fits_for_db_gains_list_length():
    """Each row's gains list should have length == n_coarse."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[3])
    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        result = group.process_gain_fits_for_db(refant_name="Tile001")
    n_coarse = _FIT_N_CHANBLOCKS // _FIT_CHANBLOCKS_PER_COARSE
    for gains in result["gains"]:
        assert len(gains) == n_coarse


# ===========================================================================
# HyperfitsSolutionGroup.flag_amplitude_outliers
# ===========================================================================


def test_flag_amplitude_outliers_catches_injected_spike():
    """A single-channel amplitude spike on one tile gets NaN'd and tagged."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[])
    group.channel_flag_reasons = [np.full((3, _FIT_N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]

    # _make_fake_group's ramp has unit amplitude everywhere (zero intrinsic
    # scatter), which is unrealistically clean: it drives the MAD in
    # iterative_poly_clip to near-zero, amplifying ordinary floating-point
    # noise into spurious "outliers" elsewhere. Real gain data always has
    # some scatter, so add a small amount here to avoid that degenerate case.
    rng = np.random.default_rng(7)
    noise = 1.0 + rng.normal(scale=0.01, size=_FIT_N_CHANBLOCKS)
    group.jones[0][1, :, 0, 0] *= noise
    group.jones[0][1, :, 1, 1] *= noise

    spike_chan = 10
    group.jones[0][1, spike_chan, 0, 0] = 1000.0 + 0j  # huge gx spike, tile index 1

    group.flag_amplitude_outliers(poly_degree=2, mad_residual_threshold=5.0)

    assert np.isnan(group.jones[0][1, spike_chan, 0, 0])
    assert group.channel_flag_reasons[0][1, spike_chan] & ChannelFlagReason.AMPLITUDE_OUTLIER
    # An unaffected channel on the same tile is untouched.
    assert not np.isnan(group.jones[0][1, 0, 0, 0])
    assert group.channel_flag_reasons[0][1, 0] == ChannelFlagReason.NONE


def test_flag_amplitude_outliers_leaves_clean_tile_untouched():
    """A tile with no injected outlier is not modified or flagged."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[])
    group.channel_flag_reasons = [np.full((3, _FIT_N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    original = group.jones[0][2].copy()  # tile index 2, no corruption injected

    group.flag_amplitude_outliers(poly_degree=2, mad_residual_threshold=5.0)

    assert np.array_equal(group.jones[0][2], original, equal_nan=True)
    assert np.all(group.channel_flag_reasons[0][2] == ChannelFlagReason.NONE)


def test_flag_amplitude_outliers_stores_fit_and_band():
    """amplitude_fit/amplitude_band are populated, one dict per file."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[])
    group.channel_flag_reasons = [np.full((3, _FIT_N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]

    group.flag_amplitude_outliers(poly_degree=2, mad_residual_threshold=5.0)

    assert len(group.amplitude_fit) == 1
    assert set(group.amplitude_fit[0].keys()) == {"gx", "gy"}
    assert group.amplitude_fit[0]["gx"].shape == (3, _FIT_N_CHANBLOCKS)
    assert len(group.amplitude_band) == 1
    assert set(group.amplitude_band[0].keys()) == {"gx", "gy"}


# ===========================================================================
# HyperfitsSolutionGroup.flag_phase_outliers
# ===========================================================================


def test_flag_phase_outliers_catches_noisy_tile():
    """A tile with pure noise (no coherent phase ramp) is a population outlier.

    Uses 10 tiles rather than a handful: with too few "good" tiles, a
    single severe outlier can inflate its own population mean/std enough
    to dodge the threshold (self-referential inflation) -- confirmed this
    is purely a small-N artifact of the test, not a real limitation for
    actual observations (128-256 tiles), by checking empirically that the
    same injected outlier is reliably caught at n_tiles=10 and n_tiles=20
    but not at n_tiles=5.

    Also adds small phase noise to the "good" tiles: _make_fake_group's
    ramp fits almost perfectly (chi2dof ~1e-11), which is unrealistically
    clean and lets population statistics be dominated by floating-point
    noise rather than genuine quality differences -- confirmed empirically
    that this let a good tile randomly cross the threshold before adding
    this noise. Real gain/phase data always has some residual scatter.
    """
    n_tiles = 10
    group = _make_fake_group(n_tiles=n_tiles, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[])
    group.tile_flag_reasons = np.full(n_tiles, TileFlagReason.NONE, dtype=object)

    rng = np.random.default_rng(1)
    for i in range(1, n_tiles - 1):  # good tiles: everyone except the ref (0) and the noisy one (last)
        phase_noise = rng.normal(scale=0.02, size=_FIT_N_CHANBLOCKS)
        group.jones[0][i, :, 0, 0] *= np.exp(1j * phase_noise)
        group.jones[0][i, :, 1, 1] *= np.exp(1j * phase_noise)

    noisy = rng.normal(size=_FIT_N_CHANBLOCKS) + 1j * rng.normal(size=_FIT_N_CHANBLOCKS)
    group.jones[0][-1, :, 0, 0] = noisy  # last tile: incoherent gx
    group.jones[0][-1, :, 1, 1] = noisy  # incoherent gy too

    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        group.flag_phase_outliers(refant_name="Tile001", phase_fit_niter=1, nstd=2.0)

    assert group.tile_flag_reasons[-1] & TileFlagReason.PHASE_OUTLIER
    assert np.all(np.isnan(group.jones[0][-1]))
    # A tile with a clean ramp (index 1) is untouched.
    assert group.tile_flag_reasons[1] == TileFlagReason.NONE
    assert not np.any(np.isnan(group.jones[0][1]))


def test_flag_phase_outliers_flavor_scoping_avoids_cross_flavor_false_positive():
    """A tile that's normal for its own flavour isn't flagged just because another flavour is tighter.

    Regression/feature test for flavour-scoped outlier rejection: builds a
    group with a large, very tight-fitting "SHAO" population and a
    smaller, moderately-noisier-but-internally-consistent "RRI"
    population -- mirroring the real observation this was based on,
    where SHAO's tight, numerically-dominant population would otherwise
    set a pooled threshold too strict for RRI's naturally wider spread.

    Confirms two things against the same data:
      1. flag_phase_outliers (flavour-scoped) does NOT flag the RRI tiles
         -- they're unremarkable within their own flavour's population.
      2. The old pol-only pooled reject_outliers call (group_cols=("pol",),
         the default) WOULD have flagged them -- confirming this is a
         real behavioural difference, not a vacuous test.
    """
    n_tiles = 20
    # Index 0 = reference tile (always). Indices 1-14 (14 tiles) = a
    # tight-fitting "SHAO" population. Indices 15-19 (5 tiles) = a
    # moderately-noisier-but-consistent "RRI" population -- normal for
    # RRI, but well outside SHAO's tight spread.
    flavors = ["SHAO"] * n_tiles
    for i in range(15, n_tiles):
        flavors[i] = "RRI"

    group = _make_fake_group(n_tiles=n_tiles, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[], flavors=flavors)
    group.tile_flag_reasons = np.full(n_tiles, TileFlagReason.NONE, dtype=object)

    rng = np.random.default_rng(2)
    for i in range(1, 15):  # SHAO: very tight
        phase_noise = rng.normal(scale=0.005, size=_FIT_N_CHANBLOCKS)
        group.jones[0][i, :, 0, 0] *= np.exp(1j * phase_noise)
        group.jones[0][i, :, 1, 1] *= np.exp(1j * phase_noise)
    for i in range(15, n_tiles):  # RRI: moderately noisier, but consistent amongst themselves
        phase_noise = rng.normal(scale=0.05, size=_FIT_N_CHANBLOCKS)
        group.jones[0][i, :, 0, 0] *= np.exp(1j * phase_noise)
        group.jones[0][i, :, 1, 1] *= np.exp(1j * phase_noise)

    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        # 1. Flavour-scoped (actual production behaviour): RRI tiles
        # should be untouched.
        group.flag_phase_outliers(refant_name="Tile001", phase_fit_niter=1, nstd=3.0)

        # 2. For comparison, the old pol-only pooled call on a fresh
        # (unflagged) copy of the same phase fits data.
        pooled_phase_fits = group.process_phase_fits(refant_name="Tile001", phase_fit_niter=1)

    pooled_phase_fits = reject_outliers(pooled_phase_fits, "chi2dof", nstd=3.0)
    pooled_phase_fits = reject_outliers(pooled_phase_fits, "sigma_resid", nstd=3.0)
    rri_tile_ids = {i + 1 for i in range(15, n_tiles)}
    pooled_rri_outliers = set(
        pooled_phase_fits.loc[
            pooled_phase_fits["outlier"] & pooled_phase_fits["tile_id"].isin(rri_tile_ids), "tile_id"
        ]
    )
    assert pooled_rri_outliers, (
        "expected the pol-only pooled threshold to flag at least one RRI tile "
        "as a false positive -- if not, this test no longer demonstrates a "
        "real behavioural difference and should be revisited"
    )

    for i in range(15, n_tiles):
        assert group.tile_flag_reasons[i] == TileFlagReason.NONE
        assert not np.any(np.isnan(group.jones[0][i]))


def test_flag_phase_outliers_stores_phase_fits():
    """phase_fits is populated with an 'outlier' column after the call."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=_FIT_N_CHANBLOCKS, flagged_ids=[])
    group.tile_flag_reasons = np.full(3, TileFlagReason.NONE, dtype=object)

    with _patched_uniform_weights(_FIT_N_CHANBLOCKS):
        group.flag_phase_outliers(refant_name="Tile001", phase_fit_niter=1, nstd=3.0)

    assert group.phase_fits is not None
    assert "outlier" in group.phase_fits.columns


# ===========================================================================
# HyperfitsSolutionGroup.flag_mostly_bad_tiles
# ===========================================================================


def test_flag_mostly_bad_tiles_promotes_when_threshold_exceeded():
    """A tile with >= threshold fraction of bad channels is promoted to fully flagged."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=10, flagged_ids=[])
    group.tile_flag_reasons = np.full(3, TileFlagReason.NONE, dtype=object)
    reasons = np.full((3, 10), ChannelFlagReason.NONE, dtype=object)
    reasons[1, :6] = ChannelFlagReason.NON_CONVERGED  # 6/10 = 60%, tile index 1
    group.channel_flag_reasons = [reasons]

    group.flag_mostly_bad_tiles(threshold=0.5)

    assert group.tile_flag_reasons[1] & TileFlagReason.MOSTLY_BAD_CHANNELS
    assert np.all(np.isnan(group.jones[0][1]))
    assert group.tile_flag_reasons[0] == TileFlagReason.NONE


def test_flag_mostly_bad_tiles_leaves_below_threshold_tile_untouched():
    """A tile below the threshold fraction is left as partially flagged."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=10, flagged_ids=[])
    group.tile_flag_reasons = np.full(3, TileFlagReason.NONE, dtype=object)
    reasons = np.full((3, 10), ChannelFlagReason.NONE, dtype=object)
    reasons[1, :4] = ChannelFlagReason.NON_CONVERGED  # 4/10 = 40%, below threshold
    group.channel_flag_reasons = [reasons]
    original = group.jones[0][1].copy()

    group.flag_mostly_bad_tiles(threshold=0.5)

    assert group.tile_flag_reasons[1] == TileFlagReason.NONE
    assert np.array_equal(group.jones[0][1], original, equal_nan=True)


def test_flag_mostly_bad_tiles_skips_already_tile_flagged():
    """A tile already tile-flagged for another reason is not double-processed."""
    group = _make_fake_group(n_tiles=3, n_chanblocks=10, flagged_ids=[])
    group.tile_flag_reasons = np.full(3, TileFlagReason.NONE, dtype=object)
    group.tile_flag_reasons[1] = TileFlagReason.METAFITS
    reasons = np.full((3, 10), ChannelFlagReason.NONE, dtype=object)
    reasons[1, :6] = ChannelFlagReason.NON_CONVERGED
    group.channel_flag_reasons = [reasons]

    group.flag_mostly_bad_tiles(threshold=0.5)

    # Still just METAFITS -- MOSTLY_BAD_CHANNELS was not additionally OR'd in.
    assert group.tile_flag_reasons[1] == TileFlagReason.METAFITS


# ===========================================================================
# HyperfitsSolutionGroup.commit
# ===========================================================================


def test_commit_writes_jones_and_digital_gains(tmp_path):
    """commit() backs up, writes the final jones, and adds DigitalGains."""
    import shutil

    from astropy.io import fits as astropy_fits

    metafits_path = str(tmp_path / "metafits.fits")
    soln_path = str(tmp_path / "solutions.fits")
    shutil.copy2(METAFITS_PATH, metafits_path)
    shutil.copy2(SOLUTIONS_PATH, soln_path)

    metafits = Metafits(metafits_path)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(soln_path)])
    group.load()
    group.apply_tile_flags()
    group.enforce_whole_jones_nan()

    backup_paths = group.commit(metafits.mwalib_context)
    assert backup_paths[0] is not None
    assert group.jones is not None

    assert backup_paths == [soln_path.replace(".fits", ".original.fits")]
    assert os.path.exists(backup_paths[0])

    with astropy_fits.open(soln_path) as hdul:
        assert "DigitalGains" in hdul["TILES"].columns.names

    reread_jones = HyperfitsSolution(soln_path).get_jones()
    assert np.allclose(reread_jones, group.jones[0], equal_nan=True)


def test_commit_backup_preserves_pristine_original(tmp_path):
    """The backup made by commit() reflects the pre-flagging state, not the final one."""
    import shutil

    metafits_path = str(tmp_path / "metafits.fits")
    soln_path = str(tmp_path / "solutions.fits")
    shutil.copy2(METAFITS_PATH, metafits_path)
    shutil.copy2(SOLUTIONS_PATH, soln_path)

    metafits = Metafits(metafits_path)
    group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(soln_path)])
    group.load()
    assert group.jones is not None
    pristine = group.jones[0].copy()
    group.apply_tile_flags()  # this will NaN at least the metafits-flagged tiles

    backup_paths = group.commit(metafits.mwalib_context)
    assert backup_paths[0] is not None

    backup_jones = HyperfitsSolution(backup_paths[0]).get_jones()
    assert np.allclose(backup_jones, pristine, equal_nan=True)
