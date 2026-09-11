"""Tests for calvin.hyperfits_solution's HyperfitsSolution.

Covers: get_jones / chanblock_converged / baseline_tile_flags / write_jones.
See tests/calvin/test_hyperfits_solution_group.py for HyperfitsSolutionGroup.
Split out of this file's former single test_hyperdrive.py -- see
docs/HYPERDRIVE_PARALLELISM.md Phase 1.
"""

import os

import numpy as np
import pytest

from tests_common import data_path

from mwax_mover.calvin.hyperfits_solution import HyperfitsSolution

# A real fixture whose filename parse_solution_channels() can parse -- same
# file used by TestSharedHduHelpersAgreeAcrossCallers in test014.
SOLUTIONS_PATH = data_path("1391522232", "1391522232_ch89_solutions.fits")


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
