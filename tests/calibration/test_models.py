"""Tests for calibration.models: PhaseFitInfo/GainFitInfo.nan()/default().

Split out of the former test014_calvin_utils.py (docs/RESTRUCTURE.md
test-tree reorg).
"""

import numpy as np
import pytest

from mwax_mover.calibration.models import GainFitInfo, MWA_NUM_COARSE_CHANS, PhaseFitInfo


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
