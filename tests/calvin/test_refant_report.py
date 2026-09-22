"""Tests for calvin.refant_report.format_refant_selection_report.

Focus: the phase-scatter gate column/label after the switch from a phase
chi2dof range gate to an upper-bound sigma_resid gate.
"""

import numpy as np

from mwax_mover.calvin.refant_report import ScoredTile, format_refant_selection_report
from mwax_mover.constants import REFTILE_PHASE_SIGMA_RESID_MAX


def _details(sigma_resid_xx: float, sigma_resid_yy: float, sigma_resid_ok: bool) -> dict:
    """A gate_details dict with every gate passing except (optionally) sigma_resid."""
    return {
        "phase_quality_ok": True,
        "phase_sigma_resid_ok": sigma_resid_ok,
        "gain_quality_ok": True,
        "dipole_ok": True,
        "nan_ok": True,
        "n_good_dipoles": 32,
        "n_nan_channels": 0,
        "nan_fraction": 0.0,
        "phase_quality_xx": 0.9,
        "phase_quality_yy": 0.9,
        "phase_sigma_resid_xx": sigma_resid_xx,
        "phase_sigma_resid_yy": sigma_resid_yy,
        "gain_quality_xx": 0.9,
        "gain_quality_yy": 0.9,
    }


def _render(scored, gate_details):
    return format_refant_selection_report(
        scored=scored,
        tile_names=np.array(["LBF4", "Tile042"]),
        tile_ids=np.array([2044, 42]),
        tile_ants=np.array([243, 25]),
        gate_details=gate_details,
        median_length={"XX": -0.45, "YY": -0.46},
        bootstrap_name="Tile011",
        bootstrap_ant=0,
        dipole_gains_available=True,
        total_chanblocks=768,
    )


def test_report_uses_sigma_resid_column_and_label():
    """The table should advertise the sigma_resid gate and SRes columns, not Chi2."""
    scored = [ScoredTile(0, 0, 0.0, 0, 0.03, 2044)]
    report = _render(scored, {2044: _details(0.05, 0.06, True)})

    assert f"phase_sigma_resid<={REFTILE_PHASE_SIGMA_RESID_MAX}" in report
    assert "SRes_XX" in report and "SRes_YY" in report
    assert "Chi2_XX" not in report and "chi2dof" not in report
    # The sigma_resid values render at 4dp.
    assert "0.0500" in report and "0.0600" in report


def test_report_flags_sigma_resid_failure():
    """A tile failing only the sigma_resid gate shows '.' in the 2nd gate slot."""
    scored = [ScoredTile(1, 0, 0.44, 0, 0.03, 2044)]
    # XX scatter past the gate; gate marked failed.
    report = _render(scored, {2044: _details(0.42, 0.05, False)})

    # Gate string is phase_quality, sigma_resid, gain_quality, dipole, nan.
    # Only sigma_resid fails here -> "P.PPP".
    assert "P.PPP" in report
    assert "0.4200" in report
