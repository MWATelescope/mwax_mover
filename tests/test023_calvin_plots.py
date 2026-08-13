"""
Tests for the mwax_calvin_plots.py module.

Covers:
  - build_tile_stats_rows / write_tile_stats_table

NOTE: the matplotlib-heavy plotting functions in this module
(plot_combined_gains, plot_outlier_gains, debug_phase_fits, etc.) are
exercised indirectly via tests/test020_calvin_solutions.py's real-fixture
integration tests and manual smoke tests during development, rather than
duplicated here as isolated unit tests -- rendering full figures against
synthetic data adds little beyond what those integration tests already
cover, and would be slow to run per-test.
"""

import io

import numpy as np
import pandas as pd
import pytest

from mwax_mover.mwax_calvin_plots import (
    _channel_reason_counts_text,
    build_tile_stats_rows,
    write_tile_stats_table,
)
from mwax_mover.mwax_hyperdrive_solutions import (
    ChannelFlagReason,
    HyperfitsSolutionGroup,
    TileFlagReason,
)

_N_TILES = 3
_N_CHANBLOCKS = 10


def _make_stats_group(tile_flag_reasons=None):
    """Build a minimal HyperfitsSolutionGroup for build_tile_stats_rows tests.

    Bypasses __init__ entirely (no real FITS files); only sets
    metafits_tiles_df, which is all build_tile_stats_rows needs from the
    group itself (everything else is passed in explicitly as snapshot
    arguments).
    """
    group = HyperfitsSolutionGroup.__new__(HyperfitsSolutionGroup)
    tile_ids = np.arange(1, _N_TILES + 1)
    group.metafits_tiles_df = pd.DataFrame(
        {
            "name": [f"Tile{i:03d}" for i in tile_ids],
            "id": tile_ids,
            "flag": [False] * _N_TILES,
            "rx": [1] * _N_TILES,
            "slot": [1] * _N_TILES,
            "flavor": "RRI",
        }
    )
    return group


def _make_jones(n_tiles=_N_TILES, n_chanblocks=_N_CHANBLOCKS, amp=1.0):
    """Build a synthetic Jones array with constant gx/gy amplitude."""
    jones = np.zeros((n_tiles, n_chanblocks, 2, 2), dtype=np.complex128)
    jones[:, :, 0, 0] = amp
    jones[:, :, 1, 1] = amp
    return jones


def _make_phase_fits(tile_ids, chi2dof=0.01, sigma_resid=0.05):
    """Build a minimal phase_fits DataFrame with XX/YY rows for each tile."""
    rows = []
    for tile_id in tile_ids:
        for pol in ("XX", "YY"):
            rows.append({"tile_id": tile_id, "pol": pol, "chi2dof": chi2dof, "sigma_resid": sigma_resid})
    return pd.DataFrame(rows)


# ===========================================================================
# _channel_reason_counts_text
# ===========================================================================


def test_channel_reason_counts_text_counts_across_files():
    """Counts accumulate across multiple files, one string per reason."""
    reasons_file1 = np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)
    reasons_file1[0, :3] = ChannelFlagReason.AMPLITUDE_OUTLIER
    reasons_file2 = np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)
    reasons_file2[0, :2] = ChannelFlagReason.AMPLITUDE_OUTLIER
    reasons_file2[0, 5] = ChannelFlagReason.NON_CONVERGED

    text = _channel_reason_counts_text(0, [reasons_file1, reasons_file2])

    assert "AMPLITUDE_OUTLIER(5ch)" in text
    assert "NON_CONVERGED(1ch)" in text


def test_channel_reason_counts_text_empty_when_no_reasons():
    """No reasons set anywhere for this tile -> empty string."""
    reasons = np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)
    assert _channel_reason_counts_text(0, [reasons]) == ""


# ===========================================================================
# build_tile_stats_rows
# ===========================================================================


def test_whole_tile_flag_shows_100_percent_not_stale_channel_fraction():
    """Regression test: a whole-tile flag (e.g. PHASE_OUTLIER) means every
    channel is bad, even though it never touches channel_reasons directly.

    Found via a real end-to-end pipeline run: a phase-outlier tile with
    16.4% of channels already marked PRE_EXISTING_NAN showed "16.4%"
    flagged instead of "100.0%", because flagged_pct was computed purely
    from channel_reasons, which flag_phase_outliers/apply_tile_flags never
    update (they NaN self.jones and set tile_flag_reasons directly).
    """
    group = _make_stats_group()
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    channel_reasons[0][1, :2] = ChannelFlagReason.PRE_EXISTING_NAN  # only 20% of tile 1's channels

    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_reasons[1] = TileFlagReason.PHASE_OUTLIER  # but the whole tile is flagged
    tile_bad_mask = tile_reasons != TileFlagReason.NONE

    phase_fits = _make_phase_fits(group.metafits_tiles_df["id"].to_numpy())

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    tile1_row = rows[1]
    assert tile1_row["fully_flagged"] is True
    assert tile1_row["flagged_pct"] == pytest.approx(100.0)
    assert tile1_row["n_bad_channels"] == tile1_row["n_total_channels"]


def test_partial_tile_reports_actual_channel_fraction():
    """A tile with no whole-tile flag reports the real per-channel fraction."""
    group = _make_stats_group()
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    channel_reasons[0][0, :3] = ChannelFlagReason.AMPLITUDE_OUTLIER  # 3/10 = 30%

    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE

    phase_fits = _make_phase_fits(group.metafits_tiles_df["id"].to_numpy())

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    assert rows[0]["fully_flagged"] is False
    assert rows[0]["flagged_pct"] == pytest.approx(30.0)
    assert "AMPLITUDE_OUTLIER(3ch)" in rows[0]["channel_reasons"]


def test_fully_flagged_tile_has_no_amplitude_stats():
    """A fully-flagged tile reports no gx/gy stats (NaN), just the reason."""
    group = _make_stats_group()
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]

    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_reasons[2] = TileFlagReason.METAFITS
    tile_bad_mask = tile_reasons != TileFlagReason.NONE

    phase_fits = _make_phase_fits(group.metafits_tiles_df["id"].to_numpy())

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    assert np.isnan(rows[2]["gx_median"])
    assert np.isnan(rows[2]["gy_median"])
    assert rows[2]["tile_reason"] == "flagged in metafits"


def test_good_tile_reports_amplitude_and_phase_stats():
    """A tile with no flags at all reports real amplitude and phase stats."""
    group = _make_stats_group()
    jones = [_make_jones(amp=2.0)]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    phase_fits = _make_phase_fits(group.metafits_tiles_df["id"].to_numpy(), chi2dof=0.02, sigma_resid=0.03)

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    assert rows[0]["gx_median"] == pytest.approx(2.0)
    assert rows[0]["gy_median"] == pytest.approx(2.0)
    assert rows[0]["chi2dof_x"] == pytest.approx(0.02)
    assert rows[0]["sigma_resid_y"] == pytest.approx(0.03)
    assert rows[0]["flagged_pct"] == pytest.approx(0.0)


# ===========================================================================
# write_tile_stats_table
# ===========================================================================


def test_write_tile_stats_table_produces_readable_output():
    """Smoke test: the table renders without error and contains expected content."""
    group = _make_stats_group()
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_reasons[2] = TileFlagReason.METAFITS
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    phase_fits = _make_phase_fits(group.metafits_tiles_df["id"].to_numpy())

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    buf = io.StringIO()
    write_tile_stats_table("TEST TABLE", rows, buf)
    output = buf.getvalue()

    assert "TEST TABLE" in output
    assert "Tile001" in output
    assert "FULLY_FLAGGED" in output
    assert "flagged in metafits" in output
    assert "Flagged" in output  # footer summary line
