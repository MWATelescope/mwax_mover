"""
Tests for the mwax_calvin_plots.py module.

Covers:
  - build_tile_stats_rows / write_tile_stats_table

NOTE: the matplotlib-heavy plotting functions in this module
(plot_combined_gains, plot_outlier_gains, plot_debug_phase_fits, etc.) are
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
    _fully_flagged_channel_summary_text,
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


def _make_phase_fits(tile_ids, chi2dof=0.01, sigma_resid=0.05, outliers=None):
    """Build a minimal phase_fits DataFrame with XX/YY rows for each tile.

    Args:
        outliers: Optional set of (tile_id, pol) tuples to mark as
            population outliers. If None (default), no 'outlier' column
            is added at all -- matching a bare process_phase_fits()
            result, and every existing caller of this helper.
    """
    rows = []
    for tile_id in tile_ids:
        for pol in ("XX", "YY"):
            row = {"tile_id": tile_id, "pol": pol, "chi2dof": chi2dof, "sigma_resid": sigma_resid}
            if outliers is not None:
                row["outlier"] = (tile_id, pol) in outliers
            rows.append(row)
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
# _fully_flagged_channel_summary_text
# ===========================================================================


def test_fully_flagged_channel_summary_text_percentage_and_breakdown():
    """Matches the requested format: '{pct}% Good (n/n)' then a
    comma-separated, count-first breakdown of every distinct reason
    present, using the actual mad_residual_threshold for the MAD label."""
    reasons = np.full((_N_TILES, 20), ChannelFlagReason.NONE, dtype=object)
    reasons[0, :4] = ChannelFlagReason.PRE_EXISTING_NAN
    reasons[0, 4:10] = ChannelFlagReason.GAIN_MAX_CUTOFF
    reasons[0, 10:11] = ChannelFlagReason.AMPLITUDE_OUTLIER
    # Remaining 9/20 channels are NONE -- "good" on their own, just swept
    # in by whatever whole-tile promotion made this tile fully flagged.

    text = _fully_flagged_channel_summary_text(0, reasons, mad_residual_threshold=10.0)
    lines = text.split("\n")

    assert lines[0] == "45% Good (9/20)"
    assert lines[1] == "4 NaN, 6 above gain cutoff, 1 outside 10 MAD"


def test_fully_flagged_channel_summary_text_zero_good_when_all_individually_flagged():
    """A tile that's 100% individually flagged (not just swept in by
    promotion) correctly shows 0% good."""
    reasons = np.full((_N_TILES, 10), ChannelFlagReason.NONE, dtype=object)
    reasons[0, :] = ChannelFlagReason.GAIN_MAX_CUTOFF

    text = _fully_flagged_channel_summary_text(0, reasons, mad_residual_threshold=10.0)

    assert text == "0% Good (0/10)\n10 above gain cutoff"


def test_fully_flagged_channel_summary_text_uses_actual_mad_threshold():
    """The MAD label reflects whatever threshold was actually used, not a
    hardcoded value."""
    reasons = np.full((_N_TILES, 10), ChannelFlagReason.NONE, dtype=object)
    reasons[0, :3] = ChannelFlagReason.AMPLITUDE_OUTLIER

    text = _fully_flagged_channel_summary_text(0, reasons, mad_residual_threshold=5.0)

    assert "3 outside 5 MAD" in text


def test_fully_flagged_channel_summary_text_only_reports_reasons_present():
    """A reason with zero channels doesn't appear in the breakdown at all."""
    reasons = np.full((_N_TILES, 10), ChannelFlagReason.NONE, dtype=object)
    reasons[0, :10] = ChannelFlagReason.PRE_EXISTING_NAN

    text = _fully_flagged_channel_summary_text(0, reasons, mad_residual_threshold=10.0)

    assert text == "0% Good (0/10)\n10 NaN"
    assert "gain cutoff" not in text
    assert "MAD" not in text


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


def test_flavor_field_comes_from_metafits_tiles_df():
    """Every row's flavor matches the tile's flavor in metafits_tiles_df,
    formatted without the ReceiverType. enum-class prefix."""
    group = _make_stats_group()
    group.metafits_tiles_df["flavor"] = ["SHAO", "RRI", "NI"]
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    phase_fits = _make_phase_fits(group.metafits_tiles_df["id"].to_numpy())

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    assert [r["flavor"] for r in rows] == ["SHAO", "RRI", "NI"]


def test_flavor_field_strips_receiver_type_enum_prefix():
    """A flavor value stringified as "ReceiverType.SHAO" (mwalib's actual
    enum repr) displays as just "SHAO"."""
    group = _make_stats_group()
    group.metafits_tiles_df["flavor"] = "ReceiverType.SHAO"
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    phase_fits = _make_phase_fits(group.metafits_tiles_df["id"].to_numpy())

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    assert rows[0]["flavor"] == "SHAO"


def test_phase_outlier_field_blank_when_neither_pol_is_an_outlier():
    """No outlier column produced at all when neither XX nor YY is flagged."""
    group = _make_stats_group()
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    tile_ids = group.metafits_tiles_df["id"].to_numpy()
    phase_fits = _make_phase_fits(tile_ids, outliers=set())

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    assert all(r["phase_outlier"] == "" for r in rows)


def test_phase_outlier_field_reports_single_pol():
    """A tile whose XX (but not YY) fit is an outlier reports just 'XX'."""
    group = _make_stats_group()
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    tile_ids = group.metafits_tiles_df["id"].to_numpy()
    phase_fits = _make_phase_fits(tile_ids, outliers={(tile_ids[0], "XX")})

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    assert rows[0]["phase_outlier"] == "XX"
    assert rows[1]["phase_outlier"] == ""


def test_phase_outlier_field_reports_both_pols():
    """A tile whose XX and YY fits are both outliers reports 'XX,YY'."""
    group = _make_stats_group()
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    tile_ids = group.metafits_tiles_df["id"].to_numpy()
    phase_fits = _make_phase_fits(tile_ids, outliers={(tile_ids[0], "XX"), (tile_ids[0], "YY")})

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    assert rows[0]["phase_outlier"] == "XX,YY"


def test_phase_outlier_field_does_not_affect_flagging_or_status():
    """A phase-outlier tile is still reported as PARTIAL/OK, per the
    permanent policy that phase-outlier status is advisory only and
    never causes flagging (see HyperfitsSolutionGroup.detect_phase_outliers).
    """
    group = _make_stats_group()
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    tile_ids = group.metafits_tiles_df["id"].to_numpy()
    phase_fits = _make_phase_fits(tile_ids, outliers={(tile_ids[0], "XX"), (tile_ids[0], "YY")})

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    assert rows[0]["fully_flagged"] is False
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


def test_write_tile_stats_table_includes_flavor_column():
    """The Flavor header and each tile's flavor value both appear in the output."""
    group = _make_stats_group()
    group.metafits_tiles_df["flavor"] = ["SHAO", "RRI", "NI"]
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    phase_fits = _make_phase_fits(group.metafits_tiles_df["id"].to_numpy())

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    buf = io.StringIO()
    write_tile_stats_table("TEST TABLE", rows, buf)
    output = buf.getvalue()

    assert "Flavor" in output
    assert "SHAO" in output
    assert "RRI" in output
    assert "NI" in output


def test_write_tile_stats_table_includes_phoutlier_column():
    """The PhOutlier header appears, and a flagged pol shows up against
    the right tile without affecting its Status column."""
    group = _make_stats_group()
    jones = [_make_jones()]
    channel_reasons = [np.full((_N_TILES, _N_CHANBLOCKS), ChannelFlagReason.NONE, dtype=object)]
    tile_reasons = np.full(_N_TILES, TileFlagReason.NONE, dtype=object)
    tile_bad_mask = tile_reasons != TileFlagReason.NONE
    tile_ids = group.metafits_tiles_df["id"].to_numpy()
    phase_fits = _make_phase_fits(tile_ids, outliers={(tile_ids[0], "XX")})

    rows = build_tile_stats_rows(group, jones, tile_bad_mask, tile_reasons, channel_reasons, phase_fits)

    buf = io.StringIO()
    write_tile_stats_table("TEST TABLE", rows, buf)
    output = buf.getvalue()

    assert "PhOutlier" in output
    tile001_line = next(line for line in output.splitlines() if "Tile001" in line.split())
    assert "XX" in tile001_line
    assert "OK" in tile001_line  # phase-outlier status never affects Status
