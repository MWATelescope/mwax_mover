"""
Tests for the mwax_calvin_plots.py module.

Covers:
  - build_tile_stats_rows / write_tile_stats_table
  - plot_debug_phase_fits byte-order handling (regression test only, see below)

NOTE: the matplotlib-heavy plotting functions in this module
(plot_combined_gains, plot_outlier_gains, plot_debug_phase_fits, etc.) are
otherwise exercised indirectly via tests/test020_calvin_solutions.py's
real-fixture integration tests and manual smoke tests during development,
rather than duplicated here as isolated unit tests -- rendering full
figures against synthetic data adds little beyond what those integration
tests already cover, and would be slow to run per-test.

The one exception is test_plot_debug_phase_fits_handles_byteswapped_input
below: test020/test022's integration tests all patch plot_debug_phase_fits
out entirely, so nothing else in the suite actually calls it with
byte-swapped (e.g. real FITS-derived, big-endian) input -- exactly the
input that triggered a numpy-2.0-incompatible ndarray.newbyteorder() call
in a since-removed local duplicate of ensure_system_byte_order.
"""

import io
import pickle
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

from mwax_mover.mwax_calvin_utils import Metafits
from mwax_mover.mwax_calvin_plots import (
    STITCH_GAP_CHANBLOCKS,
    _build_stitched_axis,
    _extract_combined_gains_bundle,
    _channel_reason_counts_text,
    _channel_summary_text,
    _stitch_files,
    _stitch_reasons,
    build_tile_stats_rows,
    generate_hyperdrive_plots,
    generate_hyperdrive_plots_for_files,
    plot_debug_phase_fits,
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
# _channel_summary_text
# ===========================================================================


def test_channel_summary_text_percentage_and_breakdown():
    """Matches the requested format: '{pct}% Good (n/n)' then a
    comma-separated, count-first breakdown of every distinct reason
    present, using the actual mad_residual_threshold for the MAD label."""
    reasons = np.full((_N_TILES, 20), ChannelFlagReason.NONE, dtype=object)
    reasons[0, :4] = ChannelFlagReason.PRE_EXISTING_NAN
    reasons[0, 4:10] = ChannelFlagReason.GAIN_MAX_CUTOFF
    reasons[0, 10:11] = ChannelFlagReason.AMPLITUDE_OUTLIER
    # Remaining 9/20 channels are NONE -- "good" on their own, just swept
    # in by whatever whole-tile promotion made this tile fully flagged.

    text = _channel_summary_text(0, reasons, mad_residual_threshold=10.0)
    lines = text.split("\n")

    assert lines[0] == "45% Good (9/20)"
    assert lines[1] == "4 NaN, 6 above gain cutoff, 1 outside 10 MAD"


def test_channel_summary_text_zero_good_when_all_individually_flagged():
    """A tile that's 100% individually flagged (not just swept in by
    promotion) correctly shows 0% good."""
    reasons = np.full((_N_TILES, 10), ChannelFlagReason.NONE, dtype=object)
    reasons[0, :] = ChannelFlagReason.GAIN_MAX_CUTOFF

    text = _channel_summary_text(0, reasons, mad_residual_threshold=10.0)

    assert text == "0% Good (0/10)\n10 above gain cutoff"


def test_channel_summary_text_uses_actual_mad_threshold():
    """The MAD label reflects whatever threshold was actually used, not a
    hardcoded value."""
    reasons = np.full((_N_TILES, 10), ChannelFlagReason.NONE, dtype=object)
    reasons[0, :3] = ChannelFlagReason.AMPLITUDE_OUTLIER

    text = _channel_summary_text(0, reasons, mad_residual_threshold=5.0)

    assert "3 outside 5 MAD" in text


def test_channel_summary_text_only_reports_reasons_present():
    """A reason with zero channels doesn't appear in the breakdown at all."""
    reasons = np.full((_N_TILES, 10), ChannelFlagReason.NONE, dtype=object)
    reasons[0, :10] = ChannelFlagReason.PRE_EXISTING_NAN

    text = _channel_summary_text(0, reasons, mad_residual_threshold=10.0)

    assert text == "0% Good (0/10)\n10 NaN"
    assert "gain cutoff" not in text
    assert "MAD" not in text


def test_channel_summary_text_clean_tile_shows_100_percent_no_second_line():
    """A tile with no flagged channels at all shows just '100% Good',
    with no second line -- this is the case now also shown on ordinary
    (non-fully-flagged) tiles, not just fully-flagged ones."""
    reasons = np.full((_N_TILES, 10), ChannelFlagReason.NONE, dtype=object)

    text = _channel_summary_text(0, reasons, mad_residual_threshold=10.0)

    assert text == "100% Good (10/10)"
    assert "\n" not in text


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


def test_plot_debug_phase_fits_handles_byteswapped_input():
    """plot_debug_phase_fits must not crash on non-native byte order.

    Regression test: FITS data (freqs/solutions/weights read straight off
    disk) is always big-endian per the FITS standard, so on a
    little-endian machine this is the realistic input shape, not an edge
    case. A previous local duplicate of ensure_system_byte_order in this
    module used ndarray.newbyteorder(), which numpy removed in 2.0 --
    silently never caught because every other test exercising this
    function mocks it out entirely.
    """
    n_chan = 4
    tile_ids = np.array([1, 2])
    tiles = pd.DataFrame(
        {
            "id": tile_ids,
            "name": [f"Tile{i:03d}" for i in tile_ids],
            "rx": [1, 1],
            "slot": [1, 2],
            "flavor": ["RRI", "RRI"],
            "flag": [False, False],
        }
    )

    rows = []
    for tile_id in tile_ids:
        for pol in ["XX", "YY"]:
            rows.append(
                {
                    "tile_id": tile_id,
                    "soln_idx": int(tile_id) - 1,
                    "pol": pol,
                    "name": f"Tile{tile_id:03d}",
                    "id": tile_id,
                    "flavor": "RRI",
                    "rx": 1,
                    "length": 1.0,
                    "intercept": 0.0,
                    "sigma_resid": 0.01,
                    "chi2dof": 1.0,
                    "quality": 1.0,
                    "stderr": 0.001,
                    "outlier": False,
                }
            )
    phase_fits = pd.DataFrame(rows)

    freqs = np.linspace(1.5e8, 1.6e8, n_chan).astype(">f8")
    soln_xx = np.ones((len(tile_ids), n_chan), dtype=">c16")
    soln_yy = np.ones((len(tile_ids), n_chan), dtype=">c16")
    weights = np.ones(n_chan, dtype=">f8")

    assert freqs.dtype.byteorder == ">"
    assert soln_xx.dtype.byteorder == ">"

    # Must not raise (in particular, no AttributeError from a stale
    # ndarray.newbyteorder() call) and must return a non-empty pivot.
    result = plot_debug_phase_fits(
        phase_fits,
        tiles,
        freqs,
        soln_xx,
        soln_yy,
        weights,
        prefix="",
        show=False,
    )

    assert result is not None
    assert len(result) > 0


# ===========================================================================
# Stitched (compressed / broken) x-axis for multi-file gain outlier plots
# ===========================================================================


class TestBuildStitchedAxis:
    """Tests for _build_stitched_axis's compressed multi-picket x-axis."""

    def test_single_file_has_no_gaps(self):
        """A contiguous observation gets a plain 0..n-1 axis and no breaks."""
        axis = _build_stitched_axis([32], [69])

        assert np.array_equal(axis["x_real"], np.arange(32))
        # No separator inserted, so padded and real are identical
        assert np.array_equal(axis["x_padded"], axis["x_real"])
        assert len(axis["gap_centres"]) == 0
        assert axis["tick_labels"] == ["69"]
        assert axis["n_real"] == 32

    def test_real_spacing_preserved_within_each_picket(self):
        """Inside a picket, channels stay exactly 1 unit apart."""
        axis = _build_stitched_axis([4, 4, 4], [62, 67, 73])

        x = axis["x_real"]
        assert len(x) == 12
        for start in (0, 4, 8):
            segment = x[start : start + 4]
            assert np.allclose(np.diff(segment), 1.0)

    def test_gap_inserted_between_pickets(self):
        """Adjacent pickets are separated by exactly STITCH_GAP_CHANBLOCKS."""
        axis = _build_stitched_axis([4, 4], [62, 67])

        x = axis["x_real"]
        # Last channel of picket 0 to first channel of picket 1
        assert x[4] - x[3] == 1 + STITCH_GAP_CHANBLOCKS

    def test_padded_axis_carries_one_nan_per_boundary(self):
        """x_padded has a NaN between pickets, which is what breaks the lines."""
        axis = _build_stitched_axis([4, 4, 4], [62, 67, 73])

        padded = axis["x_padded"]
        assert len(padded) == 12 + 2  # 2 boundaries
        assert int(np.isnan(padded).sum()) == 2
        # Real channel positions survive, in order, ignoring the separators
        assert np.array_equal(padded[~np.isnan(padded)], axis["x_real"])

    def test_gap_centres_fall_strictly_inside_the_gaps(self):
        """Break markers sit between pickets, never on top of real data."""
        axis = _build_stitched_axis([4, 4, 4], [62, 67, 73])

        x = axis["x_real"]
        assert len(axis["gap_centres"]) == 2
        for i, centre in enumerate(axis["gap_centres"]):
            last_of_prev = x[(i + 1) * 4 - 1]
            first_of_next = x[(i + 1) * 4]
            assert last_of_prev < centre < first_of_next

    def test_ticks_are_one_per_picket_at_segment_centre(self):
        """Each picket gets exactly one tick, labelled with its coarse chan."""
        axis = _build_stitched_axis([4, 4, 4], [62, 67, 73])

        x = axis["x_real"]
        assert axis["tick_labels"] == ["62", "67", "73"]
        assert len(axis["tick_pos"]) == 3
        for i, pos in enumerate(axis["tick_pos"]):
            segment = x[i * 4 : (i + 1) * 4]
            assert segment.min() <= pos <= segment.max()

    def test_handles_differing_chanblock_counts_per_file(self):
        """Files need not all have the same number of chanblocks."""
        axis = _build_stitched_axis([2, 5, 3], [62, 67, 73])

        assert axis["n_real"] == 10
        assert len(axis["x_real"]) == 10
        assert len(axis["x_padded"]) == 12
        assert len(axis["gap_centres"]) == 2

    def test_twenty_four_pickets_matches_real_picket_fence(self):
        """The real 24x32 picket-fence case: 768 real, 791 padded, 23 breaks."""
        axis = _build_stitched_axis([32] * 24, list(range(62, 62 + 24)))

        assert axis["n_real"] == 768
        assert len(axis["x_real"]) == 768
        assert len(axis["x_padded"]) == 791
        assert len(axis["gap_centres"]) == 23
        assert len(axis["tick_pos"]) == 24


class TestStitchFiles:
    """Tests for _stitch_files / _stitch_reasons channel-axis concatenation."""

    @staticmethod
    def _per_file(values):
        """One (2, n) array per file, filled with the given constant."""
        return [np.full((2, 3), v, dtype=float) for v in values]

    def test_unpadded_concatenates_directly(self):
        """pad=False yields one column per real channel, aligned with x_real."""
        out = _stitch_files(self._per_file([1, 2, 3]), False, [3, 3, 3])

        assert out.shape == (2, 9)
        assert not np.isnan(out).any()
        assert np.array_equal(out[0], [1, 1, 1, 2, 2, 2, 3, 3, 3])

    def test_padded_inserts_nan_between_files(self):
        """pad=True inserts exactly one NaN column per boundary."""
        out = _stitch_files(self._per_file([1, 2, 3]), True, [3, 3, 3])

        assert out.shape == (2, 11)
        assert int(np.isnan(out[0]).sum()) == 2
        # The NaN sits between files, not at either end
        assert np.isnan(out[0, 3]) and np.isnan(out[0, 7])
        assert not np.isnan(out[0, 0]) and not np.isnan(out[0, -1])

    def test_rejects_wrong_number_of_files(self):
        """A per_file/chanblocks_per_file mismatch is a programming error."""
        with pytest.raises(ValueError, match="expected 3 arrays, got 2"):
            _stitch_files(self._per_file([1, 2]), True, [3, 3, 3])

    def test_reasons_are_never_padded(self):
        """Flag reasons must stay one column per real channel.

        A separator column has no channel behind it, so padding these would
        corrupt every "N of M channels flagged" count and could make a
        fully-flagged tile look partially clean.
        """
        per_file = [
            np.full((2, 3), ChannelFlagReason.NONE, dtype=object),
            np.full((2, 3), ChannelFlagReason.AMPLITUDE_OUTLIER, dtype=object),
        ]

        out = _stitch_reasons(per_file)

        assert out.shape == (2, 6)
        assert list(out[0]) == [ChannelFlagReason.NONE] * 3 + [ChannelFlagReason.AMPLITUDE_OUTLIER] * 3

    def test_reasons_width_matches_unpadded_data_width(self):
        """Reasons and x_real must agree, or masks would be misaligned."""
        axis = _build_stitched_axis([3, 3], [62, 67])
        reasons = _stitch_reasons([np.full((2, 3), ChannelFlagReason.NONE, dtype=object) for _ in range(2)])
        data_real = _stitch_files(self._per_file([1, 2]), False, [3, 3])
        data_padded = _stitch_files(self._per_file([1, 2]), True, [3, 3])

        assert reasons.shape[1] == len(axis["x_real"]) == data_real.shape[1]
        assert data_padded.shape[1] == len(axis["x_padded"])


class TestExtractCombinedGainsBundle:
    """Tests for the stitched bundle handed to the page-rendering workers."""

    @staticmethod
    def _make_group(n_files=3, n_tiles=2, n_cb=4):
        """Build a minimal group with everything the bundle reads.

        Bypasses __init__ (no real FITS files) and populates only the
        attributes _extract_combined_gains_bundle touches.
        """
        group = HyperfitsSolutionGroup.__new__(HyperfitsSolutionGroup)
        tile_ids = np.arange(1, n_tiles + 1)
        group.metafits_tiles_df = pd.DataFrame(
            {
                "name": [f"Tile{i:03d}" for i in tile_ids],
                "id": tile_ids,
                "flag": [False] * n_tiles,
                "rx": [1] * n_tiles,
                "slot": [1] * n_tiles,
                "flavor": "RRI",
            }
        )
        # Only .obsid is read by the bundle. Spec'd to Metafits rather than a
        # bare stub so the type matches and a future attribute access on this
        # fake fails loudly instead of silently returning a Mock.
        metafits = MagicMock(spec=Metafits)
        metafits.obsid = 1234567890
        group.metafits = metafits
        group.all_solution_coarse_chan_indices = [62 + 5 * i for i in range(n_files)]

        # Distinct amplitude per file so stitch ordering is observable
        group.jones = []
        for f in range(n_files):
            j = np.zeros((n_tiles, n_cb, 2, 2), dtype=np.complex128)
            j[:, :, 0, 0] = f + 1
            j[:, :, 1, 1] = (f + 1) * 10
            group.jones.append(j)

        group.channel_flag_reasons = [
            np.full((n_tiles, n_cb), ChannelFlagReason.NONE, dtype=object) for _ in range(n_files)
        ]
        group.tile_flag_reasons = np.full(n_tiles, TileFlagReason.NONE, dtype=object)
        group.amplitude_fit = [
            {"gx": np.full((n_tiles, n_cb), 1.0), "gy": np.full((n_tiles, n_cb), 1.0)} for _ in range(n_files)
        ]
        group.amplitude_band = [
            {
                "gx": (np.full((n_tiles, n_cb), 0.5), np.full((n_tiles, n_cb), 1.5)),
                "gy": (np.full((n_tiles, n_cb), 0.5), np.full((n_tiles, n_cb), 1.5)),
            }
            for _ in range(n_files)
        ]
        group.mad_residual_threshold = 10.0
        return group

    def test_bundle_spans_every_file(self):
        """One bundle covers the whole observation, not one file."""
        bundle = _extract_combined_gains_bundle(self._make_group(n_files=3, n_tiles=2, n_cb=4))

        assert bundle["n_files"] == 3
        # 3 files x 4 chanblocks real, plus 2 NaN separators when padded
        assert bundle["gx_amp_real"].shape == (2, 12)
        assert bundle["gx_amp"].shape == (2, 14)
        assert bundle["chan_reasons"].shape == (2, 12)

    def test_padded_and_real_arrays_stay_aligned_with_their_axes(self):
        """Every padded array matches x_padded; every real one matches x_real."""
        bundle = _extract_combined_gains_bundle(self._make_group())
        n_padded = len(bundle["axis"]["x_padded"])
        n_real = len(bundle["axis"]["x_real"])

        for key in ("gx_amp", "gy_amp", "fit_gx", "fit_gy", "band_lower_gx", "band_upper_gx"):
            assert bundle[key].shape[1] == n_padded, f"{key} is not padded-aligned"

        for key in ("gx_amp_real", "gy_amp_real", "chan_reasons"):
            assert bundle[key].shape[1] == n_real, f"{key} is not real-aligned"

    def test_files_are_stitched_in_order(self):
        """File 0's channels come first, so the axis is monotonic in frequency."""
        bundle = _extract_combined_gains_bundle(self._make_group(n_files=3, n_tiles=2, n_cb=4))

        # gx amplitude was set to file_idx + 1
        assert np.array_equal(bundle["gx_amp_real"][0], [1] * 4 + [2] * 4 + [3] * 4)
        # gy to (file_idx + 1) * 10 -- confirms gx/gy aren't crossed
        assert np.array_equal(bundle["gy_amp_real"][0], [10] * 4 + [20] * 4 + [30] * 4)

    def test_pristine_jones_overrides_current_state(self):
        """Plots show pre-flagging values when a pristine snapshot is given."""
        group = self._make_group(n_files=2, n_tiles=2, n_cb=4)
        pristine = [j.copy() for j in group.jones]
        for j in pristine:
            j[:, :, 0, 0] = 99.0
        # Simulate flagging having NaN'd the live data
        for j in group.jones:
            j[:, :, 0, 0] = np.nan

        bundle = _extract_combined_gains_bundle(group, pristine)

        assert np.all(bundle["gx_amp_real"] == 99.0)
        assert not np.isnan(bundle["gx_amp_real"]).any()

    def test_pristine_jones_file_count_must_match(self):
        """A per-file list of the wrong length is caught, not silently zipped."""
        group = self._make_group(n_files=3)
        with pytest.raises(ValueError, match="pristine_jones has 2 files, expected 3"):
            _extract_combined_gains_bundle(group, [group.jones[0], group.jones[1]])

    def test_bundle_is_picklable(self):
        """The bundle crosses a ProcessPoolExecutor boundary, so it must pickle.

        This is the whole reason the bundle exists: a HyperfitsSolutionGroup
        holds mwalib's Rust-backed MetafitsContext and cannot be sent to a
        worker process.
        """
        bundle = _extract_combined_gains_bundle(self._make_group())

        restored = pickle.loads(pickle.dumps(bundle))

        assert restored["n_files"] == bundle["n_files"]
        assert np.array_equal(restored["gx_amp_real"], bundle["gx_amp_real"])

    def test_single_file_bundle_has_no_separators(self):
        """A contiguous observation is stitched trivially, with no NaN columns."""
        bundle = _extract_combined_gains_bundle(self._make_group(n_files=1, n_tiles=2, n_cb=4))

        assert bundle["n_files"] == 1
        assert bundle["gx_amp"].shape == bundle["gx_amp_real"].shape
        assert not np.isnan(bundle["gx_amp"]).any()
        assert len(bundle["axis"]["gap_centres"]) == 0


# ===========================================================================
# generate_hyperdrive_plots rename scoping / generate_hyperdrive_plots_for_files
# ===========================================================================


class TestGenerateHyperdrivePlotsRename:
    """Tests that the "before" rename only touches its own input's plots.

    The rename used to glob the whole output directory, which was only safe
    because it ran serially (an already-renamed file stops matching). That made
    the function impossible to parallelise and cost a full directory scan per
    file. These tests pin the scoped behaviour, since the real hyperdrive binary
    isn't available in the test environment.
    """

    @staticmethod
    def _fake_hyperdrive(output_dir, stem, suffixes=("amps", "phases")):
        """Return a run_command_ext stand-in that creates hyperdrive's plots."""

        def _run(cmd, *args, **kwargs):
            for suffix in suffixes:
                Path(output_dir, f"{stem}_{suffix}.png").write_text("fake plot")
            return True, ""

        return _run

    def test_renames_only_its_own_files(self, tmp_path):
        """Another picket's plots in the same directory are left alone."""
        stem = "1391522232_ch62_solutions"
        # A sibling picket's plots, already sitting in the shared output dir
        other_amps = tmp_path / "1391522232_ch67_solutions_amps.png"
        other_amps.write_text("other picket")

        with patch(
            "mwax_mover.mwax_calvin_plots.run_command_ext",
            side_effect=self._fake_hyperdrive(tmp_path, stem),
        ):
            success, error = generate_hyperdrive_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
            )

        assert success, error
        # Its own plots were renamed
        assert (tmp_path / f"{stem}_amps_original.png").exists()
        assert (tmp_path / f"{stem}_phases_original.png").exists()
        assert not (tmp_path / f"{stem}_amps.png").exists()
        # The other picket's plot was NOT touched
        assert other_amps.exists()
        assert not (tmp_path / "1391522232_ch67_solutions_amps_original.png").exists()

    def test_after_run_does_not_rename(self, tmp_path):
        """before=False leaves hyperdrive's filenames as produced."""
        stem = "1391522232_ch62_solutions"

        with patch(
            "mwax_mover.mwax_calvin_plots.run_command_ext",
            side_effect=self._fake_hyperdrive(tmp_path, stem),
        ):
            success, _ = generate_hyperdrive_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=False,
            )

        assert success
        assert (tmp_path / f"{stem}_amps.png").exists()
        assert not (tmp_path / f"{stem}_amps_original.png").exists()

    def test_renames_unexpected_suffixes_too(self, tmp_path):
        """A suffix we didn't anticipate is still protected from being overwritten.

        The rename globs on the solution stem rather than hardcoding
        "_amps"/"_phases", so a hyperdrive version emitting a third plot type
        doesn't silently lose its "before" copy.
        """
        stem = "1391522232_ch62_solutions"

        with patch(
            "mwax_mover.mwax_calvin_plots.run_command_ext",
            side_effect=self._fake_hyperdrive(tmp_path, stem, suffixes=("amps", "phases", "delays")),
        ):
            generate_hyperdrive_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
            )

        assert (tmp_path / f"{stem}_delays_original.png").exists()

    def test_already_renamed_files_are_not_double_renamed(self, tmp_path):
        """A second pass must not produce *_original_original.png."""
        stem = "1391522232_ch62_solutions"
        (tmp_path / f"{stem}_amps_original.png").write_text("from an earlier run")

        with patch(
            "mwax_mover.mwax_calvin_plots.run_command_ext",
            side_effect=self._fake_hyperdrive(tmp_path, stem, suffixes=("phases",)),
        ):
            generate_hyperdrive_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
            )

        assert not (tmp_path / f"{stem}_amps_original_original.png").exists()
        assert (tmp_path / f"{stem}_amps_original.png").exists()

    def test_warns_when_hyperdrive_produced_nothing(self, tmp_path, caplog):
        """A success with no matching plots is surfaced, not silently ignored."""
        stem = "1391522232_ch62_solutions"

        with patch("mwax_mover.mwax_calvin_plots.run_command_ext", return_value=(True, "")):
            success, _ = generate_hyperdrive_plots(
                1391522232,
                str(tmp_path / f"{stem}.fits"),
                "/fake/hyperdrive",
                "/fake/metafits.fits",
                str(tmp_path),
                before=True,
            )

        assert success
        assert "produced no plots matching" in caplog.text


class TestGenerateHyperdrivePlotsForFiles:
    """Tests for the concurrent per-file hyperdrive plot wrapper."""

    def test_every_file_is_attempted(self):
        """All solution files get a hyperdrive invocation."""
        files = [f"/data/obs_ch{c}_solutions.fits" for c in (62, 67, 73)]

        with patch("mwax_mover.mwax_calvin_plots.generate_hyperdrive_plots", return_value=(True, "")) as mock_gen:
            failures = generate_hyperdrive_plots_for_files(
                123, files, "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=True
            )

        assert failures == []
        assert mock_gen.call_count == 3
        assert {call.args[1] for call in mock_gen.call_args_list} == set(files)

    def test_one_failure_does_not_stop_the_others(self):
        """A failing file is reported but the rest still run."""
        files = [f"/data/obs_ch{c}_solutions.fits" for c in (62, 67, 73)]

        def _gen(obs_id, filename, *args, **kwargs):
            if "ch67" in filename:
                return False, "hyperdrive exploded"
            return True, ""

        with patch("mwax_mover.mwax_calvin_plots.generate_hyperdrive_plots", side_effect=_gen) as mock_gen:
            failures = generate_hyperdrive_plots_for_files(
                123, files, "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=True
            )

        assert mock_gen.call_count == 3
        assert len(failures) == 1
        assert "ch67" in failures[0][0]
        assert failures[0][1] == "hyperdrive exploded"

    def test_raised_exception_is_captured_not_propagated(self):
        """Plots are diagnostic: a crash must not fail the calibration."""
        files = ["/data/obs_ch62_solutions.fits", "/data/obs_ch67_solutions.fits"]

        def _gen(obs_id, filename, *args, **kwargs):
            if "ch62" in filename:
                raise RuntimeError("boom")
            return True, ""

        with patch("mwax_mover.mwax_calvin_plots.generate_hyperdrive_plots", side_effect=_gen):
            failures = generate_hyperdrive_plots_for_files(
                123, files, "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=True
            )

        assert len(failures) == 1
        assert failures[0][1] == "boom"

    def test_empty_file_list_is_a_no_op(self):
        """No files means no pool and no work."""
        with patch("mwax_mover.mwax_calvin_plots.generate_hyperdrive_plots") as mock_gen:
            assert (
                generate_hyperdrive_plots_for_files(
                    123, [], "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=True
                )
                == []
            )
        mock_gen.assert_not_called()

    def test_before_flag_is_passed_through(self):
        """The before/after distinction must survive the pool dispatch."""
        with patch("mwax_mover.mwax_calvin_plots.generate_hyperdrive_plots", return_value=(True, "")) as mock_gen:
            generate_hyperdrive_plots_for_files(
                123, ["/data/a_solutions.fits"], "/fake/hyperdrive", "/fake/metafits.fits", "/out", before=False
            )

        # (obs_id, filename, binary, metafits, output_dir, before, max_amp)
        assert mock_gen.call_args_list[0].args[5] is False
