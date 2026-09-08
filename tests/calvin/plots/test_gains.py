"""Tests for calvin.plots.gains: amplitude-outlier gain plots, stitching,
tile-flag-reason text helpers, and memory-budgeted page rendering.

Split out of the former test023_calvin_plots.py (docs/RESTRUCTURE.md
test-tree reorg).
"""

import pickle
import pytest
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd

from mwax_mover.calibration.models import Metafits
from mwax_mover.calvin.hyperdrive import ChannelFlagReason, HyperfitsSolutionGroup, TileFlagReason
from mwax_mover.calvin.plots.gains import (
    SINGLE_FILE_SUBPLOT_WIDTH_IN,
    STITCHED_SUBPLOT_WIDTH_IN,
    STITCHED_TILE_COLS,
    STITCH_GAP_CHANBLOCKS,
    _PAGE_RENDER_FALLBACK_WORKERS,
    _build_stitched_axis,
    _channel_reason_counts_text,
    _channel_summary_text,
    _extract_combined_gains_bundle,
    _grid_shape,
    _max_render_workers,
    _page_grid,
    _stitch_files,
    _stitch_reasons,
)

_N_TILES = 3
_N_CHANBLOCKS = 10


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


class TestPageGrid:
    """Tests for the shared page-geometry helper.

    Shared deliberately: _render_combined_gains_figure builds the figure from
    this and _max_render_workers predicts its memory from it, so if they ever
    disagreed the cap would be sized against a figure that isn't what gets
    made.
    """

    def test_stitched_uses_the_wide_narrow_layout(self):
        """Stitched pages get wider subplots on fewer tile columns."""
        n_rows, n_tile_cols, width_in = _page_grid(16, stitched=True)

        assert n_tile_cols == STITCHED_TILE_COLS
        assert width_in == STITCHED_SUBPLOT_WIDTH_IN
        assert n_rows == 6  # ceil(16 / 3)

    def test_single_file_keeps_the_historical_layout(self):
        """A contiguous observation's layout is unchanged by the stitching work."""
        n_rows, n_tile_cols, width_in = _page_grid(16, stitched=False)

        assert width_in == SINGLE_FILE_SUBPLOT_WIDTH_IN
        assert (n_rows, n_tile_cols) == _grid_shape(16)

    def test_every_tile_on_the_page_has_a_cell(self):
        """The grid must never be too small for the tiles it has to hold."""
        for n_tiles in (1, 3, 7, 16, 32, 64):
            for stitched in (True, False):
                n_rows, n_tile_cols, _ = _page_grid(n_tiles, stitched)
                assert n_rows * n_tile_cols >= n_tiles, f"{n_tiles=} {stitched=}"


class TestMaxRenderWorkers:
    """Tests for the concurrency cap that fixes the ENOMEM page failures.

    A stitched page peaks at a few hundred MB while matplotlib renders and saves
    it, and the pool used to default to os.cpu_count() workers -- tens of GB of
    live render buffers on a many-core node.
    """

    def test_scales_down_when_memory_is_tight(self):
        """Little memory means few concurrent renders, regardless of core count."""
        with (
            patch("mwax_mover.calvin.plots.gains.available_memory_bytes", return_value=int(2e9)),
            patch("os.cpu_count", return_value=64),
        ):
            workers = _max_render_workers(16, stitched=True, n_pages=16)

        assert 1 <= workers <= 4

    def test_memory_cap_beats_cpu_count(self):
        """The bug was sizing the pool by cores alone; memory must dominate."""
        with (
            patch("mwax_mover.calvin.plots.gains.available_memory_bytes", return_value=int(8e9)),
            patch("os.cpu_count", return_value=64),
        ):
            workers = _max_render_workers(16, stitched=True, n_pages=64)

        assert workers < 64

    def test_never_exceeds_the_page_count(self):
        """No point starting workers with no page to render."""
        with (
            patch("mwax_mover.calvin.plots.gains.available_memory_bytes", return_value=int(512e9)),
            patch("os.cpu_count", return_value=64),
        ):
            assert _max_render_workers(16, stitched=True, n_pages=3) == 3

    def test_never_exceeds_cpu_count(self):
        """Plenty of memory still shouldn't oversubscribe the CPUs."""
        with (
            patch("mwax_mover.calvin.plots.gains.available_memory_bytes", return_value=int(512e9)),
            patch("os.cpu_count", return_value=2),
        ):
            assert _max_render_workers(16, stitched=True, n_pages=16) == 2

    def test_falls_back_conservatively_when_memory_is_unknown(self):
        """Guessing low and rendering serially beats another ENOMEM."""
        with (
            patch("mwax_mover.calvin.plots.gains.available_memory_bytes", return_value=None),
            patch("os.cpu_count", return_value=64),
        ):
            assert _max_render_workers(16, stitched=True, n_pages=16) == _PAGE_RENDER_FALLBACK_WORKERS

    def test_always_at_least_one(self):
        """Even an absurdly small budget must still make progress."""
        with (
            patch("mwax_mover.calvin.plots.gains.available_memory_bytes", return_value=1024),
            patch("os.cpu_count", return_value=64),
        ):
            assert _max_render_workers(16, stitched=True, n_pages=16) == 1

    def test_budget_covers_the_measured_peak_per_page(self):
        """The cap must leave room for what a page render actually costs.

        Measured at ~322MB peak RSS for a 10800x3600 page. The chosen worker
        count multiplied by that must fit inside the available memory, or the
        cap isn't doing its job.
        """
        measured_peak_bytes = 322 * 1024 * 1024
        available = int(8e9)

        with (
            patch("mwax_mover.calvin.plots.gains.available_memory_bytes", return_value=available),
            patch("os.cpu_count", return_value=64),
        ):
            workers = _max_render_workers(16, stitched=True, n_pages=64)

        assert workers * measured_peak_bytes < available

    def test_stitched_pages_get_fewer_workers_than_single_file_pages(self):
        """A stitched page is bigger, so fewer of them fit at once."""
        with (
            patch("mwax_mover.calvin.plots.gains.available_memory_bytes", return_value=int(8e9)),
            patch("os.cpu_count", return_value=64),
        ):
            stitched = _max_render_workers(16, stitched=True, n_pages=64)
            single = _max_render_workers(16, stitched=False, n_pages=64)

        assert stitched < single


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
