"""Amplitude-outlier gain plots: plot_combined_gains and plot_outlier_gains.

Works from a HyperfitsSolutionGroup's flag-reason state (TileFlagReason/
ChannelFlagReason) rather than the old bad_mask/band/fit tuple. Renders
tile-pair pages (amplitude + residual per tile), stitching multiple
picket-fence files into one continuous x-axis where needed, and budgets
concurrent page-rendering workers against available memory.
"""

import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import cast

import matplotlib as mpl

# This is a batch/server-side pipeline that never displays a figure
# interactively -- only ever saves to file. Force the headless Agg
# backend explicitly (must happen before pyplot's first import, which is
# when backend selection is locked in) rather than letting matplotlib
# resolve to whatever interactive backend happens to be available (e.g.
# TkAgg), which wastes real time on GUI-toolkit overhead for every figure.
mpl.use("Agg")

import matplotlib.ticker as mticker
import matplotlib.transforms as mtransforms
import numpy as np
from matplotlib import pyplot as plt
from numpy.typing import NDArray

from mwax_mover.calibration.df_columns import COL_GX, COL_GY
from mwax_mover.calvin.hyperfits_solution_group import ChannelFlagReason, HyperfitsSolutionGroup, TileFlagReason
from mwax_mover.calvin.plots.layout import resolve_plot_dpi, scale_plot_figsize
from mwax_mover.core.env import available_memory_bytes

logger = logging.getLogger(__name__)


def _grid_shape(n: int) -> tuple[int, int]:
    """Compute a near-square (rows, cols) grid that fits n tile-pairs.

    Chooses cols = ceil(sqrt(n)) and rows = ceil(n / cols). Each "tile"
    occupies two actual subplot columns (amplitude + residual), so the
    caller multiplies the returned cols by 2 when building the actual
    subplot grid.

    Args:
        n: Number of tile-pairs needed.

    Returns:
        A (rows, cols) tuple, where cols is the number of tile-pairs per row.
    """
    cols = int(np.ceil(np.sqrt(n)))
    rows = int(np.ceil(n / cols))
    return rows, cols


def _paged_output_path(output_path: str, first_tile_index: int, last_tile_index: int) -> str:
    """Build a per-page output filename with a tile-range suffix.

    E.g. _paged_output_path("test.png", 0, 63) -> "test_0-63.png"

    Args:
        output_path: Base output path, e.g. "test.png" or "/some/dir/test.png".
        first_tile_index: First tile index included in this page.
        last_tile_index: Last tile index included in this page.

    Returns:
        The output path with "_{first}-{last}" inserted before the extension.
    """
    base, ext = os.path.splitext(output_path)
    return f"{base}_{first_tile_index}-{last_tile_index}{ext}"


def _format_flavor(flavor: str) -> str:
    """Format a receiver flavour for display in the stats table.

    mwalib's ReceiverType enum stringifies as e.g. "ReceiverType.SHAO" --
    strip the class-name prefix so the stats table just shows "SHAO",
    matching the short form used everywhere else (CALVIN.md, log
    messages, etc.). Passes through unchanged if there's no such prefix
    (e.g. an empty string, or a plain str already).

    Args:
        flavor: A tile's flavor value, e.g. from metafits_tiles_df.

    Returns:
        Display-formatted flavour string.
    """
    text = str(flavor)
    return text.rsplit(".", 1)[-1] if "." in text else text


def _tile_flag_reason_text(tile_idx: int, tile_reasons: NDArray[np.object_]) -> str:
    """Build a human-readable reason string for a fully-flagged tile.

    Args:
        tile_idx: Tile index (row position in the group's metafits_tiles_df).
        tile_reasons: A TileFlagReason array, shape (n_tiles,) -- e.g.
            group.tile_flag_reasons, or a copy taken at an earlier point
            (this function doesn't care which snapshot it reflects).

    Returns:
        A semicolon-separated description of every TileFlagReason bit set
        for this tile, or a note that it was fully flagged by per-channel
        reasons alone (no whole-tile reason bit set).
    """
    reason = tile_reasons[tile_idx]
    parts = []
    if reason & TileFlagReason.METAFITS:
        parts.append("flagged in metafits")
    if reason & TileFlagReason.HYPERDRIVE_TILE:
        parts.append("flagged in TILES HDU")
    if reason & TileFlagReason.HYPERDRIVE_BASELINE:
        parts.append("flagged via BASELINES-HDU inference")
    if reason & TileFlagReason.PHASE_OUTLIER:
        parts.append("phase fit is a population outlier")
    if reason & TileFlagReason.MOSTLY_BAD_CHANNELS:
        parts.append("too many bad channels (promoted to fully flagged)")
    if not parts:
        return "Fully flagged by per-channel reasons alone (no whole-tile flag reason)"
    return "; ".join(parts)


# Human-readable label for each ChannelFlagReason bit, used by
# _channel_summary_text below. AMPLITUDE_OUTLIER isn't
# here -- its label needs the actual mad_residual_threshold value, built
# separately where it's used.
_CHANNEL_REASON_LABELS = {
    ChannelFlagReason.PRE_EXISTING_NAN: "NaN",
    ChannelFlagReason.NON_CONVERGED: "not converged",
    ChannelFlagReason.PARTIAL_JONES: "partial Jones",
    ChannelFlagReason.GAIN_MAX_CUTOFF: "above gain cutoff",
}


def _channel_summary_text(tile_idx: int, file_reasons: NDArray[np.object_], mad_residual_threshold: float) -> str:
    """Build the 1-2 line top-centre summary shown on every tile except
    one flagged structurally (metafits/TILES-HDU/BASELINES-HDU-inferred --
    see _tile_flag_reason_text for that case instead, since a
    structurally-flagged tile has no per-channel data to break down).

    Used for every other tile, fully flagged or not: a clean tile shows
    "100% Good (n/n)" with no second line; a partially-flagged tile
    shows its actual good fraction and reason breakdown; a Calvin-fully-
    flagged tile (e.g. promoted via flag_mostly_bad_tiles, or simply
    100% per-channel-flagged without a specific whole-tile reason) does
    too, same format, just with 0-or-more good channels rather than
    necessarily 100%.

    Line 1 is the fraction of this tile's channels that were never
    individually flagged for their own reason -- i.e. would still be
    usable on their own, even if a whole-tile promotion (if any) swept
    some of them into NaN regardless -- as "{pct}% Good (n_good/n_total)".

    Line 2 lists every distinct per-channel reason actually present on
    this tile as "{count} {label}", comma-separated, in ascending
    reason-bit order (matching ChannelFlagReason's declaration order,
    which happens to roughly match pipeline order too) -- omitted
    entirely if there are none (a clean tile, or line 1 already covers
    everything).

    Args:
        tile_idx: Tile index (row position in metafits_tiles_df).
        file_reasons: This file's ChannelFlagReason array, shape
            (n_tiles, n_chanblocks) -- e.g. bundle["file_reasons"].
        mad_residual_threshold: The actual MAD-residual threshold used
            by flag_amplitude_outliers (see HyperfitsSolutionGroup.
            mad_residual_threshold), for an accurate "outside N MAD"
            label rather than a hardcoded guess.

    Returns:
        A 1-2 line string, ready to hand straight to ax.text().
    """
    reasons_here = file_reasons[tile_idx, :]
    n_total = len(reasons_here)
    n_good = int(np.sum(reasons_here == ChannelFlagReason.NONE))
    pct_good = 100 * n_good / n_total if n_total else 0.0
    line1 = f"{pct_good:.0f}% Good ({n_good}/{n_total})"

    parts = []
    for flag in ChannelFlagReason:
        if flag == ChannelFlagReason.NONE:
            continue
        label = (
            f"outside {mad_residual_threshold:g} MAD"
            if flag == ChannelFlagReason.AMPLITUDE_OUTLIER
            else _CHANNEL_REASON_LABELS[flag]
        )
        n = int(np.sum([bool(r & flag) for r in reasons_here]))
        if n:
            parts.append(f"{n} {label}")

    if not parts:
        return line1
    return line1 + "\n" + ", ".join(parts)


# Blank x-axis width, in chanblock units, inserted between adjacent pickets in
# a stitched plot. Deliberately narrow: a picket-fence observation's real
# frequency gaps are enormous (1391522232 spans 78.7-241.2 MHz with only 18.9%
# of that span covered by actual data), so plotting against true frequency would
# spend over 80% of the axis on emptiness and squeeze each picket's chanblocks
# into an unreadable sliver. The gap is therefore compressed to a fixed token
# width and marked, rather than drawn to scale -- the axis is explicitly not
# linear in frequency, which is why every picket boundary gets a visible break
# line and the x label says so.
STITCH_GAP_CHANBLOCKS = 6


# Per-subplot width in inches for a stitched (multi-file) plot, and the number
# of tile-pairs per row that goes with it. A stitched subplot carries every
# file's chanblocks instead of one file's, so at the single-file width of 6in
# each picket would get only ~38px -- too narrow to see individual flagged
# channels. 12in gives ~75px per picket. The narrower 3-tile-col grid trades
# width for height so the page stays 10800px wide, matching what the
# single-file layout already produces, rather than becoming twice as wide as
# anything else in the fit directory.
#
# Note total figure area is n_tiles * 2 * subplot_area regardless of grid shape,
# so the grid controls aspect ratio only -- widening subplots always costs
# proportionally more pixels (and file size). Hence 12in rather than more.
STITCHED_SUBPLOT_WIDTH_IN = 12


STITCHED_TILE_COLS = 3


# Per-subplot width in inches for a single-file plot (the historical value).
SINGLE_FILE_SUBPLOT_WIDTH_IN = 6


# X tick label size for a stitched plot, where there is one label per picket.
# Measured against the rendered label extents for the 24-picket worst case at
# STITCHED_SUBPLOT_WIDTH_IN: at 6pt the 24 labels clear each other by ~5px, at
# 7pt they collide. Raise the subplot width before raising this.
_STITCHED_TICK_FONTSIZE = 6


def _build_stitched_axis(
    chanblocks_per_file: list[int],
    coarse_chans: list[int] | NDArray[np.int_],
    gap: int = STITCH_GAP_CHANBLOCKS,
) -> dict:
    """Build a compressed ("broken") x-axis spanning every solution file.

    Each file's chanblocks keep their true uniform spacing of 1 unit, and a
    fixed *gap* of blank units is inserted between adjacent files. This is the
    single-axes equivalent of a broken-axis plot: the alternative, one real
    subplot per segment, would mean 24 axes per tile-pol (~1500 per page for a
    picket fence) and would cost more than plotting per file did in the first
    place.

    Two x arrays come back because they serve different purposes. Continuous
    series (the data trace, the polynomial fit, the acceptance band) are plotted
    against ``x_padded``, which carries a NaN between segments so matplotlib
    breaks the line and the fill instead of drawing a straight run across a gap
    that contains no data. Everything indexed per real channel (scatter markers,
    flag masks, channel counts) uses ``x_real``, which has exactly one entry per
    real chanblock and no separators, so channel indices line up with the
    flag-reason arrays.

    Args:
        chanblocks_per_file: Number of chanblocks in each solution file, in the
            same order as the group's files.
        coarse_chans: The coarse channel number for each file, used for tick
            labels. Must be the same length as *chanblocks_per_file*.
        gap: Blank width, in chanblock units, between adjacent files.

    Returns:
        A dict with ``x_real`` (one position per real chanblock), ``x_padded``
        (with a NaN between segments), ``gap_centres`` (x position of each
        boundary, for break markers), ``tick_pos``/``tick_labels`` (one per
        file, at the segment centre), and ``n_real`` / ``chanblocks_per_file``
        for the padding helper.
    """
    x_real_parts: list[NDArray[np.float64]] = []
    x_padded_parts: list[NDArray[np.float64]] = []
    gap_centres: list[float] = []
    tick_pos: list[float] = []

    cursor = 0.0
    n_files = len(chanblocks_per_file)

    for i, n_cb in enumerate(chanblocks_per_file):
        segment = np.arange(n_cb, dtype=np.float64) + cursor
        x_real_parts.append(segment)
        x_padded_parts.append(segment)
        tick_pos.append(float(segment.mean()) if n_cb else cursor)
        cursor += n_cb

        if i < n_files - 1:
            gap_centres.append(cursor + gap / 2.0 - 0.5)
            # One NaN column per boundary -- this is what breaks plot() and
            # fill_between() across the gap.
            x_padded_parts.append(np.array([np.nan]))
            cursor += gap

    return {
        "x_real": np.concatenate(x_real_parts) if x_real_parts else np.zeros(0),
        "x_padded": np.concatenate(x_padded_parts) if x_padded_parts else np.zeros(0),
        "gap_centres": np.array(gap_centres, dtype=np.float64),
        "tick_pos": np.array(tick_pos, dtype=np.float64),
        "tick_labels": [str(int(c)) for c in coarse_chans],
        "n_real": int(sum(chanblocks_per_file)),
        "chanblocks_per_file": list(chanblocks_per_file),
    }


def _stitch_files(per_file: list[NDArray], pad: bool, chanblocks_per_file: list[int]) -> NDArray[np.float64]:
    """Concatenate per-file (n_tiles, n_chanblocks) arrays along the channel axis.

    Args:
        per_file: One array per solution file, all with the same leading
            (tile) dimension.
        pad: If True, insert a single NaN column between adjacent files, giving
            an array aligned with ``x_padded`` from _build_stitched_axis. If
            False, the arrays are concatenated directly, aligned with
            ``x_real``.
        chanblocks_per_file: Unused except as a length check against *per_file*.

    Returns:
        The concatenated float64 array.

    Raises:
        ValueError: If per_file and chanblocks_per_file disagree in length.
    """
    if len(per_file) != len(chanblocks_per_file):
        raise ValueError(f"expected {len(chanblocks_per_file)} arrays, got {len(per_file)}")

    parts: list[NDArray] = []
    for i, arr in enumerate(per_file):
        parts.append(np.asarray(arr, dtype=np.float64))
        if pad and i < len(per_file) - 1:
            parts.append(np.full((arr.shape[0], 1), np.nan, dtype=np.float64))
    return np.concatenate(parts, axis=1)


def _stitch_reasons(per_file: list[NDArray[np.object_]]) -> NDArray[np.object_]:
    """Concatenate per-file channel flag-reason arrays, with no separators.

    Kept separate from _stitch_files because these are object-dtype IntFlag
    arrays, and because they must NOT be padded: a separator column has no
    channel behind it, so giving it a reason would corrupt every "N of M
    channels flagged" count and could make an all-flagged tile look partially
    clean.

    Args:
        per_file: One (n_tiles, n_chanblocks) object array per solution file.

    Returns:
        The concatenated (n_tiles, n_real_chanblocks) object array.
    """
    return np.concatenate(per_file, axis=1)


def _extract_combined_gains_bundle(
    group: HyperfitsSolutionGroup,
    pristine_jones: list[NDArray[np.complex128]] | None = None,
) -> dict:
    """Extract the plain, picklable data _render_combined_gains_figure
    needs from a HyperfitsSolutionGroup, stitched across every file.

    Exists so plot_outlier_gains can dispatch page rendering to worker
    processes: a HyperfitsSolutionGroup itself isn't picklable (it holds
    mwalib's Rust-backed MetafitsContext), but everything actually needed
    to render a page is plain numpy arrays/dicts/primitives, which are.
    Called once per observation (not per page, and no longer per file)
    since the same bundle is reused for every page.

    Every file's chanblocks are concatenated onto one compressed x-axis
    (see _build_stitched_axis), so a picket-fence observation produces one
    set of pages covering all 24 coarse channels rather than one set per
    picket. Outlier *detection* remains strictly per file -- this only
    changes presentation. That is why the fit and band arrays are stitched
    from group.amplitude_fit/amplitude_band as-is: each file's polynomial
    was fit against its own chanblocks only, and stitching never refits
    anything across a gap.

    Args:
        group: The solution group, after apply_tile_flags,
            enforce_whole_jones_nan, flag_gain_max_cutoff,
            flag_amplitude_outliers, and flag_mostly_bad_tiles have run
            (detect_phase_outliers is not required -- this bundle doesn't
            use group.phase_fits).
        pristine_jones: One Jones array per file (e.g. group.before_jones),
            to plot amplitudes from so flagged-but-not-yet-NaN'd values are
            still visible. Defaults to the group's current state.

    Returns:
        A dict of everything _render_combined_gains_figure needs.
    """
    assert group.jones is not None
    assert group.channel_flag_reasons is not None
    assert group.tile_flag_reasons is not None
    assert group.amplitude_fit is not None
    assert group.amplitude_band is not None
    assert group.mad_residual_threshold is not None

    n_files = len(group.jones)
    chanblocks_per_file = [int(file_jones.shape[1]) for file_jones in group.jones]

    if pristine_jones is None:
        gains_for_plot = group.jones
    else:
        if len(pristine_jones) != n_files:
            raise ValueError(f"pristine_jones has {len(pristine_jones)} files, expected {n_files}")
        gains_for_plot = pristine_jones

    axis = _build_stitched_axis(chanblocks_per_file, group.all_solution_coarse_chan_indices)

    return {
        "n_files": n_files,
        "axis": axis,
        # Padded (NaN between files) -- for continuous series only.
        "gx_amp": _stitch_files([np.abs(j[:, :, 0, 0]) for j in gains_for_plot], True, chanblocks_per_file),
        "gy_amp": _stitch_files([np.abs(j[:, :, 1, 1]) for j in gains_for_plot], True, chanblocks_per_file),
        "fit_gx": _stitch_files([f[COL_GX] for f in group.amplitude_fit], True, chanblocks_per_file),
        "fit_gy": _stitch_files([f[COL_GY] for f in group.amplitude_fit], True, chanblocks_per_file),
        "band_lower_gx": _stitch_files([b[COL_GX][0] for b in group.amplitude_band], True, chanblocks_per_file),
        "band_upper_gx": _stitch_files([b[COL_GX][1] for b in group.amplitude_band], True, chanblocks_per_file),
        "band_lower_gy": _stitch_files([b[COL_GY][0] for b in group.amplitude_band], True, chanblocks_per_file),
        "band_upper_gy": _stitch_files([b[COL_GY][1] for b in group.amplitude_band], True, chanblocks_per_file),
        # Unpadded -- one column per real chanblock, aligned with axis["x_real"].
        "gx_amp_real": _stitch_files([np.abs(j[:, :, 0, 0]) for j in gains_for_plot], False, chanblocks_per_file),
        "gy_amp_real": _stitch_files([np.abs(j[:, :, 1, 1]) for j in gains_for_plot], False, chanblocks_per_file),
        "chan_reasons": _stitch_reasons(group.channel_flag_reasons),
        "tile_names": group.metafits_tiles_df["name"].to_numpy(),
        "tile_reasons": group.tile_flag_reasons,
        "obsid": group.metafits.obsid,
        "mad_residual_threshold": group.mad_residual_threshold,
    }


# Peak resident memory per page render, as a multiple of the figure's raw RGBA
# buffer (width_px * height_px * 4). Measured at ~2.1x on a 10800x3600 page:
# 156MB of raw buffer against a 322MB peak RSS delta, because savefig with
# bbox_inches="tight" renders to measure the tight bounding box and then again
# to write the file. Rounded up slightly for the PNG compression buffer.
_PAGE_RENDER_MEMORY_FACTOR = 2.5


# Fraction of the memory we believe is available that we are willing to commit
# to concurrent page renders. Leaves room for the parent process (which is
# holding the whole solution group's Jones arrays) and for anything else sharing
# the allocation.
_PAGE_RENDER_MEMORY_BUDGET_FRACTION = 0.6


# Fallback when no memory limit can be determined at all. Deliberately modest:
# an unbounded pool is what caused ENOMEM in the first place, so guessing low
# and rendering a few pages serially is much better than guessing high.
_PAGE_RENDER_FALLBACK_WORKERS = 4


def _page_grid(n_tiles: int, stitched: bool) -> tuple[int, int, int]:
    """Compute the subplot grid and per-subplot width for one page.

    Shared by _render_combined_gains_figure (which builds the figure) and
    _max_render_workers (which has to predict how much memory that figure will
    take). Keeping it in one place means the memory estimate cannot silently
    drift away from the figure actually created.

    Args:
        n_tiles: Number of tile-pairs on the page.
        stitched: True for a multi-file stitched page, which needs a wider
            subplot to fit every file's chanblocks (see
            STITCHED_SUBPLOT_WIDTH_IN) on a narrower grid.

    Returns:
        A (n_rows, n_tile_cols, subplot_width_inches) tuple. Each tile-pair
        occupies two subplot columns, so the actual column count is
        n_tile_cols * 2.
    """
    if stitched:
        n_tile_cols = STITCHED_TILE_COLS
        n_rows = int(np.ceil(n_tiles / n_tile_cols))
        return n_rows, n_tile_cols, STITCHED_SUBPLOT_WIDTH_IN

    n_rows, n_tile_cols = _grid_shape(n_tiles)
    return n_rows, n_tile_cols, SINGLE_FILE_SUBPLOT_WIDTH_IN


def _max_render_workers(n_tiles: int, stitched: bool, n_pages: int) -> int:
    """Decide how many page renders may run concurrently.

    A stitched page is large -- 10800x3600px for the cal_utils default of 16
    tiles per page -- and peaks at a few hundred MB while matplotlib renders and
    saves it. An unbounded ProcessPoolExecutor defaults to os.cpu_count()
    workers, which on a many-core calvin node meant tens of GB of render buffers
    live at once and pages failing with "[Errno 12] Cannot allocate memory".

    The cap is the smallest of: the number of pages there actually are (no point
    starting idle workers), the CPU count, and how many page-sized allocations
    fit in the memory budget.

    Args:
        n_tiles: Tiles per page, which together with *stitched* fixes the figure
            size (see _page_grid).
        stitched: Whether these are stitched multi-file pages.
        n_pages: Total pages to render.

    Returns:
        Worker count, always at least 1.
    """
    n_rows, n_tile_cols, subplot_width_in = _page_grid(n_tiles, stitched)
    width_px = subplot_width_in * n_tile_cols * 2 * 150
    height_px = 4 * n_rows * 150
    bytes_per_page = int(width_px * height_px * 4 * _PAGE_RENDER_MEMORY_FACTOR)

    cpu_cap = os.cpu_count() or 1
    available = available_memory_bytes()

    if available is None:
        memory_cap = _PAGE_RENDER_FALLBACK_WORKERS
        logger.debug(
            f"Could not determine available memory; capping page renders at {_PAGE_RENDER_FALLBACK_WORKERS} workers."
        )
    else:
        budget = int(available * _PAGE_RENDER_MEMORY_BUDGET_FRACTION)
        memory_cap = max(1, budget // bytes_per_page)

    workers = max(1, min(n_pages, cpu_cap, memory_cap))

    logger.info(
        f"Rendering {n_pages} page(s) of {width_px}x{height_px}px"
        f" (~{bytes_per_page / 1e6:.0f} MB each) with {workers} worker(s)"
        f" [cpu={cpu_cap}, memory allows {memory_cap}]"
    )
    return workers


def plot_combined_gains(
    group: HyperfitsSolutionGroup,
    first_tile_index: int = 0,
    n_tiles: int = 16,
    pristine_jones: list[NDArray[np.complex128]] | None = None,
    solution_file_will_be_modified: bool = True,
) -> plt.Figure:
    """Plot gx and gy gain amplitude in separate subplots per tile, stitched
    across every file in a solution group.

    Equivalent to the retired mwax_calvin_quality.plot_combined, adapted to
    work from HyperfitsSolutionGroup's flag-reason state (after all
    flagging methods have run) instead of the old bad_mask/band/fit tuple
    from flag_bad_gains.

    Thin wrapper around _render_combined_gains_figure: extracts the plain
    data that function needs from group, then delegates to it. Kept
    separate so plot_outlier_gains can extract the bundle once per
    observation and reuse it across every page (and across worker
    processes, when saving to disk), rather than needing group itself in
    each page's rendering call.

    Args:
        group: The solution group, after apply_tile_flags,
            enforce_whole_jones_nan, flag_gain_max_cutoff,
            flag_amplitude_outliers, and flag_mostly_bad_tiles have run
            (detect_phase_outliers is not required -- this plot doesn't
            use group.phase_fits).
        first_tile_index: Index of the first tile to include in this page.
        n_tiles: Number of tiles to plot starting from first_tile_index.
            Also determines the subplot grid shape (see _grid_shape).
        pristine_jones: One Jones array per file (e.g. group.before_jones),
            each shape (n_tiles, n_chanblocks, 2, 2), taken before any
            flagging ran so flagged-but-not-yet-NaN'd values are still
            visible on the plot. Defaults to group.jones (its current
            state) if not given -- if flagging has already run, flagged
            entries will show as gaps rather than visible outlier points.
        solution_file_will_be_modified: If True, a note is added to the
            figure title.

    Returns:
        The matplotlib Figure containing the grid of per-tile subplot pairs.
    """
    bundle = _extract_combined_gains_bundle(group, pristine_jones)
    return _render_combined_gains_figure(bundle, first_tile_index, n_tiles, solution_file_will_be_modified)


def _draw_gain_subplot(
    ax: plt.Axes,
    tile: int,
    tile_name: str,
    pol_label: str,
    x_padded: NDArray[np.float64],
    x_real: NDArray[np.float64],
    data: NDArray[np.float64],
    fit_data: NDArray[np.float64],
    band_lower: NDArray[np.float64],
    band_upper: NDArray[np.float64],
    before_real: NDArray[np.float64],
    data_color: str,
    fit_color: str,
    flagged_other_mask: NDArray[np.bool_],
    gain_cutoff_mask: NDArray[np.bool_],
    n_flagged_here: int,
) -> str:
    """Draw one polarisation's subplot: data, fit line, shaded acceptance
    band, and flag markers (see _draw_tile_panel's docstring for the
    marker/shading conventions -- identical for gx and gy, just applied to
    each polarisation's own data/colour here).

    Args:
        ax: The axes to draw into (gx's or gy's).
        tile: Index of the tile being drawn.
        tile_name: Display name of the tile, for the title.
        pol_label: "gx" or "gy", used in labels and the title.
        x_padded: Padded (NaN between pickets) x-axis positions.
        x_real: Unpadded x-axis positions, aligned with the flag masks.
        data: This polarisation's per-tile amplitude data (padded).
        fit_data: This polarisation's per-tile fit line (padded).
        band_lower: This polarisation's per-tile band lower bound (padded).
        band_upper: This polarisation's per-tile band upper bound (padded).
        before_real: This polarisation's per-tile amplitude data (unpadded),
            for marker placement.
        data_color: Colour for the raw data line and band.
        fit_color: Colour for the fit line.
        flagged_other_mask: Channels flagged for a reason other than the
            gain-magnitude cutoff, marked with a black 'x'.
        gain_cutoff_mask: Channels flagged by the gain-magnitude cutoff,
            marked with a black '+'.
        n_flagged_here: Total flagged-channel count for this tile, to decide
            whether the title needs a "(no flags)" suffix.

    Returns:
        This subplot's title (before any "- FULLY FLAGGED" suffix the
        caller may still append).
    """
    ax.fill_between(
        x_padded,
        band_lower[tile],
        band_upper[tile],
        color=data_color,
        alpha=0.15,
        zorder=0,
        label=f"{pol_label} band",
    )
    ax.plot(x_padded, data[tile], color=data_color, alpha=0.7, linewidth=0.8, label=pol_label)
    ax.plot(
        x_padded, fit_data[tile], color=fit_color, linestyle="--", alpha=0.8, linewidth=0.8, label=f"{pol_label} fit"
    )
    if flagged_other_mask.any():
        ax.scatter(
            x_real[flagged_other_mask],
            before_real[tile][flagged_other_mask],
            color="black",
            marker="x",
            s=15,
            zorder=3,
            label="flagged",
        )
    if gain_cutoff_mask.any():
        ax.scatter(
            x_real[gain_cutoff_mask],
            before_real[tile][gain_cutoff_mask],
            color="black",
            marker="+",
            s=30,
            zorder=3,
            label="gain cutoff",
        )

    title = f"Tile {tile} ({tile_name}) - {pol_label} amplitude"
    if n_flagged_here == 0:
        title += " (no flags)"
    ax.set_title(title, fontsize=9)
    ax.yaxis.set_major_formatter(mticker.ScalarFormatter(useOffset=False, useMathText=True))
    ax.tick_params(labelsize=7)
    return title


def _draw_tile_panel(
    ax_gx: plt.Axes,
    ax_gy: plt.Axes,
    tile: int,
    bundle: dict,
    bad_mask: NDArray[np.bool_],
    new_amplitude_bad_mask: NDArray[np.bool_],
    new_gain_cutoff_bad_mask: NDArray[np.bool_],
) -> None:
    """Draw one tile's gx/gy subplot pair into the already-created axes.

    Every tile except one flagged structurally, before Calvin's own
    analysis ever ran (metafits / TILES-HDU / BASELINES-HDU-inferred --
    apply_tile_flags() NaNs these immediately, leaving no real data to
    show even in the pristine snapshot), gets a top-centre summary built
    by _channel_summary_text: a "{pct}% Good (n_good/n_total)" line (the
    fraction of channels that were never individually flagged for their
    own reason), then, if any channel was individually flagged, a second
    line breaking down every distinct reason present as "{count}
    {label}", comma-separated (e.g. "100 NaN, 200 above gain cutoff, 22
    outside 10 MAD"). A clean tile just shows "100% Good" with no second
    line. The structural case instead shows the simpler, single-line
    _tile_flag_reason_text ("flagged in metafits", etc.), since there's
    no per-channel data to break down for it.

    Text and border colour both track severity, not which check caught a
    channel: black text with no border colour change for a clean tile;
    orange for a partial (some-channels) flag; red reserved for a fully
    flagged tile (structural or Calvin-caused alike -- e.g. promoted via
    flag_mostly_bad_tiles, or simply 100% per-channel-flagged without a
    specific whole-tile reason). A Calvin-fully-flagged tile still has
    real pristine data, so unlike the structural case it gets the normal
    plot (below) too, with the red summary and border overlaid on top --
    the point being flagged, not the absence of data. Titles say "gx
    amplitude - FULLY FLAGGED"/"gy amplitude - FULLY FLAGGED" for any
    fully flagged tile, structural or not, so the two subplots remain
    distinguishable even with no data plotted. If the underlying
    polynomial fit has no valid channels left to fit against (e.g. a
    tile where every channel was already excluded by something else
    before flag_amplitude_outliers ran), the band/fit legitimately have
    nothing to show and are left blank -- this is an inherent data
    limitation, not a bug to work around with a fabricated fallback.

    Otherwise, each polarisation gets its data, fit line, and shaded
    acceptance band drawn by _draw_gain_subplot. Channels caught by
    amplitude-outlier detection are shaded orange and marked with a black
    'x'; channels caught by the absolute gain-magnitude sanity cutoff
    instead (see HyperfitsSolutionGroup.flag_gain_max_cutoff) are also
    shaded orange, but marked with a black '+' -- the two reasons are
    told apart by marker shape, not colour. Shading (and the 'x'/'+'
    markers) is restricted to channels with their own genuine per-channel
    reason, not every channel of a fully-flagged tile (flag_mostly_bad_tiles
    NaNs every channel of a promoted tile regardless of whether that
    specific channel ever earned its own reason, so blindly using the
    tile-wide bad mask here would mark innocent channels as if they had
    been individually caught).

    Args:
        ax_gx: The gx subplot's axes.
        ax_gy: The gy subplot's axes.
        tile: Index of the tile to draw.
        bundle: Extracted data from _extract_combined_gains_bundle.
        bad_mask: Per-tile-per-channel "bad" mask, combining every file's
            channel reasons with any whole-tile flag.
        new_amplitude_bad_mask: Channels caught specifically by
            amplitude-outlier detection.
        new_gain_cutoff_bad_mask: Channels caught specifically by the
            absolute gain-magnitude sanity cutoff.
    """
    axis = bundle["axis"]
    x_padded = axis["x_padded"]
    x_real = axis["x_real"]
    gap_centres = axis["gap_centres"]
    stitched = bundle["n_files"] > 1
    tile_names = bundle["tile_names"]

    # Padded (NaN between pickets) -- continuous series only, so nothing is
    # drawn across a frequency gap that contains no data.
    before_gx = bundle["gx_amp"]
    before_gy = bundle["gy_amp"]
    fit = {COL_GX: bundle["fit_gx"], COL_GY: bundle["fit_gy"]}
    band_lower_gx, band_upper_gx = bundle["band_lower_gx"], bundle["band_upper_gx"]
    band_lower_gy, band_upper_gy = bundle["band_lower_gy"], bundle["band_upper_gy"]

    # Unpadded -- indexed per real chanblock, aligned with x_real and with the
    # flag-reason arrays.
    before_gx_real = bundle["gx_amp_real"]
    before_gy_real = bundle["gy_amp_real"]

    file_reasons = bundle["chan_reasons"]
    tile_reasons = bundle["tile_reasons"]
    mad_residual_threshold = bundle["mad_residual_threshold"]

    flagged = bad_mask[tile, :]
    n_flagged_here = int(flagged.sum())
    has_new_amplitude_bad_mask = bool(new_amplitude_bad_mask[tile, :].any())
    has_new_gain_cutoff_bad_mask = bool(new_gain_cutoff_bad_mask[tile, :].any())
    tile_name = tile_names[tile]
    tile_fully_flagged = bool(flagged.all())

    if tile_fully_flagged:
        reason = _tile_flag_reason_text(tile, tile_reasons)
        # Tiles flagged before Calvin's own analysis ever ran
        # (apply_tile_flags() NaNs them immediately) have no real
        # data to show even in the pristine snapshot -- text only,
        # same as before. A tile Calvin itself fully flagged (e.g.
        # promoted via flag_mostly_bad_tiles) still has real pristine
        # data, so it falls through to the normal plotting path below
        # instead, with the reason text and border added on top.
        structural_reason = bool(
            tile_reasons[tile]
            & (TileFlagReason.METAFITS | TileFlagReason.HYPERDRIVE_TILE | TileFlagReason.HYPERDRIVE_BASELINE)
        )
        if structural_reason:
            for ax in (ax_gx, ax_gy):
                ax.axis("on")
                ax.set_xticks([])
                ax.set_yticks([])
                ax.text(
                    0.5,
                    0.92,
                    reason,
                    ha="center",
                    va="top",
                    wrap=True,
                    fontsize=8,
                    color="red",
                    transform=ax.transAxes,
                )
                for spine in ax.spines.values():
                    spine.set_edgecolor("red")
                    spine.set_linewidth(2.5)

            ax_gx.set_title(f"Tile {tile} ({tile_name}) - gx amplitude - FULLY FLAGGED", fontsize=9)
            ax_gy.set_title(f"Tile {tile} ({tile_name}) - gy amplitude - FULLY FLAGGED", fontsize=9)
            return

    # Channels with a genuine per-channel reason of their own, as
    # opposed to `flagged` (bad_mask), which also broadcasts a
    # whole-tile reason (e.g. MOSTLY_BAD_CHANNELS) across every
    # chanblock regardless of that channel's own history. Used below
    # to keep the black 'x' marker restricted to channels that were
    # actually individually flagged -- a channel only NaN'd because
    # flag_mostly_bad_tiles promoted the whole tile never earned its
    # own reason and shouldn't look like it did.
    channel_level_flagged = file_reasons[tile, :] != ChannelFlagReason.NONE

    # -- shade flagged channels with a translucent orange band --
    # orange indicates partial (some-channels) flagging regardless of
    # reason; red is reserved for a fully flagged tile (see the
    # border-colour logic below), not for which specific check caught
    # the channel. Amplitude-outlier and gain-max-cutoff channels get
    # the same shading; the two are told apart by marker shape ('x' vs
    # '+' below).
    #
    # Drawn as one masked fill per axis rather than an axvspan per
    # channel. A stitched picket-fence plot has every file's chanblocks
    # on one axis (768 for a 24x32 observation), so a per-channel
    # axvspan loop could add tens of thousands of Rectangle patches to
    # a single page. step="mid" reproduces the old +/-0.5 span
    # boundaries and merges runs of adjacent flagged channels.
    shade_mask = new_amplitude_bad_mask[tile, :] | new_gain_cutoff_bad_mask[tile, :]
    if shade_mask.any():
        for ax in (ax_gx, ax_gy):
            ax.fill_between(
                x_real,
                0,
                1,
                where=shade_mask.tolist(),
                step="mid",
                color="orange",
                alpha=0.15,
                zorder=0,
                transform=mtransforms.blended_transform_factory(ax.transData, ax.transAxes),
            )

    # Identical for gx and gy: neither mask depends on polarisation, only on
    # the channel's own flag reason, so both subplots share one computation.
    flagged_other = channel_level_flagged & ~new_gain_cutoff_bad_mask[tile, :]
    gain_cutoff_here = new_gain_cutoff_bad_mask[tile, :]

    # -- gx subplot: data, fit line, shaded acceptance band --
    gx_title = _draw_gain_subplot(
        ax_gx,
        tile,
        tile_name,
        COL_GX,
        x_padded,
        x_real,
        before_gx,
        fit[COL_GX],
        band_lower_gx,
        band_upper_gx,
        before_gx_real,
        "tab:blue",
        "black",
        flagged_other,
        gain_cutoff_here,
        n_flagged_here,
    )

    # -- gy subplot: data, fit line, shaded acceptance band --
    gy_title = _draw_gain_subplot(
        ax_gy,
        tile,
        tile_name,
        COL_GY,
        x_padded,
        x_real,
        before_gy,
        fit[COL_GY],
        band_lower_gy,
        band_upper_gy,
        before_gy_real,
        "tab:green",
        "gray",
        flagged_other,
        gain_cutoff_here,
        n_flagged_here,
    )

    if stitched:
        # Mark every picket boundary, and label each segment with its real
        # coarse channel number so the compressed gaps are unambiguous.
        # vlines takes the whole array, so all boundaries cost one
        # LineCollection per axis rather than one artist each.
        for ax in (ax_gx, ax_gy):
            ax.vlines(
                gap_centres,
                *ax.get_ylim(),
                color="0.55",
                linestyles=(0, (2, 2)),
                linewidth=0.9,
                zorder=1,
            )
            ax.set_xticks(axis["tick_pos"])
            ax.set_xticklabels(axis["tick_labels"], fontsize=_STITCHED_TICK_FONTSIZE)
            ax.set_xlabel("coarse channel (gaps compressed, not to scale)", fontsize=7)

    # Every tile reaching this point has already had its structural
    # case (metafits/TILES-HDU/BASELINES-HDU) handled above via
    # return -- there's no per-channel data to summarise for that
    # case, but for every other tile (clean, partially flagged, or
    # fully flagged by Calvin itself), this is worth showing
    # regardless of severity: a clean tile just shows "100% Good".
    channel_summary = _channel_summary_text(tile, file_reasons, mad_residual_threshold)
    if tile_fully_flagged:
        border_color = "red"
    elif has_new_gain_cutoff_bad_mask or has_new_amplitude_bad_mask:
        border_color = "orange"
    else:
        border_color = None
    # Text colour matches border colour (both track the same
    # severity), falling back to black for a clean tile, which gets
    # no border colour change at all.
    # NOTE: orange is too hard to read, so using black
    if border_color == "orange":
        text_color = "black"
    else:
        text_color = border_color if border_color is not None else "black"

    for ax in (ax_gx, ax_gy):
        if border_color is not None:
            for spine in ax.spines.values():
                spine.set_edgecolor(border_color)
                spine.set_linewidth(2.5)
        ax.text(
            0.5,
            0.92,
            channel_summary,
            ha="center",
            va="top",
            wrap=True,
            fontsize=8,
            color=text_color,
            transform=ax.transAxes,
            zorder=10,
            bbox={"boxstyle": "round", "facecolor": "white", "alpha": 0.75, "edgecolor": "none"},
        )

    if tile_fully_flagged:
        ax_gx.set_title(gx_title + " - FULLY FLAGGED", fontsize=9)
        ax_gy.set_title(gy_title + " - FULLY FLAGGED", fontsize=9)


def _render_combined_gains_figure(
    bundle: dict,
    first_tile_index: int,
    n_tiles: int,
    solution_file_will_be_modified: bool,
) -> plt.Figure:
    """Render one page of the combined gx/gy amplitude plot from an
    extracted data bundle (see _extract_combined_gains_bundle).

    Every solution file appears on one compressed x-axis, with a marked
    break at each picket boundary (see _build_stitched_axis). The axis is
    therefore not linear in frequency; ticks are labelled with the real
    coarse channel number of each segment.

    This is the actual rendering logic behind plot_combined_gains, kept
    as a standalone function (touching only plain data, never a
    HyperfitsSolutionGroup) so it can run directly inside a
    ProcessPoolExecutor worker.

    Builds the grid of per-tile subplot pairs and delegates each tile's
    actual drawing to _draw_tile_panel -- see its docstring for the
    flagging/shading/summary conventions that apply to every tile.

    Args:
        bundle: Extracted data from _extract_combined_gains_bundle.
        first_tile_index: Index of the first tile to include in this page.
        n_tiles: Number of tiles to plot starting from first_tile_index.
            Also determines the subplot grid shape (see _grid_shape),
            except for a stitched multi-file plot, which uses the fixed
            STITCHED_TILE_COLS instead.
        solution_file_will_be_modified: If True, a note is added to the
            figure title.

    Returns:
        The matplotlib Figure containing the grid of per-tile subplot pairs.
    """
    n_files = bundle["n_files"]
    stitched = n_files > 1

    file_reasons = bundle["chan_reasons"]
    tile_reasons = bundle["tile_reasons"]

    n_tiles_total = file_reasons.shape[0]
    last_tile_index = min(first_tile_index + n_tiles, n_tiles_total)
    tile_range = range(first_tile_index, last_tile_index)

    # Per-channel "bad" mask, combining every file's channel reasons with
    # any whole-tile flag (broadcast across every chanblock).
    bad_mask = (file_reasons != ChannelFlagReason.NONE) | (tile_reasons[:, np.newaxis] != TileFlagReason.NONE)
    # Channels caught specifically by amplitude-outlier detection.
    new_amplitude_bad_mask = np.array(
        [[bool(reason & ChannelFlagReason.AMPLITUDE_OUTLIER) for reason in row] for row in file_reasons]
    )
    # Channels caught specifically by the absolute gain-magnitude sanity
    # cutoff (see HyperfitsSolutionGroup.flag_gain_max_cutoff) -- shown
    # distinctly from ordinary amplitude outliers, since a value large
    # enough to trip this is a numerical divergence, not a borderline
    # statistical call.
    new_gain_cutoff_bad_mask = np.array(
        [[bool(reason & ChannelFlagReason.GAIN_MAX_CUTOFF) for reason in row] for row in file_reasons]
    )

    n_plotted = len(tile_range)

    # A stitched subplot has to fit every file's chanblocks, so it gets a wider
    # subplot and a narrower grid. The narrower grid is what keeps the page from
    # becoming wider than the single-file layout already produces -- note total
    # figure area is n_tiles * 2 * subplot_area regardless of grid shape, so the
    # grid trades width for height and nothing else.
    n_rows, n_tile_cols, subplot_width_in = _page_grid(n_tiles, stitched)

    n_cols = n_tile_cols * 2
    fig, axes = plt.subplots(
        n_rows,
        n_cols,
        figsize=scale_plot_figsize(subplot_width_in * n_cols, 4 * n_rows),
        dpi=resolve_plot_dpi(150),
        squeeze=False,
    )

    for i, tile in enumerate(tile_range):
        row = i // n_tile_cols
        col_pair = (i % n_tile_cols) * 2
        ax_gx = axes[row, col_pair]
        ax_gy = axes[row, col_pair + 1]
        _draw_tile_panel(ax_gx, ax_gy, tile, bundle, bad_mask, new_amplitude_bad_mask, new_gain_cutoff_bad_mask)

    for i in range(n_plotted, n_rows * n_tile_cols):
        row = i // n_tile_cols
        col_pair = (i % n_tile_cols) * 2
        axes[row, col_pair].axis("off")
        axes[row, col_pair + 1].axis("off")

    handles, labels = [], []
    for ax in axes.flat:
        for handle, label in zip(*ax.get_legend_handles_labels(), strict=True):
            if label not in labels:
                handles.append(handle)
                labels.append(label)
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.97), ncol=4)

    obsid = bundle["obsid"]
    obsid_str = f"obsid {obsid}" if obsid is not None else "obsid unknown"
    modification_str = (
        "Jones matrices of outlier gains will be NaNed"
        if solution_file_will_be_modified
        else "Outlier gains are reported only, solutions file was not modified"
    )
    if stitched:
        scope_str = f"all {n_files} coarse channels stitched, gaps compressed"
    else:
        scope_str = "single contiguous band"
    fig.suptitle(
        f"Gain amplitude with fit & MAD band for {obsid_str} "
        f"({scope_str}; tiles {first_tile_index}-{last_tile_index - 1}; NO REF TILE). {modification_str}",
        y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.90))

    return fig


def _render_and_save_combined_gains_page(
    bundle: dict,
    first_tile_index: int,
    n_tiles: int,
    solution_file_will_be_modified: bool,
    page_path: str,
) -> tuple[bool, str]:
    """Render one page and save it to disk -- a ProcessPoolExecutor worker
    entry point for plot_outlier_gains.

    Returns (success, error) rather than the Figure itself: a matplotlib
    Figure doesn't survive a process boundary usefully (and every current
    caller of plot_outlier_gains only wants the on-disk file anyway).

    Args:
        bundle: Extracted data from _extract_combined_gains_bundle.
        first_tile_index: See _render_combined_gains_figure.
        n_tiles: See _render_combined_gains_figure.
        solution_file_will_be_modified: See _render_combined_gains_figure.
        page_path: Where to save this page.

    Returns:
        (True, "") on success, or (False, error message) if rendering or
        saving raised -- logged by the caller rather than propagated, so
        one bad page doesn't take down the rest of the batch.
    """
    try:
        fig = _render_combined_gains_figure(bundle, first_tile_index, n_tiles, solution_file_will_be_modified)
        fig.savefig(page_path, dpi=resolve_plot_dpi(150), bbox_inches="tight")
        plt.close(fig)
        return True, ""
    except Exception as exc:  # reported to the caller, not raised in the worker
        return False, str(exc)


def plot_outlier_gains(
    group: HyperfitsSolutionGroup,
    n_tiles: int = 16,
    output_path: str | None = None,
    pristine_jones: list[NDArray[np.complex128]] | None = None,
    solution_file_will_be_modified: bool = True,
    max_workers: int | None = None,
) -> list[plt.Figure]:
    """Plot flagged hyperdrive calibration gains, paged by tile, for the whole
    observation.

    Every solution file is stitched onto one compressed x-axis, so a
    picket-fence observation produces one paginated set covering all its coarse
    channels instead of a separate set per picket. This is both what a human
    reviewer asked for (24 separate plots per obs was unmanageable) and the
    single largest cost in post-hyperdrive processing for a picket fence: this
    function used to be called once per solution file, so a 24-file observation
    created 24 process pools and rendered 24x as many figures as a contiguous
    one, for the same number of data points. Measured on a real 24-picket
    observation, that was the bulk of the 3-5x runtime difference against a
    contiguous observation.

    Outlier detection itself is untouched and remains strictly per file (see
    HyperfitsSolutionGroup.flag_amplitude_outliers -- a single polynomial across
    a picket-fence frequency gap would be meaningless). Only presentation is
    stitched, and each file's own fit and acceptance band are drawn over that
    file's own segment only.

    Args:
        group: The solution group (see plot_combined_gains).
        n_tiles: Number of tiles per page/figure.
        output_path: If given, each page is saved using this as the base
            filename, with "_{first}-{last}" inserted before the extension.
            Because pages now span every file, the caller should no longer
            include a per-channel component in this name.
        pristine_jones: See plot_combined_gains -- one array per file.
        solution_file_will_be_modified: See plot_combined_gains.
        max_workers: Concurrent page renders. Defaults to an automatic,
            memory-aware cap (see _max_render_workers); pass an explicit value
            to override it. A stitched page peaks at a few hundred MB, so this
            is not merely a throughput knob -- an unbounded pool is what caused
            pages to fail with "[Errno 12] Cannot allocate memory".

    Returns:
        When output_path is None: a list of matplotlib Figures, one per
        page, in tile order (rendered sequentially in-process). When
        output_path is given (every current caller): pages are instead
        rendered and saved in parallel worker processes (see
        _render_and_save_combined_gains_page), and this returns an empty
        list -- no Figure objects survive the process boundary, and
        nothing currently uses the return value in that case anyway.
    """
    assert group.jones is not None
    n_tiles_total = group.jones[0].shape[0]
    n_pages = int(np.ceil(n_tiles_total / n_tiles))

    if output_path is None:
        # Rare/unused in practice -- no current caller relies on getting
        # live Figure objects back. Keep this path simple and sequential
        # rather than adding process-pool complexity for a case nothing
        # exercises.
        figures: list[plt.Figure] = []
        for page in range(n_pages):
            first_tile_index = page * n_tiles
            figures.append(
                plot_combined_gains(
                    group,
                    first_tile_index=first_tile_index,
                    n_tiles=n_tiles,
                    pristine_jones=pristine_jones,
                    solution_file_will_be_modified=solution_file_will_be_modified,
                )
            )
        return figures

    # One bundle and one pool for the whole observation, not one per file.
    bundle = _extract_combined_gains_bundle(group, pristine_jones)

    workers = (
        max_workers
        if max_workers is not None
        else _max_render_workers(n_tiles, stitched=bundle["n_files"] > 1, n_pages=n_pages)
    )

    with ProcessPoolExecutor(max_workers=workers) as executor:
        futures = {}
        for page in range(n_pages):
            first_tile_index = page * n_tiles
            last_tile_index = min(first_tile_index + n_tiles, n_tiles_total) - 1
            page_path = _paged_output_path(output_path, first_tile_index, last_tile_index)
            future = executor.submit(
                _render_and_save_combined_gains_page,
                bundle,
                first_tile_index,
                n_tiles,
                solution_file_will_be_modified,
                page_path,
            )
            futures[future] = page_path

        for future in as_completed(futures):
            success, error = future.result()
            if not success:
                logger.warning(f"Failed to render/save {futures[future]}: {error}")

    return []


def _channel_reason_counts_text(tile_idx: int, channel_reasons: list[NDArray[np.object_]]) -> str:
    """Summarise a tile's per-channel flag reasons as counts, across all files.

    E.g. "NON_CONVERGED(4ch), AMPLITUDE_OUTLIER(20ch)" -- deliberately does
    NOT list every individual flagged channel, per the "counts per reason,
    not a full channel list" requirement. Reasons appear in the order first
    encountered, not a fixed order.

    Args:
        tile_idx: Tile index (row position in the group's metafits_tiles_df).
        channel_reasons: One array per file, shape (n_tiles, n_chanblocks).

    Returns:
        A comma-separated "REASON(Nch)" string, empty if no channel-level
        reason is set anywhere for this tile.
    """
    counts: dict[str, int] = {}
    for file_reasons in channel_reasons:
        for reason in file_reasons[tile_idx]:
            if reason == ChannelFlagReason.NONE:
                continue
            for flag in ChannelFlagReason:
                if flag != ChannelFlagReason.NONE and reason & flag:
                    flag_name = cast(str, flag.name)
                    counts[flag_name] = counts.get(flag_name, 0) + 1
    return ", ".join(f"{name}({n}ch)" for name, n in counts.items())
