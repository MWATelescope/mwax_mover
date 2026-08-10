"""Functions for checking and modifying hyperdrive calibration solution files."""

import logging
import os
import shutil
import sys
from dataclasses import dataclass
from typing import TextIO

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import mwalib
import numpy as np
import numpy.typing as npt  # noqa: F401
from astropy.io import fits

from mwax_mover.mwax_calvin_utils import parse_solution_channels

logger = logging.getLogger(__name__)


@dataclass
class CalSolutionQuality:
    """Per-tile/chanblock quality info for a hyperdrive solution file
    (single timeblock).

    Attributes:
        gains: Complex Jones matrix gains, shape (tile, chanblock, 2, 2).
            Indices [..., 0, 0] = gx, [..., 0, 1] = Dx, [..., 1, 0] = Dy,
            [..., 1, 1] = gy, matching hyperdrive's SOLUTIONS HDU layout.
        chanblock_converged: Boolean mask, shape (chanblock,). True if
            hyperdrive's RESULTS precision was non-NaN for that chanblock
            (i.e. the joint solve across all tiles converged for it).
        precision: Raw RESULTS precision values, shape (chanblock,). Lower
            is a better-converged solve; NaN means flagged or failed.
        tile_flagged: Boolean mask, shape (tile,). True if BASELINES
            weights indicate this antenna was flagged for the whole solve.
        tile_names: Tile name per antenna index, from the TILES HDU (or
            generic "TileN" placeholders if that HDU is absent).
        obsid: Observation ID (GPS timestamp) from the primary header's
            OBSID keyword, or None if not present in the file.
        n_tiles: Number of tiles/antennas.
        n_chanblocks: Number of chanblocks.
    """

    gains: np.ndarray
    chanblock_converged: np.ndarray
    precision: np.ndarray
    tile_flagged: np.ndarray
    tile_names: list[str]
    obsid: int | None
    n_tiles: int
    n_chanblocks: int
    first_coarse_channel: int
    last_coarse_channel: int


def tile_flag_reason(tile: int, quality: CalSolutionQuality) -> str:
    """Describe why a tile has zero good channels prior to polynomial-fit
    clipping.

    Checks the three criteria that make up original_bad: whole-tile
    flagging (BASELINES), chanblock non-convergence (RESULTS), and NaN
    values in the tile's Jones matrix. All that apply are reported.

    Args:
        tile: Tile/antenna index.
        quality: Parsed solution quality info.

    Returns:
        A semicolon-separated description of applicable reasons. Callers
        should only call this when original_bad[tile, :].any() is True
        (in practice, only when ALL channels for the tile are bad -- see
        build_tile_summary_table).
    """
    reasons = []

    if quality.tile_flagged[tile]:
        reasons.append("tile flagged (BASELINES)")

    n_bad_convergence = int((~quality.chanblock_converged).sum())
    if n_bad_convergence > 0:
        reasons.append(f"{n_bad_convergence} chanblock(s) non-converged (RESULTS NaN)")

    n_nan = int(np.any(np.isnan(quality.gains[tile]), axis=(-2, -1)).sum())
    if n_nan > 0:
        reasons.append(f"{n_nan} channel(s) NaN in Jones matrix")

    return "; ".join(reasons)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def load_hyperdrive_solutions(hyperdrive_solutions_filename: str) -> CalSolutionQuality:
    """Load a hyperdrive calibration solutions FITS file.

    Reads the SOLUTIONS, RESULTS, BASELINES, and (if present) TILES HDUs,
    per hyperdrive's documented format:
    https://mwatelescope.github.io/mwa_hyperdrive/defs/cal_sols_hyp.html

    Args:
        hyperdrive_solutions_filename: Path to a hyperdrive `hyp_sols.fits`-style file. Assumed to
            contain a single timeblock; if more are present, only
            timeblock 0 is used (with a printed note).

    Returns:
        A CalSolutionQuality with gains reshaped into complex 2x2 Jones
        matrices, plus convergence, per-tile flagging, and tile name info,
        for timeblock 0.
    """
    with fits.open(hyperdrive_solutions_filename) as hdul:
        solutions = hdul["SOLUTIONS"].data.astype(np.float64)
        n_timeblocks, n_tiles, n_chanblocks, _ = solutions.shape
        if n_timeblocks > 1:
            logger.warning(
                f"Note: file has {n_timeblocks} timeblocks; only timeblock 0 is used."
            )
        solutions = solutions[0]  # (tile, chanblock, 8)

        # The 8 floats per (tile, chanblock) are, in order:
        # gx_re, gx_im, Dx_re, Dx_im, Dy_re, Dy_im, gy_re, gy_im.
        gx = solutions[..., 0] + 1j * solutions[..., 1]
        dx = solutions[..., 2] + 1j * solutions[..., 3]
        dy = solutions[..., 4] + 1j * solutions[..., 5]
        gy = solutions[..., 6] + 1j * solutions[..., 7]
        gains = np.stack(
            [np.stack([gx, dx], axis=-1), np.stack([dy, gy], axis=-1)],
            axis=-2,
        )  # shape: (tile, chanblock, 2, 2)

        precision = hdul["RESULTS"].data.astype(np.float64)[0]  # (chanblock,)
        chanblock_converged = ~np.isnan(precision)

        baseline_weights = hdul["BASELINES"].data.astype(np.float64)
        tile_flagged = _tile_flags_from_baselines(baseline_weights, n_tiles)

        tile_names = _tile_names_from_tiles_hdu(hdul, n_tiles)

        obsid = None
        if "OBSID" in hdul[0].header:
            obsid = int(hdul[0].header["OBSID"])

        solution_file_channels = parse_solution_channels(hyperdrive_solutions_filename)

        if solution_file_channels is not None:
            (first_coarse_channel, last_coarse_channel) = solution_file_channels
        else:
            first_coarse_channel = -1
            last_coarse_channel = -1

    return CalSolutionQuality(
        gains=gains,
        chanblock_converged=chanblock_converged,
        precision=precision,
        tile_flagged=tile_flagged,
        tile_names=tile_names,
        obsid=obsid,
        n_tiles=n_tiles,
        n_chanblocks=n_chanblocks,
        first_coarse_channel=first_coarse_channel,
        last_coarse_channel=last_coarse_channel,
    )


def _tile_names_from_tiles_hdu(hdul: fits.HDUList, n_tiles: int) -> list[str]:
    """Read tile names from the TILES HDU, ordered by antenna index.

    Falls back to generic "TileN" placeholders if the TILES HDU is absent
    (it's an optional HDU per the hyperdrive format spec).

    Args:
        hdul: Open FITS HDUList for the solutions file.
        n_tiles: Number of tiles, used to size the fallback placeholder list.

    Returns:
        List of tile names in antenna-index order, length n_tiles.
    """
    if "TILES" not in hdul:
        logger.warning("Note: no TILES HDU found; falling back to generic tile names.")
        return [f"Tile{i}" for i in range(n_tiles)]

    tiles_table = hdul["TILES"].data
    antennas = tiles_table["Antenna"]
    tile_names_raw = tiles_table["TileName"]

    # Antenna indices should already be ascending, but sort explicitly to
    # guarantee alignment with the SOLUTIONS HDU's tile axis regardless.
    order = np.argsort(antennas)
    return [str(tile_names_raw[i]) for i in order]


def _tile_flags_from_baselines(
    baseline_weights: np.ndarray, n_tiles: int
) -> np.ndarray:
    """Infer per-tile flagging from the BASELINES HDU's NaN pattern.

    Baselines are ordered ascending: (0,1), (0,2), ..., (0,N-1), (1,2), ...
    (autocorrelations are not included in this HDU). A NaN weight on any
    baseline involving a tile means that tile was flagged for the solve.

    Args:
        baseline_weights: 1D array of baseline weights (NaN = flagged).
        n_tiles: Number of tiles/antennas in the observation.

    Returns:
        Boolean array of shape (n_tiles,), True where the tile is flagged.
    """
    tile_flagged = np.zeros(n_tiles, dtype=bool)
    idx = 0
    for i in range(n_tiles):
        for j in range(i + 1, n_tiles):
            if np.isnan(baseline_weights[idx]):
                tile_flagged[i] = True
                tile_flagged[j] = True
            idx += 1
    return tile_flagged


# ---------------------------------------------------------------------------
# Flagging
# ---------------------------------------------------------------------------


def _iterative_poly_clip(
    x: np.ndarray,
    y: np.ndarray,
    degree: int,
    residual_threshold: float,
    initial_valid: np.ndarray,
    max_iter: int = 10,
) -> tuple[np.ndarray, np.ndarray, np.ndarray, float, float]:
    """Fit a robust, sigma-clipped polynomial to y(x) and flag outliers.

    Iteratively fits a degree-N polynomial on the currently-valid points,
    computes residuals against that fit, rejects points whose residual
    exceeds residual_threshold MADs (median absolute deviations) from the
    median residual, and refits -- repeating until the valid set stops
    changing (or max_iter is reached). This guards against a single
    extreme outlier dragging a one-shot least-squares fit far enough off
    course that it masks the very outlier it should catch.

    Args:
        x: 1D array of independent variable values (e.g. chanblock index).
        y: 1D array of dependent variable values (e.g. gain amplitude).
        degree: Polynomial degree to fit.
        residual_threshold: Number of residual-MADs beyond which a point
            is considered an outlier. Dimensionless (see module-level notes
            below on units) -- e.g. 5.0 means "5x the typical residual
            scatter for this tile/pol", not an absolute gain value.
        initial_valid: Boolean mask of points eligible to be fit at all
            (e.g. already excludes points flagged for unrelated reasons
            like non-convergence). Outliers found here are only ever a
            subset of this mask.
        max_iter: Maximum number of fit/clip iterations.

    Returns:
        A tuple (valid, residual, fit):
        - valid: Boolean array, True for points considered good (within
          initial_valid and not rejected as an outlier).
        - residual: Float array, |y - fit - median_residual| / mad at
          every point (including points outside initial_valid, computed
          against the final fit). NaN everywhere if no fit could ever be
          computed (too few valid points).
        - fit: Float array, the final polynomial fit evaluated at every x.
          NaN everywhere if no fit could be computed at all.
        - mad: The median absolute deviation of residuals from the final
          fit iteration (scalar, same units as y). NaN if no fit could be
          computed. Used by callers to draw a +/- residual_threshold*mad
          shaded band around the fit line.
        - med: The median residual from the final fit iteration (scalar,
          same units as y). NaN if no fit could be computed.
    """
    n = len(y)
    valid = initial_valid.copy()
    residual = np.full(n, np.nan, dtype=np.float64)
    fit = np.full(n, np.nan, dtype=np.float64)
    mad = np.nan
    med = np.nan

    if valid.sum() < degree + 2:
        return valid, residual, fit, mad, med

    for _ in range(max_iter):
        coeffs = np.polyfit(x[valid], y[valid], degree)
        fit = np.polyval(coeffs, x)
        resid_all = y - fit

        med = np.median(resid_all[valid])
        mad = np.median(np.abs(resid_all[valid] - med))
        if mad == 0:
            residual[:] = 0.0
            break

        residual = np.abs(resid_all - med) / mad
        new_valid = initial_valid & (residual <= residual_threshold)

        if new_valid.sum() < degree + 2:
            break
        if np.array_equal(new_valid, valid):
            valid = new_valid
            break
        valid = new_valid

    return valid, residual, fit, mad, med


def flag_bad_gains(
    quality: CalSolutionQuality,
    poly_degree: int = 2,
    residual_threshold: float = 5.0,
) -> tuple[
    np.ndarray,
    np.ndarray,
    dict[str, tuple[np.ndarray, np.ndarray]],
    np.ndarray,
    dict[str, np.ndarray],
]:
    """Build boolean 'bad gain' masks combining convergence, tile flags,
    NaN gains, and a per-tile robust polynomial-fit outlier check.

    For each tile, gx and gy amplitude are each fit independently with a
    sigma-clipped polynomial vs. chanblock index (see _iterative_poly_clip).
    A channel is flagged if EITHER polarisation's fit residual exceeds
    residual_threshold -- if one polarisation's gain is corrupted, the
    other usually can't be trusted either, even if it happens to look
    smooth on its own.

    Note this deliberately does NOT compare a tile's gains against other
    tiles: real gain bandpasses vary tile-to-tile for physical reasons
    (cable length, dipole position, beam response), so cross-tile
    comparison produces false positives. Comparing a tile against its own
    smooth frequency trend avoids that.

    Args:
        quality: Parsed solution quality info from load_hyperdrive_solutions.
        poly_degree: Degree of the polynomial fit to gain amplitude vs.
            chanblock index, per tile per polarisation. Higher values fit
            more curvature (risk overfitting through a real anomaly);
            lower values risk false-flagging real bandpass curvature.
        residual_threshold: Number of residual-MADs (dimensionless; see
            _iterative_poly_clip) beyond which a channel is an outlier.

    Returns:
        A tuple (bad, new_flags, residual, original_bad, fit):
        - bad: Boolean array, shape (tile, chanblock), True where the gain
          should be treated as invalid (not just clipped). Combines
          convergence/tile/NaN criteria with the polynomial-fit check.
        - new_flags: Boolean array, same shape, True only where the
          polynomial-fit check flagged an entry that wasn't already
          flagged by convergence/tile/NaN criteria -- i.e. flags added by
          our own outlier detection that weren't already present in the
          original file.
        - band: Dict with keys "gx" and "gy", each a (lower, upper) tuple
          of float arrays, shape (tile, chanblock) -- the acceptable
          range around the polynomial fit (fit + median_residual +/-
          residual_threshold * mad), in the same units as gain amplitude.
          A value outside this band is what triggers a flag. NaN where no
          fit could be computed.
        - original_bad: Boolean array, shape (tile, chanblock), True where
          convergence/tile/NaN criteria flagged the entry, BEFORE the
          polynomial-fit check ran. Useful for distinguishing "broken in
          the original file" from "caught by our own outlier detection".
        - fit: Dict with keys "gx" and "gy", each a float array of shape
          (tile, chanblock) -- the polynomial fit curve for that
          polarisation. NaN where no fit could be computed.
    """
    n_tile, n_cb = quality.n_tiles, quality.n_chanblocks
    bad = np.zeros((n_tile, n_cb), dtype=bool)

    bad |= ~quality.chanblock_converged[np.newaxis, :]
    bad |= quality.tile_flagged[:, np.newaxis]

    gx_amp = np.abs(quality.gains[..., 0, 0])
    gy_amp = np.abs(quality.gains[..., 1, 1])

    any_nan_in_jones = np.any(np.isnan(quality.gains), axis=(-2, -1))
    bad |= any_nan_in_jones

    original_bad = bad.copy()

    fit_gx = np.full((n_tile, n_cb), np.nan, dtype=np.float64)
    fit_gy = np.full((n_tile, n_cb), np.nan, dtype=np.float64)
    band_lower_gx = np.full((n_tile, n_cb), np.nan, dtype=np.float64)
    band_upper_gx = np.full((n_tile, n_cb), np.nan, dtype=np.float64)
    band_lower_gy = np.full((n_tile, n_cb), np.nan, dtype=np.float64)
    band_upper_gy = np.full((n_tile, n_cb), np.nan, dtype=np.float64)
    chan_idx = np.arange(n_cb, dtype=np.float64)

    for tile in range(n_tile):
        initial_valid = ~original_bad[tile, :]

        valid_gx, _res_gx, fit_curve_gx, mad_gx, med_gx = _iterative_poly_clip(
            chan_idx, gx_amp[tile, :], poly_degree, residual_threshold, initial_valid
        )
        valid_gy, _res_gy, fit_curve_gy, mad_gy, med_gy = _iterative_poly_clip(
            chan_idx, gy_amp[tile, :], poly_degree, residual_threshold, initial_valid
        )

        poly_bad = initial_valid & (~valid_gx | ~valid_gy)
        bad[tile, :] |= poly_bad

        fit_gx[tile, :] = fit_curve_gx
        fit_gy[tile, :] = fit_curve_gy

        # Band = fit + median_residual +/- threshold * mad, in real gain
        # units -- reconstructs the accept/reject boundary that
        # _iterative_poly_clip applied in normalized (MAD-unit) space.
        band_lower_gx[tile, :] = fit_curve_gx + med_gx - residual_threshold * mad_gx
        band_upper_gx[tile, :] = fit_curve_gx + med_gx + residual_threshold * mad_gx
        band_lower_gy[tile, :] = fit_curve_gy + med_gy - residual_threshold * mad_gy
        band_upper_gy[tile, :] = fit_curve_gy + med_gy + residual_threshold * mad_gy

    new_flags = bad & ~original_bad
    band = {"gx": (band_lower_gx, band_upper_gx), "gy": (band_lower_gy, band_upper_gy)}
    fit = {"gx": fit_gx, "gy": fit_gy}

    return bad, new_flags, band, original_bad, fit


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------


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


def _paged_output_path(
    output_path: str, first_tile_index: int, last_tile_index: int
) -> str:
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


def plot_combined(
    quality: CalSolutionQuality,
    bad_mask: np.ndarray,
    new_flags: np.ndarray,
    band: dict[str, tuple[np.ndarray, np.ndarray]],
    fit: dict[str, np.ndarray],
    original_bad: np.ndarray,
    first_tile_index: int = 0,
    n_tiles: int = 16,
    display_gains: np.ndarray | None = None,
    solution_file_will_be_modified: bool = False,
) -> plt.Figure:
    """Plot gx and gy gain amplitude in separate subplots per tile, each
    with the polynomial fit line and a shaded +/- MAD-threshold band.

    Tiles where EVERY channel is flagged (bad_mask[tile, :].all()) have
    nothing meaningful to plot -- both subplots for that tile instead show
    the flag reason as text, with a black border (same thickness as an
    unflagged tile's default border).

    For all other tiles, each gets two adjacent subplots (gx, then gy).
    Each shows the raw gain amplitude, the polynomial fit line, and a
    shaded band showing the acceptable range around the fit (values
    outside this band are what triggers a flag). Channels caught by the
    polynomial-fit check (new_flags) are shaded with a translucent red
    vertical band so they stand out against the rest of the spectrum.

    Both subplots get a red border if the tile has any NEW flags.

    Args:
        quality: Parsed solution quality info (has gains and tile_names).
        bad_mask: Full 'bad' mask from flag_bad_gains, shape (tile, chanblock).
        new_flags: 'new_flags' mask from flag_bad_gains, same shape.
        band: Band dict from flag_bad_gains, keys "gx"/"gy", each a
            (lower, upper) tuple of arrays shape (tile, chanblock).
        fit: Fit dict from flag_bad_gains, keys "gx"/"gy", same shape.
        original_bad: original_bad mask from flag_bad_gains (or
            compute_bad_gains), shape (tile, chanblock) -- used to look up
            the flag reason text for fully-flagged tiles.
        first_tile_index: Index of the first tile to include in this page.
        n_tiles: Number of tiles to plot starting from first_tile_index.
            Also determines the subplot grid shape (see _grid_shape).
        display_gains: Gains array to compute the plotted amplitudes
            from, shape (tile, chanblock, 2, 2). Defaults to
            quality.gains if not given.
        solution_file_will_be_modified: If True, a warning is added to the figure title

    Returns:
        The matplotlib Figure containing the grid of per-tile subplot pairs.
    """
    last_tile_index = min(first_tile_index + n_tiles, quality.n_tiles)
    tile_range = range(first_tile_index, last_tile_index)
    chan_idx = np.arange(quality.n_chanblocks)

    gains_for_plot = quality.gains if display_gains is None else display_gains
    before_gx = np.abs(gains_for_plot[:, :, 0, 0])
    before_gy = np.abs(gains_for_plot[:, :, 1, 1])

    band_lower_gx, band_upper_gx = band["gx"]
    band_lower_gy, band_upper_gy = band["gy"]

    n_plotted = len(tile_range)
    n_rows, n_tile_cols = _grid_shape(n_tiles)
    n_cols = n_tile_cols * 2  # two subplots per tile: gx + gy
    fig, axes = plt.subplots(
        n_rows, n_cols, figsize=(6 * n_cols, 4 * n_rows), dpi=150, squeeze=False
    )

    for i, tile in enumerate(tile_range):
        row = i // n_tile_cols
        col_pair = (i % n_tile_cols) * 2
        ax_gx = axes[row, col_pair]
        ax_gy = axes[row, col_pair + 1]

        flagged = bad_mask[tile, :]
        n_flagged_here = int(flagged.sum())
        has_new_flags = bool(new_flags[tile, :].any())
        tile_name = quality.tile_names[tile]
        tile_fully_flagged = bool(flagged.all())

        if tile_fully_flagged:
            if original_bad[tile, :].any():
                reason = tile_flag_reason(tile, quality)
            else:
                reason = (
                    "All channels flagged by polynomial-fit outlier "
                    "detection (no non-converged/tile-flagged/NaN entries "
                    "in the original file)"
                )

            for ax in (ax_gx, ax_gy):
                ax.axis("on")
                ax.set_xticks([])
                ax.set_yticks([])
                ax.text(
                    0.5,
                    0.5,
                    reason,
                    ha="center",
                    va="center",
                    wrap=True,
                    fontsize=8,
                    transform=ax.transAxes,
                )
                for spine in ax.spines.values():
                    spine.set_edgecolor("black")
                    spine.set_linewidth(0.8)

            ax_gx.set_title(f"Tile {tile} ({tile_name}) - FULLY FLAGGED", fontsize=9)
            ax_gy.set_title(f"Tile {tile} ({tile_name}) - FULLY FLAGGED", fontsize=9)

            continue

        # -- shade NEW-flagged channels with a translucent vertical band --
        new_flagged_chans = np.where(new_flags[tile, :])[0]
        for cb in new_flagged_chans:
            ax_gx.axvspan(cb - 0.5, cb + 0.5, color="red", alpha=0.15, zorder=0)
            ax_gy.axvspan(cb - 0.5, cb + 0.5, color="red", alpha=0.15, zorder=0)

        # -- gx subplot: data, fit line, shaded acceptance band --
        ax_gx.fill_between(
            chan_idx,
            band_lower_gx[tile],
            band_upper_gx[tile],
            color="tab:blue",
            alpha=0.15,
            zorder=0,
            label="gx band",
        )
        ax_gx.plot(
            chan_idx,
            before_gx[tile],
            color="tab:blue",
            alpha=0.7,
            linewidth=0.8,
            label="gx",
        )
        ax_gx.plot(
            chan_idx,
            fit["gx"][tile],
            color="black",
            linestyle="--",
            alpha=0.8,
            linewidth=0.8,
            label="gx fit",
        )
        if flagged.any():
            ax_gx.scatter(
                chan_idx[flagged],
                before_gx[tile][flagged],
                color="black",
                marker="x",
                s=15,
                zorder=3,
                label="flagged",
            )

        gx_title = f"Tile {tile} ({tile_name}) - gx amplitude"
        if n_flagged_here == 0:
            gx_title += " (no flags)"
        ax_gx.set_title(gx_title, fontsize=9)
        ax_gx.yaxis.set_major_formatter(mticker.ScalarFormatter(useOffset=False))
        ax_gx.ticklabel_format(axis="y", style="plain")
        ax_gx.tick_params(labelsize=7)

        # -- gy subplot: data, fit line, shaded acceptance band --
        ax_gy.fill_between(
            chan_idx,
            band_lower_gy[tile],
            band_upper_gy[tile],
            color="tab:green",
            alpha=0.15,
            zorder=0,
            label="gy band",
        )
        ax_gy.plot(
            chan_idx,
            before_gy[tile],
            color="tab:green",
            alpha=0.7,
            linewidth=0.8,
            label="gy",
        )
        ax_gy.plot(
            chan_idx,
            fit["gy"][tile],
            color="gray",
            linestyle="--",
            alpha=0.8,
            linewidth=0.8,
            label="gy fit",
        )
        if flagged.any():
            ax_gy.scatter(
                chan_idx[flagged],
                before_gy[tile][flagged],
                color="black",
                marker="x",
                s=15,
                zorder=3,
                label="flagged",
            )

        gy_title = f"Tile {tile} ({tile_name}) - gy amplitude"
        if n_flagged_here == 0:
            gy_title += " (no flags)"
        ax_gy.set_title(gy_title, fontsize=9)
        ax_gy.yaxis.set_major_formatter(mticker.ScalarFormatter(useOffset=False))
        ax_gy.ticklabel_format(axis="y", style="plain")
        ax_gy.tick_params(labelsize=7)

        if has_new_flags:
            for ax in (ax_gx, ax_gy):
                for spine in ax.spines.values():
                    spine.set_edgecolor("red")
                    spine.set_linewidth(2.5)

    for i in range(n_plotted, n_rows * n_tile_cols):
        row = i // n_tile_cols
        col_pair = (i % n_tile_cols) * 2
        axes[row, col_pair].axis("off")
        axes[row, col_pair + 1].axis("off")

    handles, labels = axes[0, 0].get_legend_handles_labels()
    res_handles, res_labels = axes[0, 1].get_legend_handles_labels()
    for handle, label in zip(res_handles, res_labels):
        if label not in labels:
            handles.append(handle)
            labels.append(label)
    fig.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, 0.97), ncol=4)
    obsid_str = (
        f"obsid {quality.obsid}" if quality.obsid is not None else "obsid unknown"
    )
    chan_str = (
        f"channels {quality.first_coarse_channel}-{quality.last_coarse_channel}"
        if quality.first_coarse_channel >= 0
        else "All Channels"
    )
    modification_str = (
        "Jones matrices of outlier gains will be NaNed"
        if solution_file_will_be_modified
        else "Outlier gains are reported only, solutions file was not modified"
    )
    fig.suptitle(
        f"Gain amplitude with fit & MAD band for {obsid_str} - (channels: {chan_str}; tiles: {first_tile_index}-{last_tile_index - 1}; NO REF TILE). {modification_str}",
        y=0.995,
    )
    fig.tight_layout(rect=(0, 0, 1, 0.90))

    return fig


def _write_bad_gains_as_nan(solutions_path: str, bad_mask: np.ndarray) -> str:
    """Back up a hyperdrive solutions file, then overwrite it with bad
    entries set to NaN.

    Makes a copy of the original file at `{solutions_path}.original`
    before modifying anything (overwriting that backup if it already
    exists, with a printed warning). Then opens the original path in
    update mode and sets all 4 complex Jones matrix terms (8 underlying
    floats) to NaN for every (tile, chanblock) flagged in bad_mask.

    Args:
        solutions_path: Path to the hyperdrive solutions FITS file to
            back up and overwrite.
        bad_mask: Boolean mask, shape (tile, chanblock), True where the
            entry should be set to NaN. Assumes timeblock 0 (the only
            timeblock this tool supports).

    Returns:
        The backup file path.
    """
    backup_path = f"{solutions_path}".replace(".fits", ".original.fits")
    if os.path.exists(backup_path):
        logger.warning(
            f"Warning: backup {backup_path} already exists and will be overwritten."
        )
    shutil.copy2(solutions_path, backup_path)
    logger.debug(f"Backed up original file to {backup_path}")

    with fits.open(solutions_path, mode="update") as hdul:
        # Force native byte order: FITS files are big-endian on disk, and
        # we need a native-endian array to safely write NaN into the
        # 8-float-per-entry layout without corrupting adjacent bytes.
        data = np.array(hdul["SOLUTIONS"].data, dtype=np.float64)
        # shape: (timeblock, tile, chanblock, 8)

        # bad_mask covers timeblock 0 only (the only timeblock this tool
        # supports). data[0][bad_mask] selects the (n_flagged, 8) rows of
        # flagged (tile, chanblock) entries; broadcasting np.nan sets all
        # 8 floats (gx, Dx, Dy, gy re/im) to NaN for each.
        data[0][bad_mask] = np.nan

        hdul["SOLUTIONS"].data = data
        hdul.flush()

    n_flagged = int(bad_mask.sum())
    logger.info(
        f"Wrote NaN for {n_flagged} flagged (tile, chanblock) entries to {solutions_path}"
    )

    return backup_path


def compute_outlier_gains(
    solutions_path: str,
    poly_degree: int = 2,
    residual_threshold: float = 5.0,
    modify_gains: bool = False,
) -> tuple[
    CalSolutionQuality,
    np.ndarray,
    np.ndarray,
    dict[str, tuple[np.ndarray, np.ndarray]],
    dict[str, np.ndarray],
    np.ndarray,
    np.ndarray,
]:
    """Load a hyperdrive calibration solutions file and flag bad gains.

    Args:
        solutions_path: Path to a hyperdrive solutions FITS file.
        poly_degree: Degree of the per-tile polynomial fit to gain
            amplitude vs. chanblock index. See flag_bad_gains.
        residual_threshold: Number of residual-MADs beyond which a channel
            is considered an outlier. See flag_bad_gains.
        modify_gains: If True, every Jones matrix flagged as bad (per the
            returned bad_mask) has all 4 complex terms (gx, Dx, Dy, gy --
            i.e. all 8 underlying floats) set to NaN in the returned
            quality.gains array (in memory), AND the same NaN-out is
            written back to the FITS file on disk at solutions_path. The
            original file is first backed up to
            "{solutions_path}.original" (overwriting any existing backup
            at that path, with a printed warning) before the original
            path is overwritten.

    Returns:
        A tuple (quality, bad_mask, new_flags, band, fit, original_gains,
        original_bad). original_gains is a copy of quality.gains taken
        BEFORE any modify_gains NaN-out is applied -- pass it as
        display_gains to plot_bad_gains if you want plots to show the
        original values even when modify_gains=True. original_bad is a
        boolean mask of the entries that were already flagged in the
        original file (convergence/tile/NaN criteria) before the
        polynomial-fit check ran, so callers can distinguish "already
        broken in the original file" from "newly caught by our own
        outlier detection".
    """
    quality = load_hyperdrive_solutions(solutions_path)
    bad_mask, new_flags, band, original_bad, fit = flag_bad_gains(
        quality, poly_degree=poly_degree, residual_threshold=residual_threshold
    )

    original_gains = quality.gains.copy()

    if modify_gains:
        quality.gains[bad_mask] = np.nan + 1j * np.nan
        _write_bad_gains_as_nan(solutions_path, bad_mask)

    return quality, bad_mask, new_flags, band, fit, original_gains, original_bad


def plot_outlier_gains(
    quality: CalSolutionQuality,
    bad_mask: np.ndarray,
    new_flags: np.ndarray,
    band: dict[str, tuple[np.ndarray, np.ndarray]],
    fit: dict[str, np.ndarray],
    original_bad: np.ndarray,
    n_tiles: int = 16,
    output_path: str | None = None,
    display_gains: np.ndarray | None = None,
    solution_file_will_be_modified: bool = False,
) -> list[plt.Figure]:
    """Plot flagged hyperdrive calibration gains, paged by tile.

    Args:
        quality: Parsed solution quality info.
        bad_mask: 'bad' mask from flag_bad_gains, shape (tile, chanblock).
        new_flags: 'new_flags' mask from flag_bad_gains, same shape.
        band: Band dict from flag_bad_gains, keys "gx"/"gy".
        fit: Fit dict from flag_bad_gains, keys "gx"/"gy".
        original_bad: original_bad mask from flag_bad_gains.
        n_tiles: Number of tiles per page/figure.
        output_path: If given, each page is saved using this as the base
            filename, with "_{first}-{last}" inserted before the extension.
        display_gains: Gains array to plot amplitudes from, shape
            (tile, chanblock, 2, 2). Defaults to quality.gains if not given.
        solution_file_will_be_modified: If True, a warning is added to the solution plot
    Returns:
        A list of matplotlib Figures, one per page, in tile order.
    """
    n_page_tiles_total = quality.n_tiles
    n_pages = int(np.ceil(n_page_tiles_total / n_tiles))
    figures: list[plt.Figure] = []

    for page in range(n_pages):
        first_tile_index = page * n_tiles
        last_tile_index = min(first_tile_index + n_tiles, n_page_tiles_total) - 1

        fig = plot_combined(
            quality,
            bad_mask,
            new_flags,
            band,
            fit,
            original_bad,
            first_tile_index=first_tile_index,
            n_tiles=n_tiles,
            display_gains=display_gains,
            solution_file_will_be_modified=solution_file_will_be_modified,
        )
        figures.append(fig)

        if output_path is not None:
            page_path = _paged_output_path(
                output_path, first_tile_index, last_tile_index
            )
            fig.savefig(page_path, dpi=150, bbox_inches="tight")

    return figures


def clip_hyperdrive_solution_gains(
    hyperdrive_fits_file: str, cut_off: float, mc: mwalib.MetafitsContext
):
    """Clip hyperdrive calibration solution gains exceeding a threshold.

    Opens a hyperdrive FITS solution file, finds any Jones matrices where at
    least one complex gain amplitude exceeds the cut_off threshold, sets all
    four polarisations of those Jones matrices to NaN, and writes the result
    back to disk.

    Args:
        hyperdrive_fits_file: Path to the hyperdrive FITS solution file.
        cut_off: Threshold above which an entire Jones matrix is set to NaN.
        mc: MetafitsContext used to determine the tileid and name from the
            antenna indices in the solutions file.
    """

    HDU = "SOLUTIONS"
    # Polarisation names indexed by their position in the last axis of the
    # solutions array: 0=XX, 1=XY, 2=YX, 3=YY
    POL_NAMES = ["XX", "XY", "YX", "YY"]

    with fits.open(hyperdrive_fits_file, mode="update") as hdul:
        if HDU not in hdul:
            raise Exception(
                f"Warning: No SOLUTIONS HDU found in {hyperdrive_fits_file}"
            )

        logger.info(
            f"checking solutions file {hyperdrive_fits_file} for gains > {cut_off}"
        )

        # The SOLUTIONS HDU stores each complex gain as two consecutive float64
        # values (real, imag), so the raw FITS column layout is:
        #   shape: (time, antenna, chan, 8)  -- 8 floats = 4 pols × (re + im)
        #
        # Force native byte order (little-endian on x86): FITS files are
        # big-endian, and np.view() below requires a native-endian array to
        # safely reinterpret the memory as complex128.
        data = np.array(hdul[HDU].data, dtype=np.float64)
        # shape: (time, antenna, chan, 8)

        # Reinterpret each consecutive pair of float64 (re, im) as one
        # complex128 value. np.view() does not copy data; it aliases the same
        # memory with a different dtype, halving the size of the last axis.
        data_complex = data.view(np.complex128)
        # shape: (time, antenna, chan, 4)  -- last axis: [XX, XY, YX, YY]
        #
        # data_complex and data share the same underlying buffer, so writes to
        # data_complex are visible through data (and vice versa). This is what
        # allows us to flag via data_complex and then assign data back to the HDU.

        # Compute the amplitude (absolute value) of every complex gain sample.
        amp = np.abs(data_complex)
        # shape: (time, antenna, chan, 4)  -- last axis: [XX, XY, YX, YY]

        # Total counts used in logging below.
        total_samples = amp.size  # total (time, ant, chan, pol) samples
        total_jones = (
            amp.shape[0] * amp.shape[1] * amp.shape[2]
        )  # total (time, ant, chan) Jones matrices

        # Boolean mask: True wherever an individual gain amplitude exceeds the
        # threshold. Kept separate from the Jones-matrix flag below so we can
        # report how many individual pol samples actually triggered the cutoff.
        mask = amp > cut_off
        # shape: (time, antenna, chan, 4)  -- same layout as amp

        # Reduce across the polarisation axis: True for any (time, ant, chan)
        # where at least one of XX, XY, YX, YY exceeded the threshold.
        # If any one pol is bad the whole Jones matrix is considered unreliable,
        # so we flag all four pols together.
        any_flagged = mask.any(axis=-1)
        # shape: (time, antenna, chan)  -- pol axis removed; True = entire Jones matrix flagged

        # Set all four polarisations of every flagged Jones matrix to NaN + NaN*j.
        # Indexing data_complex with a (time, ant, chan) boolean array selects
        # rows of shape (4,) — one row per flagged Jones matrix — so the single
        # assignment sets all four pols at once.
        # Because data_complex is a view of data, this also updates data in-place;
        # no separate copy-back is needed for the flags.
        data_complex[any_flagged] = np.nan + 1j * np.nan

        # Count the pol samples that actually exceeded the cutoff (what triggered flagging).
        exceeded_count = int(mask.sum())
        # Count how many Jones matrices were flagged, and the resulting NaN pol samples
        # (always 4× the Jones matrix count since all four pols are set to NaN together).
        jones_flagged_count = int(any_flagged.sum())
        nan_pol_count = jones_flagged_count * 4

        logger.debug(
            f"Gains > {cut_off}:"
            f" {exceeded_count} / {total_samples} pol samples exceeded cutoff"
            f" ({100 * exceeded_count / total_samples:.2f}%);"
            f" {jones_flagged_count} / {total_jones} Jones matrices"
            f" ({nan_pol_count} / {total_samples} pol samples) set to NaN"
            f" ({100 * jones_flagged_count / total_jones:.2f}% of Jones matrices)"
        )

        if jones_flagged_count > 0:
            # np.argwhere(any_flagged) returns the indices of every True element.
            # Each row is one flagged Jones matrix: [time_idx, ant_idx, chan_idx]
            flagged_indices = np.argwhere(any_flagged)
            # shape: (jones_flagged_count, 3)  -- columns: [time, antenna, chan]

            # Collapse to unique (antenna, chan) pairs so we log one line per
            # affected tile+channel rather than one line per time step.
            # Slice columns 1:3 to get just [ant_idx, chan_idx].
            unique_ant_chan = np.unique(flagged_indices[:, 1:3], axis=0)
            # shape: (n_unique_ant_chan, 2)  -- columns: [antenna, chan]

            for ant_idx, chan_idx in unique_ant_chan:
                # Report the worst (max) amplitude seen across all time steps for
                # each of the four polarisations at this (antenna, chan) pair.
                # amp[:, ant_idx, chan_idx, p] selects all time steps for a fixed
                # (antenna, chan, pol) triple; shape: (n_timesteps,).
                # All four pols are shown regardless of which one(s) triggered the
                # cutoff, since the entire Jones matrix has been set to NaN.
                pol_details = ", ".join(
                    f"{POL_NAMES[p]}(Gain={amp[:, ant_idx, chan_idx, p].max():.4f})"
                    for p in range(4)
                )

                logger.debug(
                    f"  antenna_idx={ant_idx}"
                    f" [TileId: {mc.antennas[ant_idx].tile_id}"
                    f" Name: {mc.antennas[ant_idx].tile_name}],"
                    f" chan_idx={chan_idx}: all pols set to NaN: {pol_details}"
                )

        # Write the modified float64 array (which contains the NaN-flagged
        # complex pairs) back to the HDU and flush to disk.
        # Note: we assign `data` (the float64 view) rather than `data_complex`
        # because that is the shape the HDU originally held.
        hdul[HDU].data = data
        hdul.flush()

        logger.info(f"finished rewriting solutions file {hyperdrive_fits_file}")


def add_digital_gains_column(
    hyperdrive_solution_filename: str,
    metafits_context: mwalib.MetafitsContext,
):
    """Add a float32[24] digital-gains column to a FITS binary table HDU.

    Builds the 24-element (one per coarse channel) digital gain array for
    each tile/row in the given HDU, sourced from a populated mwalib
    MetafitsContext, and appends it as a new column. Rows are matched to
    metafits rf_inputs by tile ID so gains line up correctly even if HDU
    row order differs from metafits order. Only the X polarisation's
    digital gains are used, since X and Y always carry the same values.

    Args:
        hyperdrive_solution_filename: Path to the FITS file to modify.
        metafits_context: A populated mwalib.MetafitsContext instance;
            the caller is responsible for constructing/populating it.

    Returns:
        The new fits.BinTableHDU with the digital gains column added.

    Raises:
        KeyError: If a tile ID present in the HDU cannot be found among
            metafits_context.rf_inputs for the X polarisation.
    """
    hdu_name = "TILES"
    col_name = "DigitalGains"
    id_col = "Antenna"

    # Lookup: tile_id -> digital_gains, X pol only (X and Y are identical).
    gains_by_tile = {
        rf.ant: rf.digital_gains
        for rf in metafits_context.rf_inputs
        if rf.pol == mwalib.Pol.X
    }

    with fits.open(hyperdrive_solution_filename, mode="update") as hdul:
        tile_hdu = hdul[hdu_name]
        tile_ids = tile_hdu.data[id_col]

        # Does this column already exist? If so don't do anything but log a warning.
        if col_name in hdul[hdu_name].columns.names:
            logger.warning(
                f"Column '{col_name}' already exists in HDU '{hdu_name}'; skipping addition."
            )
            return tile_hdu

        try:
            gains_array = np.array(
                [gains_by_tile[int(tid)] for tid in tile_ids], dtype=np.float32
            )
            print(gains_array)
        except KeyError as e:
            raise KeyError(
                f"Tile ID {e} in HDU '{hdu_name}' not found in metafits rf_inputs for pol='X'"
            ) from e

        n_chans = gains_array.shape[1]
        new_col = fits.Column(
            name=col_name,
            format=f"{n_chans}E",
            array=gains_array,
        )

        new_hdu = fits.BinTableHDU.from_columns(
            tile_hdu.columns + new_col, header=tile_hdu.header
        )
        new_hdu.name = hdu_name

        hdul[hdul.index_of(hdu_name)] = new_hdu
        hdul.flush()

    return new_hdu


def build_tile_summary_table(
    quality: CalSolutionQuality, original_bad: np.ndarray
) -> list[dict]:
    """Build a per-tile summary for console display.

    A tile is only reported as fully flagged (with a reason string) if
    EVERY channel is bad per original_bad (convergence/tile/NaN criteria,
    before polynomial-fit clipping). Otherwise, before-cleaning gain
    amplitude stats (min/median/max/stdev, for both gx and gy) are
    computed over the tile's good channels only, with a count of how many
    channels were excluded from that calculation.

    Args:
        quality: Parsed solution quality info.
        original_bad: original_bad mask from flag_bad_gains, shape
            (tile, chanblock).

    Returns:
        List of one dict per tile, each with keys:
        - tile (int), tile_name (str)
        - fully_flagged (bool): True only if every channel is bad.
        - reason (str): populated only if fully_flagged.
        - n_good (int), n_excluded (int): channel counts.
        - gx_min, gx_median, gx_max, gx_std, gy_min, gy_median, gy_max,
          gy_std (float): NaN if fully_flagged, else computed over good
          channels only.
    """
    gx_amp = np.abs(quality.gains[..., 0, 0])
    gy_amp = np.abs(quality.gains[..., 1, 1])

    rows = []
    for tile in range(quality.n_tiles):
        tile_bad = original_bad[tile, :]
        good = ~tile_bad
        n_good = int(good.sum())
        n_excluded = int(tile_bad.sum())
        fully_flagged = n_good == 0

        row = {
            "tile": tile,
            "tile_name": quality.tile_names[tile],
            "fully_flagged": fully_flagged,
            "reason": "",
            "n_good": n_good,
            "n_excluded": n_excluded,
            "gx_min": np.nan,
            "gx_median": np.nan,
            "gx_max": np.nan,
            "gx_std": np.nan,
            "gy_min": np.nan,
            "gy_median": np.nan,
            "gy_max": np.nan,
            "gy_std": np.nan,
        }

        if fully_flagged:
            row["reason"] = tile_flag_reason(tile, quality)
        else:
            row["gx_min"] = float(np.min(gx_amp[tile, good]))
            row["gx_median"] = float(np.median(gx_amp[tile, good]))
            row["gx_max"] = float(np.max(gx_amp[tile, good]))
            row["gx_std"] = float(np.std(gx_amp[tile, good]))
            row["gy_min"] = float(np.min(gy_amp[tile, good]))
            row["gy_median"] = float(np.median(gy_amp[tile, good]))
            row["gy_max"] = float(np.max(gy_amp[tile, good]))
            row["gy_std"] = float(np.std(gy_amp[tile, good]))

        rows.append(row)

    return rows


def write_tile_summary_table(rows: list[dict], file: TextIO = sys.stdout) -> None:
    """Write a per-tile summary table to a file-like object.

    Tiles with zero good channels show their flag reason in place of gain
    stats. All other tiles show before-cleaning min/median/max/stdev gain
    amplitude for gx and gy, computed over their good channels only, with
    a note if any channels were excluded from that calculation.

    Args:
        rows: Output of build_tile_summary_table.
        file: A writable, text-mode file-like object (anything with a
            .write() method taking str -- e.g. an open file, io.StringIO,
            or sys.stdout). Defaults to sys.stdout, so existing callers
            printing to the console need no changes. To write to a real
            file: `with open(path, "w") as f: print_tile_summary_table(rows, file=f)`.
    """
    id_w = 6
    name_w = max(10, max(len(r["tile_name"]) for r in rows) + 2)
    info_w = 100

    header = f"{'TileID':<{id_w}} {'TileName':<{name_w}} {'Info':<{info_w}}"
    print(header, file=file)
    print("-" * len(header), file=file)

    for r in rows:
        if r["fully_flagged"]:
            info = f"FLAGGED (no good channels): {r['reason']}"
        else:
            info = (
                f"gx[min={r['gx_min']:.2f} med={r['gx_median']:.2f} "
                f"max={r['gx_max']:.2f} std={r['gx_std']:.2f}]  "
                f"gy[min={r['gy_min']:.2f} med={r['gy_median']:.2f} "
                f"max={r['gy_max']:.2f} std={r['gy_std']:.2f}]"
            )
            if r["n_excluded"] > 0:
                info += f"  ({r['n_excluded']} channel(s) excluded)"
        print(
            f"{r['tile']:<{id_w}} {r['tile_name']:<{name_w}} {info:<{info_w}}",
            file=file,
        )
