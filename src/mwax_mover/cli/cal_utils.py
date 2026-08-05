"""Example: parse hyperdrive calibration solutions and flag bad gains.

Uses the RESULTS (convergence), BASELINES (antenna flagging), and TILES
(tile names) HDUs, plus per-tile/per-pol robust polynomial fitting of gain
amplitude vs. frequency, to flag bad gains -- instead of a naive amplitude
clip or a cross-tile comparison (gain characteristics vary tile-to-tile for
real physical reasons -- cable length, dipole position, beam response --
so comparing a tile against its own smooth frequency trend is a cleaner
signal than comparing it against other tiles).

Per hyperdrive's FITS format:
https://mwatelescope.github.io/mwa_hyperdrive/defs/cal_sols_hyp.html

Assumes a single timeblock per solutions file (index 0 is used throughout).
"""

import argparse
import os
from dataclasses import dataclass

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
from astropy.io import fits


@dataclass
class CalSolutionQuality:
    """Per-tile/chanblock quality info for a hyperdrive solution file
    (single timeblock).

    Attributes:
        gains: Complex Jones matrix gains, shape (tile, chanblock, 2, 2).
        chanblock_converged: Boolean mask, shape (chanblock,). True if
            hyperdrive's RESULTS precision was non-NaN for that chanblock.
        precision: Raw RESULTS precision values, shape (chanblock,).
            Lower is better; NaN means flagged or failed to calibrate.
        tile_flagged: Boolean mask, shape (tile,). True if BASELINES weights
            indicate this antenna was flagged for the whole solve.
        tile_names: Tile name per antenna index, from the TILES HDU.
        n_tiles: Number of tiles/antennas.
        n_chanblocks: Number of chanblocks.
    """

    gains: np.ndarray
    chanblock_converged: np.ndarray
    precision: np.ndarray
    tile_flagged: np.ndarray
    tile_names: list[str]
    n_tiles: int
    n_chanblocks: int


def load_hyperdrive_solutions(path: str) -> CalSolutionQuality:
    """Load a hyperdrive calibration solutions FITS file.

    Args:
        path: Path to a hyperdrive `hyp_sols.fits`-style file. Assumed to
            contain a single timeblock.

    Returns:
        A CalSolutionQuality with gains reshaped into complex 2x2 Jones
        matrices, plus convergence, per-tile flagging, and tile name info
        (from the TILES HDU), for timeblock 0.
    """
    with fits.open(path) as hdul:
        solutions = hdul["SOLUTIONS"].data.astype(np.float64)
        n_timeblocks, n_tiles, n_chanblocks, _ = solutions.shape
        if n_timeblocks > 1:
            print(f"Note: file has {n_timeblocks} timeblocks; only timeblock 0 is used.")
        solutions = solutions[0]  # (tile, chanblock, 8)

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

    return CalSolutionQuality(
        gains=gains,
        chanblock_converged=chanblock_converged,
        precision=precision,
        tile_flagged=tile_flagged,
        tile_names=tile_names,
        n_tiles=n_tiles,
        n_chanblocks=n_chanblocks,
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
        print("Note: no TILES HDU found; falling back to generic tile names.")
        return [f"Tile{i}" for i in range(n_tiles)]

    tiles_table = hdul["TILES"].data
    antennas = tiles_table["Antenna"]
    tile_names_raw = tiles_table["TileName"]

    order = np.argsort(antennas)
    return [str(tile_names_raw[i]) for i in order]


def _tile_flags_from_baselines(baseline_weights: np.ndarray, n_tiles: int) -> np.ndarray:
    """Infer per-tile flagging from the BASELINES HDU's NaN pattern.

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


def _iterative_poly_clip(
    x: np.ndarray,
    y: np.ndarray,
    degree: int,
    residual_threshold: float,
    initial_valid: np.ndarray,
    max_iter: int = 10,
) -> tuple[np.ndarray, np.ndarray]:
    """Fit a robust, sigma-clipped polynomial to y(x) and flag outliers.

    Iteratively fits a degree-N polynomial on the currently-valid points,
    computes residuals against that fit, rejects points whose residual
    exceeds residual_threshold MADs, and refits -- repeating until the
    valid set stops changing (or max_iter is reached). This prevents a
    single extreme outlier from dragging a one-shot least-squares fit far
    enough off course that it masks the very outlier it should catch.

    Args:
        x: 1D array of independent variable values (e.g. chanblock index).
        y: 1D array of dependent variable values (e.g. gain amplitude).
        degree: Polynomial degree to fit.
        residual_threshold: Number of residual-MADs beyond which a point
            is considered an outlier.
        initial_valid: Boolean mask of points eligible to be fit at all
            (e.g. already excludes points flagged for unrelated reasons
            like non-convergence). Outliers are only ever a subset of this.
        max_iter: Maximum number of fit/clip iterations.

    Returns:
        A tuple (valid, residual):
        - valid: Boolean array, True for points considered good (within
          initial_valid and not rejected as an outlier).
        - residual: Float array, |y - fit| / mad at every point (including
          points outside initial_valid, computed against the final fit),
          NaN where no fit could be computed at all.
    """
    n = len(y)
    valid = initial_valid.copy()
    residual = np.full(n, np.nan, dtype=np.float64)

    if valid.sum() < degree + 2:
        return valid, residual

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
            # Refitting further would be unfit for purpose; stop here.
            break
        if np.array_equal(new_valid, valid):
            valid = new_valid
            break
        valid = new_valid

    return valid, residual


def flag_bad_gains(
    quality: CalSolutionQuality,
    poly_degree: int = 2,
    residual_threshold: float = 5.0,
) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """Build boolean 'bad gain' masks combining convergence, tile flags,
    NaN gains, and a per-tile robust polynomial-fit outlier check.

    For each tile, gx and gy amplitude are each fit independently with a
    sigma-clipped polynomial vs. chanblock index. A channel is flagged if
    EITHER polarisation's fit residual exceeds residual_threshold -- if one
    polarisation's gain is corrupted, the other usually can't be trusted
    either, even if it happens to look smooth on its own.

    Args:
        quality: Parsed solution quality info from load_hyperdrive_solutions.
        poly_degree: Degree of the polynomial fit to gain amplitude vs.
            chanblock index, per tile per polarisation.
        residual_threshold: Number of residual-MADs (from the per-tile
            polynomial fit) beyond which a channel is considered an outlier.

    Returns:
        A tuple (bad, new_flags, residual):
        - bad: Boolean array, shape (tile, chanblock), True where the gain
          should be treated as invalid (not just clipped). Combines
          convergence/tile/NaN criteria with the polynomial-fit check.
        - new_flags: Boolean array, same shape, True only where the
          polynomial-fit check flagged an entry that wasn't already
          flagged by convergence/tile/NaN criteria.
        - residual: Float array, shape (tile, chanblock), the larger of the
          gx and gy fit residuals (in MAD units) at each point. NaN where
          no fit could be computed.
    """
    n_tile, n_cb = quality.n_tiles, quality.n_chanblocks
    bad = np.zeros((n_tile, n_cb), dtype=bool)

    bad |= ~quality.chanblock_converged[np.newaxis, :]
    bad |= quality.tile_flagged[:, np.newaxis]

    gx_amp = np.abs(quality.gains[..., 0, 0])  # shape: (tile, chanblock)
    gy_amp = np.abs(quality.gains[..., 1, 1])  # shape: (tile, chanblock)

    any_nan_in_jones = np.any(np.isnan(quality.gains), axis=(-2, -1))  # (tile, chanblock)
    bad |= any_nan_in_jones

    # Snapshot of flags from the file itself (convergence, tile, NaN) before
    # the polynomial-fit check adds anything.
    original_bad = bad.copy()

    residual = np.full((n_tile, n_cb), np.nan, dtype=np.float64)
    chan_idx = np.arange(n_cb, dtype=np.float64)

    for tile in range(n_tile):
        initial_valid = ~original_bad[tile, :]

        valid_gx, residual_gx = _iterative_poly_clip(
            chan_idx, gx_amp[tile, :], poly_degree, residual_threshold, initial_valid
        )
        valid_gy, residual_gy = _iterative_poly_clip(
            chan_idx, gy_amp[tile, :], poly_degree, residual_threshold, initial_valid
        )

        # Either polarisation being an outlier is enough to distrust the
        # whole tile/channel entry.
        poly_bad = initial_valid & (~valid_gx | ~valid_gy)
        bad[tile, :] |= poly_bad

        residual[tile, :] = np.fmax(residual_gx, residual_gy)

    new_flags = bad & ~original_bad

    return bad, new_flags, residual


def interpolate_bad_gains(quality: CalSolutionQuality, bad_mask: np.ndarray) -> np.ndarray:
    """Replace flagged gains with a frequency-interpolated value per tile.

    Args:
        quality: Parsed solution quality info.
        bad_mask: Boolean mask from flag_bad_gains, shape (tile, chanblock).

    Returns:
        A copy of quality.gains with bad entries replaced by interpolated
        values (or NaN where interpolation isn't possible).
    """
    gains = quality.gains.copy()
    n_tile, n_cb = quality.n_tiles, quality.n_chanblocks
    chan_idx = np.arange(n_cb)

    for tile in range(n_tile):
        good = ~bad_mask[tile, :]
        if good.sum() < 2:
            gains[tile, ~good, :, :] = np.nan
            continue
        for a in range(2):
            for b in range(2):
                values = gains[tile, :, a, b]
                real_interp = np.interp(chan_idx, chan_idx[good], values.real[good])
                imag_interp = np.interp(chan_idx, chan_idx[good], values.imag[good])
                gains[tile, ~good, a, b] = real_interp[~good] + 1j * imag_interp[~good]

    return gains


def find_flagged_examples(bad_mask: np.ndarray, n: int = 5) -> list[tuple[int, int]]:
    """Find example (tile, chanblock) indices where bad_mask is True.

    Args:
        bad_mask: Boolean mask from flag_bad_gains, shape (tile, chanblock).
        n: Max number of example indices to return.

    Returns:
        List of up to n (tile, chanblock) index tuples.
    """
    coords = np.argwhere(bad_mask)
    return [tuple(int(x) for x in c) for c in coords[:n]]


def _grid_shape(n: int) -> tuple[int, int]:
    """Compute a near-square (rows, cols) grid that fits n tile-pairs.

    Chooses cols = ceil(sqrt(n)) and rows = ceil(n / cols). Each "tile"
    occupies two actual subplot columns (before/after + residual), so the
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


def plot_combined(
    quality: CalSolutionQuality,
    cleaned_gains: np.ndarray,
    bad_mask: np.ndarray,
    new_flags: np.ndarray,
    residual: np.ndarray,
    residual_threshold: float,
    pol: str = "gx",
    first_tile_index: int = 0,
    n_tiles: int = 16,
) -> plt.Figure:
    """Plot gain amplitude before/after cleaning, and fit residual, side by
    side for each tile, for a page of tiles starting at first_tile_index.

    Each tile gets two adjacent subplots: gain amplitude (before/after) on
    the left, polynomial-fit residual (max of gx/gy, in MAD units) on the
    right. Both share a red border if the tile has any NEW flags (caught by
    the polynomial-fit check, not already flagged by convergence/tile/NaN
    criteria from the original file).

    Args:
        quality: Parsed solution quality info (has gains and tile_names).
        cleaned_gains: Output of interpolate_bad_gains.
        bad_mask: Full 'bad' mask from flag_bad_gains, shape (tile, chanblock).
        new_flags: 'new_flags' mask from flag_bad_gains, same shape.
        residual: Residual array from flag_bad_gains, same shape.
        residual_threshold: The threshold used in flag_bad_gains, drawn as a
            horizontal reference line on the residual subplot.
        pol: Which Jones element to plot for the amplitude subplot: "gx" or
            "gy" (the residual subplot always shows the combined max of
            both polarisations, since flagging uses both).
        first_tile_index: Index of the first tile to include in this page.
        n_tiles: Number of tiles to plot starting from first_tile_index.
            Also determines the subplot grid shape (see _grid_shape).

    Returns:
        The matplotlib Figure containing the grid of per-tile subplot pairs.
    """
    pol_idx = (0, 0) if pol == "gx" else (1, 1)
    last_tile_index = min(first_tile_index + n_tiles, quality.n_tiles)
    tile_range = range(first_tile_index, last_tile_index)
    chan_idx = np.arange(quality.n_chanblocks)

    before_amp = np.abs(quality.gains[:, :, *pol_idx])
    after_amp = np.abs(cleaned_gains[:, :, *pol_idx])

    n_plotted = len(tile_range)
    n_rows, n_tile_cols = _grid_shape(n_tiles)
    n_cols = n_tile_cols * 2  # two subplots per tile
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(6 * n_cols, 4 * n_rows), dpi=150, squeeze=False)

    for i, tile in enumerate(tile_range):
        row = i // n_tile_cols
        col_pair = (i % n_tile_cols) * 2
        ax_ba = axes[row, col_pair]
        ax_res = axes[row, col_pair + 1]

        flagged = bad_mask[tile, :]
        n_flagged_here = int(flagged.sum())
        has_new_flags = bool(new_flags[tile, :].any())
        tile_name = quality.tile_names[tile]

        # -- before/after gain amplitude --
        ax_ba.plot(
            chan_idx,
            before_amp[tile],
            color="tab:red",
            alpha=0.6,
            linewidth=0.8,
            label="before",
        )
        ax_ba.plot(
            chan_idx,
            after_amp[tile],
            color="tab:blue",
            alpha=0.85,
            linewidth=0.6,
            label="after",
        )
        if flagged.any():
            ax_ba.scatter(
                chan_idx[flagged],
                before_amp[tile][flagged],
                color="black",
                marker="x",
                s=15,
                zorder=3,
                label="flagged",
            )
        ba_title = f"Tile {tile} ({tile_name}) - gain amplitude"
        if n_flagged_here == 0:
            ba_title += " (no flags)"
        ax_ba.set_title(ba_title, fontsize=9)
        ax_ba.yaxis.set_major_formatter(mticker.ScalarFormatter(useOffset=False))
        ax_ba.ticklabel_format(axis="y", style="plain")
        ax_ba.tick_params(labelsize=7)

        # -- polynomial fit residual --
        ax_res.plot(
            chan_idx,
            residual[tile],
            color="tab:purple",
            alpha=0.85,
            linewidth=0.6,
            label="residual (max gx/gy)",
        )
        ax_res.axhline(
            residual_threshold,
            color="gray",
            linestyle="--",
            linewidth=1.0,
            label="residual_threshold",
        )
        if flagged.any():
            ax_res.scatter(
                chan_idx[flagged],
                residual[tile][flagged],
                color="black",
                marker="x",
                s=15,
                zorder=3,
                label="flagged",
            )
        ax_res.set_title(f"Tile {tile} ({tile_name}) - fit residual", fontsize=9)
        ax_res.yaxis.set_major_formatter(mticker.ScalarFormatter(useOffset=False))
        ax_res.ticklabel_format(axis="y", style="plain")
        ax_res.tick_params(labelsize=7)

        if has_new_flags:
            for ax in (ax_ba, ax_res):
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
    for h, l in zip(res_handles, res_labels):
        if l not in labels:
            handles.append(h)
            labels.append(l)
    fig.legend(handles, labels, loc="upper center", ncol=len(labels))
    fig.suptitle(f"Gain amplitude & fit residual ({pol}, tiles {first_tile_index}-{last_tile_index - 1})")
    fig.tight_layout(rect=(0, 0, 1, 0.95))

    return fig


def run(
    solutions_path: str,
    poly_degree: int = 2,
    residual_threshold: float = 5.0,
    pol: str = "gx",
    n_tiles: int = 16,
    output_path: str | None = None,
    show: bool = True,
) -> tuple[CalSolutionQuality, np.ndarray, np.ndarray, list[plt.Figure]]:
    """Load, clean, and plot a hyperdrive calibration solutions file.

    Produces one figure per page of n_tiles tiles, covering all tiles in
    the file. Each figure shows, per tile, the gain amplitude before/after
    cleaning alongside the polynomial-fit residual.

    Args:
        solutions_path: Path to a hyperdrive solutions FITS file.
        poly_degree: Degree of the per-tile polynomial fit to gain
            amplitude vs. chanblock index.
        residual_threshold: Number of residual-MADs beyond which a channel
            is considered an outlier.
        pol: Which Jones element to plot: "gx" or "gy".
        n_tiles: Number of tiles per page/figure.
        output_path: If given, each page is saved using this as the base
            filename, with "_{first}-{last}" inserted before the extension.
        show: If True, attempt to call plt.show() for each figure in turn.
            Automatically skipped with a note if no display is detected.

    Returns:
        A tuple of (quality, bad_mask, cleaned_gains, figures).
    """
    quality = load_hyperdrive_solutions(solutions_path)
    bad_mask, new_flags, residual = flag_bad_gains(
        quality, poly_degree=poly_degree, residual_threshold=residual_threshold
    )
    cleaned_gains = interpolate_bad_gains(quality, bad_mask)

    n_bad = bad_mask.sum()
    n_total = bad_mask.size
    print(f"Flagged {n_bad}/{n_total} gain entries ({100 * n_bad / n_total:.2f}%)")

    if n_bad > 0:
        examples = find_flagged_examples(bad_mask, n=5)
        print("Example flagged (tile, chanblock) locations:")
        for tile, cb in examples:
            print(f"  tile={tile}, chanblock={cb}")

    has_display = None  # computed lazily, once, only if show is True

    n_page_tiles_total = quality.n_tiles
    n_pages = int(np.ceil(n_page_tiles_total / n_tiles))
    figures: list[plt.Figure] = []

    for page in range(n_pages):
        first_tile_index = page * n_tiles
        last_tile_index = min(first_tile_index + n_tiles, n_page_tiles_total) - 1

        fig = plot_combined(
            quality,
            cleaned_gains,
            bad_mask,
            new_flags,
            residual,
            residual_threshold,
            pol=pol,
            first_tile_index=first_tile_index,
            n_tiles=n_tiles,
        )
        figures.append(fig)

        if output_path is not None:
            page_path = _paged_output_path(output_path, first_tile_index, last_tile_index)
            fig.savefig(page_path, dpi=150, bbox_inches="tight")
            print(f"Saved figure to {page_path}")

        if show:
            if has_display is None:
                has_display = bool(os.environ.get("DISPLAY")) or plt.get_backend().lower() not in (
                    "agg",
                    "template",
                )
                if not has_display:
                    print(
                        "No interactive display detected (non-interactive matplotlib "
                        "backend); skipping plt.show() for all pages. Use --output "
                        "to save PNGs instead."
                    )
            if has_display:
                plt.show()

    return quality, bad_mask, cleaned_gains, figures


def main() -> None:
    """Command-line entry point: parses sys.argv and runs the cleaning/plot pipeline."""
    parser = argparse.ArgumentParser(description="Flag and clean bad gains in a hyperdrive solutions file.")
    parser.add_argument("solutions_path", help="Path to hyperdrive solutions FITS file")
    parser.add_argument(
        "--poly-degree",
        type=int,
        default=2,
        help="Degree of the per-tile polynomial fit to gain amplitude vs. frequency",
    )
    parser.add_argument(
        "--residual-threshold",
        type=float,
        default=5.0,
        help="MAD-of-residual threshold for flagging outliers from the polynomial fit",
    )
    parser.add_argument("--pol", choices=["gx", "gy"], default="gx")
    parser.add_argument(
        "--n-tiles",
        type=int,
        default=16,
        help="Tiles per page/figure; total_tiles / n_tiles figures are produced",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Base path for saved figures, e.g. test.png -> test_0-63.png, ...",
    )
    parser.add_argument("--no-show", action="store_true", help="Skip plt.show() entirely")
    args = parser.parse_args()

    run(
        solutions_path=args.solutions_path,
        poly_degree=args.poly_degree,
        residual_threshold=args.residual_threshold,
        pol=args.pol,
        n_tiles=args.n_tiles,
        output_path=args.output,
        show=not args.no_show,
    )


if __name__ == "__main__":
    main()
