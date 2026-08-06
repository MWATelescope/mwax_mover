"""cal_utils: parse hyperdrive calibration solutions, flag bad gains, and plot.

Background
----------
Hyperdrive solves for a per-tile, per-frequency-chanblock 2x2 Jones matrix
gain via sky-based calibration. Occasionally a solve diverges for a
particular tile/channel, producing a pathologically large gain (e.g. 1e15+)
instead of the normal ~1-20 range. Applying such a gain to real-time
beamforming corrupts that tile's contribution.

This tool detects bad gains using several signals from the solutions FITS
file itself:

1. RESULTS HDU: hyperdrive's own convergence precision per chanblock. NaN
   means the chanblock failed to converge or was pre-flagged.
2. BASELINES HDU: NaN baseline weights indicate a whole tile was flagged
   for the entire solve.
3. NaN values directly in the SOLUTIONS HDU's Jones matrix entries.
4. A per-tile, per-polarisation robust (sigma-clipped) polynomial fit of
   gain amplitude vs. frequency. Real gain bandpasses vary smoothly with
   frequency but differ tile-to-tile (cable length, dipole position, beam
   response, etc.), so comparing a tile against its OWN smooth frequency
   trend is a cleaner signal than comparing it against other tiles.

Bad entries are then replaced via frequency interpolation rather than
clipped to an arbitrary ceiling, since clipping only touches amplitude
(leaving a corrupted phase in place) and discards the "was this actually
bad" diagnostic information.

Assumes a single timeblock per solutions file (index 0 is used throughout).

Usage
-----
    cal_utils solutions.fits
    cal_utils solutions.fits --poly-degree 3 --residual-threshold 4.0
    cal_utils solutions.fits --output out.png --no-show
    cal_utils solutions.fits --dump-tile 139
"""

import argparse

import numpy as np
from astropy.io import fits

from mwax_mover.mwax_calvin_quality import (
    CalSolutionQuality,
    compute_bad_gains,
    plot_bad_gains,
    tile_flag_reason,
)

# ---------------------------------------------------------------------------
# Console summary table
# ---------------------------------------------------------------------------


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
            row["reason"] = tile_flag_reason(tile, quality, original_bad)
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


def print_tile_summary_table(rows: list[dict]) -> None:
    """Print a per-tile summary table to the console.

    Tiles with zero good channels show their flag reason in place of gain
    stats. All other tiles show before-cleaning min/median/max/stdev gain
    amplitude for gx and gy, computed over their good channels only, with
    a note if any channels were excluded from that calculation.

    Args:
        rows: Output of build_tile_summary_table.
    """
    id_w = 6
    name_w = max(10, max(len(r["tile_name"]) for r in rows) + 2)
    info_w = 100

    header = f"{'TileID':<{id_w}} {'TileName':<{name_w}} {'Info':<{info_w}}"
    print(header)
    print("-" * len(header))

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
        print(f"{r['tile']:<{id_w}} {r['tile_name']:<{name_w}} {info:<{info_w}}")


# ---------------------------------------------------------------------------
# Raw value dump (debugging aid)
# ---------------------------------------------------------------------------


def dump_tile_raw_values(path: str, tile_index: int) -> None:
    """Print every raw SOLUTIONS HDU value for one tile, per chanblock,
    straight from the FITS file, with no processing applied.

    Useful for sanity-checking a specific tile (e.g. one flagged as an
    outlier, or one you want to cross-check against the summary table)
    against the file's actual on-disk numbers.

    Args:
        path: Path to a hyperdrive solutions FITS file.
        tile_index: Antenna/tile index to dump (0-based, matches the
            SOLUTIONS HDU's tile axis and the TILES HDU's Antenna column).
    """
    with fits.open(path) as hdul:
        solutions = hdul["SOLUTIONS"].data.astype(np.float64)
        _, n_tiles, n_chanblocks, _ = solutions.shape

        if not (0 <= tile_index < n_tiles):
            print(f"tile_index {tile_index} out of range (file has {n_tiles} tiles)")
            return

        tile_name = "unknown"
        if "TILES" in hdul:
            tiles_table = hdul["TILES"].data
            match = tiles_table[tiles_table["Antenna"] == tile_index]
            if len(match) > 0:
                tile_name = str(match["TileName"][0])

        freqs = None
        if "CHANBLOCKS" in hdul:
            chanblocks_table = hdul["CHANBLOCKS"].data
            if "Freq" in chanblocks_table.names:
                freqs = chanblocks_table["Freq"]

        print(f"Tile {tile_index} ({tile_name}) -- raw SOLUTIONS values, timeblock 0")
        header = (
            f"{'chan':<5} {'freq_hz':<14} {'gx_re':<12} {'gx_im':<12} "
            f"{'Dx_re':<12} {'Dx_im':<12} {'Dy_re':<12} {'Dy_im':<12} "
            f"{'gy_re':<12} {'gy_im':<12}"
        )
        print(header)
        print("-" * len(header))

        for cb in range(n_chanblocks):
            vals = solutions[0, tile_index, cb, :]  # 8 raw floats, as stored
            freq_str = f"{freqs[cb]:.1f}" if freqs is not None else "n/a"
            row = f"{cb:<5} {freq_str:<14} " + " ".join(f"{v:<12.6g}" for v in vals)
            print(row)


def main() -> None:
    """Command-line entry point: parses sys.argv and runs the cleaning/plot
    pipeline, or dumps raw per-tile values if --dump-tile is given.
    """
    parser = argparse.ArgumentParser(
        description="Flag and clean bad gains in a hyperdrive solutions file."
    )
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
        default=10.0,
        help="MAD-of-residual threshold for flagging outliers from the polynomial fit",
    )
    parser.add_argument(
        "--n-tiles",
        type=int,
        default=16,
        help="Tiles per page/figure; total_tiles / n_tiles figures are produced",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Base path for saved figures, e.g. test.png -> test_0-15.png, test_16-31.png, ...",
    )
    parser.add_argument(
        "--dump-tile",
        type=int,
        default=None,
        help="Print raw SOLUTIONS values for this tile index and exit (no cleaning/plotting)",
    )
    args = parser.parse_args()

    if args.dump_tile is not None:
        dump_tile_raw_values(args.solutions_path, args.dump_tile)
        return

    (quality, bad_mask, new_flags, band, fit, original_gains, original_bad) = (
        compute_bad_gains(
            solutions_path=args.solutions_path,
            poly_degree=args.poly_degree,
            residual_threshold=args.residual_threshold,
            modify_gains=False,
        )
    )

    plot_bad_gains(
        quality,
        bad_mask,
        new_flags,
        band,
        fit,
        original_bad,
        args.n_tiles,
        args.output,
        original_gains,
    )

    n_bad = bad_mask.sum()
    n_total = bad_mask.size
    print(f"Flagged {n_bad}/{n_total} gain entries ({100 * n_bad / n_total:.2f}%)")

    print()
    table_rows = build_tile_summary_table(quality, original_bad)
    print_tile_summary_table(table_rows)
    print()


if __name__ == "__main__":
    main()
