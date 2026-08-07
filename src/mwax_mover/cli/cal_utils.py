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

from mwax_mover.mwax_calvin_quality import (
    build_tile_summary_table,
    compute_outlier_gains,
    plot_outlier_gains,
    write_tile_summary_table,
)


def main() -> None:
    """Command-line entry point: parses sys.argv and runs the cleaning/plot
    pipeline, or dumps raw per-tile values if --dump-tile is given.
    """
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

    args = parser.parse_args()

    (quality, bad_mask, new_flags, band, fit, original_gains, original_bad) = compute_outlier_gains(
        solutions_path=args.solutions_path,
        poly_degree=args.poly_degree,
        residual_threshold=args.residual_threshold,
        modify_gains=False,
    )

    plot_outlier_gains(
        quality,
        bad_mask,
        new_flags,
        band,
        fit,
        original_bad,
        args.n_tiles,
        args.output,
        original_gains,
        solution_file_will_be_modified=False,
    )

    n_bad = bad_mask.sum()
    n_total = bad_mask.size
    print(f"Flagged {n_bad}/{n_total} gain entries ({100 * n_bad / n_total:.2f}%)")

    print()
    table_rows = build_tile_summary_table(quality, original_bad)
    write_tile_summary_table(table_rows)
    print()


if __name__ == "__main__":
    main()
