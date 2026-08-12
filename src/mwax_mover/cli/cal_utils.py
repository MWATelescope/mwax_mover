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
"""

import argparse
import logging
import os
import sys
from pathlib import Path

from mwalib import MetafitsContext

from mwax_mover.mwax_calvin_quality import generate_hyperdrive_plots_and_stats
from mwax_mover.utils import download_metafits_file

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(name)s.%(funcName)s, %(message)s"))
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
for noisy_logger in ("PIL", "matplotlib", "urllib3"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)


def main() -> None:
    """Command-line entry point: parses sys.argv and runs the cleaning/plot
    pipeline, or dumps raw per-tile values if --dump-tile is given.
    """
    parser = argparse.ArgumentParser(description="Flag and clean bad gains in a hyperdrive solutions file.")
    parser.add_argument(
        "solution_filenames",
        nargs="+",  # requires at least 1; use "*" to allow 0
        metavar="FILE",
        help="Path(s) to hyperdrive solution FITS file(s).",
    )

    parser.add_argument(
        "--poly-degree",
        type=int,
        default=2,
        help="Degree of the per-tile polynomial fit to gain amplitude vs. frequency. [DEFAULT=2]",
    )

    parser.add_argument(
        "--mad-threshold",
        type=float,
        default=10.0,
        help="MAD-of-residual threshold for flagging outliers from the polynomial fit. [DEFAULT=10.0]",
    )

    parser.add_argument(
        "--gains-cutoff-max",
        type=float,
        default=None,
        help="Use the old naive cut off gains value AND modify the solutions. [DEFAULT=None]",
    )

    parser.add_argument(
        "--modify-gains",
        action="store_true",
        help="Should the solution file(s) be modified to NaN out Jones of outlier gains? [DEFAULT=False]",
    )

    parser.add_argument(
        "--plot-n-tiles",
        type=int,
        default=16,
        help="Tiles per page/figure; total_tiles / n_tiles figures are produced. [DEFAULT=16]",
    )

    parser.add_argument(
        "--output-path",
        default=".",
        help="Base path for saved figures and stats, e.g. test.png -> test_0-15.png, test_16-31.png, ... DEFAULT=[.]",
    )

    parser.add_argument(
        "--hyperdrive-binary-path",
        default="../mwa_hyperdrive/target/release/hyperdrive",
        help="Location of hyperdrive binary. [DEFAULT='./mwa_hyperdrive/target/release/hyperdrive']",
    )

    parser.add_argument(
        "--metafits-filename",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to the metafits FITS file. If not provided, the dir where the solutions files reside will be searched and if not found a new metafits will be downloaded there and used.",
    )

    args = parser.parse_args()

    #
    # Validate
    #
    if not os.path.exists(args.output_path):
        print(f"Error --output-path not found: {args.output_path}")
        sys.exit(-1)

    if not os.path.exists(args.hyperdrive_binary_path):
        print(f"Error --hyperdrive-binary-path not found: {args.hyperdrive_binary_path}")
        sys.exit(-1)

    if len(args.solution_filenames) == 0:
        print(f"No Hyperdrive solution files found with pattern: {args.solution_path}")
        sys.exit(-1)
    else:
        # Get obsid from hyperdrive solution files
        print(f"Found {len(args.solution_filenames)} solution file(s):")
        obs_id: int = os.path.basename(args.solution_filenames[0])[0:10]
        for f in args.solution_filenames:
            print(f)
            if os.path.basename(args.solution_filenames[0])[0:10] != obs_id:
                print(f"Error: The solution files passed all must be for the same obsid '{obs_id}'")
                sys.exit(-1)

    if args.metafits_filename is None:
        # user did not pass a metafits filename
        metafits_path = os.path.dirname(args.solution_filenames[0])
        metafits_filename = os.path.join(metafits_path, f"{obs_id}_metafits.fits")

        if not os.path.exists(metafits_filename):
            # Download a metafits to where the source data lives
            download_metafits_file(obs_id, metafits_path)
        else:
            # We found an existing metafits
            print(f"Found and will use {metafits_filename}")
    else:
        # user did pass a metafits filename
        metafits_filename = args.metafits_filename
        if not os.path.exists(args.metafits_filename):
            # But it didn't exist
            print(f"Error: The metafits file provided '{metafits_filename}' does not exist")
            sys.exit(-1)

    metafits_context = MetafitsContext(metafits_filename)

    generate_hyperdrive_plots_and_stats(
        metafits_context,
        args.solution_filenames,
        args.output_path,
        args.hyperdrive_binary_path,
        obs_id,
        args.poly_degree,
        args.mad_threshold,
        args.modify_gains,
        args.plot_n_tiles,
        args.gains_cutoff_max,
    )


if __name__ == "__main__":
    main()
