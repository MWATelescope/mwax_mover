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
5. A whole-observation phase-fit outlier check: a tile's cable-delay phase
   ramp fit (chi2dof, sigma_resid) is compared against the population of
   all tiles in the observation, and flagged if it's a population outlier.
6. Promotion of a partially-flagged tile to fully flagged, if too large a
   fraction of its chanblocks already carry a per-channel flag reason.

This mirrors the full flagging pipeline used by mwax_calvin_processor
(HyperfitsSolutionGroup.apply_tile_flags -> enforce_whole_jones_nan ->
flag_phase_outliers -> flag_amplitude_outliers -> flag_mostly_bad_tiles).

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
import cProfile
import logging
import os
import pstats
import sys
from pathlib import Path

from mwax_mover.mwax_calvin_plots import (
    generate_hyperdrive_plots,
    plot_outlier_gains,
    write_hyperdrive_stats,
    write_stats_and_debug_plots,
)
from mwax_mover.mwax_calvin_utils import Metafits
from mwax_mover.mwax_hyperdrive_solutions import (
    HyperfitsSolution,
    HyperfitsSolutionGroup,
)
from mwax_mover.utils import download_metafits_file

handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(name)s.%(funcName)s, %(message)s"))
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)
for noisy_logger in ("PIL", "matplotlib", "urllib3"):
    logging.getLogger(noisy_logger).setLevel(logging.WARNING)
# font_manager's "failed to find font weight X, using Y instead" notices
# are cosmetic font-substitution fallbacks, not signs of a problem --
# silence that specific logger rather than raising "matplotlib"'s whole
# threshold (which would hide other, potentially useful warnings).
logging.getLogger("matplotlib.font_manager").setLevel(logging.ERROR)


def run_pipeline(args: argparse.Namespace, obs_id: int, metafits_filename: str | Path) -> None:
    """Load, flag, plot, and report on a set of hyperdrive solution files.

    Split out from main() so --profile can wrap just this actual work with
    cProfile, without profiling argument parsing/validation or the
    metafits download (network-bound, not interesting to profile).

    Args:
        args: Parsed command-line arguments.
        obs_id: Observation ID, derived from the solution filenames.
        metafits_filename: Path to the metafits file to use.
    """
    metafits = Metafits(str(metafits_filename))
    soln_group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(f) for f in args.solution_filenames])
    soln_group.load()

    # Capture the pristine Jones matrices before flagging, so the plots
    # below can show flagged values as visible outlier points rather than
    # gaps where the data's already been NaN'd in memory.
    assert soln_group.jones is not None
    pristine_jones = [file_jones.copy() for file_jones in soln_group.jones]

    refant = soln_group.refant

    # "Before" plots: hyperdrive's own binary-generated amp/phase plots,
    # against the still-pristine on-disk files -- nothing has been
    # touched yet. Written with "_original" filenames (see
    # generate_hyperdrive_plots) so the "after" run below (same
    # filenames, since hyperdrive derives them from the input file)
    # doesn't overwrite these. (This only touches the on-disk files via
    # a read-only hyperdrive invocation; it's independent of the
    # in-memory run_flagging_pipeline() below regardless of ordering,
    # since nothing gets written to disk until commit().)
    for f in args.solution_filenames:
        plots_success, plots_error = generate_hyperdrive_plots(
            obs_id, f, args.hyperdrive_binary_path, metafits_filename, args.output_path, before=True
        )
        if not plots_success:
            print(f"Warning: 'before' hyperdrive plots failed for {f}: {plots_error}")

    # Full flagging pipeline, matching mwax_calvin_processor's
    # process_solutions() -- both now share the same
    # HyperfitsSolutionGroup.run_flagging_pipeline() implementation rather
    # than each duplicating the call sequence.
    soln_group.run_flagging_pipeline(
        refant["name"],
        args.phase_fit_niter,
        poly_degree=args.poly_degree,
        mad_residual_threshold=args.mad_threshold,
        phase_outlier_nstd=args.phase_outlier_nstd,
        tile_bad_channel_fraction_threshold=args.tile_bad_channel_fraction_threshold,
    )

    for file_idx, f in enumerate(args.solution_filenames):
        obsid_and_band = os.path.basename(f).replace("_solutions.fits", "")
        plot_outlier_gains(
            soln_group,
            file_idx,
            n_tiles=args.plot_n_tiles,
            output_path=os.path.join(args.output_path, f"{obsid_and_band}_gain_outliers_tiles.png"),
            pristine_jones=pristine_jones[file_idx],
            solution_file_will_be_modified=args.modify_solutions,
        )

    if args.modify_solutions:
        soln_group.commit(metafits.mwalib_context)

    # hyperdrive's own plots/stats run regardless of --modify-solutions,
    # matching this tool's historical behaviour (that flag only ever
    # controlled whether outlier-flagged gains were written to disk).
    for f in args.solution_filenames:
        plots_success, plots_error = generate_hyperdrive_plots(
            obs_id, f, args.hyperdrive_binary_path, metafits_filename, args.output_path, before=False
        )
        if not plots_success:
            print(f"Warning: hyperdrive plots failed for {f}: {plots_error}")

    stats_path = os.path.join(args.output_path, f"{obs_id}_stats.txt")
    with open(stats_path, "w", encoding="utf-8") as stats_fd:
        for f in args.solution_filenames:
            stats_success, stats_error = write_hyperdrive_stats(obs_id, stats_fd, f)
            if not stats_success:
                print(f"Warning: hyperdrive stats failed for {f}: {stats_error}")

    # Before/after per-tile stats (obs_id_tile_stats.txt) and the
    # phase-fit debug plots (rx_lengths/phase_fits_xx/yy/intercepts/
    # residual) -- shared with mwax_calvin_processor via
    # write_stats_and_debug_plots().
    write_stats_and_debug_plots(
        soln_group,
        refant["name"],
        args.phase_fit_niter,
        args.output_path,
        obs_id,
    )


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
        "--modify-solutions",
        action="store_true",
        help="Should the solution file(s) be modified to NaN out Jones of outlier gains? [DEFAULT=False]",
    )

    parser.add_argument(
        "--phase-fit-niter",
        type=int,
        default=3,
        help="Number of iterations for the phase ramp fit. [DEFAULT=3]",
    )

    parser.add_argument(
        "--phase-outlier-nstd",
        type=float,
        default=3.0,
        help="Number of standard deviations beyond the population mean before a tile's phase fit is an outlier. [DEFAULT=3.0]",
    )

    parser.add_argument(
        "--tile-bad-channel-fraction-threshold",
        type=float,
        default=0.5,
        help="Fraction (0-1) of a tile's chanblocks that must already be flagged bad before the whole tile is promoted to fully flagged. [DEFAULT=0.5]",
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

    DEFAULT_HYPERDRIVE_BIN = "../mwa_hyperdrive/target/release/hyperdrive"

    parser.add_argument(
        "--hyperdrive-binary-path",
        default=DEFAULT_HYPERDRIVE_BIN,
        help=f"Location of hyperdrive binary. [DEFAULT='{DEFAULT_HYPERDRIVE_BIN}']",
    )

    parser.add_argument(
        "--metafits-filename",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to the metafits FITS file. If not provided, the dir where the solutions files reside will be searched and if not found a new metafits will be downloaded there and used.",
    )

    parser.add_argument(
        "--profile",
        action="store_true",
        help=(
            "Profile the flag/plot/report pipeline with cProfile. Prints the top "
            "--profile-top-n functions by cumulative and by self (tottime) time, and "
            "saves the full stats to --profile-output for deeper inspection (e.g. with "
            "snakeviz or `python -m pstats`). Argument parsing/validation and metafits "
            "download are not profiled -- only the actual load/flag/plot/report work. "
            "[DEFAULT=False]"
        ),
    )

    parser.add_argument(
        "--profile-output",
        type=Path,
        default=None,
        metavar="FILE",
        help="Path to save the full cProfile stats to (only used with --profile). "
        "[DEFAULT={output-path}/{obsid}_profile.pstats]",
    )

    parser.add_argument(
        "--profile-top-n",
        type=int,
        default=40,
        help="Number of top functions to print when --profile is used. [DEFAULT=40]",
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

    if args.profile:
        profiler = cProfile.Profile()
        profiler.enable()
        try:
            run_pipeline(args, obs_id, metafits_filename)
        finally:
            profiler.disable()

        profile_output = args.profile_output or os.path.join(args.output_path, f"{obs_id}_profile.pstats")
        stats = pstats.Stats(profiler)
        stats.dump_stats(str(profile_output))

        print(
            f"\n{'=' * 88}\n"
            f"Profile: top {args.profile_top_n} functions by CUMULATIVE time "
            f"(function + everything it calls)\n{'=' * 88}"
        )
        stats.sort_stats("cumulative").print_stats(args.profile_top_n)

        print(
            f"\n{'=' * 88}\n"
            f"Profile: top {args.profile_top_n} functions by SELF time "
            f"(time in the function itself, excluding sub-calls)\n{'=' * 88}"
        )
        stats.sort_stats("tottime").print_stats(args.profile_top_n)

        print(
            f"Full profile data saved to {profile_output} "
            f"(view with e.g. `snakeviz {profile_output}` or `python -m pstats {profile_output}`)"
        )
    else:
        run_pipeline(args, obs_id, metafits_filename)


if __name__ == "__main__":
    main()
