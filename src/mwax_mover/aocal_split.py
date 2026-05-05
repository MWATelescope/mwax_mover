"""Utility to split an aocal calibration file into per-coarse-channel files.

Reads observation metadata (obs_id, receiver channel numbers) from a metafits
file via mwalib, then delegates splitting to
mwax_calvin_utils.split_aocal_file_into_coarse_channels.
"""

import argparse
import logging
import sys
from pathlib import Path

import mwalib

from mwax_mover.mwax_calvin_utils import split_aocal_file_into_coarse_channels, parse_solution_channels

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument list to parse.  Defaults to sys.argv when None.

    Returns:
        Parsed argument namespace with attributes:
            aocal_file (Path), metafits_file (Path), output_dir (Path).
    """
    parser = argparse.ArgumentParser(
        description=(
            "Split an aocal calibration file into one file per coarse channel. "
            "Observation metadata is read from the supplied metafits file."
        )
    )
    parser.add_argument(
        "aocal_file",
        type=Path,
        metavar="AOCAL_FILE",
        help="Path to the input aocal (.bin) calibration file.",
    )
    parser.add_argument(
        "metafits_file",
        type=Path,
        metavar="METAFITS_FILE",
        help="Path to the MWA metafits file for this observation.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        metavar="OUTPUT_DIR",
        default=Path("."),
        help="Directory to write per-channel output files into (default: current directory).",
    )
    return parser.parse_args(argv)


def get_obs_metadata(metafits_file: Path, aocal_filename: str) -> tuple[int, list[int]]:
    """Extract obs_id and ordered receiver channel numbers from a metafits file.

    Args:
        metafits_file: Path to the MWA metafits file.
        aocal_filename: Path to the aocal file.

    Returns:
        A tuple of:
            obs_id (int): GPS observation ID.
            rec_chans (list[int]): Receiver channel numbers for each coarse
                channel, in ascending order.

    Raises:
        FileNotFoundError: If metafits_file does not exist.
        Exception: If mwalib fails to open the metafits file.
    """
    if not metafits_file.exists():
        raise FileNotFoundError(f"Metafits file not found: {metafits_file}")

    context = mwalib.MetafitsContext(str(metafits_file))
    obs_id = context.obs_id

    # rec_chans = sorted(ch.rec_chan_number for ch in context.metafits_coarse_chans)
    # Figure out which channels this file has
    result = parse_solution_channels(aocal_filename)

    all_rec_chans = [ch.rec_chan_number for ch in context.metafits_coarse_chans]
    all_rec_chans_sorted = sorted([ch.rec_chan_number for ch in context.metafits_coarse_chans])

    assert all_rec_chans == all_rec_chans_sorted

    if result is None:
        this_band_chans = all_rec_chans
    else:
        this_band_chans = [ch for ch in all_rec_chans if result[0] <= ch <= result[1]]

    return obs_id, this_band_chans


def main(argv: list[str] | None = None) -> int:
    """Entry point for the split_aocal utility.

    Args:
        argv: Argument list; defaults to sys.argv when None.

    Returns:
        Exit code: 0 on success, 1 on error.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    args = parse_args(argv)

    if not args.aocal_file.exists():
        logger.error("aocal file not found: %s", args.aocal_file)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)

    try:
        obs_id, rec_chans = get_obs_metadata(args.metafits_file, args.aocal_file)
    except FileNotFoundError as exc:
        logger.error("%s", exc)
        return 1
    except Exception as exc:
        logger.error("Failed to read metafits file %s: %s", args.metafits_file, exc)
        return 1

    logger.info(
        "obs_id=%d  coarse channels (%d): %s",
        obs_id,
        len(rec_chans),
        rec_chans,
    )

    try:
        out_files = split_aocal_file_into_coarse_channels(
            obs_id,
            str(args.aocal_file),
            rec_chans,
            str(args.output_dir),
        )
    except Exception as exc:
        logger.error("Failed to split aocal file: %s", exc)
        return 1

    logger.info("Wrote %d output file(s):", len(out_files))
    for path in out_files:
        logger.info("  %s", path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
