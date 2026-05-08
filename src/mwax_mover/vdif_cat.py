"""Utility to concatenate multiple 8 second VDIF files into a single VDIF file and header"""

from collections import defaultdict

import re

import os
import glob

import argparse
import logging
import sys
from pathlib import Path


from mwax_mover.mwax_bf_vdif_utils import stitch_vdif_files_and_write_hdr

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument list to parse.  Defaults to sys.argv when None.

    Returns:
        Parsed argument namespace with attributes:
            nput_dir (Path), output_dir (Path).
    """
    parser = argparse.ArgumentParser(description=("Combine multiple 8 second VDIF files into a single VDIF and header"))

    parser.add_argument(
        "-m",
        "--metafits-file",
        type=Path,
        metavar="METAFITS_FILE",
        help="Path to the MWA metafits file for this observation.",
    )

    parser.add_argument(
        "-i",
        "--input-dir",
        type=Path,
        metavar="INPUT_DIR",
        help="Path to the input directory of 8 second VDIF files and headers.",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=Path,
        metavar="OUTPUT_DIR",
        default=Path("."),
        help="Path to output directory to write the concatenated VDIF file and header. Defaults to current dir.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for the vdif_cat utility.

    Args:
        argv: Argument list; defaults to sys.argv when None.

    Returns:
        Exit code: 0 on success, 1 on error.
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    args = parse_args(argv)

    if not args.metafits_file.exists():
        logger.error("Metafits file not found: %s", args.metafits_file)
        return 1

    obs_id: int = int(os.path.basename(args.metafits_file)[0:10])

    if not args.input_dir.exists():
        logger.error("Input dir not found: %s", args.input_dir)
        return 1

    if not args.output_dir.exists():
        logger.error("Output dir not found: %s", args.output_dir)
        return 1

    # Get the input files
    input_files = sorted(glob.glob(os.path.join(args.input_dir, f"{obs_id}_*.vdif")))

    logger.info(f"Found {len(input_files)} files for {obs_id}")

    # We could have multiple obids, rec_chans and beams. Iterate!
    pattern = r"(?P<obs_id>\d{10})_(?P<subobs_id>\d{10})_ch(?P<rec_chan>\d{3})_beam(?P<beam>\d{2})\.vdif"

    groups = defaultdict(list)

    for i in input_files:
        if m := re.search(pattern, i):
            key = (int(m.group("rec_chan")), int(m.group("beam")))
            groups[key].append(i)
        else:
            logger.info(f"Not an 8 second VDIF file of {args.obs_id}- {i}. Skipping")

    # For debug, log the list of lists
    logger.info(groups)

    # Do the stitching
    for file_list in groups.values():
        out_vdif, out_hdr = stitch_vdif_files_and_write_hdr(str(args.metafits_file), file_list, args.output_dir)

    return 0


if __name__ == "__main__":
    sys.exit(main())
