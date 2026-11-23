"""Utility to concatenate multiple 8 second VDIF files into a single VDIF file and header"""

import argparse
import glob
import logging
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

from mwax_mover.beamformer.vdif import stitch_vdif_files_and_write_hdr
from mwax_mover.version import UNKNOWN_BEAMFORMER_VERSION, get_mwax_beamformer_version

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: Argument list to parse.  Defaults to sys.argv when None.

    Returns:
        Parsed argument namespace with attributes:
            metafits_file (Path), input_dir (Path), output_dir (Path),
            beamformer_binary_path (Path or None).
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
    parser.add_argument(
        "-b",
        "--beamformer-binary-path",
        type=Path,
        metavar="BEAMFORMER_BINARY_PATH",
        default=None,
        help=(
            "Path to the MWAX beamformer binary, used to get the beamformer version for the VDIF header."
            f' If not given, the version is "{UNKNOWN_BEAMFORMER_VERSION}".'
        ),
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
            logger.info(f"Not an 8 second VDIF file of {obs_id}- {i}. Skipping")

    # For debug, log the list of lists
    logger.info(groups)

    # Get beamformer version
    if args.beamformer_binary_path is None:
        mwax_beamformer_version = UNKNOWN_BEAMFORMER_VERSION
    else:
        if not args.beamformer_binary_path.exists():
            logger.error("Beamformer binary not found: %s", args.beamformer_binary_path)
            return 1
        mwax_beamformer_version = get_mwax_beamformer_version(str(args.beamformer_binary_path))

    # Do the stitching
    for file_list in groups.values():
        stitch_vdif_files_and_write_hdr(str(args.metafits_file), file_list, args.output_dir, mwax_beamformer_version)

    return 0


if __name__ == "__main__":
    sys.exit(main())
