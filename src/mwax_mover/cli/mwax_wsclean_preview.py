#!/usr/bin/env python3
"""Make the preview PNG from WSClean FITS images, to try colour scales without running WSClean again.

The images are drawn side by side with one colour scale, in the same way as the calvin processor does
(see mwax_mover.calvin.plots.wsclean_preview). The colour scale options default to the values that
the processor uses.

Usage:
    mwax_wsclean_preview --output preview.png 1234567890_wsclean-MFS-XX-image.fits 1234567890_wsclean-MFS-YY-image.fits
    mwax_wsclean_preview --limit-sigma 20 --linear-sigma 2 -o preview.png XX.fits YY.fits
"""

import argparse
import logging
import os
import re
import sys

from mwax_mover.calvin.plots.wsclean_preview import (
    PREVIEW_LIMIT_SIGMA,
    PREVIEW_LINEAR_SIGMA,
    PREVIEW_NEGATIVE_LIMIT_SIGMA,
    PreviewStretch,
    make_preview_png,
)
from mwax_mover.constants import EXIT_FAILURE, LOG_FORMAT

# Finds the polarisation in a WSClean file name, e.g. 1234567890_wsclean-MFS-XX-image.fits
POLARIZATION_IN_NAME = re.compile(r"-(XX|XY|YX|YY|I|Q|U|V)-")

logger = logging.getLogger(__name__)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse command-line arguments.

    Args:
        argv: The arguments, or None for sys.argv.

    Returns:
        The parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Make a preview PNG from WSClean FITS images (side by side).")
    parser.add_argument("fits_files", nargs="+", help="WSClean FITS images, drawn left to right")
    parser.add_argument("-o", "--output", required=True, help="The PNG file to write")
    parser.add_argument("--title", default=None, help="Title of the figure. Default: the first file name")
    parser.add_argument(
        "--limit-sigma",
        type=float,
        default=PREVIEW_LIMIT_SIGMA,
        help=f"Top of the colour scale in multiples of the noise (default {PREVIEW_LIMIT_SIGMA:g})",
    )
    parser.add_argument(
        "--negative-limit-sigma",
        type=float,
        default=PREVIEW_NEGATIVE_LIMIT_SIGMA,
        help=f"Bottom of the colour scale in multiples of the noise, below zero (default {PREVIEW_NEGATIVE_LIMIT_SIGMA:g})",
    )
    parser.add_argument(
        "--linear-sigma",
        type=float,
        default=PREVIEW_LINEAR_SIGMA,
        help=f"The colour scale is linear within this many sigma of zero (default {PREVIEW_LINEAR_SIGMA:g})",
    )
    return parser.parse_args(argv)


def label_for(fits_filename: str) -> str:
    """Choose the label of an image: its polarisation if the file name has one, otherwise the file name.

    Args:
        fits_filename: The FITS file.

    Returns:
        The label.
    """
    name = os.path.basename(fits_filename)
    match = POLARIZATION_IN_NAME.search(name)
    return match.group(1) if match else name


def main(argv: list[str] | None = None) -> None:
    """Entry point: draw the images into the PNG.

    Args:
        argv: The arguments, or None for sys.argv.
    """
    logging.basicConfig(format=LOG_FORMAT, level=logging.INFO)
    args = parse_args(argv)

    labels = [label_for(filename) for filename in args.fits_files]
    if len(set(labels)) != len(labels):
        logger.error(f"The images need different labels, but they are {labels}.")
        sys.exit(EXIT_FAILURE)

    stretch = PreviewStretch(args.limit_sigma, args.negative_limit_sigma, args.linear_sigma)
    try:
        stretch.validate()
    except ValueError as e:
        logger.error(str(e))
        sys.exit(EXIT_FAILURE)

    title = args.title or os.path.basename(args.fits_files[0])
    if not make_preview_png(dict(zip(labels, args.fits_files, strict=True)), args.output, title, stretch):
        logger.error("No PNG was made. See the messages above.")
        sys.exit(EXIT_FAILURE)

    logger.info(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
