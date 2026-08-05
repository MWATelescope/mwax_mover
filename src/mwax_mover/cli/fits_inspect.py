#!/usr/bin/env python3
"""Inspect a FITS file: print header key values, and table or image data.

Usage:
    python fits_inspect.py <filename> <hdu>

Where <hdu> is either an integer HDU index (e.g. 1) or an HDU
EXTNAME/name string (e.g. SOLUTIONS).
"""

import argparse
import sys

import numpy as np
from astropy.io import fits
from astropy.table import Table


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments with `filename` and `hdu`.
    """
    parser = argparse.ArgumentParser(description="Print header info and data summary for a FITS HDU.")
    parser.add_argument("filename", help="Path to the FITS file")
    parser.add_argument("hdu", help="HDU index (integer) or HDU name (string), e.g. 1 or SOLUTIONS")
    return parser.parse_args()


def resolve_hdu_key(hdu_arg: str):
    """Convert the hdu command-line argument to an int index if possible.

    Args:
        hdu_arg: The raw HDU argument from the command line.

    Returns:
        int or str: An integer HDU index if hdu_arg is numeric, otherwise
        the original string (treated as an HDU name).
    """
    try:
        return int(hdu_arg)
    except ValueError:
        return hdu_arg


def print_header_table(header: fits.Header) -> None:
    """Print a table of key/value pairs from a FITS header.

    Args:
        header: The FITS header to display.
    """
    print("\n=== HEADER ===")
    key_width = max((len(k) for k in header if k), default=3)
    print(f"{'KEY'.ljust(key_width)}  VALUE")
    print("-" * (key_width + 40))
    for card in header.cards:
        key = card.keyword or ""
        value = card.value
        comment = card.comment
        line = f"{key.ljust(key_width)}  {value}"
        if comment:
            line += f"  / {comment}"
        print(line)


def print_table_data(hdu) -> None:
    """Print the contents of a table HDU (BinTableHDU or TableHDU).

    Args:
        hdu: An astropy BinTableHDU or TableHDU instance.
    """
    print("\n=== TABLE DATA ===")
    table = Table(hdu.data)
    table.pprint(max_lines=-1, max_width=-1)


def print_image_data(hdu) -> None:
    """Print the first 5 and last 5 rows of an image HDU's data array.

    Args:
        hdu: An astropy ImageHDU or PrimaryHDU instance with array data.
    """
    print("\n=== IMAGE DATA ===")
    data = hdu.data
    if data is None:
        print("(no data in this HDU)")
        return

    data = np.asarray(data)
    print(f"shape: {data.shape}, dtype: {data.dtype}")

    if data.ndim == 0:
        print(data)
        return

    n_rows = data.shape[0]
    if n_rows <= 10:
        print(data)
        return

    print("\nFirst 5 rows:")
    print(data[:5])
    print("\nLast 5 rows:")
    print(data[-5:])


def main() -> None:
    """Entry point: load the FITS file and print requested HDU info."""
    args = parse_args()
    hdu_key = resolve_hdu_key(args.hdu)

    try:
        with fits.open(args.filename) as hdul:
            try:
                hdu = hdul[hdu_key]
            except (KeyError, IndexError) as exc:
                print(f"Error: could not find HDU '{args.hdu}': {exc}", file=sys.stderr)
                sys.exit(1)

            print(f"File: {args.filename}")
            print(f"HDU: {hdu.name!r} (index {hdul.index_of(hdu.name) if hdu.name else '?'})")
            print(f"Type: {type(hdu).__name__}")

            print_header_table(hdu.header)

            if isinstance(hdu, (fits.BinTableHDU, fits.TableHDU)):
                print_table_data(hdu)
            elif isinstance(hdu, (fits.ImageHDU, fits.PrimaryHDU)):
                print_image_data(hdu)
            else:
                print(f"\n(No data preview available for HDU type {type(hdu).__name__})")

    except FileNotFoundError:
        print(f"Error: file not found: {args.filename}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
