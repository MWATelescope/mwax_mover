#!/usr/bin/env python3
"""Print fine channel frequencies and coarse channel info from a metafits file.

Usage:
    print_metafits_info <metafits_filename>
"""

import argparse
import sys

from mwalib import MetafitsContext


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments with `metafits_filename`.
    """
    parser = argparse.ArgumentParser(
        description="Print fine channel freqs and coarse channel info from a metafits file."
    )
    parser.add_argument("metafits_filename", help="Path to the metafits file")
    return parser.parse_args()


def print_fine_channels(freqs_hz) -> None:
    """Print first/last 10 fine channel frequencies.

    Args:
        freqs_hz: Sequence of fine channel frequencies in Hz.
    """
    n_chans = len(freqs_hz)
    print(f"Total fine channels: {n_chans}")

    if n_chans <= 20:
        print("\nAll fine channel frequencies (Hz):")
        for i, f in enumerate(freqs_hz):
            print(f"  [{i}] {f}")
        return

    print("\nFirst 10 fine channel frequencies (Hz):")
    for i in range(10):
        print(f"  [{i}] {freqs_hz[i]}")

    print("\nLast 10 fine channel frequencies (Hz):")
    for i in range(n_chans - 10, n_chans):
        print(f"  [{i}] {freqs_hz[i]}")


def print_coarse_channels(coarse_chans) -> None:
    """Print a table of coarse channel info: start, centre, end frequencies.

    Args:
        coarse_chans: Sequence of mwalib CoarseChannel objects.
    """
    print(f"\nTotal coarse channels: {len(coarse_chans)}")
    print("\n=== COARSE CHANNELS ===")

    header = (
        f"{'#':>3}  {'corr_ch':>7}  {'rec_ch':>6}  "
        f"{'start_hz':>12}  {'centre_hz':>12}  {'end_hz':>12}  {'width_hz':>10}"
    )
    print(header)
    print("-" * len(header))

    for i, cc in enumerate(coarse_chans):
        print(
            f"{i:>3}  "
            f"{cc.corr_chan_number:>7}  "
            f"{cc.rec_chan_number:>6}  "
            f"{cc.chan_start_hz:>12}  "
            f"{cc.chan_centre_hz:>12}  "
            f"{cc.chan_end_hz:>12}  "
            f"{cc.chan_width_hz:>10}"
        )


def main() -> None:
    """Entry point: load the metafits context and print channel info."""
    args = parse_args()

    try:
        context = MetafitsContext(args.metafits_filename)
    except Exception as exc:
        print(f"Error opening metafits file: {exc}", file=sys.stderr)
        sys.exit(1)

    print(f"Metafits: {args.metafits_filename}")

    print_fine_channels(context.metafits_fine_chan_freqs_hz)
    print_coarse_channels(context.metafits_coarse_chans)


if __name__ == "__main__":
    main()
