from pathlib import Path
import argparse
import json
import os
import sys
from mwax_mover.mwax_calvin_utils import generate_plot_index_file


def main() -> None:
    """Entry point for the generate-directory-index command line tool.

    Parses arguments and calls generate_directory_index, printing a summary
    on success or an error message on failure.
    """
    parser = argparse.ArgumentParser(
        description="Generate an index.json manifest for a local directory, "
        "suitable for upload to S3 with CloudFront serving.",
    )
    parser.add_argument(
        "--directory",
        help="Path to the fit directory to index.",
    )

    parser.add_argument(
        "--fit-id",
        default=None,
        type=int,
        help="Fit ID of the calibration fit to index. If not specified, it will be inferred from the directory arg",
    )

    parser.add_argument(
        "--front-end-url",
        help="CloudFront base URL for the directory, e.g. "
        "https://d1234abcd.cloudfront.net"
        "Must not have a trailing slash.",
    )
    parser.add_argument(
        "--output",
        default=None,
        metavar="PATH",
        help="Path to write index.json to. Defaults to index.json inside the target directory.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the generated index to stdout without writing any file.",
    )

    args = parser.parse_args()

    if args.dry_run:
        output_filename = os.devnull
    else:
        if args.output is None:
            # Not specified? output to the fit dir
            output_filename = os.path.join(args.directory, "index.json")
        else:
            # User specified it
            output_filename = os.path.join(args.output, "index.json")

    if args.fit_id is None:
        # get it from the directory
        p = Path(args.directory)
        try:
            fit_id = int(p.name)
        except Exception:
            print(f"Could not infer FitID from {args.directory}- please specify fit-id instead.")
            sys.exit(-3)
    else:
        fit_id = int(args.fit_id)

    try:
        success, index = generate_plot_index_file(
            fit_id=fit_id,
            plot_front_end_url=args.front_end_url,
            fit_dir=args.directory,
            output_filename=output_filename,
        )

        if success:
            if args.dry_run:
                # Temporarily redirect output to stdout only
                print(json.dumps(index, indent=2))
            else:
                print(f"Written {output_filename}")
    except NotADirectoryError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except OSError as e:
        print(f"Error writing index file: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
