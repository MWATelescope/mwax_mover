import shutil
import glob
import requests
from pathlib import Path
import os
import argparse
import sys
from mwax_mover.mwax_calvin_utils import generate_hyperdrive_plots, populate_index_json_entry
import json
from datetime import datetime, timezone


def download_plot_index_file(fit_id: int, solution_directory: str) -> None:
    """Downloads the plot index JSON file for a given fit ID from the MWA calibration portal.

    Fetches the index file from https://cal.mwatelescope.org/{fit_id} and writes
    it to {solution_directory}/index.json. The solution directory must already exist.

    Args:
        fit_id: The integer fit ID used to construct the download URL.
        solution_directory: Path to the directory where index.json will be saved.

    Raises:
        requests.HTTPError: If the server returns an unsuccessful HTTP status code.
        requests.ConnectionError: If a network problem (e.g. DNS failure, refused
            connection) prevents the request from completing.
        requests.Timeout: If the request exceeds the timeout threshold.
        OSError: If the output file cannot be written (e.g. directory does not
            exist, or insufficient permissions).
    """
    url = f"https://cal.mwatelescope.org/{fit_id}/index.json"
    output_path = Path(solution_directory) / "index.json"

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    output_path.write_bytes(response.content)


def update_plot_index_file_entry(solution_directory: str, filename: str, fit_id: int, plot_front_end_url: str) -> None:
    """Updates metadata fields for a named entry in a solution directory's index.json.

    Reads the index.json file from the given solution directory, locates the entry
    matching the given filename, then derives updated values for ``size_bytes`` and
    ``last_modified`` from the file on disk. For PNG files, ``image_width`` and
    ``image_height`` are also updated using
    :func:`mwax_mover.utils.get_png_dimensions`. The modified index is written
    back to index.json in place.

    Args:
        solution_directory: Path to the directory containing both index.json and
            the file to be stat'd.
        filename: The filename value to match against entries in the ``files`` list.
        fit_id: Id of the fit for this solution.
        plot_front_end_url: base url where plots live: e.g. https://cal.mwatelescope.org

    Raises:
        FileNotFoundError: If index.json or the target file does not exist in the
            solution directory.
        json.JSONDecodeError: If index.json cannot be parsed as valid JSON.
        KeyError: If the index JSON does not contain a ``files`` list.
        ValueError: If no entry matching ``filename`` is found in the ``files`` list.
        OSError: If the target file cannot be stat'd, or the updated index.json
            cannot be written back to disk.
    """
    directory = Path(solution_directory)
    index_path = directory / "index.json"
    file_path = directory / filename

    with index_path.open("r", encoding="utf-8") as f:
        index = json.load(f)

    # update generated at
    index["generated_at"] = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    entries = index.get("files", [])
    matching = [entry for entry in entries if entry["filename"] == filename]

    if matching:
        entry = matching[0]
    else:
        # add it
        entry = populate_index_json_entry(file_path, fit_id, plot_front_end_url)
        if entry is not None:
            entries.append(entry)
        else:
            # Entry is none so skip it
            return

    if file_path.suffix.lower() == ".png" and int(index["version"]) == 1:
        #
        # if we are a v1 json file the width and height are swapped
        # - I would upgrade the version to 2 and fix it but there may be other
        # unmodified files in the index.json and we don't want to mix v1 and v2 conventions
        #
        width = entry["image_width"]
        height = entry["image_height"]
        entry["image_height"] = width
        entry["image_width"] = height

    with index_path.open("w", encoding="utf-8") as f:
        json.dump(index, f, indent=2)


def main() -> None:
    """Entry point for the update_hyperdrive_plots_and_index command line tool.

    Parses arguments and calls generate_hyperdrive_plots(), downloads the old index.json,
    updates index.json then copies the files to the local upload directory for calvin controller to upload, printing a summary on success or an error message on failure.
    """
    parser = argparse.ArgumentParser(
        description="calls generate_hyperdrive_plots(), downloads the old index.json, updates index.json then re-uploads it",
    )
    parser.add_argument(
        "--solution-dir",
        required=True,
        help="Path to the directory containing the solution files",
    )

    parser.add_argument(
        "--base-upload-dir",
        required=False,
        help="Path to the directory that calvin controller uploads to S3- usually /data/calvin/plots. This util will create a dir for the fit inside the base dir.",
    )

    parser.add_argument(
        "--obs-id",
        required=True,
        type=int,
        help="Obs ID of the calibration fit to index",
    )

    parser.add_argument(
        "--fit-id",
        required=True,
        type=int,
        help="Fit ID of the calibration fit to index",
    )

    parser.add_argument(
        "--hyperdrive-binary-path",
        required=True,
        help="Path to the hyperdrive binary",
    )

    parser.add_argument(
        "--plot-front-end-url",
        required=False,
        default="https://cal.mwatelescope.org",
        help="Path to the hyperdrive binary",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually upload anything.",
    )

    args = parser.parse_args()

    dry_run: bool = args.dry_run

    solution_dir: str = args.solution_dir

    plot_front_end_url = args.plot_front_end_url

    if not os.path.exists(solution_dir):
        print(f"Solution_directory: {solution_dir} does not exist. Exiting")
        sys.exit(1)

    fit_id = int(args.fit_id)

    obs_id = int(args.obs_id)

    if dry_run:
        base_upload_dir = ""
    else:
        if args.base_upload_dir is not None:
            base_upload_dir: str = args.base_upload_dir
        else:
            print("When --dry-run is not passed, you must provide a --base-upload-dir value.")
            exit(1)

    hyperdrive_binary_path: str = args.hyperdrive_binary_path
    if not os.path.exists(hyperdrive_binary_path):
        print(f"hyperdrive binary path: {hyperdrive_binary_path} does not exist. Exiting")
        sys.exit(1)

    metafits_filename = ""
    possible_metafits_filenames = [f"{obs_id}_metafits.fits", f"{obs_id}.metafits", f"{obs_id}_metafits_ppds.fits"]

    for mf in possible_metafits_filenames:
        temp_filename = os.path.join(solution_dir, mf)
        if os.path.exists(temp_filename):
            metafits_filename = temp_filename
            break

    if metafits_filename == "":
        print(f"No metafits file could be found in {solution_dir}")
        exit(1)

    try:
        # Download index file
        download_plot_index_file(
            fit_id,
            solution_dir,
        )
    except requests.HTTPError as httpe:
        resp = httpe.response
        if resp is not None:
            if resp.status_code == 404:
                print(f"Fit id {fit_id} not found in S3")
                exit(1)
            else:
                print(f"HTTP error when downloading the index.json file: {resp.status_code}")
                exit(1)
        else:
            print(f"HTTP error when downloading the index.json file: no response received {str(httpe)}")
    except Exception as e:
        print(f"Error downloading plot file: {e}")
        exit(1)

    # Get all the solution files
    solution_files = glob.glob(os.path.join(solution_dir, "*_solutions.fits"))
    print(f"{len(solution_files)} solution files found.")

    files_to_upload = []

    # Regenerate the plots for each solutions file
    for file in solution_files:
        print(f"Generating new plots for {file} in index.json")
        success, error_message = generate_hyperdrive_plots(obs_id, file, hyperdrive_binary_path, metafits_filename)

        # Exit early on failure
        if not success:
            print(f"Error generating plots for {file}: {error_message}")
            exit(1)

    # Update index file for each solution file
    png_files = glob.glob(os.path.join(solution_dir, "*.png"))
    for png in png_files:
        print(f"Updating {png} in index.json")
        update_plot_index_file_entry(solution_dir, os.path.basename(png), fit_id, plot_front_end_url)
        files_to_upload.append(png)

    files_to_upload.append(os.path.join(solution_dir, "index.json"))

    if not args.dry_run:
        upload_dir = os.path.join(base_upload_dir, str(fit_id))

        # Make Upload dir and move files there
        try:
            os.mkdir(upload_dir)
        except FileExistsError:
            # dir already exists, no worries
            pass

        try:
            for f in files_to_upload:
                dest_filename = os.path.join(upload_dir, os.path.basename(f))
                shutil.move(f, dest_filename)
                print(f"Moved {f} to {dest_filename}")

        except Exception as e:
            print(f"Error moving files to upload dir {upload_dir}: {str(e)}")
            exit(1)
    else:
        print(f"Not uploading files: {files_to_upload} to S3 (bucket={fit_id}) as dry-run = true.")

    print("Completed successfully")


if __name__ == "__main__":
    main()
