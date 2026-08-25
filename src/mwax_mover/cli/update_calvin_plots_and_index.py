import argparse
import glob
import json
import os
import re
import shutil
import sys
from configparser import ConfigParser
from datetime import UTC, datetime
from pathlib import Path

import requests

from mwax_mover.mwax_calvin_plots import generate_hyperdrive_plots
from mwax_mover.mwax_calvin_utils import populate_index_json_entry
from mwax_mover.mwax_db import MWAXDBHandler, get_fit_info_from_slurm_job_and_obsid
from mwax_mover.utils import download_metafits_file, read_config


class SolutionDir:
    slurm_job_id: int
    obs_id: int
    fit_id: int = -1
    dir_path: str

    def log(self, message: str):
        print(f"{self.obs_id} {self.slurm_job_id} {self.fit_id}: {message}")


def download_plot_index_file(fit_id: int, solution_directory: str) -> Path:
    """Downloads the plot index JSON file for a given fit ID from the MWA calibration portal.

    Fetches the index file from https://cal.mwatelescope.org/{fit_id} and writes
    it to {solution_directory}/index.json. The solution directory must already exist.

    Args:
        fit_id: The integer fit ID used to construct the download URL.
        solution_directory: Path to the directory where index.json will be saved.

    Returns:
        Full path and filename of index.json

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

    return output_path


def update_plot_index_file_entry(
    index, solution_directory: str, filename: str, fit_id: int, plot_front_end_url: str
) -> None:
    """Updates metadata fields for a named entry in a solution directory's index.json.

    Reads the index.json file from the given solution directory, locates the entry
    matching the given filename, then derives updated values for ``size_bytes`` and
    ``last_modified`` from the file on disk. For PNG files, ``image_width`` and
    ``image_height`` are also updated using
    :func:`mwax_mover.utils.get_png_dimensions`. The modified index is written
    back to index.json in place.

    Args:
        index: JSON from the index file.
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
    file_path = directory / filename

    # update generated at
    index["generated_at"] = datetime.now(tz=UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    entries = index.get("files", [])
    matching = [entry for entry in entries if entry["filename"] == filename]

    new_entry = populate_index_json_entry(file_path, fit_id, plot_front_end_url)
    if new_entry is None:
        return

    if matching:
        idx = entries.index(matching[0])
        entries[idx] = new_entry
    else:
        entries.append(new_entry)


def parse_job_dir(directory: str) -> tuple[int, int]:
    """Extract the SLURM job ID and observation ID from a job directory path.

    Expects the final path component to be in the form ``SLURMJOBID_OBSID``,
    where both are integers (no leading zeros, no extra underscores). Works
    whether or not the path has a trailing slash.

    Args:
        directory: Path to the job directory, e.g.
            "/data/calvin/jobs/1234567_1234567890" or the same with a
            trailing slash.

    Returns:
        A tuple of (slurm_job_id, obs_id) as integers.

    Raises:
        ValueError: If the final path component doesn't match the expected
            "<digits>_<digits>" pattern.
    """
    name = Path(directory).name  # pathlib handles trailing slash correctly

    match = re.fullmatch(r"(\d+)_(\d+)", name)
    if not match:
        raise ValueError(f"Directory name '{name}' does not match expected 'SLURMJOBID_OBSID' pattern")

    slurm_job_id, obs_id = match.groups()
    return int(slurm_job_id), int(obs_id)


def main() -> None:
    """Entry point for the update_hyperdrive_plots_and_index command line tool.

    Parses arguments and calls generate_hyperdrive_plots(), downloads the old index.json,
    updates index.json then copies the files to the local upload directory for calvin controller to upload, printing a summary on success or an error message on failure.
    """
    parser = argparse.ArgumentParser(
        description="Scans recursively for solution directories. For each solution directory, calls generate_hyperdrive_plots(), downloads the old index.json, updates index.json then re-uploads it",
    )
    parser.add_argument(
        "--solution-dir",
        required=True,
        help="Path to the directory to start recursively looking for solution files. Solution dirs should end in SLURMJOBID_OBSID - e.g. /data/calvin/jobs/9176_1234567890",
    )

    parser.add_argument(
        "--base-upload-dir",
        required=False,
        help="Path to the directory that calvin controller uploads to S3- usually /data/calvin/plots. This util will create a dir for the fit inside the base dir.",
    )

    parser.add_argument(
        "--cfg",
        required=True,
        help="Path to the CalvinProcessor config file (for database credentials)",
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
        help="Base URL where the fit files are stored in S3",
    )

    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search the --solution-dir recursively for solution directories. Default FALSE.",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't actually upload anything.",
    )

    args = parser.parse_args()

    dry_run: bool = args.dry_run
    recursive: bool = args.recursive
    solution_root: str = args.solution_dir
    plot_front_end_url = args.plot_front_end_url

    if not os.path.exists(solution_root):
        print(f"Solution_directory: {solution_root} does not exist. Exiting")
        sys.exit(1)

    # Read database info from config file
    if not os.path.exists(args.cfg):
        print(f"Configuration file location {args.cfg} does not exist. Quitting.")
        sys.exit(1)

    # Parse config file
    config = ConfigParser()
    config.read_file(open(args.cfg, "r", encoding="utf-8"))
    mro_metadatadb_host = read_config(config, "mro metadata database", "host")
    mro_metadatadb_db = read_config(config, "mro metadata database", "db")
    mro_metadatadb_user = read_config(config, "mro metadata database", "user")
    # Don't require base64 encoded password if running a pytest
    mro_metadatadb_pass = read_config(config, "mro metadata database", "pass", True)
    mro_metadatadb_port = int(read_config(config, "mro metadata database", "port"))

    # Initiate database connection for mro metadata db
    db_handler = MWAXDBHandler(
        host=mro_metadatadb_host,
        port=mro_metadatadb_port,
        db_name=mro_metadatadb_db,
        user=mro_metadatadb_user,
        password=mro_metadatadb_pass,
        ssl_mode="?sslmode=require",
    )

    if dry_run:
        base_upload_dir = ""
    else:
        if args.base_upload_dir is not None:
            base_upload_dir: str = args.base_upload_dir
        else:
            print("When --dry-run is not passed, you must provide a --base-upload-dir value.")
            sys.exit(1)

    hyperdrive_binary_path: str = args.hyperdrive_binary_path
    if not os.path.exists(hyperdrive_binary_path):
        print(f"hyperdrive binary path: {hyperdrive_binary_path} does not exist. Exiting")
        sys.exit(1)

    # Start db pool
    db_handler.start_database_pool()

    #
    # if recursive let's find all the solution dirs
    #
    solutions: list[SolutionDir] = []
    if recursive:
        for root, dirs, files in os.walk(solution_root):
            # root is the directory of this iteration
            try:
                new_slurm_job_id, new_obs_id = parse_job_dir(root)

                s = SolutionDir()
                s.dir_path = root
                s.slurm_job_id = new_slurm_job_id
                s.obs_id = new_obs_id
                solutions.append(s)

            except ValueError:
                # Ignore- not a valid solution dir
                pass
    else:
        new_slurm_job_id, new_obs_id = parse_job_dir(solution_root)

        s = SolutionDir()
        s.dir_path = solution_root
        s.slurm_job_id = new_slurm_job_id
        s.obs_id = new_obs_id
        solutions.append(s)

    for sol_no, sol in enumerate(solutions):
        sol.log(f"Processing {sol_no} / {len(solutions)}")

        sol.log("Getting Fit ID...")
        result = get_fit_info_from_slurm_job_and_obsid(db_handler, sol.obs_id, sol.slurm_job_id)

        if result is not None:
            new_fit_id, fit_hyperdrive_plot_max = result
            if new_fit_id is not None:
                sol.log(f"Got Fit ID {new_fit_id} from calibration_request table in database.")
                sol.fit_id = new_fit_id
        else:
            # No fit- ignore and move on
            sol.log("Failed to get Fit ID from database. Exiting")
            continue

        metafits_filename = ""
        possible_metafits_filenames = [
            f"{sol.obs_id}_metafits.fits",
            f"{sol.obs_id}.metafits",
            f"{sol.obs_id}_metafits_ppds.fits",
        ]

        for mf in possible_metafits_filenames:
            temp_filename = os.path.join(sol.dir_path, mf)
            if os.path.exists(temp_filename):
                metafits_filename = temp_filename
                break

        if metafits_filename == "":
            sol.log(f"No metafits file could be found in {sol.dir_path}. Downloading one now...")

            metafits_filename = download_metafits_file(sol.obs_id, sol.dir_path)

        sol.log(f"Using {metafits_filename} for metadata.")

        try:
            sol.log("Downloading plot index file...")
            # Download index file
            index_filename = download_plot_index_file(
                sol.fit_id,
                sol.dir_path,
            )
        except requests.HTTPError as httpe:
            resp = httpe.response
            if resp is not None:
                if resp.status_code == 404:
                    print(f"Fit id {sol.fit_id} not found in S3")
                    sys.exit(1)
                else:
                    print(f"HTTP error when downloading the index.json file: {resp.status_code}")
                    sys.exit(1)
            else:
                print(f"HTTP error when downloading the index.json file: no response received {httpe!s}")
                sys.exit(1)

        except Exception as e:
            print(f"Error downloading plot file: {e}")
            sys.exit(1)

        # Get all the solution files
        solution_files = glob.glob(os.path.join(sol.dir_path, "*_solutions.fits"))
        sol.log(f"{len(solution_files)} solution files found.")

        files_to_upload = []

        # Regenerate the plots for each solutions file
        for file in solution_files:
            sol.log(f"Generating new plots for {file} in index.json...")
            success, error_message = generate_hyperdrive_plots(
                sol.obs_id,
                file,
                hyperdrive_binary_path,
                metafits_filename,
                sol.dir_path,
                before=False,
                max_amp=fit_hyperdrive_plot_max,
            )

            # Exit early on failure
            if not success:
                sol.log(f"Error generating plots for {file}: {error_message}")
                sys.exit(1)

        # Open and read the JSON
        with open(index_filename, "r") as f:
            index_json = json.load(f)

        # if the json file is a "version 1" then the png width and height are flipped and need to be fixed!
        if index_json.get("version") == 1:
            sol.log("This is a v1 file, so we'll fix all the png width and heights...")
            for file_entry in index_json.get("files", []):
                if file_entry.get("content_type") == "image/png":
                    width = file_entry.get("image_width")
                    height = file_entry.get("image_height")
                    file_entry["image_width"], file_entry["image_height"] = height, width
            index_json["version"] = 2

        # Update index file for each solution file
        png_files = glob.glob(os.path.join(sol.dir_path, "*.png"))
        for png in png_files:
            sol.log(f"Updating {png} in index.json")
            update_plot_index_file_entry(
                index_json, sol.dir_path, os.path.basename(png), sol.fit_id, plot_front_end_url
            )
            files_to_upload.append(png)

        # upload the solutions
        for sol_fits in solution_files:
            sol.log(f"Adding {sol_fits} in index.json")
            update_plot_index_file_entry(
                index_json, sol.dir_path, os.path.basename(sol_fits), sol.fit_id, plot_front_end_url
            )
            files_to_upload.append(sol_fits)

        orig_solution_files = glob.glob(os.path.join(sol.dir_path, "*_solutions.original.fits"))
        for orig_sol_fits in orig_solution_files:
            sol.log(f"Adding {orig_sol_fits} in index.json")
            update_plot_index_file_entry(
                index_json, sol.dir_path, os.path.basename(orig_sol_fits), sol.fit_id, plot_front_end_url
            )
            files_to_upload.append(orig_sol_fits)

        # Write index file back
        with index_filename.open("w", encoding="utf-8") as f:
            json.dump(index_json, f, indent=2)

        # upload the index
        files_to_upload.append(os.path.join(sol.dir_path, "index.json"))

        if not args.dry_run:
            upload_dir = os.path.join(base_upload_dir, str(sol.fit_id))

            # Make Upload dir and move files there
            try:
                os.mkdir(upload_dir)
            except FileExistsError:
                # dir already exists, no worries
                pass

            try:
                for f in files_to_upload:
                    dest_filename = os.path.join(upload_dir, os.path.basename(f))

                    # copy the solutions files, move the rest
                    if "_solutions.fits" in dest_filename or "_solutions.original.fits" in dest_filename:
                        shutil.copy(f, dest_filename)
                        sol.log(f"Copied {f} to {dest_filename}")
                    else:
                        shutil.move(f, dest_filename)
                        sol.log(f"Moved {f} to {dest_filename}")

            except Exception as e:
                print(f"Error moving files to upload dir {upload_dir}: {e!s}")
                sys.exit(1)
        else:
            print(f"Not uploading files: {files_to_upload} to S3 (bucket={sol.fit_id}) as dry-run = true.")

        sol.log("Complete.")

    print("Completed successfully")


if __name__ == "__main__":
    main()
