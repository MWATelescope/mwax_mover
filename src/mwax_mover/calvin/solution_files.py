"""Solution-file naming, staging, upload, and index.json generation.

get_solution_fits_filename()/parse_solution_channels()/get_sorted_solution_files()
handle the three solution-filename flavours (whole-obs, single-channel,
channel-range). export_calibration_solutions() copies solution FITS files
to the configured export directory. upload_plot_files() assembles a fit's
plots/stats in a sibling ``.staging-*`` dir (get_staging_path) and publishes
them with a single atomic rename, so a concurrently-reading controller never
observes a partially-written fit directory; reap_orphaned_staging_dirs()
cleans up a staging dir orphaned by a crash. generate_plot_index_file()/
populate_index_json_entry()/get_file_description() build the index.json
manifest uploaded alongside a fit's files. get_convergence_summary() reads
a solution file's per-channel convergence via mwax_hyperdrive_solutions --
previously a function-local import to dodge a circular dependency with the
old mwax_calvin_utils.py; no longer needed now that the shared primitives
mwax_hyperdrive_solutions.py itself needs live in calibration/, not here.
"""

import datetime
import glob
import json
import logging
import mimetypes
import os
import re
import shutil
import time
from pathlib import Path

import numpy as np

from mwax_mover.filesystem.files import delete_files_older_than, get_png_dimensions
from mwax_mover.filesystem.naming import extract_channels_from_filename
from mwax_mover.mwax_hyperdrive_solutions import HyperfitsSolution

logger = logging.getLogger(__name__)


def get_solution_fits_filename(solutions_dir: str, obs_id: int, rec_chan: int) -> str | None:
    """Find a hyperdrive solution FITS file for a specific channel.

    Searches for solution files in multiple formats:
    1. obsid_solutions.fits (all 24 channels)
    2. obsid_chNNN_solutions.fits (single channel)
    3. obsid_chNNN-MMM_solutions.fits (channel range)

    Args:
        solutions_dir: Directory containing solution files.
        obs_id: Observation ID.
        rec_chan: Receiver channel number to find.

    Returns:
        Full path to matching solution file, or None if not found.
    """
    candidates = get_sorted_solution_files(solutions_dir, obs_id, "fits")

    for filepath in candidates:
        channels = parse_solution_channels(filepath)

        if channels is None:
            return filepath

        chan_start, chan_end = channels
        if chan_start <= rec_chan <= chan_end:
            return filepath

    return None


def parse_solution_channels(filename: str) -> tuple[int, int] | None:
    """Parse channel range from a hyperdrive solution filename.

    Recognises these filename flavours (with .fits or .bin extension):
    1. obsid_solutions.{ext}           -> None (all 24 channels)
    2. obsid_chNNN_solutions.{ext}     -> (NNN, NNN)
    3. obsid_chNNN-MMM_solutions.{ext} -> (NNN, MMM)

    Channel numbers are not zero-padded.

    Args:
        filename: Filename or full path to a solution file.

    Returns:
        (start_channel, end_channel) tuple, or None if the file
        covers all channels (flavour 1).

    Raises:
        ValueError: If the filename does not match any known flavour.
    """
    basename = os.path.basename(filename)

    # Flavour 1: obsid_solutions.{fits,bin} — all 24 channels
    if re.match(r"^\d+_solutions\.(?:fits|bin)$", basename):
        return None

    # Flavour 2: obsid_chNNN_solutions.{fits,bin} — single channel
    match = re.match(r"^\d+_ch(\d+)_solutions\.(?:fits|bin)$", basename)
    if match:
        chan = int(match.group(1))
        return (chan, chan)

    # Flavour 3: obsid_chNNN-MMM_solutions.{fits,bin} — channel range
    match = re.match(r"^\d+_ch(\d+)-(\d+)_solutions\.(?:fits|bin)$", basename)
    if match:
        return (int(match.group(1)), int(match.group(2)))

    raise ValueError(f"The channels for {basename} could not be determined")


def get_sorted_solution_files(directory: str, obs_id: int, extension: str = "fits") -> list[str]:
    """Return solution files sorted numerically by channel number.

    Sorting order:
      obsid_solutions.{ext}             -> channel 0 (sorts first)
      obsid_ch95_solutions.{ext}        -> channel 95
      obsid_ch100-112_solutions.{ext}   -> channel 100 (uses range start)

    Unrecognised filenames are sorted with channel 0.

    Args:
        directory: Directory to search for solution files.
        obs_id: Observation ID to filter by.
        extension: File extension to match ("fits" or "bin").

    Returns:
        List of full paths, sorted by channel number then path.
        Or raises ValueError exception if extension doesn't match ("fits" or "bin")
    """
    # Check that the extension doesn't include a "."
    if extension != "fits" and extension != "bin":
        raise ValueError("get_sorted_solution_files() extension should be 'fits' or 'bin'")

    def _sort_key(path: str) -> tuple[int, str]:
        try:
            channels = parse_solution_channels(path)
        except ValueError:
            return (0, path)

        if channels is None:
            return (0, path)

        return (channels[0], path)

    return sorted(
        glob.glob(os.path.join(directory, f"{obs_id}_*solutions.{extension}")),
        key=_sort_key,
    )


def get_file_description(filename: str) -> str:
    """Given a filename, attempt to generate a description of it
    Args:
        filename: The filename to be described
    """

    # If it has a "chNNN" then this is a single coarse channel output
    chans = extract_channels_from_filename(filename)
    if chans is not None:
        chan_no = chans["start"]

        if "end" not in chans:
            channel_suffix = f" for receiver channel {chan_no} ({chan_no * 1.28:.3f} MHz)"
        else:
            chan_no_end = chans["end"]
            channel_suffix = (
                f" for receiver channels {chan_no}-{chan_no_end} ({chan_no * 1.28:.3f} - {chan_no_end * 1.28:.3f} MHz)"
            )
    else:
        channel_suffix = " for all coarse channels"

    desc = ""
    if "birli_readme.txt" in filename:
        desc = "Full log output of the Birli run"
    elif "hyperdrive_readme.txt" in filename:
        desc = "Full log output of the Hyperdrive run"
    elif "intercepts.png" in filename:
        desc = (
            "Plots showing, for each receiver type and polarisation, a plot of the"
            " phase intercepts in polar coordinates vs cable length"
        )
    elif "phase_fits_xx.png" in filename:
        desc = "Plot of the phase fit for each tile (phase vs frequency) for XX"
    elif "phase_fits_yy.png" in filename:
        desc = "Plot of the phase fit for each tile (phase vs frequency) for YY"
    elif "phase_fits.tsv" in filename:
        desc = "Tab separated values (TSV) file containing all of the phase fit statistics per tile"
    elif "rx_lengths.png" in filename:
        desc = "Cable length offsets in metres per receiver"
    elif "solutions_amps.png" in filename:
        desc = "Calibration solution amplitudes vs fine channel per tile"
    elif "solutions_phases.png" in filename:
        desc = "Calibration solution phase vs fine channel per tile"
    elif "solutions_amps_original.png" in filename:
        desc = "Original unmodified Hyperdrive calibration solution amplitudes vs fine channel per tile"
    elif "solutions_phases_original.png" in filename:
        desc = "Original unmodified Hyperdrive calibration solution phase vs fine channel per tile"
    elif "stats.txt" in filename:
        desc = "Before/after per-tile flagging stats, followed by Hyperdrive fine channel convergence statistics"
    elif "residual.tsv" in filename:
        desc = "Tab separated value (TSV) file of phase residuals vs frequency by receiver type and polarisation"
    elif "residual.png" in filename:
        desc = "Plot of phase residuals vs frequency by receiver type and polarisation"
    elif "gain_outliers_tiles" in filename:
        desc = "Plot of outlier gains that were removed from the calibration solutions"
    elif "_solutions.fits" in filename:
        desc = "Hyperdrive calibration solutions in FITS format."
    elif "_solutions.original.fits" in filename:
        desc = "Original unmodified Hyperdrive calibration solutions out of Hyperdrive in FITS format"

    if desc == "":
        return "Miscellaneous file"
    else:
        return f"{desc}{channel_suffix}"


def generate_plot_index_file(
    fit_id: int, plot_front_end_url: str, fit_dir: str, output_filename: str
) -> tuple[bool, dict]:
    """Scans the specified directory (non-recursively) and produces a JSON manifest
    describing each file, suitable for upload to S3 alongside the files themselves.
    The manifest includes a CloudFront URL and MIME type for each file.

    Args:
        fit_id: the fit_id of this calibration. We use this to create a dir in the plot_upload_path
        plot_front_end_url: URL base to retrieve the file. E.g. https://s3blah
        fit_dir: Directory containing the fit to index
        output_filename: full path and name of the JSON file to write

    Returns:
        bool: Success / failure
        dict: The JSON generated (if successful)
    """
    try:
        if not os.path.isdir(fit_dir):
            raise NotADirectoryError(f"Not a valid directory: {fit_dir}")

        files = []
        for filename in sorted(os.scandir(fit_dir), key=lambda e: e.name):
            if not filename.is_file():
                continue
            if filename.name == "index.json":
                continue

            new_entry = populate_index_json_entry(Path(filename), fit_id, plot_front_end_url)

            # None means it found a file we don't want to upload so skip it
            if new_entry is not None:
                files.append(new_entry)

        index = {
            "version": 2,
            "generated_at": datetime.datetime.now(tz=datetime.UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "base_url": plot_front_end_url,
            "path": str(fit_id),
            "files": files,
        }

        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2)

        return True, index
    except Exception:
        # log it and return
        logger.exception(f"Problem generating the {output_filename} file for fit {fit_id}")
        return False, {}


def populate_index_json_entry(filename: str | Path, fit_id: int, plot_front_end_url: str) -> dict | None:
    """Builds an index.json file entry dict for a given directory entry.

    Inspects the file at ``filename``, extracts metadata (size, modification
    time, MIME type, and PNG dimensions where applicable), and returns a dict
    suitable for inclusion in the ``files`` list of an index.json file.

    Only ``.png``, ``.tsv``, ``.txt`` and ``.fits`` files are supported; all
    other extensions return ``None``. Of the ``.fits`` files, only those ending
    ``solutions.fits`` or ``solutions.original.fits`` are accepted -- this
    deliberately excludes the visibility and metafits FITS files.

    Args:
        filename: A str or Path representing the file to describe.
            Must refer to an existing, stat-able file.
        fit_id: The integer fit ID, used to construct the S3 path component
            of the entry's ``url``.
        plot_front_end_url: Base URL of the calibration plot front end
            (e.g. ``"https://cal.mwatelescope.org"``). Combined with
            ``fit_id`` and the filename to form the full entry URL.

    Returns:
        A dict containing the index.json entry fields (``filename``, ``url``,
        ``size_bytes``, ``last_modified``, ``content_type``, ``description``,
        and, for PNG files, ``image_width`` and ``image_height``), or ``None``
        if the file extension is not one of ``.png``, ``.tsv``, or ``.txt``.

    Raises:
        OSError: If the file cannot be stat'd.
        Exception: Any exception raised by :func:`mwax_mover.filesystem.files.get_png_dimensions`
            for PNG files is propagated to the caller.
    """
    path = Path(filename)
    _, ext = os.path.splitext(path.name)

    if ext not in (".png", ".tsv", ".txt", ".fits"):
        return None

    # Now check for other files which slip through
    if ext == ".fits" and not (str(path).endswith("solutions.fits") or str(path).endswith("solutions.original.fits")):
        # Ignore the visibility FITS files and metafits files
        return None

    stat = path.stat()
    last_modified = datetime.datetime.fromtimestamp(stat.st_mtime, tz=datetime.UTC)
    mime_type, _ = mimetypes.guess_type(path.name)

    is_png = ext == ".png"
    width, height = None, None

    if is_png:
        width, height = get_png_dimensions(str(path))

    return {
        "filename": path.name,
        "url": f"{plot_front_end_url}/{fit_id}/{path.name}",
        "size_bytes": stat.st_size,
        "last_modified": last_modified.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "content_type": mime_type or "application/octet-stream",
        "description": get_file_description(str(path)),
        **({"image_width": width, "image_height": height} if is_png else {}),
    }


def export_calibration_solutions(solution_files: list[str], cal_export_path: str, cal_export_max_age_hours: int):
    """Export calibration solution FITS files to the configured export directory and delete stale files.

    Args:
        solution_files: List of hyperdrive solution filenames
        cal_export_path: Path to copy solution files to
        cal_export_max_age_hours: Files older than this many hours will be deleted from the cal_export_path

    Returns:
        Nothing
    """
    # if cal_export_path is set then:
    # 1. copy the solution FITS files to the export dir
    # 2. try to clean up old files

    #
    # copy the solution.fits file(s) to the export directory
    logger.info(f"Found {len(solution_files)} solution FITS files to upload.")

    for f in solution_files:
        # Copy solution fits files to the cal_export directory
        cal_dest = os.path.join(cal_export_path, os.path.basename(f))
        logger.info(f"Copying solution FITS file {f} to {cal_dest}")
        shutil.copy(f, cal_dest)

    # Clean up old files
    ext_list = ["fits", "bin"]
    files_removed = delete_files_older_than(cal_export_path, cal_export_max_age_hours * 3600, ext_list)
    if len(files_removed) > 0:
        logger.debug(
            f"Removed the following files from {cal_export_path} as they were older"
            f" than {cal_export_max_age_hours} hours: {files_removed}"
        )
    else:
        logger.debug(f"No files older than {cal_export_max_age_hours} hours found in {cal_export_path} to remove.")


STAGING_DIR_PREFIX = ".staging-"


def get_staging_path(upload_path: str) -> str:
    """Build the staging directory path used to assemble an upload directory.

    Args:
        upload_path: The final, published directory, e.g.
            ``/data/calvin/plots/1768401673707300``.

    Returns:
        A sibling directory of *upload_path* prefixed with
        ``STAGING_DIR_PREFIX``, e.g.
        ``/data/calvin/plots/.staging-1768401673707300``. The dot prefix is what
        the controller's upload thread uses to tell an in-progress directory
        from a published one.
    """
    base_path = os.path.dirname(upload_path)
    fit_dir_name = os.path.basename(upload_path)
    return os.path.join(base_path, f"{STAGING_DIR_PREFIX}{fit_dir_name}")


def reap_orphaned_staging_dirs(base_path: str, max_age_hours: int = 24) -> list[str]:
    """Delete staging directories left behind by a previous, crashed run.

    ``upload_plot_files`` assembles each fit's files in a ``.staging-*``
    directory and then publishes it with a single atomic rename. If the process
    dies partway through, the staging directory is orphaned: nothing will ever
    publish or consume it, so it must be cleaned up here.

    Intended to be called once at processor startup. Only directories older
    than *max_age_hours* are removed, so a staging directory belonging to a
    concurrently-running job is never touched. The Slurm walltime for a Calvin
    job is at most 10 hours (see create_sbatch_script), so the 24 hour default
    is comfortably beyond the lifetime of any legitimate in-flight job.

    Args:
        base_path: The plot upload base directory to scan, e.g.
            ``/data/calvin/plots``. Missing or non-directory paths are ignored.
        max_age_hours: Only remove staging directories whose modification time
            is at least this many hours in the past. Defaults to 24.

    Returns:
        A list of the staging directory paths that were successfully removed.
    """
    removed: list[str] = []

    base = Path(base_path)
    if not base.is_dir():
        logger.debug(f"reap_orphaned_staging_dirs: {base_path} is not a directory. Nothing to do.")
        return removed

    cutoff_seconds = max_age_hours * 3600
    now = time.time()

    for entry in base.iterdir():
        if not entry.name.startswith(STAGING_DIR_PREFIX):
            continue

        try:
            if not entry.is_dir():
                continue

            age_seconds = now - entry.stat().st_mtime
            if age_seconds < cutoff_seconds:
                logger.info(
                    f"reap_orphaned_staging_dirs: leaving {entry} alone"
                    f" ({age_seconds / 3600:.1f}h old, threshold is {max_age_hours}h)"
                )
                continue

            shutil.rmtree(entry)
            removed.append(str(entry))
            logger.warning(
                f"reap_orphaned_staging_dirs: removed orphaned staging dir {entry}"
                f" ({age_seconds / 3600:.1f}h old). Its plots were never published."
            )
        except Exception:
            # One bad entry must not stop us reaping the rest
            logger.exception(f"reap_orphaned_staging_dirs: could not remove {entry}. Ignoring.")

    return removed


def upload_plot_files(job_output_path: str, upload_path: str) -> bool:
    """Assemble this fit's plots and stats in a staging dir, then publish atomically.

    Files are gathered into a sibling ``.staging-<fit_id>`` directory and only
    then renamed into place as *upload_path*. Directory rename is atomic within
    a filesystem, so the controller's upload thread never observes a partially
    populated fit directory -- it either does not exist yet, or it is complete.

    This matters because the controller uploads and then deletes these
    directories from a different host over a network filesystem. Any scheme
    based on inferring completion (checking whether a directory is empty, or
    comparing file/directory mtimes against a wall clock that belongs to
    another machine) can delete a directory that is still being written to,
    losing every plot for that fit. Publishing atomically removes the
    possibility rather than narrowing the window.

    Failures are logged and reported via the return value rather than raised:
    the plots are a diagnostic aid, and losing them must not fail an otherwise
    successful calibration.

    Args:
        job_output_path: The location of all the plots, txt, tsv files for this fit.
        upload_path: Final destination directory for this fit's plots and stats,
            conventionally ``<plot_upload_path>/<fit_id>``.

    Returns:
        True if the directory was published successfully, False otherwise.
    """
    staging_path = get_staging_path(upload_path)

    try:
        # Refuse to overwrite an already-published fit. Checked before anything
        # is moved, so a collision costs nothing: the files stay in
        # job_output_path where they can be inspected or retried by hand.
        if os.path.exists(upload_path):
            logger.error(
                f"upload_plot_files: {upload_path} already exists. Aborting without"
                " uploading anything. The files remain in"
                f" {job_output_path}. This needs manual investigation."
            )
            return False

        # A staging dir surviving from a previous crashed attempt for this same
        # fit contains nothing of value (it was never published), so start clean
        # rather than merging stale files into this attempt.
        if os.path.exists(staging_path):
            logger.warning(f"upload_plot_files: removing stale staging dir {staging_path} before starting.")
            shutil.rmtree(staging_path)

        os.makedirs(staging_path)

        exts = [
            "*.png",
            "*.txt",
            "*.tsv",
            "*.json",
            "*_solutions.fits",
            "*_solutions.original.fits",
        ]
        for ext in exts:
            plot_files = glob.glob(os.path.join(job_output_path, ext))
            for file_no, pfile in enumerate(plot_files, start=1):
                try:
                    dest_filename = os.path.join(staging_path, os.path.basename(pfile))

                    # We want to keep the solutions on calvin servers so copy them, don't move them!
                    if ext in ["*_solutions.fits", "*_solutions.original.fits"]:
                        logger.debug(f"Copying {pfile} to {dest_filename} [{file_no}/{len(plot_files)}]")
                        shutil.copy(pfile, dest_filename)
                    else:
                        logger.debug(f"Moving {pfile} to {dest_filename} [{file_no}/{len(plot_files)}]")
                        shutil.move(pfile, dest_filename)

                except Exception as e:
                    logger.warning(f"Failed to move {pfile} to the {staging_path}. Error: {e!s}. Ignoring")
                    # keep going and try the next file

        # Publish. os.replace() on a directory requires the target not to exist
        # (or to be an empty directory), which the check above ensures. This is
        # the point at which the controller becomes able to see the files.
        os.replace(staging_path, upload_path)
        logger.info(f"upload_plot_files: published {upload_path} for upload.")
        return True

    except Exception as ee:
        # Something went wrong- log it and keep going. Deliberately leave the
        # staging dir in place for inspection; reap_orphaned_staging_dirs will
        # remove it on a later processor startup if it is genuinely abandoned.
        logger.warning(
            f"Failed to publish {upload_path} (staging dir {staging_path} left in place). Error: {ee!s}. Ignoring"
        )
        return False


def get_convergence_summary(solutions_fits_file: str):
    """Get a convergence summary from a solution file.

    Args:
        solutions_fits_file: Path to the solutions FITS file.

    Returns:
        List of tuples with convergence statistics.
    """
    soln = HyperfitsSolution(solutions_fits_file)
    results = soln.results
    converged_channel_indices = np.where(~np.isnan(results))
    summary = []
    summary.append(("Converged channel indices", converged_channel_indices))
    summary.append(("Total number of channels", len(results)))
    summary.append(
        (
            "Number of converged channels",
            f"{len(converged_channel_indices[0])}",
        )
    )
    summary.append(
        (
            "Fraction of converged channels",
            (f" {len(converged_channel_indices[0]) / len(results) * 100}%"),
        )
    )
    summary.append(
        (
            "Average channel convergence",
            f" {np.mean(results[converged_channel_indices])}",
        )
    )
    return summary
