"""Solution-file naming, staging, and upload.

get_solution_fits_filename()/parse_solution_channels()/get_sorted_solution_files()
handle the three solution-filename flavours (whole-obs, single-channel,
channel-range). export_calibration_solutions() copies solution FITS files
to the configured export directory. upload_plot_files() assembles a fit's
plots/stats in a sibling ``.staging-*`` dir (get_staging_path) and publishes
them with a single atomic rename, so a concurrently-reading controller never
observes a partially-written fit directory; reap_orphaned_staging_dirs()
cleans up a staging dir orphaned by a crash.

index.json generation (generate_plot_index_file, populate_index_json_entry,
get_file_description) and convergence-summary reading (get_convergence_summary)
used to live here too -- moved to calvin.plots.index and calvin.hyperdrive
respectively once calvin/plots/ existed as a real destination and merging
mwax_hyperdrive_solutions.py into calvin.hyperdrive made keeping
get_convergence_summary here create a two-file import cycle
(docs/RESTRUCTURE.md Phase 4).
"""

import glob
import logging
import os
import re
import shutil
import time
from pathlib import Path

from mwax_mover.constants import SECONDS_PER_HOUR
from mwax_mover.filesystem.files import delete_files_older_than

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


def export_calibration_solutions(
    solution_files: list[str], cal_export_path: str, cal_export_max_age_hours: int
) -> None:
    """Export calibration solution FITS files to the configured export directory and delete stale files.

    Args:
        solution_files: List of hyperdrive solution filenames
        cal_export_path: Path to copy solution files to
        cal_export_max_age_hours: Files older than this many hours will be deleted from the cal_export_path
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
    files_removed = delete_files_older_than(cal_export_path, cal_export_max_age_hours * SECONDS_PER_HOUR, ext_list)
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

    cutoff_seconds = max_age_hours * SECONDS_PER_HOUR
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
                    f" ({age_seconds / SECONDS_PER_HOUR:.1f}h old, threshold is {max_age_hours}h)"
                )
                continue

            shutil.rmtree(entry)
            removed.append(str(entry))
            logger.warning(
                f"reap_orphaned_staging_dirs: removed orphaned staging dir {entry}"
                f" ({age_seconds / SECONDS_PER_HOUR:.1f}h old). Its plots were never published."
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
