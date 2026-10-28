"""Running hyperdrive solutions-apply to make calibrated measurement sets.

run_solutions_apply() applies each band's calibration solutions to that band's
Birli measurement set, writing a calibrated measurement set for WSClean to
image. It shells out to the hyperdrive binary via a Popen handle and writes a
readme (core.command.write_readme_file) per band recording the command and
outcome, mirroring calvin.hyperdrive. The calibration itself (di-calibrate)
stays in calvin.hyperdrive.
"""

import logging
import os
import shutil
import time

from mwax_mover.constants import CALIBRATED_MS_SUFFIX, EXT_MS, SOLUTIONS_APPLY_README_SUFFIX, SOLUTIONS_FITS_SUFFIX
from mwax_mover.core.command import check_popen_finished, start_command, write_readme_file

logger = logging.getLogger(__name__)


def _band_stem(ms_file: str) -> str:
    """Get the band name of a measurement set.

    Args:
        ms_file: Path to a measurement set directory.

    Returns:
        The name without the parent path, trailing slash or extension, e.g.
        1234567890_ch75-76 (or 1234567890 if not picket fence).
    """
    return os.path.splitext(os.path.basename(ms_file.rstrip("/")))[0]


def _remove_dir(path: str, obs_id: int) -> None:
    """Remove a directory tree. Log a warning and continue if that fails.

    Args:
        path: Directory to remove.
        obs_id: Observation ID, for log messages.
    """
    try:
        shutil.rmtree(path)
    except OSError:
        logger.warning(f"{obs_id}: could not remove {path}. Ignoring.")


def _apply_one(
    ms_file: str,
    metafits_filename: str,
    job_output_path: str,
    working_path: str,
    obs_id: int,
    hyperdrive_binary_path: str,
    timeout: int,
) -> tuple[bool, str]:
    """Run hyperdrive solutions-apply on one band.

    Args:
        ms_file: Path to this band's uncalibrated measurement set (a directory).
        metafits_filename: Path to the metafits file.
        job_output_path: Directory with the band's <stem>_solutions.fits. The readme is written here.
        working_path: Directory for the calibrated measurement set.
        obs_id: Observation ID.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        timeout: Timeout in seconds for the hyperdrive run.

    Returns:
        (success, calibrated measurement set path). The path is only valid if success is True.
    """
    stem = _band_stem(ms_file)
    solutions_filename = os.path.join(job_output_path, f"{stem}_{SOLUTIONS_FITS_SUFFIX}")
    cal_ms_filename = os.path.join(working_path, f"{stem}{CALIBRATED_MS_SUFFIX}")
    readme_filename = os.path.join(job_output_path, f"{stem}{SOLUTIONS_APPLY_README_SUFFIX}")

    if not os.path.exists(solutions_filename):
        logger.error(f"{obs_id}: solutions-apply FAILED for {ms_file}: no solutions file {solutions_filename}")
        return False, cal_ms_filename

    cmdline = (
        f"{hyperdrive_binary_path} solutions-apply --no-progress-bars"
        f" --data {ms_file} {metafits_filename}"
        f" --solutions {solutions_filename}"
        f" --outputs {cal_ms_filename}"
    )
    start_time = time.monotonic()
    exit_code = -1
    stdout = ""
    stderr = ""

    try:
        logger.info(f"{obs_id}: Running hyperdrive solutions-apply on {ms_file}...")
        popen_process = start_command(cmdline, -1, False, False)
        exit_code, stdout, stderr = check_popen_finished(popen_process, timeout)
    except Exception as apply_exception:  # noqa: BLE001 - one bad apply run must not crash the calibration job
        stderr = f"Unhandled exception {apply_exception}"

    elapsed = time.monotonic() - start_time
    write_readme_file(readme_filename, cmdline, exit_code, stdout, stderr)

    if exit_code == 0:
        logger.info(f"{obs_id}: solutions-apply for {stem} successful in {elapsed:.3f} seconds")
        return True, cal_ms_filename

    logger.error(
        f"{obs_id}: solutions-apply for {stem} FAILED: exit code {exit_code} in {elapsed:.3f} seconds. StdErr: {stderr}"
    )
    # Do not leave a partial calibrated measurement set behind.
    _remove_dir(cal_ms_filename, obs_id)
    return False, cal_ms_filename


def run_solutions_apply(
    ms_files: list[str],
    metafits_filename: str,
    job_output_path: str,
    working_path: str,
    obs_id: int,
    hyperdrive_binary_path: str,
    timeout: int,
    remove_input_ms: bool,
) -> tuple[bool, list[str]]:
    """Apply each band's calibration solutions to its measurement set.

    Bands run one after the other. Each band's solutions are
    <job_output_path>/<stem>_solutions.fits, where <stem> is the name of the
    band's measurement set without its extension. This is the modified
    solution that is in the database, not the *.original.fits file.

    The run stops at the first failed band, because imaging a partial set of
    bands is not wanted. Calibrated sets already made are kept, so the caller
    can clean up.

    Args:
        ms_files: Uncalibrated Birli measurement sets (directories), one per band.
        metafits_filename: Path to the metafits file.
        job_output_path: Directory with the solutions files. Readmes are written here.
        working_path: Directory for the calibrated measurement sets.
        obs_id: Observation ID.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        timeout: Timeout in seconds for each band's hyperdrive run.
        remove_input_ms: If True, remove each band's uncalibrated measurement set
            after its apply succeeds, to free working space.

    Returns:
        (success, calibrated measurement sets). success is True only if every band succeeded.
        The list holds the calibrated sets made so far, in input order.
    """
    cal_ms_files: list[str] = []

    for ms_file in ms_files:
        success, cal_ms_filename = _apply_one(
            ms_file,
            metafits_filename,
            job_output_path,
            working_path,
            obs_id,
            hyperdrive_binary_path,
            timeout,
        )
        if not success:
            return False, cal_ms_files

        cal_ms_files.append(cal_ms_filename)

        if remove_input_ms and ms_file.rstrip("/").endswith(EXT_MS):
            _remove_dir(ms_file, obs_id)

    return True, cal_ms_files
