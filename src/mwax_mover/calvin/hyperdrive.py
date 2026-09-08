"""Running hyperdrive to produce calibration solutions, and writing convergence stats.

run_hyperdrive() shells out to the hyperdrive binary via a Popen handle and
writes a readme (calvin.pipeline.write_readme_file) recording the command
and outcome, mirroring calvin.birli.run_birli(). write_hyperdrive_stats()
writes the convergence summary (calvin.solution_files.get_convergence_summary)
for a just-produced solution file.

Reading already-produced hyperdrive solution files (HyperfitsSolution,
HyperfitsSolutionGroup) is a separate concern -- see mwax_hyperdrive_solutions.py.
This file currently holds only the run-hyperdrive/write-stats side; the two
are expected to consolidate into one calvin/hyperdrive.py module in a later
step, per docs/RESTRUCTURE.md's target structure.
"""

import logging
import os
import shutil
import time

from mwax_mover.calvin.pipeline import write_readme_file
from mwax_mover.calvin.solution_files import get_convergence_summary
from mwax_mover.core.command import check_popen_finished, run_command_popen

logger = logging.getLogger(__name__)


def run_hyperdrive(
    input_uvfits_files: list[str],
    metafits_filename: str,
    job_output_path: str,
    obs_id: int,
    hyperdrive_binary_path: str,
    source_list_filename: str,
    source_list_type: str,
    num_sources: int,
    hyperdrive_timeout: int,
    hyperdrive_extra_args: str,
) -> tuple[bool, str]:
    """Run hyperdrive calibration on UV FITS files.

    Args:
        input_uvfits_files: List of input UV FITS files, one per contiguous
            coarse-channel band (so 1 for a normal observation, up to 24 for a
            picket fence).
        metafits_filename: Path to the metafits file.
        job_output_path: Output directory for hyperdrive.
        obs_id: Observation ID.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        source_list_filename: Path to the source list file.
        source_list_type: Type of source list (e.g., 'gleam').
        num_sources: Number of sources in the list.
        hyperdrive_timeout: Timeout in seconds for hyperdrive execution.
        hyperdrive_extra_args: Any additional command line args provided from the calvin_processor config file.

    Returns:
        tuple[True, calibration_command] if all runs succeeded, [False, calibration_command] if any failed.
    """
    logger.info(
        f"{obs_id}: {len(input_uvfits_files)} contiguous bands detected."
        f" Running hyperdrive {len(input_uvfits_files)} times...."
    )

    hyperdrive_runs_success: int = 0
    stdout = ""
    stderr = ""
    elapsed = -1
    cmdline = ""
    exit_code = 0
    # Initialised here so it is always bound, even if input_uvfits_files is
    # empty, which would otherwise be an UnboundLocalError at the return sites.
    calibration_command = ""

    for hyperdrive_run, uvfits_file in enumerate(input_uvfits_files):
        obsid_and_band = os.path.basename(uvfits_file.replace(".uvfits", ""))

        # Outside the try block so it is always bound before the exception
        # handler below computes `elapsed` from it.
        start_time = time.time()

        try:
            hyperdrive_solution_full_filename = os.path.join(job_output_path, f"{obsid_and_band}_solutions.fits")
            bin_solution_filename = f"{obsid_and_band}_solutions.bin"
            bin_solution_full_filename = os.path.join(job_output_path, bin_solution_filename)

            calibration_command = (
                f"--num-sources {num_sources}"
                f" --source-list {source_list_filename}"
                f" --source-list-type {source_list_type}"
                f" {hyperdrive_extra_args}"
            )
            cmdline = (
                f"{hyperdrive_binary_path} di-calibrate"
                f" --no-progress-bars {calibration_command}"
                f" --data {uvfits_file} {metafits_filename} "
                f" --outputs {hyperdrive_solution_full_filename} {bin_solution_full_filename}"
            )

            logger.info(f"{obs_id}: Running hyperdrive on {uvfits_file}...")
            hyperdrive_popen_process = run_command_popen(cmdline, -1, False, False)

            exit_code, stdout, stderr = check_popen_finished(
                hyperdrive_popen_process,
                hyperdrive_timeout,
            )

            elapsed = time.time() - start_time

            if exit_code == 0:
                logger.info(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(input_uvfits_files)} successful"
                    f" in {elapsed:.3f} seconds"
                )

                # Joined with job_output_path so the readme lands in the job's
                # output directory rather than the current working directory.
                readme_filename = os.path.join(job_output_path, f"{obsid_and_band}_hyperdrive_readme.txt")
                write_readme_file(
                    readme_filename,
                    cmdline,
                    exit_code,
                    stdout,
                    stderr,
                )

                hyperdrive_runs_success += 1
            else:
                logger.error(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(input_uvfits_files)} FAILED:"
                    f" Exit code of {exit_code} in"
                    f" {elapsed:.3f} seconds. StdErr: {stderr}"
                )
                break

        except Exception as hyperdrive_run_exception:
            elapsed = time.time() - start_time
            logger.error(
                f"{obs_id}: hyperdrive run"
                f" {hyperdrive_run + 1}/{len(input_uvfits_files)} FAILED:"
                " Unhandled exception"
                f" {hyperdrive_run_exception} in"
                f" {elapsed:.3f} seconds. StdErr: {stderr}"
            )
            break

    if hyperdrive_runs_success != len(input_uvfits_files):
        logger.info(
            f"{obs_id}: moving failed files to {job_output_path} for manual analysis and writing readme_error.txt"
        )

        for uvfits_file in input_uvfits_files:
            shutil.move(uvfits_file, job_output_path)

        readme_filename = os.path.join(job_output_path, "readme_error.txt")
        write_readme_file(
            readme_filename,
            cmdline,
            exit_code,
            stdout,
            stderr,
        )
        return False, calibration_command

    return True, calibration_command


def write_hyperdrive_stats(
    obs_id: int,
    stats_fd,
    hyperdrive_solution_filename: str,
) -> tuple[bool, str]:
    """Write convergence statistics. (Append to existing stats file if it exists.)

    Args:
        obs_id: Observation ID.
        stats_fd: File descriptor for the statistics file.
        hyperdrive_solution_filename: Path to the hyperdrive solution FITS file.

    Returns:
        A tuple of (success: bool, error_message: str).
    """
    logger.info(f"{obs_id} Writing convergence stats for {hyperdrive_solution_filename}.")
    try:
        conv_summary_list = get_convergence_summary(hyperdrive_solution_filename)

        stats_fd.writelines(f"{row[0]}: {row[1]}\n" for row in conv_summary_list)
        stats_fd.write("\n")

        logger.info(f"{obs_id} Finished running convergence stats for {hyperdrive_solution_filename}.")
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    return True, ""
