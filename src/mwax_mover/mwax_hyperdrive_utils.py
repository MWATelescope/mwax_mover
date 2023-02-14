"""Utility Functions to support Hyperdrive usage and validation"""
import time
import numpy as np
from astropy.io import fits
from mwax_mover.mwax_command import run_command_ext


def get_convergence_results(solutions_fits_file: str):
    """Code adapted from Chris Jordan's scripts"""
    solutions = fits.open(solutions_fits_file)

    # Not sure why I need flatten!
    return solutions["RESULTS"].data.flatten()


def print_convergence_summary(solutions_fits_file: str):
    """ "Prints out a summary of the convergence of the solutions"""
    convergence_results = get_convergence_results(solutions_fits_file)
    converged_channel_indices = np.where(~np.isnan(convergence_results))
    print(f"Total number of channels:       {len(convergence_results)}")
    print(
        f"Number of converged channels:   {len(converged_channel_indices[0])}"
    )
    print(
        "Fraction of converged channels:"
        f" {len(converged_channel_indices[0]) / len(convergence_results) * 100}%"
    )
    print(
        "Average channel convergence:   "
        f" {np.mean(convergence_results[converged_channel_indices])}"
    )


def run_hyperdrive(
    logger,
    hyperdrive_binary_path: str,
    data_files_path_and_wildcard: str,
    source_list_filename: str,
    source_list_type: str,
    timeout: int,
):
    """Runs hyperdrive"""
    cmdline = (
        f"{hyperdrive_binary_path}  di-calibrate --no-progress-bars"
        f" --data {data_files_path_and_wildcard} "
        f" --source-list={source_list_filename}"
        f" --source-list-type={source_list_type}"
    )

    start_time = time.time()

    # run hyperdrive
    return_val, stdout = run_command_ext(logger, cmdline, -1, timeout, True)

    if return_val:
        elapsed = time.time() - start_time
        logger.info(f"hyperdrive run successful in {elapsed:.3f} seconds")
    else:
        elapsed = time.time() - start_time
        logger.error(
            f"hyperdrive run FAILED in {elapsed:.3f} seconds: {stdout}"
        )

    return return_val
