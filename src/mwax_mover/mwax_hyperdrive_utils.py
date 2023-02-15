"""Utility Functions to support Hyperdrive usage and validation"""
import numpy as np
from astropy.io import fits


def get_convergence_results(solutions_fits_file: str):
    """Code adapted from Chris Jordan's scripts"""
    solutions = fits.open(solutions_fits_file)

    # Not sure why I need flatten!
    return solutions["RESULTS"].data.flatten()  # pylint: disable=E1101


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
