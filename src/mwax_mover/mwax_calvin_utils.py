"""Utility Functions to support mwax_calvin processor"""
import datetime
import os
import shutil
import sys
import numpy as np
from astropy.io import fits
from mwax_mover.mwax_command import run_command_ext


def get_convergence_results(solutions_fits_file: str):
    """Code adapted from Chris Jordan's scripts"""
    solutions = fits.open(solutions_fits_file)

    # Not sure why I need flatten!
    return solutions["RESULTS"].data.flatten()  # pylint: disable=E1101


def get_convergence_summary(solutions_fits_file: str):
    """Returns a list of tuples which represent a summary
    of the convergence of the solutions"""
    convergence_results = get_convergence_results(solutions_fits_file)
    converged_channel_indices = np.where(~np.isnan(convergence_results))
    summary = []
    summary.append(("Total number of channels", len(convergence_results)))
    summary.append(
        (
            "Number of converged channels",
            f"{len(converged_channel_indices[0])}",
        )
    )
    summary.append(
        (
            "Fraction of converged channels",
            (
                f" {len(converged_channel_indices[0]) / len(convergence_results) * 100}%"
            ),
        )
    )
    summary.append(
        (
            "Average channel convergence",
            f" {np.mean(convergence_results[converged_channel_indices])}",
        )
    )
    return summary


def write_stats(
    logger,
    obs_id,
    stats_path,
    stats_filename,
    hyperdrive_solution_filename,
    hyperdrive_binary_path,
    metafits_filename,
) -> (bool, str):
    """This method produces convergence stats and plots
    Returns:
    bool = Success/fail
    str  = Error message if fail"""
    logger.info(
        f"{obs_id} Writing stats for {hyperdrive_solution_filename}..."
    )

    try:
        conv_summary_list = get_convergence_summary(
            hyperdrive_solution_filename
        )

        with open(stats_filename, "w", encoding="UTF-8") as stats:
            for row in conv_summary_list:
                stats.write(f"{row[0]}: {row[1]}\n")

        # Now run hyperdrive again to do some plots
        cmd = (
            f"{hyperdrive_binary_path} solutions-plot -m"
            f" {metafits_filename} {hyperdrive_solution_filename}"
        )

        return_value, _ = run_command_ext(
            logger, cmd, -1, timeout=10, use_shell=False
        )

        logger.debug(
            f"{obs_id} Finished running hyperdrive stats on"
            f" {hyperdrive_solution_filename}. Return={return_value}"
        )
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    try:
        if return_value:
            # Currently, hyperdrive writes the solution files to same dir as
            # the current directory mwax_calvin_processor is run from
            # Move them to the processing_complete dir
            plot_filename_base = os.path.basename(
                f"{os.path.splitext(hyperdrive_solution_filename)[0]}"
            )

            amps_plot_filename = f"{plot_filename_base}_amps.png"
            phase_plot_filename = f"{plot_filename_base}_phases.png"
            shutil.move(
                os.path.join(os.getcwd(), amps_plot_filename),
                os.path.join(stats_path, f"{amps_plot_filename}"),
            )
            shutil.move(
                os.path.join(os.getcwd(), phase_plot_filename),
                os.path.join(stats_path, f"{phase_plot_filename}"),
            )
            logger.debug(f"{obs_id} plots moved successfully to {stats_path}.")

            logger.info(f"{obs_id} write_stats() complete successfully")
        else:
            logger.debug(f"{obs_id} write_stats() failed")
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    return True, ""


def write_readme_file(logger, filename, cmd, exit_code, stdout, stderr):
    """This will create a small readme.txt file which will
    hopefully help whoever poor sap is checking why birli
    or hyperdrive did or did not work!"""
    try:
        with open(filename, "w", encoding="UTF-8") as readme:
            if exit_code == 0:
                readme.write(
                    "This run succeded at:"
                    f" {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n"
                )
            else:
                readme.write(
                    "This run failed at:"
                    f" {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n"
                )
            readme.write(f"Command: {cmd}")
            readme.write(f"Exit code: {exit_code}\n")
            readme.write(f"stdout: {stdout}\n")
            readme.write(f"stderr: {stderr}\n")

    except Exception:
        logger.warning(
            (
                f"Could not write text file {filename} describing the"
                " problem observation."
            ),
            exc_info=True,
        )


if __name__ == "__main__":
    summary_list = get_convergence_summary(sys.argv[1])
    for list_row in summary_list:
        print(f"{list_row[0]}: {list_row[1]}\n")
