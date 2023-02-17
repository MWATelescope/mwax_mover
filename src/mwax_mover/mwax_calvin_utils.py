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
    completed_process_path,
    stats_filename,
    hyperdrive_solution_filename,
    hyperdrive_binary_path,
    metafits_filename,
):
    """This method produces convergence stats and plots"""
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

        if return_value:
            # Currently, hyperdrive writes the solution files to same dir as
            # the current directory mwax_calvin_processor is run from
            # Move them to the processing_complete dir
            amps_plot_filename = f"{obs_id}_solutions_amps.png"
            phase_plot_filename = f"{obs_id}_solutions_phases.png"

            shutil.move(
                os.path.join(os.getcwd(), amps_plot_filename),
                os.path.join(
                    completed_process_path,
                    f"{obs_id}/{amps_plot_filename}",
                ),
            )
            shutil.move(
                os.path.join(os.getcwd(), phase_plot_filename),
                os.path.join(
                    completed_process_path,
                    f"{obs_id}/{phase_plot_filename}",
                ),
            )

            logger.info(f"{obs_id} write_stats() complete successfully")
        else:
            logger.debug(f"{obs_id} write_stats() failed")

    except Exception as catch_all_exception:
        logger.exception(catch_all_exception, "Error in write_stats()")


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
