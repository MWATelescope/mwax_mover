"""Utility Functions to support mwax_calvin processor"""
import datetime
import glob
import os
import shutil
import sys
import time
import numpy as np
from astropy.io import fits
from mwax_mover.mwax_command import (
    run_command_ext,
    run_command_popen,
    check_popen_finished,
)


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


def run_birli(
    processor,
    metafits_filename: str,
    uvfits_filename: str,
    obs_id: int,
    processing_dir: str,
) -> bool:
    """Execute Birli, returning true on success, false on failure"""
    birli_success: bool = False
    try:
        # Get only data files
        data_files = glob.glob(os.path.join(processing_dir, "*.fits"))
        # Remove the metafits (we specify it seperately)
        data_files.remove(metafits_filename)

        data_file_arg = ""
        for data_file in data_files:
            data_file_arg += f"{data_file} "

        # Run birli
        cmdline = (
            f"{processor.birli_binary_path}"
            f" --metafits {metafits_filename}"
            " --no-draw-progress"
            f" --uvfits-out={uvfits_filename}"
            f" --flag-edge-width={80}"
            f" {data_file_arg}"
        )

        start_time = time.time()

        processor.birli_popen_process = run_command_popen(
            processor.logger, cmdline, -1, False
        )

        exit_code, stdout, stderr = check_popen_finished(
            processor.logger,
            processor.birli_popen_process,
            processor.birli_timeout,
        )

        # return_val, stdout = run_command_ext(logger, cmdline, -1, timeout, False)
        elapsed = time.time() - start_time

        if exit_code == 0:
            ## Success!
            processor.logger.info(
                f"{obs_id}: Birli run successful in {elapsed:.3f} seconds"
            )
            birli_success = True
            processor.birli_popen_process = None

            ## Success!
            # Write out a useful file of command line info
            readme_filename = os.path.join(
                processing_dir, f"{obs_id}_birli_readme.txt"
            )
            write_readme_file(
                processor.logger,
                readme_filename,
                cmdline,
                exit_code,
                stdout,
                stderr,
            )
        else:
            processor.logger.error(
                f"{obs_id}: Birli run FAILED: Exit code of {exit_code} in"
                f" {elapsed:.3f} seconds: {stderr}"
            )
    except Exception as hyperdrive_run_exception:
        processor.logger.error(
            f"{obs_id}: hyperdrive run FAILED: Unhandled exception"
            f" {hyperdrive_run_exception} in {elapsed:.3f} seconds:"
            f" {stderr}"
        )

    if not birli_success:
        # If we are not shutting down,
        # Move the files to an error dir
        #
        # If we are shutting down, then this error is because
        # we have effectively sent it a SIGINT. This should not be a
        # reason to abandon processing. Leave it there to be picked up
        # next run (ie this will trigger the "else" which does nothing)
        if processor.running:
            error_path = os.path.join(processor.processing_error_path, obs_id)
            processor.logger.info(
                f"{obs_id}: moving failed files to {error_path} for manual"
                " analysis and writing readme_error.txt"
            )

            # Move the processing dir
            shutil.move(processing_dir, error_path)

            # Write out a useful file of error and command line info
            readme_filename = os.path.join(error_path, "readme_error.txt")
            write_readme_file(
                processor.logger,
                readme_filename,
                cmdline,
                exit_code,
                stdout,
                stderr,
            )

    return birli_success


def run_hyperdrive(
    processor,
    uvfits_files,
    metafits_filename: str,
    obs_id: int,
    processing_dir: str,
) -> int:
    """Runs hyperdrive N times and returns number of successful runs"""
    processor.logger.info(
        f"{obs_id}: {len(uvfits_files)} contiguous bands detected."
        f" Running hyperdrive {len(uvfits_files)} times...."
    )

    # Keep track of the number of successful hyperdrive runs
    hyperdrive_runs_success: int = 0

    for hyperdrive_run, uvfits_file in enumerate(uvfits_files):
        # Take the filename which for picket fence will also have
        # the band info and in all cases the obsid. We will use
        # this as a base for other files we work with
        obsid_and_band = uvfits_file.replace(".uvfits", "")

        try:
            hyperdrive_solution_filename = f"{obsid_and_band}_solutions.fits"
            bin_solution_filename = f"{obsid_and_band}_solutions.bin"

            # Run hyperdrive
            # Output to hyperdrive format and old aocal format (bin)
            cmdline = (
                f"{processor.hyperdrive_binary_path} di-calibrate"
                " --no-progress-bars --data"
                f" {uvfits_file} {metafits_filename} --num-sources 5"
                " --source-list"
                f" {processor.source_list_filename} --source-list-type"
                f" {processor.source_list_type} --outputs"
                f" {hyperdrive_solution_filename} {bin_solution_filename}"
            )

            start_time = time.time()

            # run hyperdrive
            processor.hyperdrive_popen_process = run_command_popen(
                processor.logger, cmdline, -1, False
            )

            exit_code, stdout, stderr = check_popen_finished(
                processor.logger,
                processor.hyperdrive_popen_process,
                processor.hyperdrive_timeout,
            )

            # return_val, stdout = run_command_ext(logger, cmdline, -1, timeout, False)
            elapsed = time.time() - start_time

            if exit_code == 0:
                processor.logger.info(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(uvfits_files)} successful"
                    f" in {elapsed:.3f} seconds"
                )
                processor.hyperdrive_popen_process = None

                ## Success!
                # Write out a useful file of command line info
                readme_filename = f"{obsid_and_band}_hyperdrive_readme.txt"

                write_readme_file(
                    processor.logger,
                    readme_filename,
                    cmdline,
                    exit_code,
                    stdout,
                    stderr,
                )

                hyperdrive_runs_success += 1
            else:
                processor.logger.error(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(uvfits_files)} FAILED:"
                    f" Exit code of {exit_code} in"
                    f" {elapsed:.3f} seconds. StdErr: {stderr}"
                )
                # exit for loop- since run failed
                break
        except Exception as hyperdrive_run_exception:
            processor.logger.error(
                f"{obs_id}: hyperdrive run"
                f" {hyperdrive_run + 1}/{len(uvfits_files)} FAILED:"
                " Unhandled exception"
                f" {hyperdrive_run_exception} in"
                f" {elapsed:.3f} seconds. StdErr: {stderr}"
            )
            # exit for loop since run failed
            break

    if hyperdrive_runs_success != len(uvfits_files):
        # We did not run successfully on one or all hyperdrive calls.
        # If we are not shutting down,
        # Move the files to an error dir
        #
        # If we are shutting down, then this error is because
        # we have effectively sent it a SIGINT. This should not be a
        # reason to abandon processing. Leave it there to be picked up
        # next run (ie this will trigger the "else" which does nothing)
        if processor.running:
            error_path = os.path.join(processor.processing_error_path, obs_id)
            processor.logger.info(
                f"{obs_id}: moving failed files to {error_path} for manual"
                " analysis and writing readme_error.txt"
            )

            # Move the processing dir
            shutil.move(processing_dir, error_path)

            # Write out a useful file of error and command line info
            readme_filename = os.path.join(error_path, "readme_error.txt")
            write_readme_file(
                processor.logger,
                readme_filename,
                cmdline,
                exit_code,
                stdout,
                stderr,
            )
    else:
        return True


def run_hyperdrive_stats(
    processor,
    uvfits_files,
    metafits_filename: str,
    obs_id: int,
    processing_dir: str,
) -> bool:
    """Call hyperdrive again but just to produce plots and stats"""

    # produce stats/plots
    stats_successful: int = 0

    processor.logger.info(
        f"{obs_id}: {len(uvfits_files)} contiguous bands detected."
        f" Running hyperdrive stats {len(uvfits_files)} times...."
    )

    for hyperdrive_stats_run, uvfits_file in enumerate(uvfits_files):
        # Take the filename which for picket fence will also have
        # the band info and in all cases the obsid. We will use
        # this as a base for other files we work with
        obsid_and_band = uvfits_file.replace(".uvfits", "")

        hyperdrive_solution_filename = f"{obsid_and_band}_solutions.fits"
        stats_filename = f"{obsid_and_band}_stats.txt"

        (
            stats_success,
            stats_error,
        ) = write_stats(
            processor.logger,
            obs_id,
            processing_dir,
            stats_filename,
            hyperdrive_solution_filename,
            processor.hyperdrive_binary_path,
            metafits_filename,
        )

        if stats_success:
            stats_successful += 1
        else:
            processor.logger.warning(
                f"{obs_id}: hyperdrive stats run"
                f" {hyperdrive_stats_run + 1}/{len(uvfits_files)} FAILED:"
                f" {stats_error}."
            )

    if stats_successful == len(uvfits_files):
        processor.logger.info(
            f"{obs_id}: All {stats_successful} hyperdrive stats"
            " runs successful"
        )
        return True
    else:
        processor.logger.warning(
            f"{obs_id}: Not all hyperdrive stats runs were successful."
        )
        return False


if __name__ == "__main__":
    summary_list = get_convergence_summary(sys.argv[1])
    for list_row in summary_list:
        print(f"{list_row[0]}: {list_row[1]}\n")
