"""Running wsclean to preprocess visibility data, and estimating its output size.

run_wsclean() shells out to the wsclean binary via a Popen handle (so it can be
signalled/waited on alongside other work) and writes a readme
(core.command.write_readme_file) recording the command and outcome.
estimate_wsclean_output_bytes() is a pre-flight storage-size estimate from
the observation's metafits parameters, used before running wsclean at all.
"""

import logging
import os
import shutil
import time

from mwax_mover.core.command import check_popen_finished, start_command, write_readme_file

logger = logging.getLogger(__name__)


def run_wsclean(
    ms_filenames: list[str],
    job_output_path: str,
    obs_id: int,
    wsclean_binary_path: str,
    wsclean_timeout: int,
) -> bool:
    """Execute wsclean to process visibility data.

    Args:
        ms_filenames: List of CASA measurement set dirs.
        job_output_path: Output directory for wsclean.
        obs_id: Observation ID.
        wsclean_binary_path: Path to the wsclean executable.
        wsclean_timeout: Timeout in seconds for wsclean execution.

    Returns:
        True if execution succeeded, False otherwise.
    """
    wsclean_success: bool = False
    start_time = time.monotonic()
    stderr = ""

    cmdline = None
    exit_code = None
    stdout = None
    try:
        # Run wsclean
        ms = ",".join(ms_filenames)

        cmdline = f"{wsclean_binary_path} {ms}"

        wsclean_popen_process = start_command(cmdline, -1, False, False)

        exit_code, stdout, stderr = check_popen_finished(
            wsclean_popen_process,
            wsclean_timeout,
        )

        elapsed = time.monotonic() - start_time

        if exit_code == 0:
            # Success!
            logger.info(f"{obs_id}: wsclean run successful in {elapsed:.3f} seconds")
            wsclean_success = True

            # Success!
            # Write out a useful file of command line info
            readme_filename = os.path.join(job_output_path, f"{obs_id}_wsclean_readme.txt")
            write_readme_file(
                readme_filename,
                cmdline,
                exit_code,
                stdout,
                stderr,
            )
        else:
            logger.error(f"{obs_id}: wsclean run FAILED: Exit code of {exit_code} in {elapsed:.3f} seconds: {stderr}")
    except Exception as wsclean_run_exception:  # noqa: BLE001 - one bad wsclean run must not crash the calibration job
        elapsed = time.monotonic() - start_time
        logger.error(
            f"{obs_id}: wsclean run FAILED: Unhandled exception {wsclean_run_exception} in {elapsed:.3f} seconds: {stderr}"
        )

    if not wsclean_success:
        # If we are not shutting down,
        # Move the files to an error dir
        logger.info(
            f"{obs_id}: moving failed files to {job_output_path} for manual analysis and writing readme_error.txt"
        )

        # Move the processing dir
        for ms in ms_filenames:
            shutil.move(ms, job_output_path)

        # Write out a useful file of error and command line info
        readme_filename = os.path.join(job_output_path, "readme_error.txt")
        write_readme_file(
            readme_filename,
            cmdline,
            exit_code,
            stdout,
            stderr,
        )

    return wsclean_success
