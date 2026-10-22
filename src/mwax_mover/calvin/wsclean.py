"""Running WSClean to image calibrated measurement sets.

run_wsclean() shells out to the wsclean binary via a Popen handle (so it can be
signalled/waited on alongside other work) and always writes a readme
(core.command.write_readme_file) recording the command and outcome. WSCleanArgs
holds the WSClean options and turns them into command line arguments.

A failed WSClean run does not fail the calibration job: the solutions are
already published, so run_wsclean() only returns False and the caller logs it.
"""

import logging
import os
import time
from dataclasses import dataclass

from mwax_mover.core.command import check_popen_finished, start_command, write_readme_file

logger = logging.getLogger(__name__)


@dataclass
class WSCleanArgs:
    """The WSClean options used by calvin.

    Attributes:
        name: Prefix for all WSClean output files. Must be an absolute path, because WSClean
            writes relative to the process working directory.
        temp_dir: Directory for WSClean's temporary (reordered) files.
        no_update_model_required: If True, WSClean does not write the model data column.
    """

    name: str
    temp_dir: str
    no_update_model_required: bool = True

    def to_cmdline_args(self) -> list[str]:
        """Build the WSClean command line options (without the binary or the measurement sets).

        Returns:
            The options as separate arguments, e.g. ["-name", "/out/123_wsclean", ...].
        """
        args = ["-name", self.name, "-temp-dir", self.temp_dir]

        if self.no_update_model_required:
            args.append("-no-update-model-required")

        return args


def run_wsclean(
    ms_filenames: list[str],
    job_output_path: str,
    obs_id: int,
    wsclean_binary_path: str,
    wsclean_timeout: int,
    args: WSCleanArgs,
) -> bool:
    """Execute wsclean to image calibrated measurement sets.

    The measurement sets are given to WSClean as separate arguments. A readme is
    written to job_output_path for success and for failure. Nothing is moved on
    failure, and no readme_error.txt is written.

    Args:
        ms_filenames: List of calibrated CASA measurement set dirs.
        job_output_path: Output directory for the readme.
        obs_id: Observation ID.
        wsclean_binary_path: Path to the wsclean executable.
        wsclean_timeout: Timeout in seconds for wsclean execution.
        args: The WSClean options.

    Returns:
        True if execution succeeded, False otherwise.
    """
    start_time = time.monotonic()
    stdout = ""
    stderr = ""
    exit_code = -1

    cmdline = " ".join([wsclean_binary_path, *args.to_cmdline_args(), *ms_filenames])

    try:
        wsclean_popen_process = start_command(cmdline, -1, False, False)

        exit_code, stdout, stderr = check_popen_finished(
            wsclean_popen_process,
            wsclean_timeout,
        )
    except Exception as wsclean_run_exception:  # noqa: BLE001 - one bad wsclean run must not crash the calibration job
        stderr = f"Unhandled exception {wsclean_run_exception}"

    elapsed = time.monotonic() - start_time
    write_readme_file(os.path.join(job_output_path, f"{obs_id}_wsclean_readme.txt"), cmdline, exit_code, stdout, stderr)

    if exit_code == 0:
        logger.info(f"{obs_id}: wsclean run successful in {elapsed:.3f} seconds")
        return True

    logger.error(f"{obs_id}: wsclean run FAILED: Exit code of {exit_code} in {elapsed:.3f} seconds: {stderr}")
    return False
