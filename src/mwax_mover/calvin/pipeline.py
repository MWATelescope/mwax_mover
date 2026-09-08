"""Calvin pipeline-level concepts shared across the birli/hyperdrive/slurm
run steps.

CalvinJobType distinguishes a realtime job from an MWA ASVO download job
(affects Slurm partition/priority -- see calvin.slurm.create_sbatch_script).
write_readme_file() is the generic command-log writer both
calvin.birli.run_birli() and calvin.hyperdrive.run_hyperdrive() use to
record what was run, alongside its exit code and output, next to the job's
output files.
"""

import datetime
import logging
from enum import Enum

logger = logging.getLogger(__name__)


class CalvinJobType(Enum):
    """Calvin Job Type"""

    realtime = "realtime"
    mwa_asvo = "mwa_asvo"


def write_readme_file(filename, cmd, exit_code, output, error):
    """Write a readme file documenting the result of a command or operation.

    Used both for subprocess results (birli, hyperdrive) and for recording
    Python exception details on failure.

    Args:
        filename: Path to write the readme file to.
        cmd: The command or operation that was executed.
        exit_code: The exit code or error code (0 = success).
        output: Standard output from the command, or empty string.
        error: Standard error from the command, or exception traceback text.
    """
    try:
        with open(filename, "w", encoding="UTF-8") as readme:
            if exit_code == 0:
                readme.write(f"This run succeeded at: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
            else:
                readme.write(f"This run failed at: {datetime.datetime.now().strftime('%d-%m-%Y %H:%M:%S')}\n")
            readme.write(f"Command: {cmd}\n")
            readme.write(f"Exit code: {exit_code}\n")
            readme.write(f"output: {output}\n")
            readme.write(f"error: {error}\n")

    except Exception:
        logger.warning(
            (f"Could not write text file {filename} describing the problem observation."),
            exc_info=True,
        )
