"""Building and submitting Slurm batch scripts for Calvin jobs.

create_sbatch_script() renders the sbatch script (partition/priority/
walltime depend on CalvinJobType); submit_sbatch() writes it and runs
sbatch; count_slurm_asvo_jobs() queries the Slurm queue directly via
squeue for jobs already named "asvo*".
"""

import datetime
import logging
import os
import sys

from mwax_mover.calvin.pipeline import CalvinJobType
from mwax_mover.constants import EXIT_FAILURE
from mwax_mover.core.command import run_command
from mwax_mover.core.units import is_int

logger = logging.getLogger(__name__)


def create_sbatch_script(
    config_file_path: str,
    obs_id: int,
    jobtype: CalvinJobType,
    log_path: str,
    request_ids: list[int],
    bulk_request: bool,
    processor_args: str,
) -> str:
    """Create a Slurm batch script for Calvin processing.

    Args:
        config_file_path: Path to the Calvin configuration file.
        obs_id: Observation ID.
        jobtype: Type of Calvin job (realtime or mwa_asvo).
        log_path: Global log directory path.
        request_ids: List of calibration request IDs (integers, matching the
            calibration_request.id database column).
        bulk_request: Is this a bulk request? If so lower priority.
        processor_args: Extra command-line arguments for the processor.

    Returns:
        The generated batch script as a string.
    """
    # log_path is the global log path e.g. /home/mwa/logs
    # processor_args is to allow the caller to add extra processor cmd line args.
    # E.g. MWA ASVO requires --mwa-asvo-download-url=URL
    #
    if jobtype == CalvinJobType.realtime:
        job_name = f"real{obs_id}"
        partition = "priority,gpu"
        nice = "0"  # highest priority
        wall_time = "04:00:00"
    else:
        job_name = f"asvo{obs_id}"
        partition = "gpu"
        if bulk_request:
            nice = "10000"  # lowest priority
        else:
            nice = "1000"  # lower priority than realtime jobs
        wall_time = "10:00:00"  # allow extra time for downloading from ASVO (8 hours + 2 for processing)

    job_script = f"""#!/bin/bash
#SBATCH --partition={partition}
#SBATCH --nodes=1
#SBATCH --cpus-per-task=90
#SBATCH --ntasks=1
#SBATCH --gpus-per-task=1
#SBATCH --exclusive # use all cpus
#SBATCH --mem=900G
#SBATCH --time={wall_time}
#SBATCH --account=mwa
#SBATCH --job-name={job_name}
#SBATCH --signal=USR1@360
#SBATCH --output={log_path}/%J.out
#SBATCH --error={log_path}/%J.out
#SBATCH --open-mode=append
#SBATCH --parsable
#SBATCH --nice={nice}

echo "Starting Calvin {jobtype.value} Job: $SLURM_JOBID";

# Source the python environment
cd /home/mwa/mwax_mover
source .venv/bin/activate

# Explicitly specifying these as they dont seem to be passed from the mwa env
export MWA_BEAM_FILE=/software/hyperdrive/mwa_full_embedded_element_pattern.h5
export HYPERDRIVE_CUDA_COMPUTE=86

# Process
srun --nodes=1 --ntasks=1 --cpus-per-task=90 \\
mwax_calvin_processor \\
--cfg={config_file_path} \\
--job-type={jobtype.value} \\
--obs-id={obs_id} \\
--request-ids={",".join(str(r) for r in request_ids)} \\
--slurm-job-id=$SLURM_JOBID {processor_args}

exit $?
"""

    return job_script


def submit_sbatch(script_path: str, script: str, obs_id: int, request_ids: list[int]) -> tuple[bool, int | None]:
    """Submit an sbatch script to Slurm.

    Args:
        script_path: Directory to write the script to.
        script: The batch script content.
        obs_id: Observation ID (for naming).
        request_ids: Calibration request IDs, included in the script filename
            to keep it unique (two requests for the same obs_id at the same
            second would otherwise collide - this has happened).

    Returns:
        A tuple of (success: bool, slurm_job_id: int or None).
    """
    try:
        script_filename: str = os.path.join(
            script_path,
            datetime.datetime.now().strftime(f"%Y%m%d-%H%M%S-{obs_id}-{'-'.join(str(i) for i in request_ids)}.sh"),
        )
        cmdline = f"sbatch {script_filename}"

        # Create an sbatch file
        with open(script_filename, "w") as job_script:
            job_script.write(script)
    except Exception:
        logger.exception(f"{obs_id!s} failure creating temp sbatch script.")
        return (False, None)

    # Submit the job
    return_val: bool = False
    stdout = ""
    try:
        return_val, stdout = run_command(cmdline, None, 60, True)

        # remove crlf from stdout
        stdout = stdout.replace("\n", " ")

        # Success- get the new job id
        # sbatch should send this to std out:
        # "Submitted batch job 34987"
        if return_val:
            logger.info(f"{script_filename} successfully submitted to Slurm. Stdout: {stdout}")
            slurm_job_id_string = stdout.replace("Submitted batch job ", "")
            if is_int(slurm_job_id_string):
                return (True, int(slurm_job_id_string))
            else:
                # This deserves to be a massive failure, as if SBATCH returned true it should always give
                # us the SLURM job id!
                logger.error(f"Slurm job submitted OK, but could not get slurm_job_id from: {stdout}. Aborting")
                sys.exit(EXIT_FAILURE)
        else:
            logger.error(f"{script_filename} failed to be submitted to SLURM. Error {stdout}")

    except Exception:
        logger.exception(f"{script_filename} failure running sbatch.")
        return_val = False

    # Every path where run_command succeeded and returned True already
    # returned or exited above (successful parse -> return; unparseable
    # slurm_job_id -> sys.exit). Reaching here therefore always means
    # return_val is False, from either the "else" branch above or the
    # except block just above -- but that isn't provable by static
    # analysis (mypy flags this function as possibly falling off the end
    # without a return), so this is unconditional rather than gated on
    # `if not return_val` to make the guarantee explicit and satisfy it.
    return (False, None)


def count_slurm_asvo_jobs() -> int:
    """
    Count all SLURM jobs in the queue with names starting with 'asvo'.

    Returns:
        The number of matching jobs, or -1 if the command failed.
    """
    try:
        success, output = run_command(
            command="squeue --format=%j --noheader",
            numa_node=None,
        )
    except Exception:
        logger.exception("count_slurm_asvo_jobs() failed")
        return -1

    if not success:
        logger.error("count_slurm_asvo_jobs() returned -1")
        return -1

    return sum(1 for line in output.splitlines() if line.startswith("asvo"))
