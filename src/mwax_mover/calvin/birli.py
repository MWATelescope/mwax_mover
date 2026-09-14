"""Running Birli to preprocess visibility data, and estimating its output size.

run_birli() shells out to the Birli binary via a Popen handle (so it can be
signalled/waited on alongside other work) and writes a readme
(core.command.write_readme_file) recording the command and outcome.
estimate_birli_output_bytes() is a pre-flight storage-size estimate from
the observation's metafits parameters, used before running Birli at all.
"""

import glob
import logging
import os
import shutil
import time

import numpy as np
from mwalib import MetafitsContext

from mwax_mover.calibration.models import Metafits
from mwax_mover.constants import SOLUTIONS_FITS_SUFFIX
from mwax_mover.core.command import check_popen_finished, start_command, write_readme_file

logger = logging.getLogger(__name__)


def run_birli(
    input_data_path: str,
    metafits_filename: str,
    uvfits_filename: str,
    job_output_path: str,
    obs_id: int,
    oversampled: bool,
    birli_binary_path: str,
    birli_max_mem_gib: int,
    birli_timeout: int,
    birli_freq_res_hz: int,
    birli_int_time_res_sec: float,
    birli_edge_width_hz: int,
) -> bool:
    """Execute Birli to process visibility data.

    Args:
        input_data_path: Path to input visibility FITS files.
        metafits_filename: Path to the metafits file.
        uvfits_filename: Output path for UV FITS file.
        job_output_path: Output directory for Birli.
        obs_id: Observation ID.
        oversampled: Whether the observation is oversampled.
        birli_binary_path: Path to the Birli executable.
        birli_max_mem_gib: Maximum memory in GiB for Birli.
        birli_timeout: Timeout in seconds for Birli execution.
        birli_freq_res_hz: Frequency resolution in Hz.
        birli_int_time_res_sec: Integration time resolution in seconds.
        birli_edge_width_hz: Edge width in Hz to flag.

    Returns:
        True if execution succeeded, False otherwise.
    """
    birli_success: bool = False
    start_time = time.monotonic()
    stderr = ""

    cmdline = None
    exit_code = None
    stdout = None
    try:
        # Get only data files
        data_files = glob.glob(os.path.join(input_data_path, f"{obs_id}_*_*_*.fits"))

        data_file_arg = ""
        for data_file in data_files:
            if data_file.endswith(SOLUTIONS_FITS_SUFFIX):
                continue
            if data_file.endswith("metafits_ppds.fits"):
                continue
            data_file_arg += f"{data_file} "

        metafits = Metafits(metafits_filename)
        fine_chan_width_hz = metafits.chan_info.fine_chan_width_hz
        time_time_s = metafits.time_info.int_time_s

        # set default edge_width res from config
        if oversampled:
            # For oversampled obs we don't flag edges and we don't correct passband
            edge_width_hz = 0
        else:
            edge_width_hz = birli_edge_width_hz  # default
            edge_width_hz = np.max([fine_chan_width_hz, edge_width_hz])
            assert edge_width_hz >= fine_chan_width_hz, f"{edge_width_hz=} must be >= {fine_chan_width_hz=}"
            assert edge_width_hz % fine_chan_width_hz == 0, f"{edge_width_hz=} must multiple of {fine_chan_width_hz=}"

        # set minimum freq res from config
        min_freq_res = birli_freq_res_hz
        avg_arg = ""
        if fine_chan_width_hz < min_freq_res:
            avg_arg += f" --avg-freq-res={int(min_freq_res / 1e3)}"

        # set minimum time res from config
        min_time_res = birli_int_time_res_sec
        if time_time_s < min_time_res:
            avg_arg += f" --avg-time-res={min_time_res}"

        # Run birli
        cmdline = (
            f"{birli_binary_path}"
            f" --metafits {metafits_filename}"
            " --no-draw-progress"
            f" --uvfits-out={uvfits_filename}"
            f" --flag-edge-width={int(edge_width_hz / 1e3)}"
            f" --max-memory={birli_max_mem_gib}"
            f" {avg_arg} {data_file_arg}"
        )

        birli_popen_process = start_command(cmdline, -1, False, False)

        exit_code, stdout, stderr = check_popen_finished(
            birli_popen_process,
            birli_timeout,
        )

        elapsed = time.monotonic() - start_time

        if exit_code == 0:
            # Success!
            logger.info(f"{obs_id}: Birli run successful in {elapsed:.3f} seconds")
            birli_success = True

            # Success!
            # Write out a useful file of command line info
            readme_filename = os.path.join(job_output_path, f"{obs_id}_birli_readme.txt")
            write_readme_file(
                readme_filename,
                cmdline,
                exit_code,
                stdout,
                stderr,
            )
        else:
            logger.error(f"{obs_id}: Birli run FAILED: Exit code of {exit_code} in {elapsed:.3f} seconds: {stderr}")
    except Exception as birli_run_exception:
        elapsed = time.monotonic() - start_time
        logger.error(
            f"{obs_id}: birli run FAILED: Unhandled exception {birli_run_exception} in {elapsed:.3f} seconds: {stderr}"
        )

    if not birli_success:
        # If we are not shutting down,
        # Move the files to an error dir
        logger.info(
            f"{obs_id}: moving failed files to {job_output_path} for manual analysis and writing readme_error.txt"
        )

        # Move the processing dir
        shutil.move(input_data_path, job_output_path)

        # Write out a useful file of error and command line info
        readme_filename = os.path.join(job_output_path, "readme_error.txt")
        write_readme_file(
            readme_filename,
            cmdline,
            exit_code,
            stdout,
            stderr,
        )

    return birli_success


def estimate_birli_output_bytes(
    metafits_context: MetafitsContext,
    birli_freq_res_khz: int,
    birli_int_time_res_sec: float,
    bytes_per_r_and_i: int = 13,
) -> int:
    """Estimate the output file size from Birli processing.

    Args:
        metafits_context: Metafits context with observation parameters.
        birli_freq_res_khz: Frequency resolution in kHz.
        birli_int_time_res_sec: Integration time resolution in seconds.
        bytes_per_r_and_i: Bytes per visibility (default: 13).

    Returns:
        Estimated output size in bytes.
    """
    #
    # bytes_per_visibility comes from Birli
    #
    # baselines = tiles * (tiles + 1) / 2  (autocorrelations included)
    # timesteps = duration / birli_int_time_res_sec
    # coarse_chans = 24
    # fine_channels = 30.72 MHz / birli_freq_res_khz
    # pols = 4 (XX,XY,YX,YY)
    # bytes_per_visibility = 8+4+1
    # Total bytes = (timesteps * coarse_chans * fine_channels * baselines * pols * bytes_per_visibility )
    #
    # (Normally you would use values * bytes_per_value but Birli has more outputs than this)
    #
    # Total GB = bytes / 1000.^3
    baselines: int = metafits_context.num_baselines  # 144T (10440)
    timesteps: int = int(metafits_context.sched_duration_ms / (birli_int_time_res_sec * 1000.0))  # 60
    coarse_channels: int = metafits_context.num_metafits_coarse_chans
    fine_channels: int = int(
        metafits_context.coarse_chan_width_hz / (birli_freq_res_khz * 1000.0)
    )  # 1280000 / 80000 == 16
    pols: int = metafits_context.num_visibility_pols  # (XX,XY,YX,YY) # 4

    # Uncomment for debug
    # print(f"{timesteps}ts * {coarse_channels * fine_channels}ch"
    #       f" * {baselines}bl * {pols}pol * {bytes_per_r_and_i} bytes")

    return timesteps * coarse_channels * fine_channels * baselines * pols * bytes_per_r_and_i
