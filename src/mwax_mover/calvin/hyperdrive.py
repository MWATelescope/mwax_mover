"""Running hyperdrive, and reading its output convergence statistics.

run_hyperdrive() shells out to the hyperdrive binary via a Popen handle and
writes a readme (core.command.write_readme_file) recording the command and
outcome, mirroring calvin.birli.run_birli(). write_hyperdrive_stats() writes
get_convergence_summary()'s convergence summary for a just-produced solution
file. estimate_di_calibrate_peak_ram_bytes(), _uvfits_num_coarse_chans(),
and _max_hyperdrive_workers() support parallelising run_hyperdrive() across
picket-fence bands -- see docs/HYPERDRIVE_PARALLELISM.md Phases 2-4. See
calvin.hyperfits_solution/calvin.hyperfits_solution_group for
reading/flagging solutions (this module used to hold those two classes too
-- see docs/HYPERDRIVE_PARALLELISM.md Phase 1 for the split) and calvin.plots
for plotting.
"""

import logging
import os
import shutil
import time

import mwalib
import numpy as np
from astropy.io import fits

from mwax_mover.calvin.hyperfits_solution import HyperfitsSolution
from mwax_mover.constants import (
    EXT_UVFITS,
    F32_BYTES,
    HYPERDRIVE_FALLBACK_WORKERS,
    HYPERDRIVE_MEMORY_HEADROOM_FRACTION,
    JONES_F32_BYTES,
    JONES_F64_BYTES,
)
from mwax_mover.core.command import check_popen_finished, start_command, write_readme_file
from mwax_mover.core.env import available_memory_bytes

logger = logging.getLogger(__name__)


def _uvfits_num_coarse_chans(uvfits_filename: str, metafits_context: mwalib.MetafitsContext) -> int:
    """Determine how many MWA coarse channels a uvfits file covers.

    Reads only the primary HDU's header (no data): scans CTYPE2..CTYPEn for
    the FREQ axis (its index isn't fixed -- it depends on how many random-
    group parameter axes precede it, e.g. it's CTYPE4 for a typical
    Birli-produced file) and computes that axis's total bandwidth from
    NAXIS * |CDELT|. Divides by the coarse-channel width (a fixed ~1.28 MHz
    for MWA, from metafits_context.coarse_chan_width_hz) rather than the
    metafits' raw fine-channel count, since Birli's own --avg-freq-res can
    coarsen the fine-channel resolution independently of the metafits --
    dividing by fine-channel *count* would silently give the wrong answer
    whenever Birli's averaging differs from the metafits' native
    resolution; dividing actual bandwidth by the coarse-channel width
    (which averaging never changes) doesn't have that failure mode.

    Args:
        uvfits_filename: Path to a single uvfits file (one picket).
        metafits_context: Metafits context for the observation.

    Returns:
        Number of coarse channels this uvfits file covers, at least 1.

    Raises:
        StopIteration: If no axis has CTYPE == 'FREQ' (malformed uvfits).
    """
    header = fits.getheader(uvfits_filename)
    naxis = header["NAXIS"]
    freq_axis = next(i for i in range(2, naxis + 1) if header.get(f"CTYPE{i}") == "FREQ")
    bandwidth_hz = header[f"NAXIS{freq_axis}"] * abs(header[f"CDELT{freq_axis}"])
    return max(1, round(bandwidth_hz / metafits_context.coarse_chan_width_hz))


def estimate_di_calibrate_peak_ram_bytes(
    metafits_context: mwalib.MetafitsContext,
    edge_width_hz: int,
    num_sources: int,
    coarse_chan_start: int,
    coarse_chan_end: int,
) -> int:
    """Estimate mwa_hyperdrive di-calibrate's peak host RAM, derived almost
    entirely from an already-populated mwalib.MetafitsContext for a
    contiguous band of an MWA calibration observation.

    Based on reading MWATelescope/mwa_hyperdrive source, the dominant
    memory consumers during a di-calibrate run are:

    1. The three big visibility arrays (vis_data, vis_model, vis_weights),
       shaped (n_timesteps, n_chanblocks, n_cross_baselines) -- see
       `DiCalParams::get_cal_vis()` in mwa_hyperdrive: src/params/di_calibration.rs.
    2. The sky-model component flux-density arrays, shaped
       (n_chanblocks, n_components) per component type (points/gaussians/
       shapelets) -- see `ComponentList::new()` in
       mwa_hyperdrive:src/srclist/types/components/mod.rs.
    3. The transient per-timestep beam-response cache, shaped
       (n_unique_beam_freqs, n_components) -- see `get_beam_responses()`
       in mwa_hyperdrive: src/model/cpu.rs. Assumes one unique beam tile
       (no per-tile dipole flagging).
    4. The DI solutions array, shaped (n_unflagged_tiles, n_chanblocks),
       assuming one calibration timeblock (hyperdrive's `-t 0` default,
       i.e. all timesteps averaged into a single solution).

    Fine-channel and tile flagging replicate hyperdrive's own defaults
    for *raw* MWA correlator data (mwa_hyperdrive: src/io/read/raw/mod.rs):
    a tile is flagged if its X-pol input is flagged, and fine channels are
    flagged 80 kHz's worth at each coarse-channel edge, plus the centre
    channel for legacy (non-MWAX) data. This assumes default resolution (no
    --time-average/--freq-average) and no extra --tile-flags.

    n_points/n_gaussians/n_shapelets (sky-model *component* counts, not
    source counts) aren't in the metafits -- mwalib has no idea what sky
    model you're using. Read them off hyperdrive's own "Using N sources
    with a total of M components" log line (printed even with --dry-run)
    or as has been done in this function, just guess.

    Args:
        metafits_context: Metafits context to get metafits values.
        edge_width_hz: The amount that each coarse channel edge is flagged (in Hz).
        num_sources: Fed from the config file, how many sources should Calvin tell
            Hyperdrive to use for the skymodel.
        coarse_chan_start: Receiver channel number of first coarse channel in this contiguous band.
        coarse_chan_end: Receiver channel number of last coarse channel in this contiguous band.

    Returns:
        An int which is the max RAM consumption, in bytes, estimated based on the input
    """
    n_points: int = num_sources  # Most sources in the sky model are point sources anyway
    n_gaussians: int = num_sources // 4  # no good way to estimate this, so guess for now
    n_shapelets: int = num_sources // 8  # no good way to estimate this, so guess for now

    n_unflagged_tiles = sum(1 for rf in metafits_context.rf_inputs if rf.pol == mwalib.Pol.X and not rf.flagged)
    n_cross_baselines = n_unflagged_tiles * (n_unflagged_tiles - 1) // 2

    n_coarse_channels = (coarse_chan_end - coarse_chan_start) + 1
    num_fine_chans_per_coarse = metafits_context.num_corr_fine_chans_per_coarse

    num_flagged_per_edge = edge_width_hz // metafits_context.corr_fine_chan_width_hz
    num_flagged_per_coarse = 2 * num_flagged_per_edge
    n_chanblocks = n_coarse_channels * max(num_fine_chans_per_coarse - num_flagged_per_coarse, 0)

    n_timesteps = metafits_context.num_metafits_timesteps

    # The FEE beam snaps to its own ~1.28 MHz-spaced frequency grid;
    # empirically, about 2 unique beam frequencies per coarse channel
    # (fine channels near a coarse-channel boundary often snap to the
    # neighbouring tabulated frequency rather than their own).
    n_unique_beam_freqs = 2 * n_coarse_channels

    n_components_total = n_points + n_gaussians + n_shapelets
    n_components_max = max(n_points, n_gaussians, n_shapelets)

    vis_arrays = n_timesteps * n_chanblocks * n_cross_baselines * (2 * JONES_F32_BYTES + F32_BYTES)
    sky_model_components = n_chanblocks * n_components_total * JONES_F64_BYTES
    beam_response_cache = n_unique_beam_freqs * n_components_max * JONES_F64_BYTES
    solutions_array = n_unflagged_tiles * n_chanblocks * JONES_F64_BYTES

    return vis_arrays + sky_model_components + beam_response_cache + solutions_array


def _max_hyperdrive_workers(per_run_bytes: list[int]) -> int:
    """Decide how many hyperdrive di-calibrate runs may run concurrently.

    Bounded by live available memory only (no CPU cap -- each hyperdrive
    process may itself use several threads internally, so capping by CPU
    count here could leave memory idle for no benefit). Mirrors
    calvin.plots.gains._max_render_workers's shape, but sized against
    available_memory_bytes() with a fixed headroom fraction rather than
    that function's page-count/CPU-count/memory three-way min.

    Args:
        per_run_bytes: Estimated peak RAM for each picket's hyperdrive run
            (see estimate_di_calibrate_peak_ram_bytes), one entry per
            picket about to run.

    Returns:
        Worker count, always at least 1.
    """
    available = available_memory_bytes()

    if available is None:
        logger.debug(
            f"Could not determine available memory; capping concurrent hyperdrive runs at "
            f"{HYPERDRIVE_FALLBACK_WORKERS}."
        )
        memory_cap = HYPERDRIVE_FALLBACK_WORKERS
    else:
        budget = int(available * (1 - HYPERDRIVE_MEMORY_HEADROOM_FRACTION))
        worst_case = max(per_run_bytes)
        memory_cap = max(1, budget // worst_case)

    workers = max(1, min(len(per_run_bytes), memory_cap))
    logger.info(f"Running {len(per_run_bytes)} hyperdrive run(s) with {workers} concurrent worker(s).")
    return workers


def run_hyperdrive(
    input_uvfits_files: list[str],
    metafits_filename: str,
    job_output_path: str,
    obs_id: int,
    hyperdrive_binary_path: str,
    source_list_filename: str,
    source_list_type: str,
    num_sources: int,
    hyperdrive_timeout: int,
    hyperdrive_extra_args: str,
) -> tuple[bool, str]:
    """Run hyperdrive calibration on UV FITS files.

    Args:
        input_uvfits_files: List of input UV FITS files, one per contiguous
            coarse-channel band (so 1 for a normal observation, up to 24 for a
            picket fence).
        metafits_filename: Path to the metafits file.
        job_output_path: Output directory for hyperdrive.
        obs_id: Observation ID.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        source_list_filename: Path to the source list file.
        source_list_type: Type of source list (e.g., 'gleam').
        num_sources: Number of sources in the list.
        hyperdrive_timeout: Timeout in seconds for hyperdrive execution.
        hyperdrive_extra_args: Any additional command line args provided from the calvin_processor config file.

    Returns:
        tuple[True, calibration_command] if all runs succeeded, [False, calibration_command] if any failed.
    """
    logger.info(
        f"{obs_id}: {len(input_uvfits_files)} contiguous bands detected."
        f" Running hyperdrive {len(input_uvfits_files)} times...."
    )

    hyperdrive_runs_success: int = 0
    stdout = ""
    stderr = ""
    elapsed = -1
    cmdline = ""
    exit_code = 0
    # Initialised here so it is always bound, even if input_uvfits_files is
    # empty, which would otherwise be an UnboundLocalError at the return sites.
    calibration_command = ""

    for hyperdrive_run, uvfits_file in enumerate(input_uvfits_files):
        obsid_and_band = os.path.basename(uvfits_file.replace(EXT_UVFITS, ""))

        # Outside the try block so it is always bound before the exception
        # handler below computes `elapsed` from it.
        start_time = time.monotonic()

        try:
            hyperdrive_solution_full_filename = os.path.join(job_output_path, f"{obsid_and_band}_solutions.fits")
            bin_solution_filename = f"{obsid_and_band}_solutions.bin"
            bin_solution_full_filename = os.path.join(job_output_path, bin_solution_filename)

            calibration_command = (
                f"--num-sources {num_sources}"
                f" --source-list {source_list_filename}"
                f" --source-list-type {source_list_type}"
                f" {hyperdrive_extra_args}"
            )
            cmdline = (
                f"{hyperdrive_binary_path} di-calibrate"
                f" --no-progress-bars {calibration_command}"
                f" --data {uvfits_file} {metafits_filename} "
                f" --outputs {hyperdrive_solution_full_filename} {bin_solution_full_filename}"
            )

            logger.info(f"{obs_id}: Running hyperdrive on {uvfits_file}...")
            hyperdrive_popen_process = start_command(cmdline, -1, False, False)

            exit_code, stdout, stderr = check_popen_finished(
                hyperdrive_popen_process,
                hyperdrive_timeout,
            )

            elapsed = time.monotonic() - start_time

            if exit_code == 0:
                logger.info(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(input_uvfits_files)} successful"
                    f" in {elapsed:.3f} seconds"
                )

                # Joined with job_output_path so the readme lands in the job's
                # output directory rather than the current working directory.
                readme_filename = os.path.join(job_output_path, f"{obsid_and_band}_hyperdrive_readme.txt")
                write_readme_file(
                    readme_filename,
                    cmdline,
                    exit_code,
                    stdout,
                    stderr,
                )

                hyperdrive_runs_success += 1
            else:
                logger.error(
                    f"{obs_id}: hyperdrive run"
                    f" {hyperdrive_run + 1}/{len(input_uvfits_files)} FAILED:"
                    f" Exit code of {exit_code} in"
                    f" {elapsed:.3f} seconds. StdErr: {stderr}"
                )
                break

        except Exception as hyperdrive_run_exception:
            elapsed = time.monotonic() - start_time
            logger.error(
                f"{obs_id}: hyperdrive run"
                f" {hyperdrive_run + 1}/{len(input_uvfits_files)} FAILED:"
                " Unhandled exception"
                f" {hyperdrive_run_exception} in"
                f" {elapsed:.3f} seconds. StdErr: {stderr}"
            )
            break

    if hyperdrive_runs_success != len(input_uvfits_files):
        logger.info(
            f"{obs_id}: moving failed files to {job_output_path} for manual analysis and writing readme_error.txt"
        )

        for uvfits_file in input_uvfits_files:
            shutil.move(uvfits_file, job_output_path)

        readme_filename = os.path.join(job_output_path, "readme_error.txt")
        write_readme_file(
            readme_filename,
            cmdline,
            exit_code,
            stdout,
            stderr,
        )
        return False, calibration_command

    return True, calibration_command


def write_hyperdrive_stats(
    obs_id: int,
    stats_fd,
    hyperdrive_solution_filename: str,
) -> tuple[bool, str]:
    """Write convergence statistics. (Append to existing stats file if it exists.)

    Args:
        obs_id: Observation ID.
        stats_fd: File descriptor for the statistics file.
        hyperdrive_solution_filename: Path to the hyperdrive solution FITS file.

    Returns:
        A tuple of (success: bool, error_message: str).
    """
    logger.info(f"{obs_id} Writing convergence stats for {hyperdrive_solution_filename}.")
    try:
        conv_summary_list = get_convergence_summary(hyperdrive_solution_filename)

        stats_fd.writelines(f"{row[0]}: {row[1]}\n" for row in conv_summary_list)
        stats_fd.write("\n")

        logger.info(f"{obs_id} Finished running convergence stats for {hyperdrive_solution_filename}.")
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    return True, ""


def get_convergence_summary(solutions_fits_file: str):
    """Get a convergence summary from a solution file.

    Args:
        solutions_fits_file: Path to the solutions FITS file.

    Returns:
        List of tuples with convergence statistics.
    """
    soln = HyperfitsSolution(solutions_fits_file)
    results = soln.results
    converged_channel_indices = np.where(~np.isnan(results))
    summary = []
    summary.append(("Converged channel indices", converged_channel_indices))
    summary.append(("Total number of channels", len(results)))
    summary.append(
        (
            "Number of converged channels",
            f"{len(converged_channel_indices[0])}",
        )
    )
    summary.append(
        (
            "Fraction of converged channels",
            (f" {len(converged_channel_indices[0]) / len(results) * 100}%"),
        )
    )
    summary.append(
        (
            "Average channel convergence",
            f" {np.mean(results[converged_channel_indices])}",
        )
    )
    return summary
