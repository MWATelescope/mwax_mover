"""Generating solution plots via the hyperdrive binary itself.

generate_hyperdrive_plots() runs hyperdrive's own `solutions-plot`
subcommand for a single solution file; generate_hyperdrive_plots_for_files()
runs it across several files concurrently. (Reading and writing
convergence stats for an already-produced solution file is a related but
separate concern -- see calvin.hyperdrive.write_hyperdrive_stats.)
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from mwax_mover.core.command import run_command_ext

logger = logging.getLogger(__name__)


def generate_hyperdrive_plots(
    obs_id: int,
    hyperdrive_solution_filename: str,
    hyperdrive_binary_path: str,
    metafits_filename: str,
    output_dir: str,
    before: bool,
    max_amp: int | None = None,
) -> tuple[bool, str]:
    """Generate solution plots via the hyperdrive binary itself.

    This is the single implementation. A second, near-identical copy used to
    live in mwax_calvin_utils.py -- it accepted max_amp but not before, and it
    discarded run_command_ext's return code, so a failed hyperdrive run was
    reported as a success. Callers of that copy now come here instead.

    Args:
        obs_id: Observation ID.
        hyperdrive_solution_filename: Path to the hyperdrive solution FITS file.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        metafits_filename: Path to the metafits file.
        output_dir: path to where we write the plots
        before: True if this run is BEFORE Calvin flags outliers, False if
            after. Only used to generate the correct filenames: a BEFORE run's
            amp/phase plots are renamed with an "_original" suffix so the AFTER
            run (which hyperdrive names identically) does not overwrite them.
        max_amp: Optionally pass a max value for Hyperdrive to clip to when
            plotting amps. None means let Hyperdrive figure it out.

    Returns:
        A tuple of (success: bool, error_message: str).
    """
    logger.info(
        f"{obs_id} generating {'original unmodified' if before else 'after flagging'}"
        f" hyperdrive plots for {hyperdrive_solution_filename}..."
    )

    try:
        hyp_soln_plot_args = f" --output-directory {output_dir}"

        if max_amp is not None:
            hyp_soln_plot_args += f" --max-amp {max_amp}"

        cmd = (
            f"{hyperdrive_binary_path} solutions-plot {hyp_soln_plot_args} "
            f"-m"
            f" {metafits_filename} {hyperdrive_solution_filename}"
        )

        success, output = run_command_ext(cmd, -1, timeout=60, use_shell=False)

        if not success:
            logger.warning(f"{obs_id} hyperdrive solutions-plot failed for {hyperdrive_solution_filename}: {output}")
            return False, output

        if before:
            # Rename this call's own output so the AFTER run (which hyperdrive
            # names identically) does not overwrite it.
            #
            # Derived from the input filename rather than globbed from the
            # directory. A directory-wide glob picked up every other solution
            # file's plots too, which was only safe because it ran serially and
            # an already-renamed file stops matching -- it made this function
            # unsafe to call concurrently, and cost a full directory scan per
            # file (O(N^2) for a picket fence). hyperdrive derives its plot
            # names from the solution file's stem, so the two files this call
            # produced can be named exactly.
            # Scoped by glob on the stem rather than by assuming hyperdrive's
            # exact plot suffixes ("_amps"/"_phases"): if a hyperdrive version
            # emits a different or additional suffix, this still renames it,
            # whereas hardcoding the two names would silently leave the new one
            # to be overwritten by the AFTER run.
            directory = Path(output_dir)
            soln_stem = Path(hyperdrive_solution_filename).stem

            renamed = 0
            for produced in list(directory.glob(f"{soln_stem}_*.png")):
                if produced.stem.endswith("_original"):
                    continue
                produced.rename(produced.with_name(f"{produced.stem}_original{produced.suffix}"))
                renamed += 1

            if renamed == 0:
                logger.warning(
                    f"{obs_id} hyperdrive reported success but produced no plots matching"
                    f" {soln_stem}_*.png in {output_dir}. The 'after' run may overwrite"
                    " whatever it did produce."
                )

        logger.info(f"{obs_id} Finished running hyperdrive plots on {hyperdrive_solution_filename}.")
    except Exception as catch_all_exception:
        return False, str(catch_all_exception)

    return True, ""


def generate_hyperdrive_plots_for_files(
    obs_id: int,
    solution_filenames: list[str],
    hyperdrive_binary_path: str,
    metafits_filename: str,
    output_dir: str,
    before: bool,
    max_amp: int | None = None,
    max_workers: int | None = None,
) -> list[tuple[str, str]]:
    """Run generate_hyperdrive_plots for every solution file, concurrently.

    Each call is an external ``hyperdrive solutions-plot`` process, so these are
    IO/subprocess bound and a thread pool parallelises them fine -- the GIL is
    released while waiting on the child. This matters for a picket-fence
    observation: the pipeline invokes this once per solution file for the
    "before" pass and again for the "after" pass, so 24 files meant 48 serial
    process launches versus 2 for a contiguous observation.

    Safe to run concurrently only because generate_hyperdrive_plots's "before"
    rename is scoped to its own input file's stem. It previously globbed the
    whole output directory, which would have had concurrent calls renaming each
    other's files.

    Failures are collected and returned rather than raised: these plots are a
    diagnostic aid, and one file failing should not abort the rest or fail an
    otherwise good calibration.

    Args:
        obs_id: Observation ID.
        solution_filenames: Every hyperdrive solution FITS file to plot.
        hyperdrive_binary_path: Path to the hyperdrive executable.
        metafits_filename: Path to the metafits file.
        output_dir: Where to write the plots.
        before: See generate_hyperdrive_plots.
        max_amp: See generate_hyperdrive_plots.
        max_workers: Concurrent hyperdrive processes. Defaults to
            min(len(solution_filenames), os.cpu_count()), so a contiguous
            observation still runs exactly one process.

    Returns:
        A list of (solution_filename, error_message) for the files that failed,
        empty if every file succeeded.
    """
    if not solution_filenames:
        return []

    workers = max_workers if max_workers is not None else min(len(solution_filenames), os.cpu_count() or 1)

    failures: list[tuple[str, str]] = []

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {
            executor.submit(
                generate_hyperdrive_plots,
                obs_id,
                f,
                hyperdrive_binary_path,
                metafits_filename,
                output_dir,
                before,
                max_amp,
            ): f
            for f in solution_filenames
        }

        for future in as_completed(futures):
            filename = futures[future]
            try:
                success, error = future.result()
            except Exception as exc:  # noqa: BLE001 -- reported, not raised
                failures.append((filename, str(exc)))
                continue
            if not success:
                failures.append((filename, error))

    return failures
