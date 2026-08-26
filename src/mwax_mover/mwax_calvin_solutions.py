"""Post-processing of hyperdrive calibration solutions for the Calvin pipeline.

Provides process_solutions(), which loads hyperfits solution files and metafits,
determines a reference antenna, runs the full flagging pipeline (tile flags,
whole-Jones-NaN enforcement, gain-magnitude cutoff, amplitude-outlier
flagging, mostly-bad-tile promotion, then report-only phase-outlier detection
last -- see HyperfitsSolutionGroup.run_flagging_pipeline), commits the result
to disk, fits final phases and gains, and inserts the resulting calibration fit
and solution records into the MWA metadata database.
"""

import logging
import os
import traceback
from concurrent.futures import ThreadPoolExecutor

import numpy as np

from mwax_mover.mwax_calvin_plots import (
    generate_hyperdrive_plots_for_files,
    plot_outlier_gains,
    write_hyperdrive_stats,
    write_stats_and_debug_plots,
)
from mwax_mover.mwax_calvin_utils import (
    GainFitInfo,
    Metafits,
    PhaseFitInfo,
    get_sorted_solution_files,
    pad_gain_fit_info,
    write_readme_file,
)
from mwax_mover.mwax_db import (
    MWAXDBHandler,
    insert_calibration_fits_row,
    insert_calibration_solutions_row,
)
from mwax_mover.mwax_hyperdrive_solutions import (
    HyperfitsSolution,
    HyperfitsSolutionGroup,
)
from mwax_mover.version import get_mwax_mover_version_string

logger = logging.getLogger(__name__)


def process_solutions(
    db_handler_object: MWAXDBHandler,
    obs_id: int,
    input_data_path: str,
    output_data_path: str,
    phase_fit_niter: int,
    source_list: str,
    num_sources: int,
    calibration_command: str,
    gain_max_cutoff: float | None,
    gain_outlier_poly_degree: int,
    gain_outlier_mad_residual_threshold: float,
    gain_outlier_modify_gains: bool | None,
    gain_outlier_plot_n_tiles_per_page: int,
    tile_bad_channel_fraction: float,
    hyperdrive_binary_path: str,
    phase_outlier_nstd: float = 3.0,
) -> tuple[bool, str, int | None]:
    """Process hyperdrive calibration solutions and insert into the database.

    Loads hyperfits solution files and metafits, determines a reference
    antenna, runs the full flagging pipeline, generates before/after plots
    and a before/after per-tile stats text file, commits the final state
    to disk, fits final phases and gains, and inserts the resulting
    calibration fit and solution records into the MWA metadata database.

    Args:
        db_handler_object: Database handler for inserting calibration data.
        obs_id: The observation ID.
        input_data_path: Path to input metafits files.
        output_data_path: Path to output solution files and results.
        phase_fit_niter: Number of iterations for phase fitting.
        source_list: Source list identifier used for the calibration.
        num_sources: Number of sources in the calibration.
        calibration_command: Full hyperdrive command line used to generate the calibration.
        gain_max_cutoff: Absolute gain-amplitude ceiling (see
            HyperfitsSolutionGroup.flag_gain_max_cutoff) -- any (tile,
            chanblock) entry whose gx or gy amplitude exceeds this is
            flagged bad, run early in the flagging pipeline (before
            phase-outlier detection and amplitude-outlier flagging).
            None disables this check. Reinstated after a period where it
            was accepted here only for calibration_fits table provenance
            and never actually applied (via the now-removed
            clip_hyperdrive_solution_gains); it's real again.
        gain_outlier_poly_degree: Degree of polynomial for gain-amplitude
            outlier detection (see HyperfitsSolutionGroup.flag_amplitude_outliers).
        gain_outlier_mad_residual_threshold: MAD residual threshold for
            gain-amplitude outlier detection.
        gain_outlier_modify_gains: Kept for calibration_fits table provenance
            only -- unlike gain_max_cutoff, this one's toggle
            (compute outlier flags but only write them to disk if True) no
            longer exists: the new flagging pipeline always modifies
            self.jones in memory and commit() always writes it. Pass
            whatever the caller's config currently has, or True, since it
            no longer changes any actual behaviour here.
        gain_outlier_plot_n_tiles_per_page: Number of tiles per page in the
            paginated before/after amplitude-outlier plots.
        tile_bad_channel_fraction: Fraction (0-1) of a tile's
            chanblocks that must already be flagged bad, combined across
            all per-channel reasons, before the whole tile is promoted to
            fully flagged (see HyperfitsSolutionGroup.flag_mostly_bad_tiles).
        hyperdrive_binary_path: Path to the hyperdrive binary, used for its
            own before/after solutions-plot generation.
        phase_outlier_nstd: Number of (MAD-derived) standard-deviation-
            equivalents beyond the population's robust median before a
            tile's phase fit is reported as an outlier (see
            HyperfitsSolutionGroup.detect_phase_outliers). Purely
            advisory -- does not affect flagging.

    Returns:
        A tuple containing:
        - success (bool): True if processing completed successfully.
        - error_message (str): Error message if unsuccessful, empty string otherwise.
        - fit_id (int|None): The calibration fit ID if successful, None otherwise.
    """
    conn = None

    try:
        metafits_file = os.path.join(input_data_path, f"{obs_id}_metafits.fits")

        logger.debug(f"{input_data_path} - {metafits_file=}")

        fits_solution_files = get_sorted_solution_files(output_data_path, obs_id, "fits")

        logger.debug(f"{output_data_path} - reading {fits_solution_files=}")

        metafits = Metafits(metafits_file)
        soln_group = HyperfitsSolutionGroup(metafits, [HyperfitsSolution(f) for f in fits_solution_files])
        soln_group.load()

        # get tiles
        tiles = soln_group.metafits_tiles_df
        logger.debug(f"metafits tiles:\n{tiles.to_string(max_rows=999)}")

        # Early-exit check, now against combined_tile_flags (metafits + TILES
        # HDU + BASELINES-inferred) rather than metafits alone, matching the
        # more complete check refant/apply_tile_flags use.
        if not (~soln_group.combined_tile_flags).any():
            # Even though this is a "failure" we want to return True
            # so we can release the obs if it is realtime- i.e. there's
            # nothing more we can do
            return True, "No unflagged tiles found", None

        refant = soln_group.refant
        logger.debug(f"{refant['name']=} ({refant['id']})")

        # get channel info
        chaninfo = soln_group.metafits_chan_info
        logger.debug(f"{chaninfo=}")
        all_coarse_chan_ranges = chaninfo.coarse_chan_ranges

        if len(fits_solution_files) != len(all_coarse_chan_ranges):
            raise RuntimeError(
                f"number of solution files ({len(fits_solution_files)})"
                f" does not match number of coarse chan ranges in metafits {len(all_coarse_chan_ranges)}"
            )

        chanblocks_per_coarse = soln_group.chanblocks_per_coarse

        # Build the full sorted list of coarse channel indices from the metafits.
        # This is used below to NaN-pad gains for any missing channels.
        all_metafits_coarse_chans = np.sort(np.concatenate(chaninfo.coarse_chan_ranges))
        solution_coarse_chans = soln_group.all_solution_coarse_chan_indices
        n_metafits_coarse = len(all_metafits_coarse_chans)

        if len(solution_coarse_chans) < n_metafits_coarse:
            logger.warning(
                f"{obs_id}: Only {len(solution_coarse_chans)} of {n_metafits_coarse} "
                f"metafits coarse channels have solutions. "
                f"Missing channels will be padded with NaN in the calibration solutions."
            )

        logger.debug(f"{chanblocks_per_coarse=} fine channels per coarse channel")

        # "Before" plots: hyperdrive's own binary-generated amp/phase plots,
        # against the still-pristine on-disk files -- nothing has been
        # touched yet. Written to separate "before" filenames so the
        # "after" run below (same filenames, since hyperdrive derives them
        # from the input file) doesn't overwrite these. (This only touches
        # the on-disk files via a read-only hyperdrive invocation; it's
        # independent of the in-memory apply_tile_flags() below regardless
        # of ordering, since nothing gets written to disk until commit().)
        # Run concurrently: each is an external hyperdrive process, and a
        # picket fence has one solution file per coarse channel (24 serial
        # launches here and another 24 below, versus 2 for a contiguous obs).
        for failed_file, plots_error in generate_hyperdrive_plots_for_files(
            obs_id, fits_solution_files, hyperdrive_binary_path, metafits_file, output_data_path, before=True
        ):
            logger.warning(f"{obs_id}: 'before' hyperdrive plots failed for {failed_file}: {plots_error}")

        # apply_tile_flags() runs first (cheap, structural) so the "before"
        # snapshot captured right after it reflects the pre-existing
        # metafits/TILES-HDU/BASELINES-HDU flags -- i.e. "before OUR OWN
        # outlier detection", not literally "before anything at all"
        # (a metafits-flagged tile isn't meaningfully "pristine" anyway,
        # since its data was never trustworthy in the first place).
        #
        # Runs the full flagging pipeline (apply_tile_flags ->
        # enforce_whole_jones_nan -> flag_gain_max_cutoff ->
        # flag_amplitude_outliers -> flag_mostly_bad_tiles ->
        # detect_phase_outliers (report-only, runs last -- see its
        # docstring for why)) and
        # captures the "before" snapshot (soln_group.before_jones etc.)
        # along the way -- shared with cal_utils via
        # HyperfitsSolutionGroup.run_flagging_pipeline() rather than each
        # duplicating the call sequence.
        soln_group.run_flagging_pipeline(
            refant["name"],
            phase_fit_niter,
            poly_degree=gain_outlier_poly_degree,
            mad_residual_threshold=gain_outlier_mad_residual_threshold,
            phase_outlier_nstd=phase_outlier_nstd,
            tile_bad_channel_fraction=tile_bad_channel_fraction,
            gain_max_cutoff=gain_max_cutoff,
        )
        assert soln_group.before_jones is not None

        # "After" plots: calvin's own outlier plots, from soln_group's
        # final in-memory state -- no staleness, since nothing downstream
        # changes the data further.
        #
        # ONE paginated set for the whole observation, with every coarse
        # channel stitched onto a single compressed x-axis, rather than a
        # set per solution file. For a picket fence that is 5 pages instead
        # of 120 and one process pool instead of 24 -- measured as the bulk
        # of the runtime gap against a contiguous observation. Detection is
        # still per file; only the plot is stitched. Filenames therefore no
        # longer carry a _ch<N> component (generate_plot_index_file matches
        # on the "gain_outliers_tiles" substring, so the index still
        # categorises these correctly).
        plot_outlier_gains(
            soln_group,
            n_tiles=gain_outlier_plot_n_tiles_per_page,
            output_path=os.path.join(output_data_path, f"{obs_id}_gain_outliers_tiles.png"),
            pristine_jones=soln_group.before_jones,
            solution_file_will_be_modified=True,
        )

        soln_group.commit(metafits.mwalib_context)

        # "After" plots: hyperdrive's own binary-generated amp/phase plots,
        # against the now-committed files.
        for failed_file, plots_error in generate_hyperdrive_plots_for_files(
            obs_id, fits_solution_files, hyperdrive_binary_path, metafits_file, output_data_path, before=False
        ):
            logger.warning(f"{obs_id}: hyperdrive plots failed for {failed_file}: {plots_error}")

        # Single combined stats file: before/after per-tile stats first,
        # hyperdrive convergence stats below -- written by
        # write_stats_and_debug_plots() (shared with cal_utils) and
        # write_hyperdrive_stats() into the same fd, in that order.
        #
        # Final phase/gain fit, computed on the fully-flagged, committed
        # data -- this is what gets reported to the DB. write_stats_and_
        # debug_plots() computes the final phase fit, writes the
        # before/after per-tile stats section, and generates the phase-fit
        # debug plots -- shared with cal_utils rather than each duplicating
        # this reporting.
        stats_path = os.path.join(output_data_path, f"{obs_id}_stats.txt")
        with open(stats_path, "w", encoding="utf-8") as stats_fd:
            with ThreadPoolExecutor(max_workers=1) as fitting_executor:
                gain_future = fitting_executor.submit(soln_group.process_gain_fits_for_db, refant["name"])
                phase_fits = write_stats_and_debug_plots(
                    soln_group,
                    refant["name"],
                    phase_fit_niter,
                    output_data_path,
                    obs_id,
                    stats_fd,
                    phase_outlier_nstd=phase_outlier_nstd,
                )
                gain_fits = gain_future.result()

            for f in fits_solution_files:
                stats_success, stats_error = write_hyperdrive_stats(obs_id, stats_fd, f)
                if not stats_success:
                    logger.warning(f"{obs_id}: hyperdrive stats failed for {f}: {stats_error}")

        soln_tile_ids = tiles["id"].to_numpy()

        # get a database connection, unless we are using dummy connection (for testing)
        with db_handler_object.pool.connection() as conn:
            # Start a transaction
            with conn.transaction():
                # Create a cursor
                transaction_cursor = conn.cursor()

                (success, fit_id) = insert_calibration_fits_row(
                    db_handler_object,
                    transaction_cursor,
                    obs_id=obs_id,
                    code_version=get_mwax_mover_version_string(),
                    creator="calvin",
                    fit_niter=phase_fit_niter,
                    fit_limit=None,
                    source_list=source_list,
                    num_sources=num_sources,
                    calibration_command=calibration_command,
                    gain_max_cutoff=gain_max_cutoff,
                    gain_outlier_poly_degree=gain_outlier_poly_degree,
                    gain_outlier_mad_residual_threshold=gain_outlier_mad_residual_threshold,
                    gain_outlier_modify_gains=gain_outlier_modify_gains,
                    tile_bad_channel_fraction=tile_bad_channel_fraction,
                    phase_outlier_nstd_threshold=phase_outlier_nstd,
                )

                if fit_id is None or not success:
                    logger.error("failed to insert calibration fit")
                    # This will trigger a rollback of the calibration_fit row
                    raise Exception("failed to insert calibration fit")

                # Pre-index both DataFrames by (tile_id, pol) so each per-tile
                # lookup is O(1) instead of O(n) boolean-mask scan.
                gain_indexed = gain_fits.set_index(["tile_id", "pol"])
                phase_indexed = phase_fits.set_index(["tile_id", "pol"])

                for tile_id in soln_tile_ids:
                    some_fits = False

                    try:
                        x_gains = gain_indexed.loc[(tile_id, "XX")]
                        if len(x_gains.gains) < n_metafits_coarse:
                            x_gains = pad_gain_fit_info(
                                x_gains,
                                solution_coarse_chans,
                                all_metafits_coarse_chans,
                            )
                        some_fits = True
                    except KeyError:
                        x_gains = GainFitInfo.nan(n_metafits_coarse)

                    try:
                        y_gains = gain_indexed.loc[(tile_id, "YY")]
                        if len(y_gains.gains) < n_metafits_coarse:
                            y_gains = pad_gain_fit_info(
                                y_gains,
                                solution_coarse_chans,
                                all_metafits_coarse_chans,
                            )
                        some_fits = True
                    except KeyError:
                        y_gains = GainFitInfo.nan(n_metafits_coarse)

                    try:
                        x_phase = phase_indexed.loc[(tile_id, "XX")]
                        some_fits = True
                    except KeyError:
                        x_phase = PhaseFitInfo.nan()

                    try:
                        y_phase = phase_indexed.loc[(tile_id, "YY")]
                        some_fits = True
                    except KeyError:
                        y_phase = PhaseFitInfo.nan()

                    if not some_fits:
                        # We could `continue` here to avoid inserting an all-NaN row, but
                        # we preserve the existing behaviour of inserting it for now.
                        logger.warning(
                            f"No phase or gain fits found for tile_id={tile_id} in obs_id={obs_id}. "
                            "Inserting all-NaN calibration solution row."
                        )

                    success = insert_calibration_solutions_row(
                        db_handler_object,
                        transaction_cursor,
                        int(fit_id),
                        int(obs_id),
                        int(tile_id),
                        -1 * x_phase.length,  # legacy calibration pipeline used inverse convention
                        x_phase.intercept,
                        x_gains.gains,
                        -1 * y_phase.length,  # legacy calibration pipeline used inverse convention
                        y_phase.intercept,
                        y_gains.gains,
                        x_gains.pol1,
                        y_gains.pol1,
                        x_phase.sigma_resid,
                        x_phase.chi2dof,
                        x_phase.quality,
                        y_phase.sigma_resid,
                        y_phase.chi2dof,
                        y_phase.quality,
                        x_gains.quality,
                        y_gains.quality,
                        x_gains.sigma_resid,
                        y_gains.sigma_resid,
                        x_gains.pol0,
                        y_gains.pol0,
                    )

                    if not success:
                        logger.error(f"failed to insert calibration solution for tile {tile_id}")
                        # This will trigger a rollback of the calibration_fit row and any
                        # calibration_solutions child rows
                        raise Exception(f"failed to insert calibration solution for tile {tile_id}")

        return True, "", int(fit_id)

    except Exception:
        error_text = f"Error in process_solutions():\n{traceback.format_exc()}"
        logger.exception(error_text)

        # Write an error readme
        write_readme_file(
            os.path.join(output_data_path, "readme_error.txt"),
            "process_solutions()",
            -999,
            "",
            error_text,
        )

        return False, error_text.replace("\n", ""), None
