"""Post-processing of hyperdrive calibration solutions for the Calvin pipeline.

Provides process_solutions(), which loads hyperfits solution files and metafits,
determines a reference antenna, fits phases and gains per coarse channel, and
inserts the resulting calibration fit and solution records into the MWA metadata
database.
"""

import re

import glob
import logging
import os
import traceback
from typing import Optional

import numpy as np

from mwax_mover.mwax_db import (
    MWAXDBHandler,
    insert_calibration_fits_row,
    insert_calibration_solutions_row,
)
from mwax_mover.mwax_calvin_utils import (
    GainFitInfo,
    Metafits,
    HyperfitsSolution,
    HyperfitsSolutionGroup,
    PhaseFitInfo,
    debug_phase_fits,
    process_phase_fits,
    process_gain_fits,
    write_readme_file,
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
    produce_debug_plots: bool,
) -> tuple[bool, str, Optional[int]]:
    """Process hyperdrive calibration solutions and insert into the database.

    Loads hyperfits solution files and metafits, determines a reference antenna,
    fits phases and gains per coarse channel, and inserts the resulting calibration
    fit and solution records into the MWA metadata database.

    Args:
        db_handler_object: Database handler for inserting calibration data.
        obs_id: The observation ID.
        input_data_path: Path to input metafits files.
        output_data_path: Path to output solution files and results.
        phase_fit_niter: Number of iterations for phase fitting.
        source_list: Source list identifier used for the calibration.
        num_sources: Number of sources in the calibration.
        produce_debug_plots: Whether to produce debug plots.

    Returns:
        A tuple containing:
        - success (bool): True if processing completed successfully.
        - error_message (str): Error message if unsuccessful, empty string otherwise.
        - fit_id (int|None): The calibration fit ID if successful, None otherwise.
    """
    conn = None

    try:
        metafits_files = glob.glob(os.path.join(input_data_path, "*_metafits.fits"))

        logger.debug(f"{input_data_path} - {metafits_files=}")

        def _solution_sort_key(path: str) -> tuple[int, str]:
            """Sort solution files numerically by channel number if present.

            Handles filenames like:
              obsid_solutions.fits          -> channel 0 (sorts first)
              obsid_ch95_solutions.fits     -> channel 95
              obsid_ch100-112_solutions.fits -> channel 100 (uses range start)
            Falls back to lexicographic sort on the full path for any
            unrecognised format.
            """
            fname = os.path.basename(path)
            # Flavour 2: single channel  e.g. obsid_ch95_solutions.fits
            m = re.search(r"_ch(\d+)_solutions\.fits$", fname)
            if m:
                return (int(m.group(1)), path)
            # Flavour 3: channel range  e.g. obsid_ch100-112_solutions.fits
            m = re.search(r"_ch(\d+)-\d+_solutions\.fits$", fname)
            if m:
                return (int(m.group(1)), path)
            # Flavour 1: all-channel file  e.g. obsid_solutions.fits — sorts first
            return (0, path)

        fits_solution_files = sorted(
            glob.glob(os.path.join(output_data_path, "*_solutions.fits")),
            key=_solution_sort_key,
        )

        logger.debug(f"{output_data_path} - uploading {fits_solution_files=}")

        soln_group = HyperfitsSolutionGroup(
            [Metafits(f) for f in metafits_files], [HyperfitsSolution(f) for f in fits_solution_files]
        )

        # get tiles
        tiles = soln_group.metafits_tiles_df
        logger.debug(f"metafits tiles:\n{tiles.to_string(max_rows=999)}")

        # determine refant
        unflagged_tiles = tiles[tiles.flag == 0]

        if not len(unflagged_tiles):
            # Even though this is a "failure" we want to return True
            # so we can release the obs if it is realtime- i.e. there's
            # nothing more we can do
            return True, "No unflagged tiles found", None

        refant = unflagged_tiles.sort_values(by=["id"]).iloc[0]
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

        # all_chanblocks_hz = soln_group.all_chanblocks_hz
        all_chanblocks_hz = np.concatenate(soln_group.all_chanblocks_hz)

        logger.debug(f"{chanblocks_per_coarse=}, {all_chanblocks_hz=}")

        soln_tile_ids, all_xx_solns_noref, all_yy_solns_noref = soln_group.get_solns()
        _, all_xx_solns, all_yy_solns = soln_group.get_solns(refant["name"])

        weights = soln_group.weights

        phase_fits = process_phase_fits(
            output_data_path,
            unflagged_tiles,
            all_chanblocks_hz,
            all_xx_solns,
            all_yy_solns,
            weights,
            soln_tile_ids,
            phase_fit_niter,
        )

        gain_fits = process_gain_fits(
            unflagged_tiles,
            all_chanblocks_hz,
            all_xx_solns_noref,
            all_yy_solns_noref,
            weights,
            soln_tile_ids,
            chanblocks_per_coarse,
        )

        # if ~np.any(np.isfinite(phase_fits["length"])):
        #     logger.warning(f"{item} - no valid phase fits found, continuing anyway")

        # Matplotlib stuff seems to break pytest so we will
        # pass false in for pytest stuff (or if we don't want debug)
        if produce_debug_plots:
            # This line was:
            # phase_fits_pivot = debug_phase_fits(...
            # But the phase_fits_pivot return value is not used
            debug_phase_fits(
                phase_fits,
                tiles,
                all_chanblocks_hz,
                all_xx_solns[0],
                all_yy_solns[0],
                weights,
                prefix=f"{output_data_path}/{obs_id}_",
                plot_residual=True,
            )

        # phase_fits_pivot = pivot_phase_fits(phase_fits, tiles)
        # logger.debug(f"{item} - fits:\n{phase_fits_pivot.to_string(max_rows=512)}")

        success = True

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
                )

                if fit_id is None or not success:
                    logger.error("failed to insert calibration fit")
                    # This will trigger a rollback of the calibration_fit row
                    raise Exception("failed to insert calibration fit")

                for tile_id in soln_tile_ids:
                    some_fits = False

                    try:
                        x_gains = gain_fits[(gain_fits.tile_id == tile_id) & (gain_fits.pol == "XX")].iloc[0]
                        some_fits = True
                    except IndexError:
                        x_gains = GainFitInfo.nan()

                    try:
                        y_gains = gain_fits[(gain_fits.tile_id == tile_id) & (gain_fits.pol == "YY")].iloc[0]
                        some_fits = True
                    except IndexError:
                        y_gains = GainFitInfo.nan()

                    try:
                        x_phase = phase_fits[(phase_fits.tile_id == tile_id) & (phase_fits.pol == "XX")].iloc[0]
                        some_fits = True
                    except IndexError:
                        x_phase = PhaseFitInfo.nan()

                    try:
                        y_phase = phase_fits[(phase_fits.tile_id == tile_id) & (phase_fits.pol == "YY")].iloc[0]
                        some_fits = True
                    except IndexError:
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
        error_text = f"Error in upload_handler:\n{traceback.format_exc()}"
        logger.exception(error_text)

        # Write an error readme
        write_readme_file(
            os.path.join(output_data_path, "readme_error.txt"),
            "upload_handler()",
            -999,
            "",
            error_text,
        )

        return False, error_text.replace("\n", ""), None
