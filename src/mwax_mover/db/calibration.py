"""Query and DML functions for the MWA metadata database's calibration tables.

Covers calibration_request (queuing and status tracking through MWA ASVO
submission, Slurm processing, download, and calibration), calibration_fits
(the per-obsid calibration 'header'), and calibration_solutions (per-tile
solutions).
"""

import datetime
import logging
import math
import time

import psycopg

from mwax_mover.db.handler import MWAXDBHandler

logger = logging.getLogger(__name__)


def insert_calibration_request_row(
    db_handler_object: MWAXDBHandler, obs_id: int, realtime: bool, bulk_request: bool
) -> bool:
    """Inserts a new calibration_request row and return true if successful

    Args:
        db_handler_object: object to handle database calls.
        obs_id: observation id.
        realtime: True if this is a realtime calibration request (high priority)
        bulk_request: True if this is for a background calibration task (low priority).

    Returns:
        Success (bool)
    """

    sql = "INSERT INTO calibration_request(cal_id, realtime, bulk_request) VALUES (%s, %s, %s);"

    sql_values = (obs_id, realtime, bulk_request)

    try:
        db_handler_object.execute_dml(sql, sql_values, 1)

        logger.info(f"{obs_id}: Successfully inserted into calibration_request table.")
        return True

    except Exception:
        logger.exception(
            f"{obs_id}: error inserting calibration_request record in table. SQL was {sql} Values: {sql_values}"
        )
        return False


def insert_calibration_fits_row(
    db_handler_object,
    transaction_cursor: psycopg.Cursor | None,
    obs_id: int,
    code_version: str,
    creator: str,
    fit_niter: int,
    fit_limit: int | None,
    source_list: str,
    num_sources: int,
    calibration_command: str,
    gain_max_cutoff: float | None,
    gain_outlier_poly_degree: int | None,
    gain_outlier_mad_residual_threshold: float | None,
    gain_outlier_modify_gains: bool | None,
    tile_bad_channel_fraction: float | None = None,
    phase_outlier_nstd_threshold: float | None = None,
) -> tuple[bool, int | None]:
    """Inserts a new calibration_fits row and return the fit_id if successful
    This row represents the calibration 'header' for an obsid.

        Returns:
            Success (bool), fit_id (int or None)
    """
    sql = (
        "INSERT INTO calibration_fits"
        " (fitid,obsid,code_version,fit_time,creator,fit_niter,fit_limit,source_list"
        ",num_sources,calibration_command,gain_max_cutoff,gain_outlier_poly_degree"
        ",gain_outlier_mad_residual_threshold,gain_outlier_modify_gains"
        ",tile_bad_channel_fraction,phase_outlier_nstd_threshold)"
        " VALUES (%s,%s,%s,now(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
    )

    # Fit ID is the Unix timestamp multiplied by 10**6 so it's an int
    fit_id = math.floor(time.time() * 10**6)

    sql_values = (
        fit_id,
        obs_id,
        code_version,
        creator,
        fit_niter,
        fit_limit,
        source_list,
        num_sources,
        calibration_command,
        gain_max_cutoff,
        gain_outlier_poly_degree,
        gain_outlier_mad_residual_threshold,
        gain_outlier_modify_gains,
        tile_bad_channel_fraction,
        phase_outlier_nstd_threshold,
    )

    try:
        db_handler_object.execute_dml_row_within_transaction(sql, sql_values, transaction_cursor)

        logger.info(f"{obs_id}: Successfully wrote into calibration_fits table. fit_id={fit_id}")
        return (True, fit_id)

    except Exception:
        logger.exception(
            f"{obs_id}: error inserting calibration_fits record in table. SQL was {sql} Values: {sql_values}"
        )
        # NOTE: deliberately does NOT roll back here. The caller owns the
        # transaction (calvin.pipeline.process_solutions runs this inside
        # a `with conn.transaction():` block and raises on a False return),
        # so rolling back from in here meant two things were trying to unwind
        # the same transaction. Rollback is the caller's job; we just report.
        return (False, None)


def insert_calibration_solutions_row(
    db_handler_object: MWAXDBHandler,
    transaction_cursor: psycopg.Cursor,
    fit_id: int,
    obs_id: int,
    tile_id: int,
    x_delay_m: float,
    x_intercept: float,
    x_gains: list[float],
    y_delay_m: float,
    y_intercept: float,
    y_gains: list[float],
    x_gains_pol1: list[float],
    y_gains_pol1: list[float],
    x_phase_sigma_resid: float,
    x_phase_chi2dof: float,
    x_phase_fit_quality: float,
    y_phase_sigma_resid: float,
    y_phase_chi2dof: float,
    y_phase_fit_quality: float,
    x_gains_fit_quality: float,
    y_gains_fit_quality: float,
    x_gains_sigma_resid: list[float],
    y_gains_sigma_resid: list[float],
    x_gains_pol0: list[float],
    y_gains_pol0: list[float],
) -> bool:
    """Insert a  calibration_solutions row.
    This row represents the calibration solution for a tile/obsid.
    We assume that caller is passing in a valid transaction cursor which means
    the caller has to manage commiting or rolling back the fit, plus
    1..n calibration_solutions rows.

    Args:
        db_handler_object: A populated database handler (dummy or real).
        transaction_cursor: An open transaction cursor; the caller manages
            commit/rollback of this row alongside its parent
            calibration_fit row.
        fit_id: ID of the parent calibration_fit row this solution belongs to.
        obs_id: The observation ID this calibration solution is for.
        tile_id: The tile ID this row's solution is for.
        x_delay_m: XX polarisation's fitted equivalent cable length in
            metres -- the negative of PhaseFitInfo.length, since the legacy
            calibration pipeline used the inverse sign convention.
        x_intercept: XX polarisation's fitted phase intercept, in radians
            (PhaseFitInfo.intercept).
        x_gains: XX polarisation's per-coarse-channel gain values
            (GainFitInfo.gains).
        y_delay_m: YY polarisation's fitted equivalent cable length in
            metres. See x_delay_m for the sign convention.
        y_intercept: YY polarisation's fitted phase intercept, in radians.
        y_gains: YY polarisation's per-coarse-channel gain values.
        x_gains_pol1: XX polarisation's per-coarse-channel order-1 (slope)
            coefficient of the within-coarse-channel gain amplitude fit
            (GainFitInfo.pol1). Diagnostic only.
        y_gains_pol1: YY polarisation's equivalent of x_gains_pol1.
        x_phase_sigma_resid: XX polarisation's phase-fit residual standard
            deviation, in radians (PhaseFitInfo.sigma_resid).
        x_phase_chi2dof: XX polarisation's phase-fit chi-squared per degree
            of freedom (PhaseFitInfo.chi2dof).
        x_phase_fit_quality: XX polarisation's phase-fit quality -- fraction
            of frequency channels surviving the sigma-clip, in [0, 1]
            (PhaseFitInfo.quality).
        y_phase_sigma_resid: YY polarisation's equivalent of x_phase_sigma_resid.
        y_phase_chi2dof: YY polarisation's equivalent of x_phase_chi2dof.
        y_phase_fit_quality: YY polarisation's equivalent of x_phase_fit_quality.
        x_gains_fit_quality: XX polarisation's gain-fit quality (GainFitInfo.quality).
        y_gains_fit_quality: YY polarisation's equivalent of x_gains_fit_quality.
        x_gains_sigma_resid: XX polarisation's per-coarse-channel gain-fit
            residual standard deviation (GainFitInfo.sigma_resid).
        y_gains_sigma_resid: YY polarisation's equivalent of x_gains_sigma_resid.
        x_gains_pol0: XX polarisation's per-coarse-channel order-0 (intercept)
            coefficient of the within-coarse-channel gain amplitude fit
            (GainFitInfo.pol0). Diagnostic only.
        y_gains_pol0: YY polarisation's equivalent of x_gains_pol0.

    Returns:
        True if the insert succeeded, False otherwise.
    """

    sql = """INSERT INTO calibration_solutions (fitid,obsid,tileid,
                                                x_delay_m,x_intercept,x_gains,
                                                y_delay_m,y_intercept,y_gains,
                                                x_gains_pol1,y_gains_pol1,
                                                x_phase_sigma_resid,x_phase_chi2dof,x_phase_fit_quality,
                                                y_phase_sigma_resid,y_phase_chi2dof,y_phase_fit_quality,
                                                x_gains_fit_quality,y_gains_fit_quality,
                                                x_gains_sigma_resid,y_gains_sigma_resid,
                                                x_gains_pol0,y_gains_pol0)
                            VALUES (%s,%s,%s,
                                    %s,%s,%s,
                                    %s,%s,%s,
                                    %s,%s,
                                    %s,%s,%s,
                                    %s,%s,%s,
                                    %s,%s,
                                    %s,%s,
                                    %s,%s)"""

    # Create the tuple of values
    sql_values = (
        fit_id,
        obs_id,
        tile_id,
        x_delay_m,
        x_intercept,
        x_gains,
        y_delay_m,
        y_intercept,
        y_gains,
        x_gains_pol1,
        y_gains_pol1,
        x_phase_sigma_resid,
        x_phase_chi2dof,
        x_phase_fit_quality,
        y_phase_sigma_resid,
        y_phase_chi2dof,
        y_phase_fit_quality,
        x_gains_fit_quality,
        y_gains_fit_quality,
        x_gains_sigma_resid,
        y_gains_sigma_resid,
        x_gains_pol0,
        y_gains_pol0,
    )

    try:
        db_handler_object.execute_dml_row_within_transaction(sql, sql_values, transaction_cursor)

        logger.info(f"{obs_id} tile {tile_id}: Successfully wrote into calibration_solutions table")
        return True

    except Exception:
        logger.exception(
            f"{obs_id}: error inserting calibration_solutions record in table. SQL was {sql} Values {sql_values}"
        )
        return False


def get_unattempted_unrequested_cal_obsids(db_handler_object: MWAXDBHandler, oldest_obs_id: int) -> list[int] | None:
    """Find calibrator observations with no calibration_request row yet.

    Args:
        db_handler_object: A populated database handler (dummy or real).
        oldest_obs_id: Ignore any observation older than this obs_id, so we do
            not keep reconsidering the entire archive.

    Returns:
        A list of obs_ids needing a calibration request, or None if none found.

    Raises:
        Exception: If the database query fails.
    """
    # This SQL gets all calibrator obs which have not yet been calibrated and
    # have not had a cal request added yet
    sql = """SELECT m.starttime as obs_id
            FROM mwa_setting m
            INNER JOIN schedule_metadata s ON m.starttime = s.observation_number
            LEFT OUTER JOIN calibration_fits f ON f.obsid = s.observation_number
            LEFT OUTER JOIN calibration_request c ON c.cal_id = s.observation_number
            LEFT OUTER JOIN data_files d ON d.observation_num = s.observation_number
            WHERE
            m.mode = 'MWAX_CORRELATOR'       	  -- Obs must be correlator
            AND m.projectid <> 'C123'             -- Ignore C123 (non archive jobs)
            AND d.filename IS NOT NULL 			  -- Ensure we have data files
            AND d.deleted_timestamp IS NULL 	  -- Ensure they are not deleted
            AND d.filetype = 18 				  -- Ensure the files are MWAX_VISIBILITIES
            AND s.calibration IS True 			  -- Is a calibrator obs
            AND f.fitid IS NULL   				  -- No cal solution has been generated
            AND c.id IS NULL      				  -- No cal request has been created yet
            AND s.observation_number > %s         -- Oldest Obsid which is the last one handled by old calvinproc
            AND UPPER(s.calibrators) <> 'SUN'     -- Don't try to calibrate on the SUN!
            GROUP BY m.starttime
            HAVING COUNT(d.filename)>0            -- This obs should have some data files
            ORDER BY m.starttime asc"""

    # Run SQL
    rows = db_handler_object.select_many_rows_postgres(
        sql,
        [
            oldest_obs_id,
        ],
    )

    # Return a list or None if no rows
    if len(rows) > 0:
        return [int(r["obs_id"]) for r in rows]
    else:
        return None


#
# Calvin controller
#
def get_unattempted_calibration_requests(
    db_handler_object: MWAXDBHandler,
) -> list[tuple[int, int, bool, bool]] | None:
    """Return the details of the next oldest unattempted calibration_requests.

    Args:
        db_handler_object: A populated database handler (dummy or real).

    Returns:
        A list of (request_id, cal_id, realtime, bulk_request) tuples, or None if
        none were found.

    Raises:
        Exception: If the database query fails.
    """

    # How this works!
    # For realtime jobs:
    # * M&C will insert a row (with realtime=TRUE)
    # * calvin_controller calls this function from the main loop to get new unattempted requests
    # * The below SELECT will grab the new realtime calibration request row
    # * calvin_controller will:
    #   * try to submit slurm job
    #     * on success, update row with slurm_job_id, slurm_hostname and slurm_job_submitted_datetime
    #       * from that point the job is in the hands of the calvin_processor.
    #     * on failure (e.g. slurm down), do nothing, but try again, it will be picked up in the next loop
    #
    # For mwa_asvo jobs:
    # * M&C will insert a row (with realtime=FALSE) based on an ASVO calibration request
    # * calvin_controller calls this function from the main loop to get new unattempted requests
    # * The below SELECT will grab the new mwa_asvo calibration request row
    # * calvin_controller will:
    #   * try to submit mwa_asvo job via Giant squid
    #     * on success, update row with download_mwa_asvo_job_submitted_datetime, download_mwa_asvo_job_id
    #     * on failure, e.g. MWA ASVO in maintenance, do nothing, but try again, it will be picked up in the next loop
    #   * keep checking via giant-squid the job status
    #     * on "ready/complete", go to next step (submit slurm job passing the download URL)
    #     * on "error" update request with download_error_datetime and download_error_message
    #   * try to submit slurm job
    #     * on success, update row with slurm_job_id, slurm_hostname and slurm_job_submitted_datetime
    #       * from that point the job is in the hands of the calvin_processor.
    #     * on failure, do nothing, but try again in the next loop

    sql_get = """
    SELECT c.id as request_id, c.cal_id as obs_id, c.realtime, c.bulk_request
    FROM public.calibration_request c
    WHERE
    -- Not yet submitted to slurm
    c.slurm_job_id IS NULL AND
    (
        (
            -- MWA ASVO case
            c.realtime IS FALSE
            -- Next 2 clauses prevent old calvin2 rows from being picked up!
            AND c.download_completed_datetime IS NULL
            AND c.download_error_datetime IS NULL
            -- Check for failed giant squid submission
            AND c.download_mwa_asvo_job_submitted_error_datetime IS NULL
        )
        OR
        (
            -- realtime case
            c.realtime IS TRUE
        )
    )
    -- Always ensure bulk requests are sorted last for non-realtime
    ORDER BY c.bulk_request, c.request_added_datetime"""

    return_list: list[tuple[int, int, bool, bool]] = []

    try:
        # Get the next request, if any
        results_rows = db_handler_object.select_many_rows_postgres(
            sql_get,
            parm_list=[],
        )

        if len(results_rows) == 0:
            logger.debug("No requests to process.")
            return None

        for row in results_rows:
            # We got one!
            request_id: int = int(row["request_id"])
            obs_id: int = int(row["obs_id"])
            realtime: bool = bool(row["realtime"])
            bulk_request: bool = bool(row["bulk_request"])

            return_list.append((request_id, obs_id, realtime, bulk_request))

        return return_list

    except Exception:
        logger.exception("Exception")
        raise


def update_calibration_request_mwa_asvo_job_status(
    db_handler_object: MWAXDBHandler,
    request_ids: list[int],
    mwa_asvo_job_id: int | None,
    mwa_asvo_job_submitted_datetime: datetime.datetime | None,
    mwa_asvo_job_submitted_error_datetime: datetime.datetime | None,
    mwa_asvo_job_submitted_error_message: str | None,
):
    """Update a calibration_request request with status info regarding the MWA ASVO job submitted.

    Args:
        db_handler_object: A populated database handler (dummy or real).
        request_ids: The request_id(s) of the calibration_request to update
            (could be many including old!).
        mwa_asvo_job_id: The MWA ASVO job ID that was submitted, or None on error.
        mwa_asvo_job_submitted_datetime: The date/time the MWA ASVO job was
            submitted, or None on error.
        mwa_asvo_job_submitted_error_datetime: The date/time the MWA ASVO job
            failed to be submitted, or None on success.
        mwa_asvo_job_submitted_error_message: The error when submitting, or
            None on success.

    Raises:
        Exception: If the database update fails.
    """

    sql = """
    UPDATE public.calibration_request
    SET
        download_mwa_asvo_job_id = %s,
        download_mwa_asvo_job_submitted_datetime = %s,
        download_mwa_asvo_job_submitted_error_datetime = %s,
        download_mwa_asvo_job_submitted_error_message = %s
    WHERE
    id = ANY(%s)"""

    params = [
        mwa_asvo_job_id,
        mwa_asvo_job_submitted_datetime,
        mwa_asvo_job_submitted_error_datetime,
        mwa_asvo_job_submitted_error_message,
        request_ids,
    ]

    try:
        db_handler_object.execute_dml(sql, params, len(request_ids))
        logger.debug("Successfully updated calibration_request table.")

    except Exception:
        logger.exception(f"error updating calibration_request record. SQL was {sql}, params were: {params}")

        # Re-raise error
        raise


def update_calibration_request_slurm_status(
    db_handler_object: MWAXDBHandler,
    request_ids: list[int],
    slurm_job_id: int | None,
    slurm_job_submitted_datetime: datetime.datetime | None,
    slurm_job_submitted_error_datetime: datetime.datetime | None,
    slurm_job_submitted_error_message: str | None,
):
    """Record the outcome of submitting a Slurm job for one or more requests.

    Args:
        db_handler_object: A populated database handler (dummy or real).
        request_ids: The calibration request IDs to update.
        slurm_job_id: The submitted Slurm job ID, or None on failure.
        slurm_job_submitted_datetime: When the job was submitted, or None on failure.
        slurm_job_submitted_error_datetime: When submission failed, or None on success.
        slurm_job_submitted_error_message: Why submission failed, or None on success.

    Raises:
        Exception: If the database update fails.
    """
    sql = """
    UPDATE public.calibration_request
    SET
        slurm_job_id = %s,
        download_slurm_job_submitted_datetime = %s,
        download_slurm_job_submitted_error_datetime = %s,
        download_slurm_job_submitted_error_message = %s
    WHERE
    id = ANY(%s)"""

    params = [
        slurm_job_id,
        slurm_job_submitted_datetime,
        slurm_job_submitted_error_datetime,
        slurm_job_submitted_error_message,
        request_ids,
    ]

    try:
        # Update the rows
        db_handler_object.execute_dml(sql, params, len(request_ids))
        logger.debug("Successfully updated calibration_request table.")

    except Exception:
        logger.exception(f"error updating calibration_request record. SQL was {sql}, params were: {params}")

        # Re-raise error
        raise


#
# Calvin processor functions
#
def update_calibration_request_download_complete_status(
    db_handler_object: MWAXDBHandler,
    slurm_job_id: int | None,
    request_ids: list[int],
    download_completed_datetime: datetime.datetime | None,
    download_error_datetime: datetime.datetime | None,
    download_error_message: str | None,
):
    """Update a calibration_request with updated download completed status info.

    Args:
        db_handler_object: A populated database handler (dummy or real).
        slurm_job_id: Slurm job id for this run, if we have it.
        request_ids: All the request ids for this job.
        download_completed_datetime: Date/time the download succeeded, or
            None on error.
        download_error_datetime: Date/time the download failed with an error,
            or None on success.
        download_error_message: Error message if download_error_datetime is
            provided, or None on success.

    Raises:
        Exception: If the database update fails.
        ValueError: If download_completed_datetime is not mutually exclusive
            with download_error_datetime and download_error_message.
    """

    sql = """
        UPDATE public.calibration_request
        SET
            download_completed_datetime = %s,
            download_error_datetime = %s,
            download_error_message = %s
        WHERE"""

    if slurm_job_id:
        sql = f"{sql} slurm_job_id = %s"
        params = [
            download_completed_datetime,
            download_error_datetime,
            download_error_message,
            slurm_job_id,
        ]
    else:
        sql = f"{sql} id = ANY(%s)"
        params = [
            download_completed_datetime,
            download_error_datetime,
            download_error_message,
            request_ids,
        ]

    # check for validity, raise exception if not valid
    if (
        download_completed_datetime is not None and download_error_datetime is None and download_error_message is None
    ) ^ (
        download_completed_datetime is None
        and download_error_datetime is not None
        and download_error_message is not None
    ):
        pass
    else:
        raise ValueError(
            "download_completed_datetime is mutually exclusive with download_error_datetime and download_error_message "
            f"{download_completed_datetime, download_error_datetime, download_error_message}"
        )

    try:
        db_handler_object.execute_dml(sql, params, None)
        logger.debug("Successfully updated calibration_request table.")

    except Exception:
        logger.exception(f"error updating calibration_request record. SQL was {sql}, params were: {params}")

        # Re-raise error
        raise


def update_calibration_request_assign_hostname_start_download(
    db_handler_object: MWAXDBHandler,
    slurm_job_id: int,
    slurm_hostname: str,
    download_started_datetime: datetime.datetime,
):
    """Record which host a Slurm job landed on, and that its download has begun.

    Args:
        db_handler_object: A populated database handler (dummy or real).
        slurm_job_id: The Slurm job ID whose request row should be updated.
        slurm_hostname: The calvin host now working on this request.
        download_started_datetime: When the download started.

    Raises:
        Exception: If the database update fails.
    """
    sql = """
    UPDATE public.calibration_request
    SET
        assigned_hostname = %s,
        assigned_datetime = %s,
        download_started_datetime = %s
    WHERE
    slurm_job_id = %s"""

    params = [
        slurm_hostname,
        download_started_datetime,
        download_started_datetime,
        slurm_job_id,
    ]

    try:
        # Update the row
        db_handler_object.execute_dml(sql, params, None)

        logger.debug("Successfully updated calibration_request table.")

    except Exception:
        logger.exception(f"error updating calibration_request record. SQL was {sql}, params were: {params}")

        # Re-raise error
        raise


def update_calibration_request_calibration_started_status(
    db_handler_object: MWAXDBHandler,
    slurm_job_id: int,
    calibration_started_datetime: datetime.datetime,
):
    """Update a calibration_request with updated calibration start status info.

    This makes the very valid assumption that the download has completed too.

    Args:
        db_handler_object: A populated database handler (dummy or real).
        slurm_job_id: Identifies the row/rows of requests for this slurm job.
        calibration_started_datetime: The date/time the calibration started.

    Raises:
        Exception: If the database update fails.
    """

    sql = """
    UPDATE public.calibration_request
    SET
        download_completed_datetime = %s,
        calibration_started_datetime = %s,
        calibration_completed_datetime = NULL,
        calibration_fit_id = NULL,
        calibration_error_datetime = NULL,
        calibration_error_message = NULL
    WHERE
    slurm_job_id = %s"""

    params = []

    try:
        params = [
            calibration_started_datetime,
            calibration_started_datetime,
            slurm_job_id,
        ]

        db_handler_object.execute_dml(sql, params, None)
        logger.debug("Successfully updated calibration_request table.")

    except Exception:
        logger.exception(f"error updating calibration_request record. SQL was {sql}, params were: {params}")

        # Re-raise error
        raise


def update_calibration_request_calibration_complete_status(
    db_handler_object: MWAXDBHandler,
    slurm_job_id: int,
    calibration_completed_datetime: datetime.datetime | None,
    calibration_fit_id: int | None,
    calibration_error_datetime: datetime.datetime | None,
    calibration_error_message: str | None,
):
    """Update a calibration_request with updated calibration completed status info.

    Args:
        db_handler_object: A populated database handler (dummy or real).
        slurm_job_id: Identifies the row/rows of requests for this slurm job.
        calibration_completed_datetime: Date/time the calibration succeeded,
            or None on error.
        calibration_fit_id: ID of the fit inserted, or None on error.
        calibration_error_datetime: Date/time the calibration failed with an
            error, or None on success.
        calibration_error_message: Error message if calibration_error_datetime
            is provided, or None on success.

    Raises:
        Exception: If the database update fails.
        ValueError: If (calibration_completed_datetime, calibration_fit_id)
            is not mutually exclusive with (calibration_error_datetime,
            calibration_error_message).
    """

    sql = """
    UPDATE public.calibration_request
    SET
        calibration_completed_datetime = %s,
        calibration_fit_id = %s,
        calibration_error_datetime = %s,
        calibration_error_message = %s
    WHERE
    slurm_job_id = %s"""
    params = ""

    # check for validity, raise exception if not valid
    # ^ is XOR if you were wondering!
    if (
        calibration_completed_datetime is not None
        and calibration_fit_id is not None
        and calibration_error_datetime is None
        and calibration_error_message is None
    ) ^ (
        calibration_completed_datetime is None
        and calibration_fit_id is None
        and calibration_error_datetime is not None
        and calibration_error_message is not None
    ):
        pass
    else:
        raise ValueError(
            "calibration_completed_datetime and calibration_fit_id are mutually exclusive with "
            "calibration_error_datetime and calibration_error_message"
        )

    try:
        params = [
            calibration_completed_datetime,
            calibration_fit_id,
            calibration_error_datetime,
            calibration_error_message,
            slurm_job_id,
        ]

        db_handler_object.execute_dml(sql, params, None)
        logger.debug("Successfully updated calibration_request table.")

    except Exception:
        logger.exception(f"error updating calibration_request record. SQL was {sql}, params were: {params}")

        # Re-raise error
        raise


def get_fit_info_from_slurm_job_and_obsid(
    db_handler_object: MWAXDBHandler, obs_id: int, slurm_job_id: int
) -> tuple[int, int | None] | None:
    """Look up a fit_id, and whether hyperdrive's amp plots need a max-amp clip.

    Args:
        db_handler_object: A populated database handler (dummy or real).
        obs_id: The observation ID of the fit.
        slurm_job_id: The Slurm job ID that produced the fit. Together with
            obs_id this is unique for calvin fits.

    Returns:
        A (fit_id, amp_plot_max) tuple, where amp_plot_max is 100 or None, or
        None if no matching fit was found. Will not find fits from before the
        calibration_request table was introduced.

    Raises:
        Exception: If the database query fails.
    """
    # This SQL looks up a fitid and determines if a max amp is needed to be passed to hyperdrive amp plots
    # from the calibration_request and fits table based on an obsid and a slurm jobid.
    # This will be unique for calvin fits.
    # Won't work for any fits prior to the introduction of the calibration_request table
    sql = """SELECT r.calibration_fit_id, f.creator, f.code_version, 
                CASE
                    WHEN f.gain_max_cutoff IS NULL
                     AND r.calibration_fit_id IS NOT NULL
                     AND f.gain_outlier_modify_gains IS NULL THEN 100
                ELSE NULL END as amp_plot_max
            FROM calibration_request r
            LEFT OUTER JOIN calibration_fits f ON f.fitid=r.calibration_fit_id AND f.obsid=r.cal_id
            WHERE cal_id = %s AND slurm_job_id=%s"""

    # Run SQL
    rows = db_handler_object.select_one_row_postgres(
        sql,
        [obs_id, slurm_job_id],
    )

    # Return a list or None if no rows
    if len(rows) > 0:
        fit_id = rows["calibration_fit_id"]
        if fit_id is None:
            return None
        else:
            amp_plot_max = rows["amp_plot_max"]
            if amp_plot_max is not None:
                amp_plot_max = int(amp_plot_max)
            return (int(fit_id), amp_plot_max)
    else:
        return None
