"""Module for database operations"""

import datetime
import os
import math
import time
from typing import Optional, Tuple
import psycopg
import psycopg.errors
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from tenacity import (
    retry,
    stop_after_attempt,
    wait_fixed,
    wait_random,
    retry_if_exception_type,
)
from mwax_mover.utils import ArchiveLocation


class MWAXDBHandler:
    """Class which takes care of the primitive database functions"""

    def __init__(
        self,
        logger,
        host: str,
        port: int,
        db_name,
        user: str,
        password: str,
    ):
        self.logger = logger
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password

        self.pool = ConnectionPool(
            min_size=1,
            max_size=3,
            open=False,
            check=ConnectionPool.check_connection,
            conninfo=f"postgresql://{user}:{password}@{host}:{port}/{db_name}",
        )

    def start_database_pool(self):
        # Check we are not already started
        if self.pool.closed:
            self.pool.open(wait=True)

    def stop_database_pool(self):
        # Gracefully close the connections in the pool
        if self.pool:
            try:
                self.pool.close()
            except Exception:
                pass

    def select_one_row_postgres(self, sql: str, parm_list):
        """Returns a single row from postgres given SQL and params"""
        # Assuming we have a connection, try to do the database operation
        rows = self.select_postgres(sql, parm_list, 1)

        # Just return the first row
        return rows[0]

    def select_many_rows_postgres(self, sql: str, parm_list):
        """Returns a single row from postgres given SQL and params"""
        # Assuming we have a connection, try to do the database operation
        rows = self.select_postgres(sql, parm_list, None)

        # Just return all rows
        return rows

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(60),
        retry=retry_if_exception_type(
            (
                psycopg.errors.ConnectionFailure,
                psycopg.errors.ConnectionException,
                psycopg.errors.ConnectionTimeout,
                psycopg.errors.OperationalError,
            )
        ),
    )
    def select_postgres(self, sql, parm_list, expected_rows: None | int):
        """Returns rows from postgres given SQL and params. If expected rows is passed
        then it will check it returned the correct number of rows and riase exception
        if not"""
        # Assuming we have a connection, try to do the database operation
        try:
            with self.pool.connection() as conn:
                with conn.cursor(row_factory=dict_row) as cursor:
                    # Run the sql
                    cursor.execute(sql, parm_list)

                    # Fetch results as a list of tuples
                    rows = cursor.fetchall()

                    # Check how many rows we affected
                    rows_affected = len(rows)

                    if expected_rows:
                        # if we passed in how many rows we were expecting, check it!
                        if expected_rows == rows_affected:
                            return rows
                        else:
                            # Something went wrong
                            self.logger.error(
                                "select_postgres(): Error- queried" f" {rows_affected} rows, expected 1. SQL={sql}"
                            )
                            raise Exception(
                                "select_postgres(): Error- queried" f" {rows_affected} rows, expected 1. SQL={sql}"
                            )
                    else:
                        # We don't know how many rows, so cool, return them
                        return rows

        except Exception:
            self.logger.exception("select_postgres(): postgres exception")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(30),
        retry=retry_if_exception_type(
            (
                psycopg.errors.ConnectionFailure,
                psycopg.errors.ConnectionException,
                psycopg.errors.ConnectionTimeout,
                psycopg.errors.OperationalError,
            )
        ),
    )
    def execute_single_dml_row(self, sql: str, parm_list):
        """This executes an INSERT, UPDATE or DELETE that should affect 1
        row only. Since this is all in a with (context) block, rollback is
        called on failure and commit on success. Exceptions are raised on error."""
        self.execute_dml(sql, parm_list, expected_rows=1)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(30),
        retry=retry_if_exception_type(
            (
                psycopg.errors.ConnectionFailure,
                psycopg.errors.ConnectionException,
                psycopg.errors.ConnectionTimeout,
                psycopg.errors.OperationalError,
            )
        ),
    )
    def execute_dml(self, sql, parm_list, expected_rows: None | int):
        """This executes an INSERT, UPDATE or DELETE that should affect 0,1 or many
        rows. Since this is all in a with (context) block, rollback is
        called on failure and commit on success. Exceptions are raised on error."""

        # Assuming we have a connection, try to do the database operation
        try:
            with self.pool.connection() as conn:
                with conn.cursor() as cursor:
                    # Run the sql
                    cursor.execute(sql, parm_list)
                    conn.commit()

                    # Check how many rows we affected
                    rows_affected = cursor.rowcount

                    if expected_rows:
                        if rows_affected != expected_rows:
                            # An exception in here will trigger a rollback
                            # which is good
                            self.logger.error(
                                "execute_dml(): Error- query"
                                f" affected {rows_affected} rows, expected {expected_rows}."
                                f" SQL={sql}"
                            )
                            raise Exception(
                                "execute_dml(): Error- query"
                                f" affected {rows_affected} rows, expected {expected_rows}."
                                f" SQL={sql}"
                            )

        except psycopg.errors.ForeignKeyViolation:
            # Trying to insert or update but a value of a field violates the FK constraint-
            # e.g. insert into data_files fails due to observation_num not existing in mwa_setting.starttime
            # We need to reraise the error so our caller can handle in this "insert_data_file_row" case!
            self.logger.exception("execute_dml(): postgres ForeignKeyViolation")
            # Reraise error
            raise

        except Exception:
            # Any other error- likely to be a database error rather than
            # connection based
            self.logger.exception("execute_dml(): postgres Exception")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(60),
        retry=retry_if_exception_type(
            (
                psycopg.errors.ConnectionFailure,
                psycopg.errors.ConnectionException,
                psycopg.errors.ConnectionTimeout,
                psycopg.errors.OperationalError,
            )
        ),
    )
    def select_postgres_within_transaction(self, sql: str, parm_list, expected_rows: None | int, transaction_cursor):
        """Returns rows from postgres given SQL and params. If expected rows is passed
        then it will check it returned the correct number of rows and riase exception
        if not. If no rows result from the query, then an empty list is returned (by fetchall)"""
        # Assuming we have a connection, try to do the database operation
        try:
            # Run the sql
            transaction_cursor.execute(sql, parm_list)

            # Fetch results as a list of tuples
            rows = transaction_cursor.fetchall()

            # Check how many rows we affected
            rows_affected = len(rows)

            if expected_rows:
                # if we passed in how many rows we were expecting, check it!
                if expected_rows == rows_affected:
                    return rows
                else:
                    # Something went wrong
                    self.logger.error(
                        "select_postgres_within_transaction(): Error- queried"
                        f" {rows_affected} rows, expected 1. SQL={sql}"
                    )
                    raise Exception(
                        "select_postgres_within_transaction(): Error- queried"
                        f" {rows_affected} rows, expected 1. SQL={sql}"
                    )
            else:
                # We don't know how many rows, so cool, return them
                return rows

        except Exception:
            self.logger.exception("select_postgres_within_transaction(): postgres exception")
            raise

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(30),
        retry=retry_if_exception_type(
            (
                psycopg.errors.ConnectionFailure,
                psycopg.errors.ConnectionException,
                psycopg.errors.ConnectionTimeout,
                psycopg.errors.OperationalError,
            )
        ),
    )
    def execute_dml_row_within_transaction(self, sql, parm_list, transaction_cursor: psycopg.Cursor):
        """This executes an INSERT, UPDATE or DELETE that should only affect
        one row.

        NOTES: it is up to the caller to supply a cursor which all of the operations
        within the transaction share. Also it is up to the caller to call:
        1. conn = self.pool.getconn() # get a connection
        2. curs = conn.cursor()
        3. Call this method (possibly multiple times), passing in "curs"
        4. conn.rollback() # On exception or failure
        5. conn.commit() # On success
        6. self.pool.putconn(conn)

        We don't retry on FK or UK exceptions because, well, retrying won't fix anything!
        """

        # Assuming we have a connection, try to do the database operation
        # using our cursor
        try:
            # Run the sql
            transaction_cursor.execute(sql, parm_list)

            # Check how many rows we affected
            rows_affected = transaction_cursor.rowcount

            if rows_affected != 1:
                # An exception in here will trigger a rollback
                # which is good
                self.logger.error(
                    "execute_dml_row_within_transaction(): Error- query"
                    f" affected {rows_affected} rows, expected 1."
                    f" SQL={sql}"
                )
                raise Exception(
                    "execute_dml_row_within_transaction(): Error- query"
                    f" affected {rows_affected} rows, expected 1."
                    f" SQL={sql}"
                )

        except Exception:
            self.logger.exception("execute_single_dml_row_within_transaction(): postgres Exception")
            raise


#
# High level functions to do what we want specifically
#
class DataFileRow:
    """A class that abstracts the key fields of a MWA data_files row"""

    def __init__(self):
        self.observation_num: int = 0
        self.size = -1
        self.checksum = ""


def get_data_file_row(db_handler_object: MWAXDBHandler, full_filename: str, obs_id: int) -> DataFileRow:
    """Return a data file row instance on success or None on Failure"""
    # Prepare the fields
    # immediately add this file to the db so we insert a record into metadata
    # data_files table
    filename = os.path.basename(full_filename)

    sql = """SELECT observation_num,
                    size,
                    checksum
            FROM data_files
            WHERE filename = %s AND observation_num = %s"""
    try:
        # Run query and get the data_files row info for this file
        row = db_handler_object.select_one_row_postgres(
            sql,
            (
                filename,
                obs_id,
            ),
        )

        data_files_row = DataFileRow()
        data_files_row.observation_num = row["observation_num"]
        data_files_row.size = row["size"]
        data_files_row.checksum = row["checksum"]

        db_handler_object.logger.info(
            f"{full_filename} get_data_file_row() Successfully read from" f" data_files table {vars(data_files_row)}"
        )
        return data_files_row

    except Exception as select_exception:  # pylint: disable=broad-except
        db_handler_object.logger.error(
            f"{full_filename} get_data_file_row() error selecting data_files"
            f" record in data_files table: {select_exception}. SQL was {sql}"
        )
        raise Exception from select_exception


def insert_data_file_row(
    db_handler_object,
    obsid: int,
    archive_filename: str,
    filetype: int,
    hostname: str,
    checksum_type: int,
    checksum: str,
    trigger_id,
    file_size: int,
) -> bool:
    """Insert a data_files row"""
    # Prepare the fields
    # immediately add this file to the db so we insert a record into metadata
    # data_files table
    remote_archived = False
    filename = os.path.basename(archive_filename)

    if trigger_id == -1:
        trigger_id = None

    # We actually do an insert
    sql = ""

    try:
        sql = """INSERT INTO data_files
            (observation_num,
            filetype,
            size,
            filename,
            host,
            deleted,
            remote_archived,
            checksum_type,
            checksum,
            trigger_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        db_handler_object.execute_single_dml_row(
            sql,
            (
                str(obsid),
                filetype,
                file_size,
                filename,
                hostname,
                False,
                remote_archived,
                checksum_type,
                checksum,
                trigger_id,
            ),
        )

        db_handler_object.logger.info(f"{filename} insert_data_file_row() Successfully wrote into" " data_files table")
        return True

    except psycopg.errors.ForeignKeyViolation:
        # In this scenario it means M&C deleted the observation BUT the metafits was already generated
        # so mwax_u2s et al. thought it was still a real observation
        # we should just delete this file and move on
        db_handler_object.logger.warning(
            f"{filename} insert_data_file_row() observation_num {obsid} has been deleted by M&C."
            "Deleting this data file."
        )
        os.remove(archive_filename)

        # returning True here will cause the item to be ack'd off the queue so it is not tried again
        # but we need the caller to check if the file still exists- otherwise we may archive it!
        return True

    except Exception as upsert_exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            upsert_exception,
            f"{filename} insert_data_file_row() error inserting data_files"
            f" record in data_files table. SQL was {sql}",
        )
        return False


def update_data_file_row_as_archived(
    db_handler_object,
    obsid: int,
    archive_filename: str,
    location: ArchiveLocation,
    bucket: str,
    folder: Optional[str],
) -> bool:
    """Updates a data_files row as archived (at Pawsey)"""
    # Prepare the fields
    filename = os.path.basename(archive_filename)

    # We actually do an update
    sql = ""

    try:
        sql = """UPDATE data_files
                SET
                    remote_archived = True,
                    bucket = %s,
                    folder = %s,
                    location = %s
                WHERE
                    observation_num = %s
                    AND filename = %s"""

        db_handler_object.execute_single_dml_row(
            sql,
            (
                bucket,
                folder,
                location.value,
                str(obsid),
                filename,
            ),
        )

        db_handler_object.logger.info(
            f"{filename} update_data_file_row_as_archived() Successfully" " updated data_files table"
        )
        return True

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            f"{filename} update_data_file_row_as_archived() error updating"
            f" data_files record in data_files table. SQL"
            f" was {sql}"
        )
        return False


def insert_calibration_request_row(db_handler_object: MWAXDBHandler, obs_id: int, realtime: bool) -> bool:
    """Inserts a new calibration_request row and return true if successful

    Returns:
        Success (bool)
    """

    sql = "INSERT INTO calibration_request(cal_id, realtime) VALUES (%s, %s);"

    sql_values = (
        obs_id,
        realtime,
    )

    try:
        db_handler_object.execute_dml(sql, sql_values, 1)

        db_handler_object.logger.info(
            f"{obs_id}: insert_calibration_request_row() Successfully inserted into calibration_request table."
        )
        return True

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            f"{obs_id}: insert_calibration_request_row() error inserting"
            f" calibration_request record in table. SQL was"
            f" {sql} Values: {sql_values}"
        )
        return False


def insert_calibration_fits_row(
    db_handler_object,
    transaction_cursor: Optional[psycopg.Cursor],
    obs_id: int,
    code_version: str,
    creator: str,
    fit_niter: int,
    fit_limit: Optional[int],
    source_list: str,
    num_sources: int
) -> Tuple[bool, int | None]:
    """Inserts a new calibration_fits row and return the fit_id if successful
    This row represents the calibration 'header' for an obsid.

        Returns:
            Success (bool), fit_id (int or None)
    """

    sql = (
        "INSERT INTO calibration_fits"
        " (fitid,obsid,code_version,fit_time,creator,fit_niter,fit_limit,source_list,num_sources)"
        " VALUES (%s,%s,%s,now(),%s,%s,%s,%s,%s);"
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
        num_sources
    )

    try:
        db_handler_object.execute_dml_row_within_transaction(sql, sql_values, transaction_cursor)

        db_handler_object.logger.info(
            f"{obs_id}: insert_calibration_fits_row() Successfully wrote "
            f"into calibration_fits table. fit_id={fit_id}"
        )
        return (True, fit_id)
    
    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            f"{obs_id}: insert_calibration_fits_row() error inserting"
            f" calibration_fits record in table. SQL was"
            f" {sql} Values: {sql_values}"
        )
        if transaction_cursor:
            transaction_cursor.connection.rollback()
        return (False, None)


def insert_calibration_solutions_row(
    db_handler_object,
    transaction_cursor: Optional[psycopg.Cursor],
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
    1..n calibration_solutions rows."""

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

        db_handler_object.logger.info(
            f"{obs_id} tile {tile_id}: insert_calibration_solutions_row()"
            " Successfully wrote into insert_calibration_solutions table"
        )
        return True

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            f"{obs_id}: insert_calibration_solutions_row() error inserting"
            f" insert_calibration_solutions record in table. SQL was {sql} Values {sql_values}"
        )
        return False


def get_unattempted_unrequested_cal_obsids(db_handler_object: MWAXDBHandler, oldest_obs_id: int) -> Optional[list[int]]:
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
def get_unattempted_calsolution_requests(db_handler_object: MWAXDBHandler) -> list[Tuple[int, int, bool]] | None:
    """Returns the deatils of the next oldest unattempted calibration_requests.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            hostname (str): The name of the current host so we can specify who is working on this request

    Returns:
            list of Tuple(request_id, cal_id, realtime) OR None if none found. Raises exceptions on error
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
    SELECT c.id as request_id, c.cal_id as obs_id, c.realtime
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
    ORDER BY c.request_added_datetime"""

    return_list: list[Tuple[int, int, bool]] = []

    try:
        # get the connection
        with db_handler_object.pool.connection() as conn:
            # Create a cursor
            with conn.cursor() as cursor:
                # Get the next request, if any
                results_rows = db_handler_object.select_many_rows_postgres(
                    sql_get,
                    parm_list=[],
                )

                if len(results_rows) == 0:
                    db_handler_object.logger.debug("get_unattempted_calsolution_requests(): No requests to process.")
                    return None

                cursor.row_factory = dict_row

                for row in results_rows:
                    # We got one!
                    request_id: int = int(row["request_id"])
                    obs_id: int = int(row["obs_id"])
                    realtime: bool = bool(row["realtime"])

                    return_list.append((request_id, obs_id, realtime))

        return return_list

    except Exception:
        db_handler_object.logger.exception("get_unattempted_calsolution_requests(): Exception")
        raise


def update_calsolution_request_submit_mwa_asvo_job_status(
    db_handler_object: MWAXDBHandler,
    request_ids: list[int],
    mwa_asvo_job_id: Optional[int],
    mwa_asvo_job_submitted_datetime: Optional[datetime.datetime],
    mwa_asvo_job_submitted_error_datetime: Optional[datetime.datetime],
    mwa_asvo_job_submitted_error_message: Optional[str],
):
    """Update a calibration_request request with status info regarding the MWA ASVO job submitted.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            request_ids (int): The request_id of the calibration_request to update
                               (could be many including old!).
            mwa_asvo_job_submitted_error_datetime (datetime): The date/time the MWA ASVO job failed to be submitted
            mwa_asvo_job_submitted_error_message (str): The error when submitting

    Returns:
            Nothing. Raises exceptions on error"""

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
        db_handler_object.logger.debug(
            "update_calsolution_request_submit_mwa_asvo_job_status(): Successfully updated "
            "calibration_request table."
        )

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            "update_calsolution_request_submit_mwa_asvo_job_status(): error updating calibration_request record. SQL"
            f" was {sql}, params were: {params}"
        )

        # Re-raise error
        raise


def update_calibration_request_slurm_status(
    db_handler_object: MWAXDBHandler,
    request_ids: list[int],
    slurm_job_id: Optional[int],
    slurm_job_submitted_datetime: Optional[datetime.datetime],
    slurm_job_submitted_error_datetime: Optional[datetime.datetime],
    slurm_job_submitted_error_message: Optional[str],
):
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
        db_handler_object.logger.debug(
            "update_calsolution_request_download_complete_status(): Successfully updated " "calibration_request table."
        )

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            "update_calsolution_request_download_complete_status(): error updating calibration_request "
            f"record. SQL was {sql}, params were: {params}"
        )

        # Re-raise error
        raise


#
# Calvin processor functions
#
def update_calsolution_request_download_complete_status(
    db_handler_object: MWAXDBHandler,
    slurm_job_id: Optional[int],
    request_ids: list[int],
    download_completed_datetime: datetime.datetime | None,
    download_error_datetime: datetime.datetime | None,
    download_error_message: str | None,
):
    """Update a calsolution request with updated download completed status info.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            slurm_job_id Optional(int): slurm job id for this run (if we have it)
            request_ids: list[int]: All the request ids for this job
            download_completed_datetime (datetime): Date/time the download succeeded or None on error
            download_error_datetime (datetime): Date/time the download failed with an error OR None if success
            download_error_message (str): Error message if download_error_datetime is provided OR None if success

    Returns:
            Nothing. Raises exceptions on error"""

    sql = """
        UPDATE public.calibration_request
        SET
            download_completed_datetime = %s,
            download_error_datetime = %s,
            download_error_message = %s
        WHERE"""

    if slurm_job_id:
        sql = f"{sql} slurm_job_id = %s"
        params = [download_completed_datetime, download_error_datetime, download_error_message, slurm_job_id]
    else:
        sql = f"{sql} id = ANY(%s)"
        params = [download_completed_datetime, download_error_datetime, download_error_message, request_ids]

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
        db_handler_object.logger.debug(
            "update_calsolution_request_download_complete_status(): Successfully updated " "calibration_request table."
        )

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            "update_calsolution_request_download_complete_status(): error updating calibration_request "
            f"record. SQL was {sql}, params were: {params}"
        )

        # Re-raise error
        raise


def update_calibration_request_assign_hostname_start_download(
    db_handler_object: MWAXDBHandler,
    slurm_job_id: int,
    slurm_hostname: str,
    download_started_datetime: datetime.datetime,
):
    sql = """
    UPDATE public.calibration_request
    SET
        assigned_hostname = %s,
        assigned_datetime = %s,
        download_started_datetime = %s
    WHERE
    slurm_job_id = %s"""

    params = [slurm_hostname, download_started_datetime, download_started_datetime, slurm_job_id]

    try:
        # Update the row
        db_handler_object.execute_dml(sql, params, None)

        db_handler_object.logger.debug(
            "update_calibration_request_assign_hostname_start_download(): Successfully updated "
            "calibration_request table."
        )

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            "update_calibration_request_assign_hostname_start_download(): error updating calibration_request "
            f"record. SQL was {sql}, params were: {params}"
        )

        # Re-raise error
        raise


def update_calsolution_request_calibration_started_status(
    db_handler_object: MWAXDBHandler,
    slurm_job_id: int,
    calibration_started_datetime: datetime.datetime,
):
    """Update a calsolution request with updated calibration start status info.
    This makes the very valid assumption that the download has completed too.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            obs_id (int): Obs ID this cal soltution is for
            request_ids (int) | None: The request_id(s) of the calibration_request to update
                               (could be many including old!). None is passed by calvin_processor
                               as it has no idea about which request_ids this could be for (if any)
            calibration_started_datetime (datetime): The date/time the calibration started

    Returns:
            Nothing. Raises exceptions on error"""

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
        params = [calibration_started_datetime, calibration_started_datetime, slurm_job_id]

        db_handler_object.execute_dml(sql, params, None)
        db_handler_object.logger.debug(
            "update_calsolution_request_calibration_started_status(): Successfully updated "
            "calibration_request table."
        )

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            "update_calsolution_request_calibration_started_status(): error updating calibration_request "
            f"record. SQL was {sql}, params were: {params}"
        )

        # Re-raise error
        raise


def update_calsolution_request_calibration_complete_status(
    db_handler_object: MWAXDBHandler,
    slurm_job_id: int,
    calibration_completed_datetime: Optional[datetime.datetime],
    calibration_fit_id: Optional[int],
    calibration_error_datetime: Optional[datetime.datetime],
    calibration_error_message: Optional[str],
):
    """Update a calsolution request with updated calibration completed status info.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            slurm_job_id (int): identifies the row/rows of requests for this slurm job
            calibration_completed_datetime (datetime): Date/time the calibration succeeded or None on error
            calibration_fit_id (int): ID of the fit inserted or None on error
            calibration_error_datetime (datetime): Date/time the calibration failed with an error OR None if success
            calibration_error_message (str): Error message if calibration_error_datetime is provided OR None if success

    Returns:
            Nothing. Raises exceptions on error"""

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
        db_handler_object.logger.debug(
            "update_calsolution_request_calibration_complete_status(): Successfully updated "
            "calibration_request table."
        )

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            "update_calsolution_request_calibration_complete_status(): error updating "
            f"calibration_request record. SQL was {sql}, params were: {params}"
        )

        # Re-raise error
        raise
