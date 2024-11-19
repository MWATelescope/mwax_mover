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
    retry_if_not_exception_type,
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
            self.pool.close()

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

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(60))
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
        retry=retry_if_not_exception_type(psycopg.errors.ForeignKeyViolation),
    )
    def execute_single_dml_row(self, sql: str, parm_list):
        """This executes an INSERT, UPDATE or DELETE that should affect 1
        row only. Since this is all in a with (context) block, rollback is
        called on failure and commit on success. Exceptions are raised on error."""
        self.execute_dml(sql, parm_list, expected_rows=1)

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(30),
        retry=retry_if_not_exception_type(psycopg.errors.ForeignKeyViolation),
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

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(60))
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
        retry=retry_if_not_exception_type(psycopg.errors.ForeignKeyViolation)
        | retry_if_not_exception_type(psycopg.errors.UniqueViolation),
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


@retry(
    stop=stop_after_attempt(3),
    wait=wait_random(10, 60),
    retry=retry_if_exception_type(psycopg.errors.UniqueViolation),
)
def insert_calibration_fits_row(
    db_handler_object,
    transaction_cursor: Optional[psycopg.Cursor],
    obs_id: int,
    code_version: str,
    creator: str,
    fit_niter: int = 10,
    fit_limit: int = 20,
) -> Tuple[bool, int | None]:
    """Inserts a new calibration_fits row and return the fit_id if successful
    This row represents the calibration 'header' for an obsid.

        Returns:
            Success (bool), fit_id (int or None)
    """

    sql = (
        "INSERT INTO calibration_fits"
        " (fitid,obsid,code_version,fit_time,creator,fit_niter,fit_limit)"
        " VALUES (%s,%s,%s,now(),%s,%s,%s);"
    )

    # Fit ID is the Unix timestamp to nearest integer
    fit_id = math.floor(time.time())

    sql_values = (
        fit_id,
        obs_id,
        code_version,
        creator,
        fit_niter,
        fit_limit,
    )

    try:
        db_handler_object.execute_dml_row_within_transaction(sql, sql_values, transaction_cursor)

        db_handler_object.logger.info(
            f"{obs_id}: insert_calibration_fits_row() Successfully wrote "
            f"into calibration_fits table. fit_id={fit_id}"
        )
        return (True, fit_id)

    except psycopg.errors.UniqueViolation:
        # We have a collision with fit_id- since it is the PK of the table and
        # it is just the integer UNIX timestep (down to 1 second resolution) it
        # is unlikely, but POSSIBLE to have a conflict if another calvin is
        # inserting a fit at the same second in time! So just warn and try again.
        # The 'raise' will trigger a retry based on the function's tenacity decorator
        db_handler_object.logger.warning(
            f"{obs_id}: insert_calibration_fits_row() error inserting "
            f"calibration_fits record in table- Unique Key violation on fit_id {fit_id}. Retrying with "
            "a new fit_id!"
        )
        raise

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


def get_this_hosts_previously_started_download_requests(db_handler_object: MWAXDBHandler, hostname: str):
    """Returns a list of any incomplete jobs assigned to this host (only up to the download step).
    Maybe we crashed - so we need to pickup from where we left off!

    Columns: id, cal_id, download_mwa_asvo_job_submitted_datetime, download_mwa_asvo_job_id
    """

    sql = """
    SELECT c.id, c.cal_id, c.download_mwa_asvo_job_submitted_datetime, c.download_mwa_asvo_job_id
    FROM public.calibration_request c
    WHERE
    (
        c.assigned_hostname = %s AND
        (
            c.download_completed_datetime IS NULL
            AND c.download_error_datetime IS NULL
        )
    )
    ORDER BY c.request_added_datetime;
    """

    try:
        return db_handler_object.select_many_rows_postgres(
            sql,
            [
                hostname,
            ],
        )

    except Exception:
        db_handler_object.logger.exception(
            f"get_this_hosts_previously_started_requests() error " f" . SQL was {sql} Values {hostname}"
        )
        raise


def assign_next_unattempted_calsolution_request(db_handler_object, hostname: str) -> Tuple[int, int] | None:
    """Assigns then returns the deatils of the next oldest unattempted calibration_request.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            hostname (str): The name of the current host so we can specify who is working on this request

    Returns:
            Tuple(request_id, cal_id) OR None if none found. Raises exceptions on error
    """

    sql_get = """
    SELECT c.id, c.cal_id
    FROM public.calibration_request c
    WHERE
    (
        (
            -- Anything unassigned
            c.assigned_hostname IS NULL -- not attempted at all
        )
        OR
        (
            -- Assigned to me but not completed
            -- Don't worry we check with our existing list (in code)
            -- so we don't re-re try the job!
            c.assigned_hostname = %s
            AND c.download_completed_datetime IS NULL
            AND c.download_error_datetime IS NULL
        )
    )
    AND NOT EXISTS
    -- Check if the SAME calid is in progress on another host if so ignore!
    (
        SELECT 1
        FROM public.calibration_request q
        WHERE
        q.cal_id = c.cal_id
        AND q.assigned_hostname <> %s
        AND
        (
                q.download_completed_datetime IS NULL
            AND q.download_error_datetime IS NULL
            AND q.calibration_completed_datetime IS NULL
            AND q.calibration_error_datetime IS NULL
        )
    )
    -- The for update clause locks the selected rows
    -- so we don't have a race condition where two or
    -- more calvins grab the same job
    ORDER BY c.request_added_datetime LIMIT 1"""

    sql_update = """
    UPDATE public.calibration_request
    SET
        assigned_datetime = Now(),
        assigned_hostname = %s
    WHERE
    id = %s"""

    try:
        # get the connection
        with db_handler_object.pool.connection() as conn:
            with conn.transaction():
                # Create a cursor
                with conn.cursor() as cursor:
                    # Get the next request, if any
                    results_rows = db_handler_object.select_postgres_within_transaction(
                        sql_get,
                        parm_list=[
                            hostname,
                            hostname,
                        ],
                        expected_rows=None,
                        transaction_cursor=cursor,
                    )

                    if len(results_rows) == 0:
                        db_handler_object.logger.debug(
                            "assign_next_unattempted_calsolution_request(): No requests to process."
                        )
                        return None

                    if len(results_rows) > 1:
                        db_handler_object.logger.error(
                            "assign_next_unattempted_calsolution_request(): Error- expected 1 row, but "
                            f"retrieved {len(results_rows)} rows"
                        )
                        return None

                    cursor.row_factory = dict_row

                    # We got one!
                    request_id: int = int(results_rows[0][0])
                    cal_id: int = int(results_rows[0][1])

                    # Update the row, so no one else does!
                    db_handler_object.execute_dml_row_within_transaction(
                        sql_update,
                        parm_list=[hostname, request_id],
                        transaction_cursor=cursor,
                    )

        return request_id, cal_id

    except Exception:
        db_handler_object.logger.exception("select_unattempted_calsolution_requests(): Exception")
        raise


def update_calsolution_request_submit_mwa_asvo_job(
    db_handler_object: MWAXDBHandler,
    request_ids: list[int],
    mwa_asvo_submitted_datetime: datetime.datetime,
    mwa_asvo_job_id: int,
):
    """Update a calsolution request with status info regarding the MWA ASVO job submission.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            request_ids (int): The request_id of the calibration_request to update
                               (could be many including old!).
            mwa_asvo_submitted_datetime (datetime): The date/time the MWA ASVO job was successfully submitted
            mwa_asvo_job_id (int): The MWA ASVO Job ID which is handling the retrieval of this obsid

    Returns:
            Nothing. Raises exceptions on error"""

    sql = """
    UPDATE public.calibration_request
    SET
        download_mwa_asvo_job_submitted_datetime = %s,
        download_mwa_asvo_job_id = %s
    WHERE
    id = ANY(%s)"""

    params = [mwa_asvo_submitted_datetime, mwa_asvo_job_id, request_ids]

    try:
        db_handler_object.execute_dml(sql, params, None)
        db_handler_object.logger.debug(
            "update_calsolution_request_submit_mwa_asvo_job(): Successfully updated " "calibration_request table."
        )

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            "update_calsolution_request_submit_mwa_asvo_job(): error updating calibration_request record. SQL"
            f" was {sql}, params were: {params}"
        )

        # Re-raise error
        raise


def update_calsolution_request_download_started_status(
    db_handler_object: MWAXDBHandler,
    request_ids: list[int],
    download_started_datetime: datetime.datetime,
):
    """Update a calsolution request with updated download start status info.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            request_ids (int): The request_id(s) of the calibration_request to update
                               (could be many including old!)
            download_started_datetime (datetime): The date/time the download started

    Returns:
            Nothing. Raises exceptions on error"""

    sql = """
    UPDATE public.calibration_request
    SET
        download_started_datetime = %s,
        download_completed_datetime = NULL,
        download_error_datetime = NULL,
        download_error_message = NULL
    WHERE
    id = ANY(%s)"""

    params = [download_started_datetime, request_ids]

    try:
        db_handler_object.execute_dml(sql, params, None)
        db_handler_object.logger.debug(
            "update_calsolution_request_download_started_status(): Successfully updated " "calibration_request table."
        )

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            "update_calsolution_request_download_started_status(): error updating calibration_request "
            f"record. SQL was {sql}, params were: {params}"
        )

        # Re-raise error
        raise


def update_calsolution_request_download_complete_status(
    db_handler_object: MWAXDBHandler,
    request_ids: list[int],
    download_completed_datetime: datetime.datetime | None,
    download_error_datetime: datetime.datetime | None,
    download_error_message: str | None,
):
    """Update a calsolution request with updated download completed status info.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            request_ids (int): The request_id(s) of the calibration_request to update
                               (could be many including old!)
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
    WHERE
    id = ANY(%s)"""

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

    params = [download_completed_datetime, download_error_datetime, download_error_message, request_ids]

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


def get_incomplete_request_ids_for_obsid(db_handler_object: MWAXDBHandler, obs_id: int):
    sql = """SELECT id
          FROM calibration_request
          WHERE cal_id =  %s
          AND calibration_completed_datetime IS NULL
          AND calibration_error_datetime IS NULL"""

    # Run SQL
    rows = db_handler_object.select_many_rows_postgres(
        sql,
        [
            obs_id,
        ],
    )

    # Return a list or None if no rows
    if len(rows) > 0:
        return [r["id"] for r in rows]
    else:
        return None


def update_calsolution_request_calibration_started_status(
    db_handler_object: MWAXDBHandler,
    obs_id: int,
    request_ids: list[int] | None,
    calibration_started_datetime: datetime.datetime,
):
    """Update a calsolution request with updated calibration start status info.

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
        calibration_started_datetime = %s,
        calibration_completed_datetime = NULL,
        calibration_fit_id = NULL,
        calibration_error_datetime = NULL,
        calibration_error_message = NULL
    WHERE
    id = ANY(%s)"""

    params = []

    # calvin_processor won't know the requestids so get them (if any!)
    if request_ids is None:
        # We need to get the request ids
        request_ids = get_incomplete_request_ids_for_obsid(db_handler_object, obs_id)

    try:
        params = [calibration_started_datetime, request_ids]

        if request_ids is not None:
            # update the params varible if the request_ids changed (above)
            params = [calibration_started_datetime, request_ids]

            db_handler_object.execute_dml(sql, params, None)
            db_handler_object.logger.debug(
                "update_calsolution_request_calibration_started_status(): Successfully updated "
                "calibration_request table."
            )
        else:
            db_handler_object.logger.debug(
                "update_calsolution_request_calibration_started_status(): No requests to update "
                "in the calibration_request table."
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
    obs_id: int,
    request_ids: Optional[list[int]],
    calibration_completed_datetime: Optional[datetime.datetime],
    calibration_fit_id: Optional[int],
    calibration_error_datetime: Optional[datetime.datetime],
    calibration_error_message: Optional[str],
):
    """Update a calsolution request with updated calibration completed status info.

    Parameters:
            db_handler_object (MWAXDBHandler): A populated database handler (dummy or real)
            obs_id (int): Obs ID this cal soltution is for
            request_ids (int) | None: The request_id(s) of the calibration_request to update
                               (could be many including old!). None is passed by calvin_processor
                               as it has no idea about which request_ids this could be for (if any)
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
    id = ANY(%s)"""
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

    # calvin_processor won't know the requestids so get them (if any!)
    if request_ids is None:
        # We need to get the request ids
        request_ids = get_incomplete_request_ids_for_obsid(db_handler_object, obs_id)

    try:
        if request_ids is not None:
            params = [
                calibration_completed_datetime,
                calibration_fit_id,
                calibration_error_datetime,
                calibration_error_message,
                request_ids,
            ]

            db_handler_object.execute_dml(sql, params, None)
            db_handler_object.logger.debug(
                "update_calsolution_request_calibration_complete_status(): Successfully updated "
                "calibration_request table."
            )
        else:
            db_handler_object.logger.debug(
                "update_calsolution_request_calibration_complete_status(): No requests to update "
                "in the calibration_request table."
            )

    except Exception:  # pylint: disable=broad-except
        db_handler_object.logger.exception(
            "update_calsolution_request_calibration_complete_status(): error updating "
            f"calibration_request record. SQL was {sql}, params were: {params}"
        )

        # Re-raise error
        raise
