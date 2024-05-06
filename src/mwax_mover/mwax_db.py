"""Module for database operations"""

import os
import math
import time
from typing import Optional, Tuple
import psycopg2
import psycopg2.pool
from psycopg2 import InterfaceError, OperationalError
from tenacity import retry, stop_after_attempt, wait_fixed


DUMMY_DB = "dummy"


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
        self.dummy = self.host == DUMMY_DB

        if self.dummy:
            self.pool = None
        else:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                1, 3, user=self.user, password=self.password, host=self.host, port=self.port, database=self.db_name
            )

    def select_one_row(self, sql: str, parm_list: list) -> int:
        """
        Returns a single row from SQL and params, handling
        both the real and dummy database case.
        """
        if self.dummy:
            return 1
        else:
            return self.select_one_row_postgres(sql, parm_list)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(30))
    def select_one_row_postgres(self, sql: str, parm_list: list):
        """Returns a single row from postgres given SQL and params"""
        # Assuming we have a connection, try to do the database operation
        try:
            conn = self.pool.getconn()

            with conn.cursor() as cursor:
                # Run the sql
                cursor.execute(sql, parm_list)

                # Fetch results as a list of tuples
                rows = cursor.fetchall()

                # Check how many rows we affected
                rows_affected = len(rows)

                if rows_affected == 1:
                    # Success - return the tuple
                    return rows[0]
                else:
                    # Something went wrong
                    self.logger.error(
                        "select_one_row_postgres(): Error- queried" f" {rows_affected} rows, expected 1. SQL={sql}"
                    )
                    raise Exception(
                        "select_one_row_postgres(): Error- queried" f" {rows_affected} rows, expected 1. SQL={sql}"
                    )

        except OperationalError as conn_error:
            self.logger.error("select_one_row_postgres(): postgres OperationalError-" f" {conn_error}")
            # Reraise error
            raise conn_error

        except InterfaceError as int_error:
            self.logger.error(f"select_one_row_postgres(): postgres InterfaceError- {int_error}")
            # Reraise error
            raise int_error

        except psycopg2.ProgrammingError as prog_error:
            # A programming/SQL error - e.g. table does not exist. Don't
            # reconnect connection
            self.logger.error("select_one_row_postgres(): postgres ProgrammingError-" f" {prog_error}")
            # Reraise error
            raise prog_error

        except Exception as exception_info:
            # Any other error- likely to be a database error rather than
            # connection based
            self.logger.error(f"select_one_row_postgres(): unknown Error- {exception_info}")
            raise exception_info
        finally:
            self.pool.putconn(conn)

    def execute_single_dml_row(self, sql: str, parm_list: list):
        """This executes an INSERT, UPDATE or DELETE that should only affect
        one row. Since this is all in a with (context) block, rollback is
        called on failure and commit on success."""

        # Assuming we have a connection, try to do the database operation
        try:
            conn = self.pool.getconn()

            with conn.cursor() as cursor:
                # Run the sql
                cursor.execute(sql, parm_list)

                # Check how many rows we affected
                rows_affected = cursor.rowcount

                if rows_affected != 1:
                    # An exception in here will trigger a rollback
                    # which is good
                    self.logger.error(
                        "execute_single_dml_row(): Error- query"
                        f" affected {rows_affected} rows, expected 1."
                        f" SQL={sql}"
                    )
                    raise Exception(
                        "execute_single_dml_row(): Error- query"
                        f" affected {rows_affected} rows, expected 1."
                        f" SQL={sql}"
                    )

        except OperationalError as conn_error:
            self.logger.error(f"execute_single_dml_row(): postgres OperationalError- {conn_error}")
            # Reraise error
            raise conn_error

        except InterfaceError as int_error:
            self.logger.error(f"execute_single_dml_row(): postgres InterfaceError- {int_error}")
            # Reraise error
            raise int_error

        except psycopg2.ProgrammingError as prog_error:
            # A programming/SQL error - e.g. table does not exist. Don't
            # reconnect connection
            self.logger.error(f"execute_single_dml_row(): postgres ProgrammingError- {prog_error}")
            # Reraise error
            raise prog_error

        except Exception as exception_info:
            # Any other error- likely to be a database error rather than
            # connection based
            self.logger.error(f"execute_single_dml_row(): unknown Error- {exception_info}")
            raise exception_info
        finally:
            self.pool.putconn(conn)

    def execute_dml_row_within_transaction(
        self, sql: str, parm_list: list, transaction_cursor: psycopg2.extensions.cursor
    ):
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

        except OperationalError as conn_error:
            self.logger.error(f"execute_single_dml_row(): postgres OperationalError- {conn_error}")
            # Reraise error
            raise conn_error

        except InterfaceError as int_error:
            self.logger.error(f"execute_single_dml_row(): postgres InterfaceError- {int_error}")
            # Reraise error
            raise int_error

        except psycopg2.ProgrammingError as prog_error:
            # A programming/SQL error - e.g. table does not exist. Don't
            # reconnect connection
            self.logger.error(f"execute_single_dml_row(): postgres ProgrammingError- {prog_error}")
            # Reraise error
            raise prog_error

        except Exception as exception_info:
            # Any other error- likely to be a database error rather than
            # connection based
            self.logger.error(f"execute_single_dml_row(): unknown Error- {exception_info}")
            raise exception_info


#
# High level functions to do what we want specifically
#
class DataFileRow:
    """A class that abstracts the key fields of a MWA data_files row"""

    def __init__(self):
        self.observation_num = ""
        self.size = -1
        self.checksum = ""


def get_data_file_row(db_handler_object, full_filename: str, obs_id: int) -> DataFileRow:
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
        if db_handler_object.dummy:
            db_handler_object.logger.warning(
                f"{full_filename} get_data_file_row() Using dummy database"
                " connection. No data is really being queried."
            )
            time.sleep(2)  # simulate a slow transaction
            return None
        else:
            # Run query and get the data_files row info for this file
            obsid, size, checksum = db_handler_object.select_one_row(
                sql,
                (
                    filename,
                    obs_id,
                ),
            )

            data_files_row = DataFileRow()
            data_files_row.observation_num = int(obsid)
            data_files_row.size = int(size)
            data_files_row.checksum = checksum

            db_handler_object.logger.info(
                f"{full_filename} get_data_file_row() Successfully read from"
                f" data_files table {vars(data_files_row)}"
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
        if db_handler_object.dummy:
            db_handler_object.logger.warning(
                f"{filename} insert_data_file_row() Using dummy database"
                " connection. No data is really being inserted."
            )
            time.sleep(10)  # simulate a slow transaction
            return True
        else:
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

            db_handler_object.logger.info(
                f"{filename} insert_data_file_row() Successfully wrote into" " data_files table"
            )
            return True

    except Exception as upsert_exception:  # pylint: disable=broad-except
        db_handler_object.logger.error(
            f"{filename} insert_data_file_row() error inserting data_files"
            f" record in data_files table: {upsert_exception}. SQL was {sql}"
        )
        return False


def update_data_file_row_as_archived(
    db_handler_object,
    obsid: int,
    archive_filename: str,
    location: int,
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
                location,
                str(obsid),
                filename,
            ),
        )

        if db_handler_object.dummy:
            db_handler_object.logger.warning(
                f"{filename} update_data_file_row_as_archived() Using"
                " dummy database connection. No data is really being"
                " updated."
            )
            time.sleep(10)  # simulate a slow transaction
            return True
        else:
            db_handler_object.logger.info(
                f"{filename} update_data_file_row_as_archived() Successfully" " updated data_files table"
            )
            return True

    except Exception as upsert_exception:  # pylint: disable=broad-except
        db_handler_object.logger.error(
            f"{filename} update_data_file_row_as_archived() error updating"
            f" data_files record in data_files table: {upsert_exception}. SQL"
            f" was {sql}"
        )
        return False


def insert_calibration_fits_row(
    db_handler_object,
    transaction_cursor: Optional[psycopg2.extensions.cursor],
    obs_id: int,
    code_version: str,
    creator: str,
    fit_niter: int = 10,
    fit_limit: int = 20,
) -> Tuple[bool, int, Optional[psycopg2.extensions.cursor]]:
    """Inserts a new calibration_fits row and return the fit_id if successful
    This row represents the calibration 'header' for an obsid."""
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
        if db_handler_object.dummy:
            db_handler_object.logger.warning(
                "insert_calibration_fits_row(): Using dummy database"
                " connection. No data is really being inserted. SQL="
                f"'{sql}' Values: {sql_values}"
            )
            time.sleep(1)  # simulate a slow transaction
            return (True, fit_id)
        else:
            db_handler_object.execute_dml_row_within_transaction(sql, sql_values, transaction_cursor)

            db_handler_object.logger.info(
                f"{obs_id}: insert_calibration_fits_row() Successfully wrote "
                f"into calibration_fits table. fit_id={fit_id}"
            )
            return (True, fit_id)

    except Exception as insert_exception:  # pylint: disable=broad-except
        db_handler_object.logger.error(
            f"{obs_id}: insert_data_file_row() error inserting"
            f" calibration_fits record in table: {insert_exception}. SQL was"
            f" {sql} Values: {sql_values}"
        )
        db_handler_object.con.rollback()
        return (False, None)


def insert_calibration_solutions_row(
    db_handler_object,
    transaction_cursor: Optional[psycopg2.extensions.cursor],
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
    Unless using a DUMMY db_handler object, we assume that caller is
    passing in a valid transaction cursor (psycopg2.cursor) which means
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
        if db_handler_object.dummy:
            db_handler_object.logger.warning(
                "insert_calibration_fits_row(): Using dummy database"
                " connection. No data is really being inserted. SQL="
                f"'{sql}' Values: {sql_values}"
            )
            time.sleep(1)  # simulate a slow transaction
            return True
        else:
            db_handler_object.execute_dml_row_within_transaction(sql, sql_values, transaction_cursor)

            db_handler_object.logger.info(
                f"{obs_id} tile {tile_id}: insert_calibration_solutions_row()"
                " Successfully wrote into insert_calibration_solutions table"
            )
            return True

    except Exception as insert_exception:  # pylint: disable=broad-except
        db_handler_object.logger.error(
            f"{obs_id}: insert_calibration_solutions_row() error inserting"
            " insert_calibration_solutions record in table:"
            f" {insert_exception}. SQL was {sql} Values {sql_values}"
        )
        return False
