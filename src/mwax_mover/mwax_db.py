"""Module for database operations"""

import os
import threading
import time
from typing import Optional
import psycopg2
from psycopg2 import InterfaceError, OperationalError
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_not_exception_type


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
        self.con = None

        # We use a mutex so we only run one db command at a time
        self.db_lock = threading.Lock()

    def connect(self):
        """Attempts to connect to the database"""
        try:
            self.logger.info(
                "MWAXDBHandler.connect(): Attempting to connect to database: "
                f"{self.user}@{self.host}:{self.port}/{self.db_name}"
            )

            if not self.dummy:
                self.con = psycopg2.connect(
                    host=self.host,
                    database=self.db_name,
                    user=self.user,
                    password=self.password,
                )

            self.logger.info(
                "MWAXDBHandler.connect(): Connected to database:" f" {self.user}@{self.host}:{self.port}/{self.db_name}"
            )

        except OperationalError as err:
            self.logger.error(
                "MWAXDBHandler.connect(): error connecting to database:"
                f" {self.user}@{self.host}:{self.port}/{self.db_name} Error:"
                f" {err}"
            )
            raise err

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
        # We have a mutex here to ensure only 1 user of the connection at a
        # time
        with self.db_lock:
            # Do we have a database connection?
            if self.con is None:
                self.connect()

            # Assuming we have a connection, try to do the database operation
            try:
                with self.con as con:
                    with con.cursor() as cursor:
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
                                "select_one_row_postgres(): Error- queried"
                                f" {rows_affected} rows, expected 1. SQL={sql}"
                            )
                            raise Exception(
                                "select_one_row_postgres(): Error- queried"
                                f" {rows_affected} rows, expected 1. SQL={sql}"
                            )

            except OperationalError as conn_error:
                # Our connection is toast. Clear it so we attempt a reconnect
                self.con = None
                self.logger.error("select_one_row_postgres(): postgres OperationalError-" f" {conn_error}")
                # Reraise error
                raise conn_error

            except InterfaceError as int_error:
                # Our connection is toast. Clear it so we attempt a reconnect
                self.con = None
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

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_fixed(30),
        retry=retry_if_not_exception_type(psycopg2.errors.ForeignKeyViolation),
    )
    def execute_single_dml_row(self, sql: str, parm_list: list):
        """This executes an INSERT, UPDATE or DELETE that should only affect
        one row"""

        # We have a mutex here to ensure only 1 user of the connection at a
        # time
        with self.db_lock:
            # Do we have a database connection?
            if self.con is None:
                self.connect()

            # Assuming we have a connection, try to do the database operation
            try:
                with self.con as con:
                    with con.cursor() as cursor:
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
                # Our connection is toast. Clear it so we attempt a reconnect
                self.con = None
                self.logger.error(f"execute_single_dml_row(): postgres OperationalError- {conn_error}")
                # Reraise error
                raise conn_error

            except InterfaceError as int_error:
                # Our connection is toast. Clear it so we attempt a reconnect
                self.con = None
                self.logger.error(f"execute_single_dml_row(): postgres InterfaceError- {int_error}")
                # Reraise error
                raise int_error

            except psycopg2.errors.ForeignKeyViolation as fk_error:
                # Trying to insert or update but a value of a field violates the FK constraint-
                # e.g. insert into data_files fails due to observation_num not existing in mwa_setting.starttime
                # We need to reraise the error so our caller can handle in this "insert_data_file_row" case!
                self.logger.error(f"execute_single_dml_row(): postgres ForeignKeyViolation- {fk_error}")
                # Reraise error
                raise fk_error

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
            # We have a mutex here to ensure only 1 user of the connection at
            # a time
            with db_handler_object.db_lock:
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
    deleted = False
    remote_archived = False
    filename = os.path.basename(archive_filename)

    if trigger_id == -1:
        trigger_id = None

    # We actually do an insert
    sql = ""

    try:
        if db_handler_object.dummy:
            # We have a mutex here to ensure only 1 user of
            # the connection at a time
            with db_handler_object.db_lock:
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
                deleted,
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
                    deleted,
                    checksum_type,
                    checksum,
                    trigger_id,
                ),
            )

            db_handler_object.logger.info(
                f"{filename} insert_data_file_row() Successfully wrote into" " data_files table"
            )
            return True

    except psycopg2.errors.ForeignKeyViolation:
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
            # We have a mutex here to ensure only 1 user of
            # the connection at a time
            with db_handler_object.db_lock:
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
