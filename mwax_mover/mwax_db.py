import os
import psycopg2
from psycopg2 import InterfaceError, OperationalError
from tenacity import retry, stop_after_attempt, wait_fixed
from typing import Optional
import threading
import time

DUMMY_DB = "dummy"


class MWAXDBHandler:
    def __init__(self, logger, host: str, port: int, db, user: str, password: str):
        self.logger = logger
        self.host = host
        self.port = port
        self.db = db
        self.user = user
        self.password = password
        self.dummy = self.host == DUMMY_DB
        self.con = None

        # We use a mutex so we only run one db command at a time
        self.db_lock = threading.Lock()

    def connect(self):
        try:
            self.logger.info(
                f"MWAXDBHandler.connect(): Attempting to connect to database: "
                f"{self.user}@{self.host}:{self.port}/{self.db}"
            )

            self.con = psycopg2.connect(
                host=self.host, database=self.db, user=self.user, password=self.password
            )

            self.logger.info(
                f"MWAXDBHandler.connect(): Connected to database: {self.user}@{self.host}:{self.port}/{self.db}"
            )

        except OperationalError as err:
            self.logger.error(
                f"MWAXDBHandler.connect(): error connecting to database: "
                f"{self.user}@{self.host}:{self.port}/{self.db} Error: {err}"
            )
            raise err

    def select_one_row(self, sql: str, parm_list: list) -> int:
        if self.dummy:
            return 1
        else:
            return self.select_one_row_postgres(sql, parm_list)

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(30))
    def select_one_row_postgres(self, sql: str, parm_list: list):
        # We have a mutex here to ensure only 1 user of the connection at a time
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
                                f"select_one_row_postgres(): Error- queried {rows_affected} rows, expected 1. SQL={sql}"
                            )
                            raise Exception(
                                f"select_one_row_postgres(): Error- queried {rows_affected} rows, expected 1. SQL={sql}"
                            )

            except OperationalError as conn_error:
                # Our connection is toast. Clear it so we attempt a reconnect
                self.con = None
                self.logger.error(
                    f"select_one_row_postgres(): postgres OperationalError- {conn_error}"
                )
                # Reraise error
                raise conn_error

            except InterfaceError as int_error:
                # Our connection is toast. Clear it so we attempt a reconnect
                self.con = None
                self.logger.error(
                    f"select_one_row_postgres(): postgres InterfaceError- {int_error}"
                )
                # Reraise error
                raise int_error

            except psycopg2.ProgrammingError as prog_error:
                # A programming/SQL error - e.g. table does not exist. Don't reconnect connection
                self.logger.error(
                    f"select_one_row_postgres(): postgres ProgrammingError- {prog_error}"
                )
                # Reraise error
                raise prog_error

            except Exception as exception_info:
                # Any other error- likely to be a database error rather than connection based
                self.logger.error(
                    f"select_one_row_postgres(): unknown Error- {exception_info}"
                )
                raise exception_info

    @retry(stop=stop_after_attempt(3), wait=wait_fixed(30))
    def execute_single_dml_row(self, sql: str, parm_list: list):
        """This executes an INSERT, UPDATE or DELETE that should only affect one row"""

        # We have a mutex here to ensure only 1 user of the connection at a time
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
                            # An exception in here will trigger a rollback which is good
                            self.logger.error(
                                f"execute_single_dml_row(): Error- query affected {rows_affected} rows, expected 1. SQL={sql}"
                            )
                            raise Exception(
                                f"execute_single_dml_row(): Error- query affected {rows_affected} rows, expected 1. SQL={sql}"
                            )

            except OperationalError as conn_error:
                # Our connection is toast. Clear it so we attempt a reconnect
                self.con = None
                self.logger.error(
                    f"execute_single_dml_row(): postgres OperationalError- {conn_error}"
                )
                # Reraise error
                raise conn_error

            except InterfaceError as int_error:
                # Our connection is toast. Clear it so we attempt a reconnect
                self.con = None
                self.logger.error(
                    f"execute_single_dml_row(): postgres InterfaceError- {int_error}"
                )
                # Reraise error
                raise int_error

            except psycopg2.ProgrammingError as prog_error:
                # A programming/SQL error - e.g. table does not exist. Don't reconnect connection
                self.logger.error(
                    f"execute_single_dml_row(): postgres ProgrammingError- {prog_error}"
                )
                # Reraise error
                raise prog_error

            except Exception as exception_info:
                # Any other error- likely to be a database error rather than connection based
                self.logger.error(
                    f"execute_single_dml_row(): unknown Error- {exception_info}"
                )
                raise exception_info


#
# High level functions to do what we want specifically
#
class DataFileRow:
    def __init__(self):
        self.observation_num = ""
        self.size = -1
        self.checksum = ""


# Return a data file row instance on success or None on Failure
def get_data_file_row(db_handler_object, full_filename: str) -> DataFileRow:
    # Prepare the fields
    # immediately add this file to the db so we insert a record into metadata data_files table
    filename = os.path.basename(full_filename)

    sql = """SELECT observation_num,                    
                    size,
                    checksum
            FROM data_files
            WHERE filename = %s"""

    try:
        # Run query and get the data_files row info for this file
        obsid, size, checksum = db_handler_object.select_one_row(sql, (filename,))

        data_files_row = DataFileRow()
        data_files_row.observation_num = int(obsid)
        data_files_row.size = int(size)
        data_files_row.checksum = checksum

        if db_handler_object.dummy:
            # We have a mutex here to ensure only 1 user of the connection at a time
            with db_handler_object.db_lock:
                db_handler_object.logger.warning(
                    f"{full_filename} get_data_file_row() Using dummy database connection. "
                    f"No data is really being queried."
                )
                time.sleep(2)  # simulate a slow transaction
                return None
        else:
            db_handler_object.logger.info(
                f"{full_filename} get_data_file_row() Successfully read from data_files table {vars(data_files_row)}"
            )
            return data_files_row

    except Exception as upsert_exception:
        db_handler_object.logger.error(
            f"{full_filename} get_data_file_row() error upserting data_files record in "
            f"data_files table: {upsert_exception}. SQL was {sql}"
        )
        return None


def insert_data_file_row(
    db_handler_object,
    obsid: int,
    archive_filename: str,
    filetype: int,
    hostname: str,
    checksum_type: int,
    checksum: str,
) -> bool:
    # Prepare the fields
    # immediately add this file to the db so we insert a record into metadata data_files table
    filename = os.path.basename(archive_filename)
    file_size = os.stat(archive_filename).st_size
    deleted = False
    remote_archived = False

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
                deleted,                                
                checksum_type,
                checksum)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

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
            ),
        )

        if db_handler_object.dummy:
            # We have a mutex here to ensure only 1 user of
            # the connection at a time
            with db_handler_object.db_lock:
                db_handler_object.logger.warning(
                    f"{filename} insert_data_file_row() Using dummy database connection. "
                    f"No data is really being inserted."
                )
                time.sleep(10)  # simulate a slow transaction
                return True
        else:
            db_handler_object.logger.info(
                f"{filename} insert_data_file_row() Successfully wrote into data_files table"
            )
            return True

    except Exception as upsert_exception:
        db_handler_object.logger.error(
            f"{filename} insert_data_file_row() error inserting data_files record in "
            f"data_files table: {upsert_exception}. SQL was {sql}"
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
                    f"{filename} update_data_file_row_as_archived() Using dummy database connection. "
                    f"No data is really being updated."
                )
                time.sleep(10)  # simulate a slow transaction
                return True
        else:
            db_handler_object.logger.info(
                f"{filename} update_data_file_row_as_archived() Successfully updated data_files table"
            )
            return True

    except Exception as upsert_exception:
        db_handler_object.logger.error(
            f"{filename} update_data_file_row_as_archived() error updating data_files record in "
            f"data_files table: {upsert_exception}. SQL was {sql}"
        )
        return False
