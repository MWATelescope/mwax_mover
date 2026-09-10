"""PostgreSQL database connection pooling and query execution for the MWA metadata database.

Provides MWAXDBHandler, which wraps a psycopg / psycopg_pool connection pool with
retry logic (via tenacity) for transient connection failures. The domain-specific
query and DML functions that use it live alongside it in this package, split by
table/domain: data_files.py and calibration.py.
"""

import logging
from configparser import ConfigParser

import psycopg
import psycopg.errors
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_fixed,
)

from mwax_mover.constants import SECTION_MWA_DATABASE
from mwax_mover.core.config import read_config

logger = logging.getLogger(__name__)


class MWAXDBHandler:
    """Class which takes care of the primitive database functions"""

    def __init__(self, host: str, port: int, db_name, user: str, password: str, ssl_mode: str | None = None):
        """Initialize the MWAXDBHandler with database connection parameters.

        Args:
            host: The database host address.
            port: The database port number.
            db_name: The database name.
            user: The database user.
            password: The database password.
            ssl_mode: The suffix for ssl e.g. '?sslmode=require' or None to not specify
        """
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.pool: ConnectionPool
        if self.host != "dummy":
            self.pool = ConnectionPool(
                min_size=1,
                max_size=3,
                open=False,
                check=ConnectionPool.check_connection,
                conninfo=(
                    f"postgresql://{user}:{password}@{host}:{port}/{db_name}{'' if ssl_mode is None else ssl_mode}"
                ),
            )

    @classmethod
    def from_config(cls, config: ConfigParser) -> "MWAXDBHandler":
        """Build an MWAXDBHandler from the [mwa database] config section.

        Reads host, db, user, pass, and port from SECTION_MWA_DATABASE,
        applying the same "only base64-decode a real password" predicate
        every caller used to write out by hand: pass is decoded unless db
        is "dummy" (see docs/CLEANUP.md 1.2).

        Args:
            config: A ConfigParser instance with the configuration already loaded.

        Returns:
            A new MWAXDBHandler for the database described by [mwa database].
        """
        db_name = read_config(config, SECTION_MWA_DATABASE, "db")
        return cls(
            host=read_config(config, SECTION_MWA_DATABASE, "host"),
            port=int(read_config(config, SECTION_MWA_DATABASE, "port")),
            db_name=db_name,
            user=read_config(config, SECTION_MWA_DATABASE, "user"),
            password=read_config(config, SECTION_MWA_DATABASE, "pass", db_name != "dummy"),
        )

    def close(self):
        """Close the database connection pool if it is open."""
        # This set of 3 ifs covers all cases where the pool may not be instantiated or open
        if getattr(self, "pool", None) is None:
            return
        if self.pool is None:
            return
        if self.pool.closed:
            return
        self.pool.close()

    def __del__(self):
        self.close()

    def start_database_pool(self):
        """Open the database connection pool if it is closed."""
        # Check we are not already started
        if self.pool.closed:
            self.pool.open(wait=True)

    def select_one_row_postgres(self, sql: str, parm_list):
        """Retrieve a single row from the database.

        Args:
            sql: SQL query string.
            parm_list: Parameter list for the SQL query.

        Returns:
            A dictionary representing the single row from the query result.
        """
        # Assuming we have a connection, try to do the database operation
        rows = self.select_postgres(sql, parm_list, 1)

        # Just return the first row
        return rows[0]

    def select_many_rows_postgres(self, sql: str, parm_list):
        """Retrieve multiple rows from the database.

        Args:
            sql: SQL query string.
            parm_list: Parameter list for the SQL query.

        Returns:
            A list of dictionaries representing rows from the query result.
        """
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
        """Execute a SELECT query against the database with retry logic.

        Args:
            sql: SQL query string.
            parm_list: Parameter list for the SQL query.
            expected_rows: Expected number of rows. If None, any number is accepted.
                If specified, raises an exception if the actual count doesn't match.

        Returns:
            A list of dictionaries representing rows from the query result.

        Raises:
            Exception: If expected_rows is specified and the row count doesn't match.
        """
        # Assuming we have a connection, try to do the database operation
        try:
            with (
                self.pool.connection() as conn,
                conn.cursor(row_factory=dict_row) as cursor,
            ):
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
                        logger.error(f"Error- queried {rows_affected} rows, expected 1. SQL={sql}")
                        raise Exception(f"Error- queried {rows_affected} rows, expected 1. SQL={sql}")
                else:
                    # We don't know how many rows, so cool, return them
                    return rows

        except Exception:
            logger.exception("postgres exception")
            raise

    def execute_single_dml_row(self, sql: str, parm_list):
        """Execute an INSERT, UPDATE, or DELETE statement affecting exactly one row.

        Automatically commits on success and rolls back on failure within
        a transaction context. Connection-failure retries are handled by
        execute_dml, which this delegates to.

        NOTE: this method used to carry its own @retry decorator identical to
        execute_dml's. Since it does nothing but call execute_dml, the two
        nested retries multiplied: up to 9 attempts rather than 3, and a
        worst case of roughly 5 minutes (3 x 30s inner waits, repeated 3 times)
        instead of 1. The outer decorator has been removed so the retry policy
        is defined in exactly one place.

        Args:
            sql: SQL DML statement.
            parm_list: Parameter list for the SQL statement.

        Raises:
            Exception: If the statement doesn't affect exactly one row or
                if a database error occurs.
        """
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
        """Execute an INSERT, UPDATE, or DELETE statement with retry logic.

        Automatically commits on success and rolls back on failure within
        a transaction context.

        Args:
            sql: SQL DML statement.
            parm_list: Parameter list for the SQL statement.
            expected_rows: Expected number of rows affected. If None, any number is accepted.
                If specified, raises an exception if the actual count doesn't match.

        Raises:
            Exception: If expected_rows is specified and the row count doesn't match,
                or if a database error occurs.
            psycopg.errors.ForeignKeyViolation: If a foreign key constraint is violated.
        """

        # Assuming we have a connection, try to do the database operation
        try:
            with self.pool.connection() as conn, conn.cursor() as cursor:
                # Run the sql
                cursor.execute(sql, parm_list)
                conn.commit()

                # Check how many rows we affected
                rows_affected = cursor.rowcount

                if expected_rows:
                    if rows_affected != expected_rows:
                        # An exception in here will trigger a rollback
                        # which is good
                        logger.error(f"Error- query affected {rows_affected} rows, expected {expected_rows}. SQL={sql}")
                        raise Exception(
                            f"Error- query affected {rows_affected} rows, expected {expected_rows}. SQL={sql}"
                        )

        except psycopg.errors.ForeignKeyViolation:
            # Trying to insert or update but a value of a field violates the FK constraint-
            # e.g. insert into data_files fails due to observation_num not existing in mwa_setting.starttime
            # We need to reraise the error so our caller can handle in this "insert_data_file_row" case!
            logger.exception("postgres ForeignKeyViolation")
            # Reraise error
            raise

        except Exception:
            # Any other error- likely to be a database error rather than
            # connection based
            logger.exception("postgres Exception")
            raise

    def execute_dml_row_within_transaction(self, sql, parm_list, transaction_cursor: psycopg.Cursor):
        """Execute an INSERT, UPDATE, or DELETE statement within a transaction.

        This method does not handle commit/rollback - those are the caller's responsibility.
        The caller must obtain a connection and cursor, call this method (potentially
        multiple times), and then commit or rollback the transaction.

        Args:
            sql: SQL DML statement.
            parm_list: Parameter list for the SQL statement.
            transaction_cursor: A psycopg.Cursor object from an active transaction.

        Raises:
            Exception: If the statement doesn't affect exactly one row or
                if a database error occurs.
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
                logger.error(f"Error- query affected {rows_affected} rows, expected 1. SQL={sql}")
                raise Exception(f"Error- query affected {rows_affected} rows, expected 1. SQL={sql}")

        except Exception:
            logger.exception("postgres Exception")
            raise
