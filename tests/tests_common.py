import logging
from configparser import ConfigParser
from mwax_mover.mwax_db import MWAXDBHandler


def get_test_db_handler(logger: logging.Logger):
    #
    # For these tests to work, please create a config file
    #
    config = ConfigParser()
    config.read_file(open("tests/tests_common.cfg", "r", encoding="utf-8"))

    host = config.get("test database", "host")
    port = config.getint("test database", "port")
    db_name = config.get("test database", "db")
    user = config.get("test database", "user")
    password = config.get("test database", "pass")

    return MWAXDBHandler(logger, host, port, db_name, user, password)


def create_test_database(test_db_handler: MWAXDBHandler, creation_sql_filename):
    # Connect to local db, drop and then recreate the database
    # expects:
    # * Database to already be created
    # * test_db_handler should already have pool started
    # * Database name should NOT be "mwa" - just in case we accidently run this in prod!
    # * Ditto for hostname- should be localhost - just in case!

    assert (
        test_db_handler.pool.closed is False
    ), "Ensure you call MWAXDBHandler.start_database_pool() before calling this!"

    assert test_db_handler.db_name != "mwa"
    assert test_db_handler.host == "localhost"

    # Read script
    with open(creation_sql_filename, "r") as file:
        creation_sql_script = file.read()

    with test_db_handler.pool.getconn() as conn:
        conn.execute(creation_sql_script)  # type: ignore