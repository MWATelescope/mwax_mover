import logging
from configparser import ConfigParser
import os
from pathlib import Path
import shutil
from mwax_mover.mwax_db import MWAXDBHandler


def setup_test_directories(test_filename: str) -> None:
    """
    Ensure all configured directories exist. If a directory already exists,
    clear its contents (files/subdirectories) but keep the directory itself.
    """
    # The directories we create will be based on the test file name
    # e.g. /data/mwax_mover_testing/test001
    base = f"/data/mwax_mover_testing/{os.path.splitext(os.path.basename(test_filename))[0]}/"

    paths = [
        "/dev/shm/mwax",
        "/voltdata/incoming",
        "/voltdata/outgoing",
        "/voltdata/dont_archive",
        "/vulcan/packet_stats_dump",
        "/vulcan/packet_stats_destination",
        "/visdata/incoming",
        "/visdata/dont_archive",
        "/visdata/processing_stats",
        "/visdata/outgoing",
        "/vulcan/mwax_stats_dump",
        "/visdata/cal_outgoing",
        "/vulcan/metafits",
        "/bf_pipe",
        "/voltdata/bf/incoming",
        "/voltdata/bf/outgoing",
        "/voltdata/bf/dont_archive",
        "/vulcan/mwax_aocal",
    ]

    def _is_dangerous_path(p: Path) -> bool:
        s = str(p.resolve())
        if s == "/":
            return True
        # Guardrail: require deeper-than /data/mwax_mover_testing
        if len(p.resolve().parts) < 4:
            return True
        return False

    def _clear_directory(dir_path: Path) -> None:
        for entry in dir_path.iterdir():
            if entry.is_symlink() or entry.is_file():
                entry.unlink(missing_ok=True)
            elif entry.is_dir():
                shutil.rmtree(entry)
            else:
                # For FIFOs/sockets/etc.
                entry.unlink(missing_ok=True)

    for p_str in paths:
        p = Path(f"{base}{p_str}")

        if _is_dangerous_path(p):
            raise ValueError(f"Refusing to operate on potentially dangerous path: {p}")

        if p.exists():
            if not p.is_dir():
                raise NotADirectoryError(f"Path exists but is not a directory: {p}")
            _clear_directory(p)
        else:
            p.mkdir(parents=True, exist_ok=True)


def get_test_db_handler(logger: logging.Logger):
    #
    # For these tests to work, please create a config file
    # which has details to a local TEST database.
    #
    # FOR THE LOVE OF GOD DO NOT USE A PROD DATABASE!
    #
    config = ConfigParser()
    config.read_file(open("tests/tests_common.cfg", "r", encoding="utf-8"))

    host = config.get("test database", "host")
    port = config.getint("test database", "port")
    db_name = config.get("test database", "db")
    user = config.get("test database", "user")
    password = config.get("test database", "pass")

    return MWAXDBHandler(logger, host, port, db_name, user, password)


def run_create_test_db_object_script(logger: logging.Logger, creation_sql_filename):
    # Connect to test db, drop and then recreate the database objects
    # expects:
    # * db mwax_mover_test must already exist
    # * Database name should NOT be "mwa" - just in case we accidently run this in prod!
    # * Ditto for hostname- should be localhost - just in case!

    test_db_handler = get_test_db_handler(logger)
    test_db_handler.start_database_pool()

    assert test_db_handler.db_name == "mwax_mover_test"
    assert test_db_handler.host == "localhost"

    # Read script
    with open(creation_sql_filename, "r") as file:
        creation_sql_script = file.read()

    with test_db_handler.pool.getconn() as conn:
        conn.execute(creation_sql_script)  # type: ignore
