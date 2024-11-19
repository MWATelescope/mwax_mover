import logging
from mwax_mover.mwax_db import get_data_file_row, DataFileRow
from tests_common import get_test_db_handler, run_create_test_db_object_script


def test_connection_pool():
    logger = logging.getLogger(__name__)

    run_create_test_db_object_script(logger, "tests/test_mwax_db.sql")

    db_handler = get_test_db_handler(logger)
    db_handler.start_database_pool()

    # Get a connection and do one insert and one rollback of an insert and then check results
    db_handler.execute_dml("INSERT INTO test_table VALUES (4, 'committed')", None, 1)

    # Check table can be read in a loop (in theory this should use/reuse connections)
    for i in range(0, 6):
        results = db_handler.select_many_rows_postgres("SELECT id, name FROM test_table WHERE id>=4 ORDER BY id", None)

        assert len(results) == 1
        assert results[0]["id"] == 4
        assert results[0]["name"] == "committed"

    db_handler.stop_database_pool()


def test_dbhandler_get_data_file_row():
    logger = logging.getLogger(__name__)

    run_create_test_db_object_script(logger, "tests/test_mwax_db_dbhandler.sql")

    db_handler = get_test_db_handler(logger)
    db_handler.start_database_pool()

    obs_id = 1234567890
    filename = "/visdata/incoming/1234567890_202411191234_ch123_000.fits"

    row: DataFileRow = get_data_file_row(db_handler, filename, obs_id)

    # Check it!
    assert row.checksum == "1a2b3c4d5e6f"
    assert row.observation_num == 1234567890
    assert row.size == 1024
