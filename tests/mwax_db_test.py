import logging
from tests_common import get_test_db_handler, create_test_database


def test_connection_pool():
    logger = logging.getLogger(__name__)

    db_handler = get_test_db_handler(logger)

    db_handler.start_database_pool()

    create_test_database(db_handler, "tests/test_mwax_db.sql")

    # Get a connection and do one insert and one rollback of an insert and then check results
    db_handler.execute_dml("INSERT INTO test_table VALUES (4, 'committed')", None, 1)

    # Check table can be read in a loop (in theory this should use/reuse connections)
    for i in range(0, 6):
        results = db_handler.select_many_rows_postgres("SELECT id, name FROM test_table WHERE id>=4 ORDER BY id", None)

        assert len(results) == 1
        assert results[0]["id"] == 4
        assert results[0]["name"] == "committed"

    db_handler.stop_database_pool()
