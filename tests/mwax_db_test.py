import logging
import os
import mwax_mover.utils
from mwax_mover.mwax_db import get_data_file_row, insert_data_file_row, update_data_file_row_as_archived, DataFileRow
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


def test_dbhandler_insert_get_and_update_data_file_row():
    logger = logging.getLogger(__name__)

    run_create_test_db_object_script(logger, "tests/test_mwax_db_dbhandler.sql")

    db_handler = get_test_db_handler(logger)
    db_handler.start_database_pool()

    obs_id = 1234567890
    filename = "/visdata/incoming/1234567890_202411191234_ch123_000.fits"
    base_filename = os.path.basename(filename)
    md5_sum = "1a2b3c4d5e6f"
    file_size = 28765420
    filetype = mwax_mover.utils.MWADataFileType.MWAX_VISIBILITIES.value
    checksum_type = 1  # md5
    hostname = "test"

    #
    # Test insert_data_file_row
    #
    insert_success = insert_data_file_row(
        db_handler,
        obsid=obs_id,
        archive_filename=filename,
        filetype=filetype,
        hostname=hostname,
        checksum_type=checksum_type,
        checksum=md5_sum,
        trigger_id=None,
        file_size=file_size,
    )
    assert insert_success

    #
    # Test get_data_file_row
    #
    row: DataFileRow = get_data_file_row(db_handler, filename, obs_id)

    assert row.checksum == md5_sum
    assert row.observation_num == obs_id
    assert row.size == file_size

    #
    # Test update_data_file_row_as_archived
    #
    location = mwax_mover.utils.ArchiveLocation.Acacia
    bucket = "mwaingest-12345"

    update_success = update_data_file_row_as_archived(
        db_handler, obsid=obs_id, archive_filename=filename, location=location, bucket=bucket, folder=None
    )

    assert update_success

    #
    # Do manual SQL to verify
    #
    sql = """
          SELECT observation_num, filetype, size, filename, modtime, host, remote_archived, deleted,
                 location, deleted_timestamp, checksum_type, checksum, folder, bucket, trigger_id
          FROM data_files
          WHERE observation_num=%s AND filename=%s
          """

    sql_row = db_handler.select_one_row_postgres(sql, [str(obs_id), base_filename])

    assert sql_row["observation_num"] == obs_id
    assert sql_row["filetype"] == filetype
    assert sql_row["size"] == file_size
    assert sql_row["filename"] == base_filename
    assert sql_row["modtime"] is not None
    assert sql_row["host"] == hostname
    assert sql_row["remote_archived"]
    assert not sql_row["deleted"]
    assert sql_row["location"] == location.value
    assert sql_row["deleted_timestamp"] is None
    assert sql_row["checksum_type"] == checksum_type
    assert sql_row["checksum"] == md5_sum
    assert sql_row["folder"] is None
    assert sql_row["bucket"] == bucket
    assert sql_row["trigger_id"] is None
