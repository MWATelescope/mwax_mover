"""Query and DML functions for the MWA metadata database's data_files table.

Covers reading a file's recorded size/checksum, inserting a new row on receipt
of a file, and marking a row as archived once it has been shipped to Pawsey.
"""

import logging
import os

import psycopg.errors

from mwax_mover.db.handler import MWAXDBHandler
from mwax_mover.utils import ArchiveLocation

logger = logging.getLogger(__name__)


class DataFileRow:
    """A class that abstracts the key fields of a MWA data_files row"""

    def __init__(self):
        """Initialize a DataFileRow with default values."""
        self.observation_num: int = 0
        self.size = -1
        self.checksum = ""


def get_data_file_row(db_handler_object: MWAXDBHandler, full_filename: str, obs_id: int) -> DataFileRow:
    """Retrieve a data file record from the database.

    Args:
        db_handler_object: MWAXDBHandler instance.
        full_filename: Full path to the data file.
        obs_id: The observation ID.

    Returns:
        A DataFileRow instance containing the file's metadata.

    Raises:
        Exception: If the database query fails.
    """
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

        logger.info(f"{full_filename} Successfully read from data_files table {vars(data_files_row)}")
        return data_files_row

    except Exception as select_exception:
        logger.error(
            f"{full_filename} error selecting data_files record in data_files table: {select_exception}. SQL was {sql}"
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
            deleted,
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
                False,
                remote_archived,
                checksum_type,
                checksum,
                trigger_id,
            ),
        )

        logger.info(f"{filename} Successfully wrote into data_files table")
        return True

    except psycopg.errors.ForeignKeyViolation:
        # In this scenario it means M&C deleted the observation BUT the metafits was already generated
        # so mwax_u2s et al. thought it was still a real observation
        # we should just delete this file and move on
        logger.warning(f"{filename} observation_num {obsid} has been deleted by M&C.Deleting this data file.")
        os.remove(archive_filename)

        # returning True here will cause the item to be ack'd off the queue so it is not tried again
        # but we need the caller to check if the file still exists- otherwise we may archive it!
        return True

    except Exception as upsert_exception:
        logger.exception(
            upsert_exception,
            f"{filename} error inserting data_files record in data_files table. SQL was {sql}",
        )
        return False


def update_data_file_row_as_archived(
    db_handler_object,
    obsid: int,
    archive_filename: str,
    location: ArchiveLocation,
    bucket: str,
    folder: str | None,
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

        logger.info(f"{filename} Successfully updated data_files table")
        return True

    except Exception:
        logger.exception(f"{filename} error updating data_files record in data_files table. SQL was {sql}")
        return False
