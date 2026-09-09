"""Watch-queue-worker that validates and archives mwacache files to Pawsey Long-Term Storage.

Runs on the mwacache servers at Curtin. For each file, validates the filename,
verifies its size and MD5 checksum against the metadata database, archives
it to Acacia or Banksia via rclone, updates the metadata database to record
the archive location and bucket, then deletes the local copy.
"""

import logging
import os

from mwax_mover import constants
from mwax_mover.archive import archiver
from mwax_mover.db import data_files
from mwax_mover.db.data_files import DataFileRow, get_data_file_row
from mwax_mover.db.handler import MWAXDBHandler
from mwax_mover.filesystem.files import do_checksum_md5, remove_file
from mwax_mover.filesystem.naming import (
    ArchiveLocation,
    ValidationData,
    get_bucket_name_for_location,
    validate_filename,
)
from mwax_mover.queues.watch_queue_worker import MWAXPriorityWatchQueueWorker

logger = logging.getLogger(__name__)


class PawseyOutgoingProcessor(MWAXPriorityWatchQueueWorker):
    """Validates and archives mwacache files to Pawsey Long-Term Storage.

    Instantiated by MWACacheArchiveProcessor. See the module docstring for detail.
    """

    def __init__(
        self,
        name: str,
        metafits_path: str,
        watch_paths_exts: list[tuple[str, str]],
        high_priority_correlator_projects: list[str],
        high_priority_vcs_projects: list[str],
        db_handler_object: MWAXDBHandler,
        s3_profile: str,
        archive_to_location: ArchiveLocation,
        rclone_check_wait_secs: int,
        recursive: bool = False,
    ):
        """Initialize the PawseyOutgoingProcessor.

        Args:
            name: Processor name for logging and identification.
            metafits_path: Path to the metafits file for priority detection.
            watch_paths_exts: List of (directory, file_extension) tuples to monitor.
            high_priority_correlator_projects: List of high-priority correlator project IDs.
            high_priority_vcs_projects: List of high-priority VCS project IDs.
            db_handler_object: Database handler for the MWA metadata database.
            s3_profile: rclone profile name to upload with (see rclone.conf).
            archive_to_location: Target archive location (Acacia, Banksia, or AcaciaMWA).
            rclone_check_wait_secs: Number of seconds to wait between rclone copy and
                rclone check (to allow banksia VSS's to sync)
            recursive: Whether to watch each incoming path's subdirectories as
                well. Comes from the per-host `recursive` config option.
        """
        super().__init__(
            name,
            metafits_path,
            watch_paths_exts,
            mode=constants.MODE_WATCH_DIR_FOR_RENAME,
            exclude_pattern=".part*",
            high_priority_correlator_projects=high_priority_correlator_projects,
            high_priority_vcs_projects=high_priority_vcs_projects,
            recursive=recursive,
            requeue_to_eoq_on_failure=True,
        )
        self.db_handler_object = db_handler_object
        self.s3_profile = s3_profile
        self.archive_to_location = archive_to_location
        self.rclone_check_wait_secs = rclone_check_wait_secs

    def handler(self, item: str) -> bool:
        """Validate and archive a mwacache file to Pawsey Long-Term Storage.

        Validates the filename, verifies file size and MD5 checksum against remote
        metadata, archives to Acacia or Banksia via rclone, updates the MRO metadata
        database with archive location, and deletes the local source file.

        Args:
            item: Full path to the mwacache file to archive.

        Returns:
            True if the file was successfully archived and deleted, False otherwise.

        Raises:
            NotImplementedError: If the archive location is not Acacia, Banksia, or AcaciaMWA.
        """
        logger.info(f"{item}: Started...")

        # validate the filename
        val: ValidationData = validate_filename(item, self.metafits_path)

        # do some sanity checks!
        if val.valid:
            # Get the file size
            actual_file_size = os.stat(item).st_size
            logger.debug(f"{item}: file size on disk is {actual_file_size} bytes")

            # Lookup file from db
            data_files_row: DataFileRow = get_data_file_row(self.db_handler_object, item, val.obs_id)
            database_file_size = data_files_row.size

            # Check for 0 size
            if actual_file_size == 0:
                # File size is 0- lets just blow it away
                logger.warning(f"{item}: File size is 0 bytes. Deleting file")
                remove_file(item, raise_error=False)

                # even though its a problem,we return true as we are finished
                # with the item and it should not be requeued
                return True
            elif actual_file_size != database_file_size:
                # File size is incorrect- lets just blow it away
                logger.warning(
                    f"{item}: File size {actual_file_size} does not match {database_file_size}. Deleting file"
                )
                remove_file(item, raise_error=False)

                # even though its a problem,we return true as we are finished
                # with the item and it should not be requeued
                return True

            logger.debug(f"{item}: File size matches metadata. Checking md5sum...")

            # Check md5sum
            actual_checksum = do_checksum_md5(item, None, 600)

            # Compare
            if actual_checksum != data_files_row.checksum:
                logger.warning(f"{item}: checksum {actual_checksum} does not match {data_files_row.checksum}.")
                return False

            logger.debug(f"{item}: md5 checksum matches")

            # Determine where to archive it
            bucket = get_bucket_name_for_location(
                item,
                self.archive_to_location,
            )

            archive_success = False

            if (
                self.archive_to_location == ArchiveLocation.AcaciaIngest
                or self.archive_to_location == ArchiveLocation.Banksia
                or self.archive_to_location == ArchiveLocation.AcaciaMWA
            ):  # Acacia or Banksia
                archive_success = archiver.archive_file_rclone_haproxy(
                    self.s3_profile,
                    item,
                    bucket,
                    data_files_row.checksum,
                    rclone_check_wait_secs=self.rclone_check_wait_secs,
                )
            else:
                raise NotImplementedError(f"Location {self.archive_to_location.value} not implemented")

            if archive_success:
                # Update record in metadata database
                if not data_files.update_data_file_row_as_archived(
                    self.db_handler_object,
                    val.obs_id,
                    item,
                    self.archive_to_location,
                    bucket,
                    None,
                ):
                    # if something went wrong, requeue
                    return False

                # If all is well, we have the file safely archived and the
                # database updated, so remove the file
                logger.debug(f"{item}: Deleting file")
                remove_file(item, raise_error=False)

                logger.info(f"{item}: Finished")
                return True
            else:
                return False
        else:
            # The filename was not valid
            logger.error(f"{item}: {val.validation_message}")
            return False
