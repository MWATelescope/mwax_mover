import os

from mwax_mover import mwax_db
from mwax_mover.mwax_db import DataFileRow, MWAXDBHandler, get_data_file_row
from mwax_mover import mwa_archiver, mwax_mover, utils
from mwax_mover.utils import ArchiveLocation
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker
import logging

logger = logging.getLogger(__name__)


class PawseyOutgoingProcessor(MWAXPriorityWatchQueueWorker):
    def __init__(
        self,
        metafits_path: str,
        watch_paths_and_exts: list[tuple[str, str]],
        list_of_corr_hi_priority_projects: list[str],
        list_of_vcs_hi_priority_projects: list[str],
        mro_db_handler_object: MWAXDBHandler,
        remote_db_handler_object: MWAXDBHandler,
        s3_profile: str,
        s3_ceph_endpoints: list[str],
        archive_to_location: ArchiveLocation,
    ):
        super().__init__(
            "OutgoingProcessor",
            metafits_path,
            watch_paths_and_exts,
            mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
            exclude_pattern=".part*",
            corr_hi_priority_projects=list_of_corr_hi_priority_projects,
            vcs_hi_priority_projects=list_of_vcs_hi_priority_projects,
            requeue_to_eoq_on_failure=True,
        )
        self.mro_db_handler_object = mro_db_handler_object
        self.remote_db_handler_object = remote_db_handler_object
        self.s3_profile = s3_profile
        self.s3_ceph_endpoints = s3_ceph_endpoints
        self.archive_to_location = archive_to_location

    def handler(self, item: str) -> bool:
        """Handles sending files to Pawsey"""
        logger.info(f"{item}- archive_handler() Started...")

        # validate the filename
        val: utils.ValidationData = utils.validate_filename(item, self.metafits_path)

        # do some sanity checks!
        if val.valid:
            # Get the file size
            actual_file_size = os.stat(item).st_size
            logger.debug(f"{item}- archive_handler() file size on disk is {actual_file_size} bytes")

            # Lookup file from db
            data_files_row: DataFileRow = get_data_file_row(self.remote_db_handler_object, item, val.obs_id)
            database_file_size = data_files_row.size

            # Check for 0 size
            if actual_file_size == 0:
                # File size is 0- lets just blow it away
                logger.warning(f"{item}- archive_handler() File size is 0 bytes. Deleting file")
                utils.remove_file(item, raise_error=False)

                # even though its a problem,we return true as we are finished
                # with the item and it should not be requeued
                return True
            elif actual_file_size != database_file_size:
                # File size is incorrect- lets just blow it away
                logger.warning(
                    f"{item}- archive_handler() File size"
                    f" {actual_file_size} does not match {database_file_size}."
                    " Deleting file"
                )
                utils.remove_file(item, raise_error=False)

                # even though its a problem,we return true as we are finished
                # with the item and it should not be requeued
                return True

            logger.debug(f"{item}- archive_handler() File size matches metadata. Checking md5sum... database")

            # Check md5sum
            actual_checksum = utils.do_checksum_md5(item, None, 600)

            # Compare
            if actual_checksum != data_files_row.checksum:
                logger.warning(
                    f"{item}- archive_handler() checksum {actual_checksum} does not match {data_files_row.checksum}."
                )
                return False

            logger.debug(f"{item}- archive_handler() md5 checksum matches")

            # Determine where to archive it
            bucket = utils.determine_bucket(
                item,
                self.archive_to_location,
            )

            archive_success = False

            if (
                self.archive_to_location == ArchiveLocation.AcaciaIngest
                or self.archive_to_location == ArchiveLocation.Banksia
                or self.archive_to_location == ArchiveLocation.AcaciaMWA
            ):  # Acacia or Banksia
                archive_success = mwa_archiver.archive_file_rclone(
                    self.s3_profile,
                    self.s3_ceph_endpoints,
                    item,
                    bucket,
                    data_files_row.checksum,
                )
            else:
                raise NotImplementedError(f"Location {self.archive_to_location.value} not implemented")

            if archive_success:
                # Update record in metadata database
                if not mwax_db.update_data_file_row_as_archived(
                    self.mro_db_handler_object,
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
                logger.debug(f"{item}- archive_handler() Deleting file")
                utils.remove_file(item, raise_error=False)

                logger.info(f"{item}- archive_handler() Finished")
                return True
            else:
                return False
        else:
            # The filename was not valid
            logger.error(f"{item}- archive_handler() {val.validation_message}")
            return False
