"""Watch-queue-worker that checksums incoming files, records them in the metadata DB, and routes them onward.

Computes the MD5 checksum of each arriving file, inserts a data_files record into
the MWA metadata database, then moves the file to the appropriate outgoing or
dont_archive directory based on file type (visibilities, voltages, PPD, VDIF,
filterbank) and whether the observation's project should be archived.
"""

from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker
from mwax_mover import utils
from mwax_mover.utils import ValidationData, MWADataFileType
from mwax_mover.mwax_db import MWAXDBHandler, insert_data_file_row
import logging
import os
from typing import Optional

logger = logging.getLogger(__name__)


class ChecksumAndDBProcessor(MWAXPriorityWatchQueueWorker):
    def __init__(
        self,
        metafits_path: str,
        visdata_incoming_path: str,
        visdata_processing_stats_path: str,
        visdata_outgoing_path: str,
        visdata_dont_archive_path: str,
        voltdata_incoming_path: str,
        voltdata_outgoing_path: str,
        voltdata_dont_archive_path: str,
        bf_stitching_path: str,
        bf_outgoing_path: str,
        bf_dont_archive_path: str,
        list_of_corr_hi_priority_projects: list[str],
        list_of_vcs_hi_priority_projects: list[str],
        db_handler_object: MWAXDBHandler,
        archiving_enabled: bool,
    ):
        """Initialise the processor and register the watch directories.

        Args:
            metafits_path: Directory containing metafits files used during filename validation.
            visdata_incoming_path: Directory watched for incoming visibility FITS files (.fits).
            visdata_processing_stats_path: Destination for visibilities that need stats processing.
            visdata_outgoing_path: Destination for PPD files being sent to the archive.
            visdata_dont_archive_path: Destination for PPD files that should not be archived.
            voltdata_incoming_path: Directory watched for incoming voltage subfiles (.sub).
            voltdata_outgoing_path: Destination for voltage subfiles being sent to the archive.
            voltdata_dont_archive_path: Destination for voltage subfiles that should not be archived.
            bf_stitching_path: Directory watched for beamformer files awaiting stitching (all extensions).
            bf_outgoing_path: Destination for beamformer files being sent to the archive.
            bf_dont_archive_path: Destination for beamformer files that should not be archived.
            list_of_corr_hi_priority_projects: Project IDs that get elevated priority in the correlator queue.
            list_of_vcs_hi_priority_projects: Project IDs that get elevated priority in the VCS queue.
            db_handler_object: Initialised MWAXDBHandler used for metadata database inserts.
            archiving_enabled: When False the checksum/DB step is skipped and files are routed
                to dont_archive paths regardless of their project ID.
        """
        super().__init__(
            "ChecksumAndDBProcessor",
            metafits_path,
            [
                (visdata_incoming_path, ".fits"),
                (voltdata_incoming_path, ".sub"),
                (bf_stitching_path, ".*"),
            ],
            mode=MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
            corr_hi_priority_projects=list_of_corr_hi_priority_projects,
            vcs_hi_priority_projects=list_of_vcs_hi_priority_projects,
            requeue_to_eoq_on_failure=False,
        )
        self.visdata_incoming_path = visdata_incoming_path
        self.voltdata_incoming_path = voltdata_incoming_path
        self.bf_stitching_path = bf_stitching_path
        self.visdata_processing_stats_path = visdata_processing_stats_path

        self.visdata_outgoing_path = visdata_outgoing_path
        self.voltdata_outgoing_path = voltdata_outgoing_path
        self.bf_outgoing_path = bf_outgoing_path

        self.visdata_dont_archive_path = visdata_dont_archive_path
        self.voltdata_dont_archive_path = voltdata_dont_archive_path
        self.bf_dont_archive_path = bf_dont_archive_path

        self.db_handler_object = db_handler_object
        self.archiving_enabled = archiving_enabled

    def _checksum_and_insert_db(self, item: str, val: ValidationData) -> Optional[bool]:
        """Compute the MD5 checksum of *item* and insert a record in the metadata DB.

        Returns:
            ``True``  — the file disappeared before or after the DB insert; the caller
                        should treat the item as done and return ``True``.
            ``False`` — the DB insert failed; the caller should return ``False`` so the
                        item is not re-queued (``requeue_to_eoq_on_failure=False``).
            ``None``  — checksum and DB insert both succeeded and the file still exists;
                        the caller should proceed to routing.
        """
        try:
            file_size = os.stat(item).st_size

            checksum_type_id: int = 1  # MD5
            checksum: str = utils.do_checksum_md5(item, None, 180)

            # If the file is a VCS subfile, check whether it came from a triggered
            # observation and retrieve the trigger_id (None if not triggered).
            if val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
                trigger_id = utils.read_subfile_trigger_value(item)
            else:
                trigger_id = None

        except FileNotFoundError:
            logger.warning(f"{item}- file was removed while processing.")
            return True

        if not insert_data_file_row(
            self.db_handler_object,
            val.obs_id,
            item,
            val.filetype_id,
            self.hostname,
            checksum_type_id,
            checksum,
            trigger_id,
            file_size,
        ):
            return False

        # The insert_data_file_row call may delete the file on an FK error.
        if not os.path.exists(item):
            return True

        return None

    def _get_destination(self, item: str, val: ValidationData, archive: bool) -> Optional[str]:
        """Return the full destination path for *item* based on its file type and archive flag.

        Visibilities always go to ``visdata_processing_stats`` regardless of the archive
        flag (stats must be generated even for no-archive projects).

        Args:
            item: Full path of the file being processed (used to extract the basename).
            val: Validated filename metadata including ``filetype_id``.
            archive: ``True`` if the file should be routed toward the archive
                     (outgoing paths); ``False`` routes to dont_archive paths.

        Returns:
            The destination file path string, or ``None`` if ``val.filetype_id`` is
            not a recognised ``MWADataFileType``.
        """
        basename = os.path.basename(item)

        if val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
            dest_dir = self.voltdata_outgoing_path if archive else self.voltdata_dont_archive_path
        elif val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
            # Stats are always produced, even for no-archive projects.
            dest_dir = self.visdata_processing_stats_path
        elif val.filetype_id == MWADataFileType.MWA_PPD_FILE.value:
            dest_dir = self.visdata_outgoing_path if archive else self.visdata_dont_archive_path
        elif val.filetype_id in (MWADataFileType.VDIF.value, MWADataFileType.FILTERBANK.value):
            dest_dir = self.bf_outgoing_path if archive else self.bf_dont_archive_path
        else:
            return None

        return os.path.join(dest_dir, basename)

    def handler(self, item: str) -> bool:
        """Checksum, record in the DB, then route the file to its next destination.

        This is the first handler executed when a new file arrives. It:
        1. Validates the filename and extracts metadata.
        2. (When archiving is enabled) Computes the MD5 checksum and inserts a
           ``data_files`` record into the MWA metadata database.
        3. Determines the destination directory based on file type and whether the
           project should be archived, then moves the file there.

        Returns:
            ``True`` if the file was successfully handled (or had already been removed).
            ``False`` if validation failed, the DB insert failed, or the filetype was
            not recognised.
        """
        logger.info(f"{item}: Started")

        val: ValidationData = utils.validate_filename(item, self.metafits_path)

        if not val.valid:
            logger.error(f"{item}- {val.validation_message}")
            return False

        if self.archiving_enabled:
            result = self._checksum_and_insert_db(item, val)
            if result is not None:
                return result

        should_archive = utils.should_project_be_archived(val.project_id) and self.archiving_enabled
        dest = self._get_destination(item, val, archive=should_archive)

        if dest is None:
            logger.error(
                f"{item}- not a valid file extension {val.filetype_id} / {val.file_ext}"
            )
            return False

        logger.debug(f"{item}- moving file to {os.path.dirname(dest)}")
        os.rename(item, dest)
        logger.info(
            f"{item}- moved file to"
            f" {os.path.dirname(dest)}. Queue size: {self.pqueue.qsize()}"
        )

        logger.info(f"{item}- Finished")
        return True
