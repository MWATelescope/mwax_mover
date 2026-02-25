from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker
from mwax_mover import utils
from mwax_mover.utils import ValidationData, MWADataFileType
from mwax_mover.mwax_db import MWAXDBHandler, insert_data_file_row
from logging import Logger
import os


class ChecksumAndDBProcessor(MWAXPriorityWatchQueueWorker):
    def __init__(
        self,
        logger: Logger,
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
        super().__init__(
            "ChecksumAndDBProcessor",
            logger,
            metafits_path,
            [
                (visdata_incoming_path, ".fits"),
                (voltdata_incoming_path, ".sub"),
                (bf_stitching_path, ".*"),
            ],
            MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
            list_of_corr_hi_priority_projects,
            list_of_vcs_hi_priority_projects,
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

    def handler(self, item: str) -> bool:
        """This is the first handler executed when we start to archive a new file"""
        self.logger.info(f"{item}: checksum_and_db_handler() Started")

        # validate the filename
        val: ValidationData = utils.validate_filename(self.logger, item, self.metafits_path)

        if val.valid:
            if self.archiving_enabled:
                try:
                    # Determine file size
                    file_size = os.stat(item).st_size

                    # checksum then add this file to the db so we insert a record into
                    # metadata data_files table
                    checksum_type_id: int = 1  # MD5
                    checksum: str = utils.do_checksum_md5(self.logger, item, None, 180)

                    # if file is a VCS subfile, check if it is from a trigger and
                    # grab the trigger_id as an int
                    # If not found it will return None
                    if val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
                        trigger_id = utils.read_subfile_trigger_value(item)
                    else:
                        trigger_id = None
                except FileNotFoundError:
                    # The filename was not valid
                    self.logger.warning(f"{item}- checksum_and_db_handler() file was removed while processing.")
                    return True

                # Insert record into metadata database
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
                    # if something went wrong, requeue
                    return False

                # Check to see if the file is still there- the above call could have deleted it
                # if we got an FK error
                if not os.path.exists(item):
                    # Return True, telling the queue worker we are done with this item
                    return True

            #
            # If the project_id is the special code C123 then
            # do not archive it.
            #
            if utils.should_project_be_archived(val.project_id) and self.archiving_enabled:
                # immediately add this file (and a ptr to it's queue) to the
                # voltage or vis queue which will deal with archiving
                if val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
                    # move to voltdata/outgoing
                    # Take the input filename - strip the path, then append the
                    # output path
                    outgoing_filename = os.path.join(self.voltdata_outgoing_path, os.path.basename(item))

                    self.logger.debug(f"{item}- checksum_and_db_handler() moving subfile to volt outgoing dir")
                    os.rename(item, outgoing_filename)

                    self.logger.info(
                        f"{item}- checksum_and_db_handler() moved subfile to volt"
                        " outgoing dir Queue size:"
                        f" {self.pqueue.qsize()}"
                    )
                elif val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
                    # move to visdata/processing_stats
                    # Take the input filename - strip the path, then append the
                    # output path
                    outgoing_filename = os.path.join(self.visdata_processing_stats_path, os.path.basename(item))

                    self.logger.debug(
                        f"{item}- checksum_and_db_handler() moving visibility file to vis processing stats dir"
                    )
                    os.rename(item, outgoing_filename)

                    self.logger.info(
                        f"{item}- checksum_and_db_handler() moved visibility file"
                        " to vis processing stats dir. Queue size:"
                        f" {self.pqueue.qsize()}"
                    )

                elif val.filetype_id == MWADataFileType.MWA_PPD_FILE.value:
                    # move to visdata/outgoing
                    # Take the input filename - strip the path, then append the
                    # output path
                    outgoing_filename = os.path.join(self.visdata_outgoing_path, os.path.basename(item))

                    self.logger.debug(f"{item}- checksum_and_db_handler() moving metafits file to vis outgoing dir")
                    os.rename(item, outgoing_filename)

                    self.logger.info(
                        f"{item}- checksum_and_db_handler() moved metafits file to"
                        " vis outgoing dir. Queue size:"
                        f" {self.pqueue.qsize()}"
                    )

                elif (
                    val.filetype_id == MWADataFileType.VDIF.value or val.filetype_id == MWADataFileType.FILTERBANK.value
                ):
                    # move to voltdata/bf/outgoing
                    # Take the input filename - strip the path, then append the
                    # output path
                    outgoing_filename = os.path.join(self.bf_outgoing_path, os.path.basename(item))

                    self.logger.debug(
                        f"{item}- checksum_and_db_handler() moving beamformer data file to bf outgoing dir"
                    )
                    os.rename(item, outgoing_filename)

                    self.logger.info(
                        f"{item}- checksum_and_db_handler() moved beamformer data file to"
                        " bf outgoing dir. Queue size:"
                        f" {self.pqueue.qsize()}"
                    )
                else:
                    self.logger.error(
                        f"{item}- checksum_and_db_handler() - not a valid file"
                        f" extension {val.filetype_id} / {val.file_ext}"
                    )
                    return False
            else:
                #
                # This is a "do not archive" project
                #
                # If it is visibilities, then produce stats, otherwise move to dont_archive
                if val.filetype_id == MWADataFileType.MWAX_VISIBILITIES.value:
                    # We still want to produce stats
                    outgoing_filename = os.path.join(self.visdata_processing_stats_path, os.path.basename(item))

                    self.logger.debug(
                        f"{item}- checksum_and_db_handler() moving visibility file to vis processing stats dir"
                    )
                    os.rename(item, outgoing_filename)

                    self.logger.info(
                        f"{item}- checksum_and_db_handler() moved visibility file"
                        " to vis processing stats dir Queue size:"
                        f" {self.pqueue.qsize()}"
                    )
                    # Note that the cal_handler code needs to ensure it does not
                    # archvive any do not archive projects
                elif val.filetype_id == MWADataFileType.MWAX_VOLTAGES.value:
                    outgoing_filename = os.path.join(self.voltdata_dont_archive_path, os.path.basename(item))
                    self.logger.debug(
                        f"{item}- checksum_and_db_handler() moving file to {self.voltdata_dont_archive_path}"
                    )
                    os.rename(item, outgoing_filename)

                elif val.filetype_id == MWADataFileType.MWA_PPD_FILE.value:
                    outgoing_filename = os.path.join(self.visdata_dont_archive_path, os.path.basename(item))
                    self.logger.debug(
                        f"{item}- checksum_and_db_handler() moving file to {self.visdata_dont_archive_path}"
                    )
                    os.rename(item, outgoing_filename)
                elif (
                    val.filetype_id == MWADataFileType.VDIF.value or val.filetype_id == MWADataFileType.FILTERBANK.value
                ):
                    outgoing_filename = os.path.join(self.bf_dont_archive_path, os.path.basename(item))
                    self.logger.debug(f"{item}- checksum_and_db_handler() moving file to {self.bf_dont_archive_path}")

                    os.rename(item, outgoing_filename)
                else:
                    self.logger.error(
                        f"{item}- checksum_and_db_handler() - not a valid file"
                        f" extension {val.filetype_id} / {val.file_ext}"
                    )
                    return False
        else:
            # The filename was not valid
            self.logger.error(f"{item}- checksum_and_db_handler() {val.validation_message}")
            return False

        self.logger.info(f"{item}- checksum_and_db_handler() Finished")
        return True
