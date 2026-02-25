from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker
from mwax_mover.mwa_archiver import archive_file_xrootd
from mwax_mover.utils import remove_file
from logging import Logger


class OutgoingProcessor(MWAXPriorityWatchQueueWorker):
    def __init__(
        self,
        logger: Logger,
        metafits_path: str,
        visdata_outgoing_path: str,
        voltdata_outgoing_path: str,
        bf_outgoing_path: str,
        list_of_corr_hi_priority_projects: list[str],
        list_of_vcs_hi_priority_projects: list[str],
        archive_command_numa_node: int,
        archive_destination_host: str,
        archive_command_timeout_sec: int,
    ):
        super().__init__(
            "OutgoingProcessor",
            logger,
            metafits_path,
            [
                (visdata_outgoing_path, ".fits"),
                (voltdata_outgoing_path, ".sub"),
                (bf_outgoing_path, ".*"),
            ],
            MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
            list_of_corr_hi_priority_projects,
            list_of_vcs_hi_priority_projects,
        )
        self.archive_command_numa_node = archive_command_numa_node
        self.archive_destination_host = archive_destination_host
        self.archive_command_timeout_sec = archive_command_timeout_sec

    def handler(self, item: str) -> bool:
        """This is called whenever a file is moved into the
        outgoing_vis or outgoing_volt directories. For each file attempt to
        send to the mwacache boxes then remove the file"""
        self.logger.info(f"{item}- archive_handler() Started...")

        if (
            archive_file_xrootd(
                self.logger,
                item,
                int(self.archive_command_numa_node),
                self.archive_destination_host,
                self.archive_command_timeout_sec,
            )
            is not True
        ):
            return False

        self.logger.debug(f"{item}- archive_handler() Deleting file")
        remove_file(self.logger, item, raise_error=False)

        self.logger.info(f"{item}- archive_handler() Finished")
        return True
