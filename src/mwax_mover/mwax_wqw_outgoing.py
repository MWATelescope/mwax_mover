"""Watch-queue-worker that archives outgoing MWAX files to the mwacache servers via xrootd.

Watches the vis, volt, and beamformer outgoing directories on an MWAX box. For
each file, transfers it to the designated mwacache host using archive_file_xrootd(),
then deletes the local copy on success.
"""

from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker
from mwax_mover.mwa_archiver import archive_file_xrootd
from mwax_mover.utils import remove_file
import logging

logger = logging.getLogger(__name__)


class OutgoingProcessor(MWAXPriorityWatchQueueWorker):
    def __init__(
        self,
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
            metafits_path,
            [
                (visdata_outgoing_path, ".fits"),
                (voltdata_outgoing_path, ".sub"),
                (bf_outgoing_path, ".*"),
            ],
            mode=MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
            corr_hi_priority_projects=list_of_corr_hi_priority_projects,
            vcs_hi_priority_projects=list_of_vcs_hi_priority_projects,
            requeue_to_eoq_on_failure=False,
        )
        self.archive_command_numa_node = archive_command_numa_node
        self.archive_destination_host = archive_destination_host
        self.archive_command_timeout_sec = archive_command_timeout_sec

    def handler(self, item: str) -> bool:
        """This is called whenever a file is moved into the
        outgoing_vis or outgoing_volt directories. For each file attempt to
        send to the mwacache boxes then remove the file"""
        logger.info(f"{item}: Started...")

        if (
            archive_file_xrootd(
                item,
                int(self.archive_command_numa_node),
                self.archive_destination_host,
                self.archive_command_timeout_sec,
            )
            is not True
        ):
            return False

        logger.debug(f"{item}: Deleting file")
        remove_file(item, raise_error=False)

        logger.info(f"{item}: Finished")
        return True
