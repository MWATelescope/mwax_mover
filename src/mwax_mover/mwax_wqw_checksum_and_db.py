from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME_OR_NEW
from mwax_mover.mwax_watch_queue_worker import MWAXPriorityWatchQueueWorker
from logging import Logger


class ChecksumAndDBProcessor(MWAXPriorityWatchQueueWorker):
    def __init__(
        self,
        logger: Logger.logger,
        metafits_path: str,
        visdata_incoming_path: str,
        list_of_corr_hi_priority_projects: list[str],
        list_of_vcs_hi_priority_projects: list[str],
    ):
        super().__init__(
            "ChecksumAndDBProcessor",
            logger,
            metafits_path,
            [
                (visdata_incoming_path, ".fits"),
            ],
            MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
            list_of_corr_hi_priority_projects,
            list_of_vcs_hi_priority_projects,
        )

    def handler(self, item: str) -> bool:
        self.logger.info(f"Handling item: {item}")
        return True
