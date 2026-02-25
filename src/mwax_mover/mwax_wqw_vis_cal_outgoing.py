from mwax_mover.mwax_watch_queue_worker import MWAXWatchQueueWorker
from mwax_mover.mwax_mover import MODE_WATCH_DIR_FOR_RENAME
from logging import Logger


class VisCalOutgoingProcessor(MWAXWatchQueueWorker):
    def __init__(
        self,
        logger: Logger,
        visdata_outgoing_cal_path: str,
        outgoing_cal_list: list[str],
    ):
        super().__init__(
            "VisSCalOutgoingProcessor",
            logger,
            [(visdata_outgoing_cal_path, ".fits")],
            MODE_WATCH_DIR_FOR_RENAME,
            exclude_pattern=None,
        )

        self.visdata_outgoing_cal_path = visdata_outgoing_cal_path
        self.outgoing_cal_list = outgoing_cal_list

    def handler(self, item: str) -> bool:
        self.outgoing_cal_list.append(item)
        return True
