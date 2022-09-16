"""Module for handling FilterBank file creation from a ringbuffer"""
import logging
import logging.handlers
import os
import queue
import threading
from mwax_mover import mwax_mover, mwa_archiver
from mwax_mover import mwax_queue_worker
from mwax_mover import mwax_watcher


class FilterbankProcessor:
    """Class for handling FilterBank file creation from a ringbuffer"""

    def __init__(
        self,
        context,
        hostname: str,
        fildata_path: str,
        archive_host: str,
        archive_port: int,
        archive_command_numa_node: int,
    ):
        self.subfile_distributor_context = context

        # Setup logging
        self.logger = logging.getLogger(__name__)
        # pass all logged events to the parent (subfile distributor/main log)
        self.logger.propagate = True
        self.logger.setLevel(logging.DEBUG)
        file_log = logging.FileHandler(
            filename=os.path.join(
                self.subfile_distributor_context.cfg_log_path,
                f"{__name__}.log",
            )
        )
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(
            logging.Formatter(
                "%(asctime)s, %(levelname)s, %(threadName)s, %(message)s"
            )
        )
        self.logger.addHandler(file_log)

        self.hostname = hostname

        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_NEW
        self.archiving_paused = False

        self.watcher_threads = []
        self.worker_threads = []

        self.watch_dir_fil = fildata_path
        self.queue_fil = queue.Queue()
        self.watcher_fil = None
        self.queue_worker_fil = None

        self.archive_destination_host = archive_host
        self.archive_destination_port = archive_port
        self.archive_command_numa_node = archive_command_numa_node

    def start(self):
        """Start the processor"""
        # Create watcher for filterbank data -> filterbank queue
        self.watcher_fil = mwax_watcher.Watcher(
            path=self.watch_dir_fil,
            dest_queue=self.queue_fil,
            pattern=".fil",
            log=self.logger,
            mode=self.mwax_mover_mode,
            recursive=False,
        )

        # Create queueworker for filterbank queue
        self.queue_worker_fil = mwax_queue_worker.QueueWorker(
            label="Filterbank Archive",
            source_queue=self.queue_fil,
            executable_path=None,
            event_handler=self.filterbank_handler,
            log=self.logger,
            exit_once_queue_empty=False,
        )

        # Setup thread for watching filesystem
        watcher_fil_thread = threading.Thread(
            name="watch_fil", target=self.watcher_fil.start, daemon=True
        )
        self.watcher_threads.append(watcher_fil_thread)
        watcher_fil_thread.start()

        # Setup thread for processing items
        queue_worker_fil_thread = threading.Thread(
            name="work_fil", target=self.queue_worker_fil.start, daemon=True
        )
        self.worker_threads.append(queue_worker_fil_thread)
        queue_worker_fil_thread.start()

    def filterbank_handler(self, item: str) -> bool:
        """Handler for after a fil file is created"""
        if not self.archiving_paused:
            self.logger.info(
                f"{item}- FilterbankProcessor.filterbank_handler is handling"
                f" {item}: copy to {self.archive_destination_host}..."
            )

            if not mwa_archiver.archive_file_xrootd(
                self.logger,
                item,
                self.archive_command_numa_node,
                self.archive_destination_host,
                120,
            ):
                return False

            self.logger.debug(f"{item}- filterbank_handler() Deleting file")
            mwax_mover.remove_file(self.logger, item, raise_error=False)

            self.logger.info(f"{item}- filterbank_handler() Finished")
            return True

    def pause_archiving(self, paused: bool):
        """Pauses archiving"""
        if self.archiving_paused != paused:
            if paused:
                self.logger.info("Pausing archiving")
            else:
                self.logger.info("Resuming archiving")

            if self.queue_worker_fil:
                self.queue_worker_fil.pause(paused)

            self.archiving_paused = paused

    def stop(self):
        """Stop processor"""
        self.watcher_fil.stop()
        self.queue_worker_fil.stop()

        # Wait for threads to finish
        for watcher_thread in self.watcher_threads:
            if watcher_thread:
                thread_name = watcher_thread.name
                self.logger.debug(f"Watcher {thread_name} Stopping...")
                if watcher_thread.is_alive():
                    watcher_thread.join()
                self.logger.debug(f"Watcher {thread_name} Stopped")

        for worker_thread in self.worker_threads:
            if worker_thread:
                thread_name = worker_thread.name
                self.logger.debug(f"QueueWorker {thread_name} Stopping...")
                if worker_thread.is_alive():
                    worker_thread.join()
                self.logger.debug(f"QueueWorker {thread_name} Stopped")

    def get_status(self) -> dict:
        """Returns processor status as a dictionary"""
        watcher_list = []
        status = dict({"name": "fil_watcher"})
        status.update(self.watcher_fil.get_status())
        watcher_list.append(status)

        worker_list = []
        status = dict({"name": "fil_archiver"})
        status.update(self.queue_worker_fil.get_status())
        worker_list.append(status)

        if self.archiving_paused:
            archiving = "paused"
        else:
            archiving = "running"

        return_status = {
            "type": type(self).__name__,
            "archiving": archiving,
            "watchers": watcher_list,
            "workers": worker_list,
        }

        return return_status
