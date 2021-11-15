from mwax_mover import mwax_mover, mwa_archiver
from mwax_mover import mwax_queue_worker
from mwax_mover import mwax_watcher
import logging
import logging.handlers
import os
import queue
import threading


class FilterbankProcessor:
    def __init__(self, context, hostname: str, fildata_path: str, archive_host: str, archive_port: int,
                 archive_command_numa_node: int):
        self.subfile_distributor_context = context

        # Setup logging
        self.logger = logging.getLogger(__name__)
        # pass all logged events to the parent (subfile distributor/main log)
        self.logger.propagate = True
        self.logger.setLevel(logging.DEBUG)
        file_log = logging.FileHandler(filename=os.path.join(self.subfile_distributor_context.cfg_log_path,
                                                             f"{__name__}.log"))
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(logging.Formatter(
            '%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
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
        # Create watcher for filterbank data -> filterbank queue
        self.watcher_fil = mwax_watcher.Watcher(path=self.watch_dir_fil, q=self.queue_fil,
                                                pattern=".fil", log=self.logger,
                                                mode=self.mwax_mover_mode, recursive=False)

        # Create queueworker for filterbank queue
        self.queue_worker_fil = mwax_queue_worker.QueueWorker(label="Filterbank Archive",
                                                              q=self.queue_fil,
                                                              executable_path=None,
                                                              event_handler=self.filterbank_handler,
                                                              log=self.logger,
                                                              exit_once_queue_empty=False)

        # Setup thread for watching filesystem
        watcher_fil_thread = threading.Thread(
            name="watch_fil", target=self.watcher_fil.start, daemon=True)
        self.watcher_threads.append(watcher_fil_thread)
        watcher_fil_thread.start()

        # Setup thread for processing items
        queue_worker_fil_thread = threading.Thread(
            name="work_fil", target=self.queue_worker_fil.start, daemon=True)
        self.worker_threads.append(queue_worker_fil_thread)
        queue_worker_fil_thread.start()

    def filterbank_handler(self, item: str) -> bool:
        if not self.archiving_paused:
            self.logger.info(f"{item}- FilterbankProcessor.filterbank_handler is handling {item}: "
                             f"copy to {self.archive_destination_host}...")

            if not mwa_archiver.archive_file_xrootd(self.logger, item, self.archive_command_numa_node,
                                                    self.archive_destination_host, 120):
                return False

            self.logger.debug(f"{item}- filterbank_handler() Deleting file")
            mwax_mover.remove_file(self.logger, item, raise_error=False)

            self.logger.info(f"{item}- filterbank_handler() Finished")
            return True

    def pause_archiving(self, paused: bool):
        if self.archiving_paused != paused:
            if paused:
                self.logger.info(f"Pausing archiving")
            else:
                self.logger.info(f"Resuming archiving")

            if self.queue_worker_fil:
                self.queue_worker_fil.pause(paused)

            self.archiving_paused = paused

    def stop(self):
        self.watcher_fil.stop()
        self.queue_worker_fil.stop()

        # Wait for threads to finish
        for t in self.watcher_threads:
            if t:
                thread_name = t.name
                self.logger.debug(f"Watcher {thread_name} Stopping...")
                if t.isAlive:
                    t.join()
                self.logger.debug(f"Watcher {thread_name} Stopped")

        for t in self.worker_threads:
            if t:
                thread_name = t.name
                self.logger.debug(f"QueueWorker {thread_name} Stopping...")
                if t.isAlive():
                    t.join()
                self.logger.debug(f"QueueWorker {thread_name} Stopped")

    def get_status(self) -> dict:
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

        return_status = {"type": type(self).__name__,
                         "archiving": archiving,
                         "watchers": watcher_list,
                         "workers": worker_list}

        return return_status
