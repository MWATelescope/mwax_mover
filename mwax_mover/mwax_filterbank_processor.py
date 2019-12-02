from mwax_mover import mwax_mover
from mwax_mover import mwax_queue_worker
from mwax_mover import mwax_watcher
import os
import queue
import threading


class FilterbankProcessor:
    def __init__(self, logger, hostname, fildata_path, filterbank_host, filterbank_port,
                 filterbank_destination_path, filterbank_bbcp_streams):
        self.logger = logger

        self.hostname = hostname

        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_NEW
        self.archiving_paused = False

        self.watcher_threads = []
        self.worker_threads = []

        self.watch_dir_fil = fildata_path
        self.queue_fil = queue.Queue()
        self.watcher_fil = None
        self.queue_worker_fil = None

        self.filterbank_host = filterbank_host
        self.filterbank_port = filterbank_port
        self.filterbank_destination_path = filterbank_destination_path
        self.filterbank_bbcp_streams = filterbank_bbcp_streams

    def start(self):
        # Create watcher for filterbank data -> filterbank queue
        self.watcher_fil = mwax_watcher.Watcher(path=self.watch_dir_fil, q=self.queue_fil,
                                                pattern=".fil", log=self.logger,
                                                mode=self.mwax_mover_mode)

        # Create queueworker for filterbank queue
        self.queue_worker_fil = mwax_queue_worker.QueueWorker(label="Filterbank Archive",
                                                              q=self.queue_fil,
                                                              executable_path=None,
                                                              event_handler=self.filterbank_handler,
                                                              mode=self.mwax_mover_mode,
                                                              log=self.logger)

        # Setup thread for watching filesystem
        watcher_fil_thread = threading.Thread(name="watch_fil", target=self.watcher_fil.start, daemon=True)
        self.watcher_threads.append(watcher_fil_thread)
        watcher_fil_thread.start()

        # Setup thread for processing items
        queue_worker_fil_thread = threading.Thread(name="work_fil", target=self.queue_worker_fil.start, daemon=True)
        self.worker_threads.append(queue_worker_fil_thread)
        queue_worker_fil_thread.start()

    def filterbank_handler(self, item):
        if not self.archiving_paused:
            self.logger.info(f"{item}- ArchiveProcessor.archive_handler is handling {item}...")

            # Get filename without path
            filename_only = os.path.basename(item)
            destination_filename = os.path.join(self.filterbank_destination_path, filename_only)

            command = f"bbcp -w=32m -s {self.filterbank_bbcp_streams} " \
                      f"{item} {self.filterbank_host}{destination_filename}"
            return_value = mwax_mover.run_command(command, 32)

            self.logger.info(f"{item}- ArchiveProcessor.archive_handler finished handling.")
            return return_value
        else:
            return False

    def pause_archiving(self, paused):
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

    def get_status(self):
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
