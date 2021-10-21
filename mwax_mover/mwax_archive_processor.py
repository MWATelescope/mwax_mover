from mwax_mover import mwax_mover, mwax_db, mwax_queue_worker, mwax_watcher, mwa_archiver, utils
import logging
import logging.handlers
import os
import queue
import threading


class MWAXArchiveProcessor:
    def __init__(self,
                 context,
                 hostname: str,
                 archive_command_numa_node: int,
                 archive_host: str,
                 archive_port: str,
                 db_handler_object,
                 voltdata_path: str,
                 visdata_path: str):
        self.subfile_distributor_context = context

        # Setup logging
        self.logger = logging.getLogger(__name__)
        self.logger.propagate = True  # pass all logged events to the parent (subfile distributor/main log)
        self.logger.setLevel(logging.DEBUG)
        file_log = logging.FileHandler(filename=os.path.join(self.subfile_distributor_context.cfg_log_path,
                                                             f"{__name__}.log"))
        file_log.setLevel(logging.DEBUG)
        file_log.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(threadName)s, %(message)s'))
        self.logger.addHandler(file_log)

        self.db_handler_object = db_handler_object

        self.hostname = hostname
        self.archive_destination_host = archive_host
        self.archive_destination_port = archive_port
        self.archive_command_numa_node = archive_command_numa_node

        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_NEW
        self.archiving_paused = False

        self.queue_db = queue.Queue()
        self.queue_worker_db = None

        self.watcher_threads = []
        self.worker_threads = []

        self.watch_dir_volt = voltdata_path
        self.queue_volt = queue.Queue()
        self.watcher_volt = None
        self.queue_worker_volt = None

        self.watch_dir_vis = visdata_path
        self.queue_vis = queue.Queue()
        self.watcher_vis = None
        self.queue_worker_vis = None

    def start(self):
        # Create watcher for voltage data -> db queue
        self.watcher_volt = mwax_watcher.Watcher(path=self.watch_dir_volt, q=self.queue_db,
                                                 pattern=".sub", log=self.logger,
                                                 mode=self.mwax_mover_mode, recursive=False)

        # Create watcher for visibility data -> db queue
        self.watcher_vis = mwax_watcher.Watcher(path=self.watch_dir_vis, q=self.queue_db,
                                                pattern=".fits", log=self.logger,
                                                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME, recursive=False)

        # Create queueworker for the db queue
        self.queue_worker_db = mwax_queue_worker.QueueWorker(label="MWA Metadata DB",
                                                             q=self.queue_db,
                                                             executable_path=None,
                                                             event_handler=self.db_handler,
                                                             mode=self.mwax_mover_mode,
                                                             log=self.logger)

        # Create queueworker for voltage queue
        self.queue_worker_volt = mwax_queue_worker.QueueWorker(label="Subfile Archiver",
                                                               q=self.queue_volt,
                                                               executable_path=None,
                                                               event_handler=self.archive_handler,
                                                               mode=self.mwax_mover_mode,
                                                               log=self.logger)

        # Create queueworker for visibility queue
        self.queue_worker_vis = mwax_queue_worker.QueueWorker(label="Visibility Archiver",
                                                              q=self.queue_vis,
                                                              executable_path=None,
                                                              event_handler=self.archive_handler,
                                                              mode=self.mwax_mover_mode,
                                                              log=self.logger)

        # Setup thread for processing items from db queue
        queue_worker_db_thread = threading.Thread(name="work_db", target=self.queue_worker_db.start, daemon=True)
        self.worker_threads.append(queue_worker_db_thread)
        queue_worker_db_thread.start()

        # Setup thread for watching filesystem
        watcher_volt_thread = threading.Thread(name="watch_volt", target=self.watcher_volt.start, daemon=True)
        self.watcher_threads.append(watcher_volt_thread)
        watcher_volt_thread.start()

        # Setup thread for processing items
        queue_worker_volt_thread = threading.Thread(name="work_volt", target=self.queue_worker_volt.start,
                                                    daemon=True)
        self.worker_threads.append(queue_worker_volt_thread)
        queue_worker_volt_thread.start()

        # Setup thread for watching filesystem
        watcher_vis_thread = threading.Thread(name="watch_vis", target=self.watcher_vis.start, daemon=True)
        self.watcher_threads.append(watcher_vis_thread)
        watcher_vis_thread.start()

        # Setup thread for processing items
        queue_worker_vis_thread = threading.Thread(name="work_vis", target=self.queue_worker_vis.start, daemon=True)
        self.worker_threads.append(queue_worker_vis_thread)
        queue_worker_vis_thread.start()

    def db_handler(self, item: str) -> bool:
        self.logger.info(f"{item}- db_handler() Started")

        # immediately add this file to the db so we insert a record into metadata data_files table

        # validate the filename
        location = 1 # DMF for now
        (valid, obs_id, filetype, file_ext, _, _, validation_message) = mwa_archiver.validate_filename(item, location)

        if valid:
            # Insert record into metadata database
            if not mwax_db.upsert_data_file_row(self.db_handler_object, item, filetype, self.hostname,
                                                False, None, None):
                # if something went wrong, requeue
                return False

            # immediately add this file (and a ptr to it's queue) to the voltage or
            # vis queue which will deal with archiving
            if file_ext == ".sub":
                self.queue_volt.put(item)
                self.logger.info(f"{item}- db_handler() Added to voltage queue for archiving. "
                                 f"Queue size: {self.queue_volt.qsize()}")
            elif file_ext == ".fits":
                self.queue_vis.put(item)
                self.logger.info(f"{item}- db_handler() Added to visibility queue for archiving. "
                                 f"Queue size: {self.queue_vis.qsize()}")
            else:
                self.logger.error(f"{item}- db_handler() - not a valid file extension")
                return False
        else:
            # The filename was not valid
            self.logger.error(f"{item}- db_handler() {validation_message}")
            return False

        self.logger.info(f"{item}- db_handler() Finished")
        return True

    def archive_handler(self, item: str) -> bool:
        self.logger.info(f"{item}- archive_handler() Started...")

        if mwa_archiver.archive_file_xrootd(self.logger, item, self.archive_command_numa_node, self.archive_destination_host, 120) is not True:
            return False

        self.logger.debug(f"{item}- archive_handler() Deleting file")
        mwax_mover.remove_file(self.logger, item)

        self.logger.info(f"{item}- archive_handler() Finished")
        return True

    def pause_archiving(self, paused: bool):
        if self.archiving_paused != paused:
            if paused:
                self.logger.info(f"Pausing archiving")
            else:
                self.logger.info(f"Resuming archiving")

            if self.queue_worker_volt:
                self.queue_worker_volt.pause(paused)
            if self.queue_worker_vis:
                self.queue_worker_vis.pause(paused)

            self.archiving_paused = paused

    def stop(self):
        self.watcher_volt.stop()
        self.watcher_vis.stop()
        self.queue_worker_db.stop()
        self.queue_worker_volt.stop()
        self.queue_worker_vis.stop()

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

        if self.watcher_volt:
            status = dict({"name": "voltdata_watcher"})
            status.update(self.watcher_volt.get_status())
            watcher_list.append(status)

        if self.watcher_vis:
            status = dict({"name": "visdata_watcher"})
            status.update(self.watcher_vis.get_status())
            watcher_list.append(status)

        worker_list = []

        if self.queue_worker_db:
            status = dict({"name": "db_worker"})
            status.update(self.queue_worker_db.get_status())
            worker_list.append(status)

        if self.queue_worker_volt:
            status = dict({"name": "volt_archiver"})
            status.update(self.queue_worker_volt.get_status())
            worker_list.append(status)

        if self.queue_worker_vis:
            status = dict({"name": "vis_archiver"})
            status.update(self.queue_worker_vis.get_status())
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
