from mwax_mover import mwax_mover, mwax_queue_worker, mwax_watcher, mwax_command
import logging
import logging.handlers
import os
import queue
import threading
import time


class ArchiveProcessor:
    def __init__(self, context, hostname: str, archive_command_numa_node: int, archive_host: str, archive_port: str,
                 db_handler_object, voltdata_path: str, visdata_path: str):
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
                                                 mode=self.mwax_mover_mode)

        # Create watcher for visibility data -> db queue
        self.watcher_vis = mwax_watcher.Watcher(path=self.watch_dir_vis, q=self.queue_db,
                                                pattern=".fits", log=self.logger,
                                                mode=self.mwax_mover_mode)

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
        # Get info
        filename = os.path.basename(item)
        obsid = filename[0:10]
        file_ext = os.path.splitext(item)[1]
        file_size = os.stat(item).st_size
        filetype = None
        remote_archived = False     # This gets set to True by NGAS at Pawsey
        deleted = False
        site_path = f"http://mwangas/RETRIEVE?file_id={obsid}"

        if file_ext.lower() == ".sub":
            filetype = 17
        elif file_ext.lower() == ".fits":
            filetype = 8
        else:
            # Error - unknown filetype
            self.logger.error(f"{item}- db_handler() Could not handle unknown extension {file_ext}")
            exit(3)

        # Insert record into metadata database
        if not self.insert_data_file_row(obsid, filetype, file_size,
                                         filename, site_path, self.hostname, remote_archived, deleted):
            # if something went wrong, requeue
            return False

        # immediately add this file (and a ptr to it's queue) to the voltage or vis queue which will deal with archiving
        if file_ext.lower() == ".sub":
            self.queue_volt.put(item)
            self.logger.info(f"{item}- db_handler() Added to voltage queue for archiving. Queue size: {self.queue_volt.qsize()}")
        elif file_ext.lower() == ".fits":
            self.queue_vis.put(item)
            self.logger.info(f"{item}- db_handler() Added to visibility queue for archiving. Queue size: {self.queue_vis.qsize()}")

        self.logger.info(f"{item}- db_handler() Finished")
        return True

    def archive_handler(self, item: str) -> bool:
        self.logger.info(f"{item}- archive_handler() Started...")

        if self.archive_file_xrootd(item, self.archive_command_numa_node, self.archive_destination_host) is not True:
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

    def insert_data_file_row(self, obsid: int, filetype: int, file_size: int, filename: str, site_path: str,
                             hostname: str, remote_archived: bool, deleted: bool) -> bool:
        sql = f"INSERT INTO data_files " \
              f"(observation_num, filetype, size, filename, site_path, host, remote_archived, deleted) " \
              f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (filename) DO NOTHING"

        try:
            rows_inserted = self.db_handler_object.insert_one_row(sql, (str(obsid), filetype, file_size,
                                                                        filename, site_path, hostname,
                                                                        remote_archived, deleted))

            if self.db_handler_object.dummy:
                self.logger.warning(f"{filename} insert_data_file_row() Using dummy database connection. No data is really being inserted")
            else:
                if rows_inserted == 1:
                    self.logger.info(f"{filename} insert_data_file_row() Successfully inserted into data_files table")
                else:
                    self.logger.info(f"{filename} insert_data_file_row() Row already exists in data_files table")

            return_value = True
        except Exception as insert_exception:
            self.logger.error(f"{filename} insert_data_file_row() inserting data_files record in "
                              f"data_files table: {insert_exception}")
            return_value = False

        return return_value

    def archive_file_xrootd(self, full_filename: str, archive_numa_node: int, archive_destination_host: str):
        self.logger.info(f"{full_filename} attempting archive_file_xrootd...")

        size = os.path.getsize(full_filename)

        command = f"numactl --cpunodebind={archive_numa_node} --membind={archive_numa_node} /usr/local/bin/xrdcp --force --cksum adler32 --silent --streams 2 --tlsnodata {full_filename} xroot://{archive_destination_host}"
        start_time = time.time()
        return_value = mwax_command.run_shell_command(self.logger, command)
        elapsed = time.time() - start_time

        size_gigabytes = size / (1000*1000*1000)
        gbps_per_sec = (size_gigabytes * 8) / elapsed

        if return_value:
            self.logger.info(f"{full_filename} archive_file_xrootd success ({size_gigabytes:.3f}GB at {gbps_per_sec:.3f} Gbps)")

        return return_value

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
