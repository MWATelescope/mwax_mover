from mwax_mover import mwax_mover
from mwax_mover import mwax_queue_worker
from mwax_mover import mwax_watcher
import os
import queue
import threading
from urllib.parse import urlencode
from urllib.request import urlopen
import urllib.error


class ArchiveProcessor:
    def __init__(self, logger, hostname, archive_host, archive_port, db_handler_object, voltdata_path, visdata_path):
        self.logger = logger

        self.db_handler_object = db_handler_object

        self.hostname = hostname
        self.archive_destination_host = archive_host
        self.archive_destination_port = archive_port

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
        self.queue_worker_volt = mwax_queue_worker.QueueWorker(label="Subfile Archive",
                                                               q=self.queue_volt,
                                                               executable_path=None,
                                                               event_handler=self.archive_handler,
                                                               mode=self.mwax_mover_mode,
                                                               log=self.logger)

        # Create queueworker for visibility queue
        self.queue_worker_vis = mwax_queue_worker.QueueWorker(label="Visibility Archive",
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

    def db_handler(self, item):
        self.logger.info(f"{item}- ArchiveProcessor.handler is handling {item}...")

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
            self.logger.error(f"{item}- Could not handle unknown extension {file_ext}")
            exit(3)

        # Insert record into metadata database
        if not self.insert_data_file_row(obsid, filetype, file_size,
                                         filename, site_path, self.hostname, remote_archived, deleted):
            # if something went wrong, requeue
            return False

        # immediately add this file (and a ptr to it's queue) to the voltage or vis queue which will deal with archiving
        if file_ext.lower() == ".sub":
            self.queue_volt.put(item)
        elif file_ext.lower() == ".fits":
            self.queue_vis.put(item)

        self.logger.info(f"{item}- ArchiveProcessor.db_handler finished handling.")
        return True

    def archive_handler(self, item):
        if not self.archiving_paused:
            self.logger.info(f"{item}- ArchiveProcessor.archive_handler is handling {item}...")

            if self.archive_file(item) != 200:
                return False

            self.logger.info(f"{item}- ArchiveProcessor.archive_handler finished handling.")
            return True
        else:
            return False

    def pause_archiving(self, paused):
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

    def insert_data_file_row(self, obsid, filetype, file_size, filename, site_path, hostname, remote_archived, deleted):
        sql = f"INSERT INTO data_files " \
              f"(observation_num, filetype, size, filename, site_path, host, remote_archived, deleted) " \
              f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (filename) DO NOTHING"

        try:
            rows_inserted = self.db_handler_object.insert_one_row(sql, (obsid, filetype, file_size,
                                                                        filename, site_path, hostname,
                                                                        remote_archived, deleted))

            if self.db_handler_object.dummy:
                self.logger.warning(f"{filename} Using dummy database connection. No data is really being inserted.")
            else:
                if rows_inserted == 1:
                    self.logger.info(f"{filename} successfully inserted into data_files table in metdata database.")
                else:
                    self.logger.info(f"{filename} already exists in data_files table in metdata database.")

            return_value = True
        except Exception as insert_exception:
            self.logger.error(f"{filename} error inserting data_files record in "
                              f"metadata database: {insert_exception}")
            return_value = False

        return return_value

    def archive_file(self, full_filename):
        self.logger.info(f"{full_filename} attempting archive_file...")

        self.logger.info(f"{full_filename} calculating checksum...")
        checksum = mwax_mover.calculate_checksum(full_filename)
        self.logger.debug(f"{full_filename} checksum == {checksum}")

        query_args = {'fileUri': f'ngas@{self.archive_destination_host}:{full_filename}',
                      'bport': self.archive_destination_port,
                      'bwinsize': '=32m',
                      'bnum_streams': 12,
                      'mimeType': 'application/octet-stream',
                      'bchecksum': str(checksum)}

        encoded_args = urlencode(query_args)

        bbcpurl = f"http://{self.archive_destination_host}/BBCPARC?{encoded_args}"

        resp = None
        try:
            resp = urlopen(bbcpurl, timeout=7200)
            data = []
            while True:
                buff = resp.read()
                if not buff:
                    break
                data.append(buff.decode('utf-8'))

            return 200, '', ''.join(data)
        except urllib.error.URLError as url_error:
            self.logger.error(f"{full_filename} failed to archive ({url_error.errno} {url_error.reason}")
            return url_error.errno, '', url_error.reason

        finally:
            if resp:
                resp.close()
            self.logger.info(f"{full_filename} archive_file completed")

    def get_status(self):
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
