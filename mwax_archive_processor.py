import mwax_mover
import mwax_queue_worker
import mwax_subfile_distributor
import mwax_watcher
import os
import queue
import threading
import time
from urllib.parse import urlencode
from urllib.request import urlopen
import urllib.error


class ArchiveProcessor:
    def __init__(self, logger, mode, archive_host, archive_port):
        self.logger = logger

        self.archive_destination_host = archive_host
        self.archive_destination_port = archive_port

        self.mode = mode
        self.mwax_mover_mode = mwax_mover.MODE_WATCH_DIR_FOR_NEW

        self.queue_db = queue.Queue()
        self.queue_worker_db = None

        self.watcher_threads = []
        self.worker_threads = []

        self.watch_dir_volt = "/voltdata"
        self.queue_volt = queue.Queue()
        self.watcher_volt = None
        self.queue_worker_volt = None

        self.watch_dir_vis = "/visdata"
        self.queue_vis = queue.Queue()
        self.watcher_vis = None
        self.queue_worker_vis = None

        self.watch_dir_fil = "/visdata"
        self.queue_fil = queue.Queue()
        self.watcher_fil = None
        self.queue_worker_fil = None

    def start(self):
        if self.mode == mwax_subfile_distributor.MODE_MWAX_CORRELATOR:
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

        elif self.mode == mwax_subfile_distributor.MODE_MWAX_BEAMFORMER:
            # Create watcher for filterbank data -> filterbank queue
            self.watcher_fil = mwax_watcher.Watcher(path=self.watch_dir_fil, q=self.queue_fil,
                                                    pattern=".fil", log=self.logger,
                                                    mode=self.mwax_mover_mode)

            # Create queueworker for filterbank queue
            self.queue_worker_fil = mwax_queue_worker.QueueWorker(label="Filterbank Archive",
                                                                  q=self.queue_fil,
                                                                  executable_path=None,
                                                                  event_handler=self.archive_handler,
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

    def db_handler(self, item, origin_queue):
        self.logger.info(f"{item}- ArchiveProcessor.handler is handling {item}...")

        # immediately add this file to the db so we insert a record into metadata data_files table
        # Get info
        filename = os.path.basename(item)
        obsid = filename[0:10]
        file_ext = os.path.splitext(item)[1]
        file_size = os.stat(item).st_size
        filetype = None
        hostname = mwax_mover.get_hostname()
        remote_archived = False     # This gets set to True by NGAS at Pawsey
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
        if not self.insert_data_file_row(obsid, filetype, file_size, filename, site_path, hostname, remote_archived):
            # if something went wrong, requeue after 2 seconds
            time.sleep(2)
            origin_queue.put(item)

        # immediately add this file (and a ptr to it's queue) to the voltage or vis queue which will deal with archiving
        if file_ext.lower() == ".sub":
            self.queue_volt.put(item)
        elif file_ext.lower() == ".fits":
            self.queue_vis.put(item)

        self.logger.info(f"{item}- ArchiveProcessor.db_handler finished handling.")

    def archive_handler(self, item, origin_queue):
        self.logger.info(f"{item}- ArchiveProcessor.archive_handler is handling {item}...")

        if self.archive_file(item) != 200:
            # Retry
            origin_queue.put(item)

        self.logger.info(f"{item}- ArchiveProcessor.archive_handler finished handling.")

    def stop(self):
        if self.mode == mwax_subfile_distributor.MODE_MWAX_CORRELATOR:
            self.watcher_volt.stop()
            self.watcher_vis.stop()
            self.queue_worker_db.stop()
            self.queue_worker_volt.stop()
            self.queue_worker_vis.stop()

        elif self.mode == mwax_subfile_distributor.MODE_MWAX_BEAMFORMER:
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

    def insert_data_file_row(self, obsid, filetype, file_size, filename, site_path, hostname, remote_archived):
        self.logger.info(f"INSERT INTO data_files (observation_num, filetype, size, filename, "
                         f"site_path, host, remote_archived) "
                         f"VALUES ({obsid}, {filetype}, {file_size}, {filename}, "
                         f"{site_path}, {hostname}, {remote_archived})")

        return True

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