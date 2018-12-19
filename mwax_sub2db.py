import argparse
import glob
import logging
import logging.handlers
import os
import queue
import signal
import subprocess
import sys
import threading
import time
from watchdog.observers import Observer
from watchdog.events import PatternMatchingEventHandler

EVENT_TYPE_MOVED = 'moved'
EVENT_TYPE_DELETED = 'deleted'
EVENT_TYPE_CREATED = 'created'
EVENT_TYPE_MODIFIED = 'modified'

SUBFILE_EXTENSION = ".sub"
FINISHED_EXTENSION = ".done"

MODE_CORRELATOR = "CORRELATOR"
MODE_OFFLINE_CORRELATOR = "OFFLINE_CORRELATOR"
MODE_VOLTAGE_CAPTURE = "VOLTAGE_CAPTURE"


class RenameFileHandler(PatternMatchingEventHandler):
    def __init__(self, pattern, q, log):
        self.q = q
        self.logger = log
        PatternMatchingEventHandler.__init__(self, patterns=pattern)

    def on_moved(self, event):
        dest_filename = event.dest_path

        if os.path.splitext(dest_filename)[1] == SUBFILE_EXTENSION:
            self.q.put(dest_filename)
            self.logger.info(f'Added {dest_filename} to queue')


class Watcher(object):
    def __init__(self, path='.', recursive=True, q=None, pattern="*.*", log=None):
        self.logger = log
        self._observer = Observer()
        self._observer.schedule(RenameFileHandler(pattern, q, self.logger), path, recursive)

    def start(self):
        self.logger.info("Watcher starting...")
        self._observer.start()

    def stop(self):
        self.logger.info("Watcher stopping...")
        if self._observer.isAlive():
            self._observer.stop()
            self._observer.join()


class QueueWorker(object):
    def __init__(self, q, dada_diskdb_path, dada_key, mode, log):
        self._q = q
        self._dada_diskdb_path = dada_diskdb_path
        self._dada_key = dada_key
        self._running = False
        self._mode = mode
        self.logger = log

    def start(self):
        self.logger.info("QueueWorker starting...")
        self._running = True

        while self._running or self._q.qsize() > 0:
            try:
                item = self._q.get(block=True, timeout=0.5)
                self.logger.info(f"Processing {item}...")
                self.run_command(item)
                self._q.task_done()
                self.logger.info(f"Processing {item} Complete... Queue size: {self._q.qsize()}")
            except queue.Empty:
                if self._mode == MODE_OFFLINE_CORRELATOR:
                    # Queue is complete. Stop now
                    self.logger.info("Finished processing queue.")
                    self.stop()
                    return
                else:
                    pass

    def stop(self):
        self._running = False
        if self._q.qsize() == 0:
            self.logger.info("QueueWorker stopping...")
        else:
            self.logger.info("QueueWorker stopping... finishing remaining items")

    def run_command(self, filename):
        command = f"{self._dada_diskdb_path} -k {self._dada_key} -f {filename} -s"
        # Example: "dada_diskdb -k 1234 -f 1216447872_02_256_201.sub -s"

        new_filename_no_ext = os.path.splitext(filename)[0]
        new_filename = os.path.join(os.path.dirname(filename), new_filename_no_ext + FINISHED_EXTENSION)

        try:
            # Execute the command
            completed_process = subprocess.run(command, shell=True)

            return_code = completed_process.returncode
            stderror = completed_process.stderr

            if return_code == 0:
                # Success - rename the file
                try:
                    os.rename(filename, new_filename)
                except Exception as rename_exception:
                    self.logger.error(f"Error renaming {filename} to {new_filename}: {str(rename_exception)}")
            else:
                self.logger.error(f"Error executing {command}. Return code: {return_code} StdErr: {stderror}")

        except subprocess.CalledProcessError:
            self.logger.error(f"Error executing {command} StdErr: {stderror}")

        except Exception as command_exception:
            self.logger.error(f"Error executing {command}: {str(command_exception)}")


class Processor:
    def __init__(self):
        # init the logging subsystem
        self.logger = logging.getLogger('mwax_sub2db')

        # init vars
        self.watch_dir = None
        self.dada_key = None
        self.dada_diskdb = None
        self.mode = None
        self.q = None
        self.watch = None
        self.queueworker = None
        self.running = False

    def initialise(self):
        # start logging
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        rot = logging.FileHandler('mwax_sub2db.log')
        rot.setLevel(logging.DEBUG)
        rot.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
        self.logger.addHandler(rot)

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
        self.logger.addHandler(ch)

        self.logger.info("Starting mwax_sub2db processor...")

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = "mwax_sub2db: a command line tool which is part of the mwax correlator for the MWA.\n" \
                             "In CORRELATOR mode: Watches a directory and file pattern (for sub files). On new file, " \
                             "launches diskdb to add the file to the designated PSRDADA ringbuffer.\n" \
                             "In OFFLINE CORRELATOR mode: Retrieves a file list from a directory and file pattern. " \
                             "For each file, launches diskdb to add the file to the designated PSRDADA ringbuffer.\n" \
                             "In VOLTAGE_CAPTURE mode: Watches a directory and file pattern. On new file, " \
                             "copy the sub file to local disk, where the archiver daemon will send to Pawsey/NGAS.\n" \
                             "\n*.sub files are sub-observations of 8 seconds containing high time res data."
        parser.add_argument("-w", "--watchdir", required=True, help="Directory to watch for *.sub files")
        parser.add_argument("-k", "--key", required=True, help="PSRDADA ringbuffer key of the destination data block")
        parser.add_argument("-d", "--dadadiskdbpath", required=True,
                            help="Absolute path to PSRDADA dada_diskdb executable")
        parser.add_argument("-m", "--mode", required=True, default=MODE_CORRELATOR,
                            choices=[MODE_CORRELATOR, MODE_OFFLINE_CORRELATOR, MODE_VOLTAGE_CAPTURE, ],
                            help=f"Mode to run- default={MODE_CORRELATOR}")
        args = vars(parser.parse_args())

        # Check args
        self.watch_dir = args["watchdir"]
        self.dada_key = args["key"]
        self.dada_diskdb = args["dadadiskdbpath"]
        self.mode = args["mode"]

        if self.mode != MODE_CORRELATOR and self.mode != MODE_OFFLINE_CORRELATOR:
            print(f"Error: --mode {self.mode} is not supported.")
            exit(1)

        if not os.path.isdir(self.watch_dir):
            print(f"Error: --watchdir {self.watch_dir} does not exist or you don't have permission.")
            exit(1)

        if not os.path.isfile(self.dada_diskdb):
            print(f"Error: --dadadiskdbpath {self.dada_diskdb} executable does not exist or you don't have permission.")
            exit(1)

        # Create a queue for dealing with files
        self.q = queue.Queue()

        # Create watcher
        self.watch = Watcher(path=self.watch_dir, recursive=False, q=self.q,
                             pattern=f"*.{SUBFILE_EXTENSION}", log=self.logger)

        # Create queueworker
        self.queueworker = QueueWorker(q=self.q, dada_diskdb_path=self.dada_diskdb,
                                       dada_key=self.dada_key, mode=self.mode, log=self.logger)

        self.running = True
        self.logger.info("Processor Initialised...")

    def signal_handler(self, signum, frame):
        self.logger.warning("Interrupted. Shutting down...")
        self.running = False
        self.watch.stop()
        self.queueworker.stop()

    def start(self):
        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)

        self.logger.info(f"Running in {self.mode}")

        if self.mode == MODE_CORRELATOR:
            # Setup thread for watching filesystem
            watcher_thread = threading.Thread(target=self.watch.start, daemon=True)

            # Start watcher
            watcher_thread.start()
        elif self.mode == MODE_OFFLINE_CORRELATOR:
            # we don't need a watcher
            watcher_thread = None

            # Just loop through all files and add them to the queue
            pattern = os.path.join(self.watch_dir, "*" + SUBFILE_EXTENSION)
            self.logger.info(f"Scanning {self.watch_dir} for files matching {'*' + SUBFILE_EXTENSION}...")

            files = glob.glob(pattern)

            self.logger.info(f"Found {len(files)} files")

            for file in sorted(files):
                self.q.put(file)
                self.logger.info(f'Added {file} to queue')
        else:
            # Unsupported modes
            watcher_thread = None

        # Setup thread for processing items
        queueworker_thread = threading.Thread(target=self.queueworker.start, daemon=True)

        # Start queue worker
        queueworker_thread.start()

        while self.running:
            if queueworker_thread.isAlive():
                time.sleep(1)
            else:
                self.running = False
                break

        # Wait for threads to finish
        if watcher_thread:
            watcher_thread.join()

        if queueworker_thread.isAlive():
            queueworker_thread.join()

        # Finished
        self.logger.info("Completed Successfully")


if __name__ == '__main__':
    p = Processor()

    try:
        p.initialise()
        p.start()
        sys.exit(0)
    except Exception as e:
        if p.logger:
            p.logger.exception(str(e))
        else:
            print(str(e))
