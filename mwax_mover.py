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

FILE_REPLACEMENT_TOKEN = "__FILE__"

MODE_WATCH_DIR_FOR_RENAME = "WATCH_DIR_FOR_RENAME"
MODE_WATCH_DIR_FOR_NEW = "WATCH_DIR_FOR_NEW"
MODE_PROCESS_DIR = "PROCESS_DIR"


class RenameFileHandler(PatternMatchingEventHandler):
    def __init__(self, pattern, q, log):
        self.q = q
        self.logger = log
        # this takes the pattern- e.g. *.sub and returns the last string after the "."
        self.watch_ext = "." + pattern.split(".")[-1]
        PatternMatchingEventHandler.__init__(self, patterns=pattern)

    def on_moved(self, event):
        dest_filename = event.dest_path

        if os.path.splitext(dest_filename)[1] == self.watch_ext:
            self.q.put(dest_filename)
            self.logger.info(f'Added {dest_filename} to queue')


class NewFileHandler(PatternMatchingEventHandler):
    def __init__(self, pattern, q, log):
        self.q = q
        self.logger = log
        # this takes the pattern- e.g. *.sub and returns the last string after the "."
        self.watch_ext = "." + pattern.split(".")[-1]
        PatternMatchingEventHandler.__init__(self, patterns=pattern)

    def on_created(self, event):
        dest_filename = event.src_path

        if os.path.splitext(dest_filename)[1] == self.watch_ext:
            self.q.put(dest_filename)
            self.logger.info(f'Added {dest_filename} to queue')


class Watcher(object):
    def __init__(self, path='.', recursive=True, q=None, pattern=None, log=None, mode=None):
        self.logger = log
        self._observer = Observer()
        self.mode = mode

        if self.mode == MODE_WATCH_DIR_FOR_RENAME:
            self._observer.schedule(RenameFileHandler(pattern, q, self.logger), path, recursive)
        elif self.mode == MODE_WATCH_DIR_FOR_NEW:
            self._observer.schedule(NewFileHandler(pattern, q, self.logger), path, recursive)

    def start(self):
        self.logger.info("Watcher starting...")
        self._observer.start()

    def stop(self):
        self.logger.info("Watcher stopping...")
        if self._observer.isAlive():
            self._observer.stop()
            self._observer.join()


class QueueWorker(object):
    def __init__(self, q, executable_path, mode, log, rename_ext):
        self._q = q
        self._executable_path = executable_path
        self._running = False
        self._mode = mode
        self.logger = log
        self.rename_ext = rename_ext

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
                if self._mode == MODE_PROCESS_DIR:
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
        command = f"{self._executable_path}"

        # Substitute the filename into the command
        command = command.replace(FILE_REPLACEMENT_TOKEN, filename)

        # Example: "dada_diskdb -k 1234 -f 1216447872_02_256_201.sub -s"

        new_filename_no_ext = os.path.splitext(filename)[0]
        new_filename = os.path.join(os.path.dirname(filename), new_filename_no_ext + self.rename_ext)

        try:
            # Execute the command
            completed_process = subprocess.run(command, shell=True)

            return_code = completed_process.returncode
            stderror = completed_process.stderr

            if return_code == 0:
                # Success - rename the file
                try:
                    self.logger.info(f"Renaming {filename} to {new_filename}")
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
        self.logger = logging.getLogger('mwax_mover')

        # init vars
        self.watch_dir = None
        self.watch_ext = None
        self.rename_ext = None
        self.executable = None
        self.mode = None
        self.q = None
        self.watch = None
        self.queueworker = None
        self.running = False

    def initialise(self):

        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = "mwax_mover: a command line tool which is part of the mwax correlator for the MWA.\n"
        parser.add_argument("-w", "--watchdir", required=True, help="Directory to watch for files with watchext "
                                                                    "extension")
        parser.add_argument("-x", "--watchext", required=True, help="Extension to watch for")
        parser.add_argument("-r", "--renameext", required=True, help="Extension to rename file to")
        parser.add_argument("-e", "--executablepath", required=True,
                            help=f"Absolute path to executable to launch. {FILE_REPLACEMENT_TOKEN} "
                                 f"will be substituted with the abs path of the filename being processed.")
        parser.add_argument("-m", "--mode", required=True, default=None,
                            choices=[MODE_WATCH_DIR_FOR_NEW, MODE_WATCH_DIR_FOR_RENAME, MODE_PROCESS_DIR, ],
                            help=f"Mode to run:\n"
                            f"{MODE_WATCH_DIR_FOR_NEW}: Watch watchdir for new files forever. Launch executable. "
                                 f"Rename file.\n" 
                            f"{MODE_WATCH_DIR_FOR_RENAME}: Watch watchdir for renamed files forever. "
                                 f"Launch executable. Rename file.\n" 
                            f"{MODE_PROCESS_DIR}: For each file in watchdir, launch executable. Rename file. Exit.\n")
        args = vars(parser.parse_args())

        # Check args
        self.watch_dir = args["watchdir"]
        self.watch_ext = args["watchext"]
        self.rename_ext = args["renameext"]
        self.executable = args["executablepath"]
        self.mode = args["mode"]

        if not os.path.isdir(self.watch_dir):
            print(f"Error: --watchdir '{self.watch_dir}' does not exist or you don't have permission")
            exit(1)

        if not self.watch_ext[0] == ".":
            print(f"Error: --watchext '{self.watch_ext}' should start with a '.' e.g. '.sub'")
            exit(1)

        if not self.rename_ext[0] == ".":
            print(f"Error: --renameext '{self.rename_ext}' should start with a '.' e.g. '.done'")
            exit(1)

        # start logging
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
        self.logger.addHandler(ch)

        self.logger.info("Starting mwax_mover processor...")

        # Create a queue for dealing with files
        self.q = queue.Queue()

        # Create watcher
        self.watch = Watcher(path=self.watch_dir, recursive=False, q=self.q,
                             pattern=f"*.{self.watch_ext}", log=self.logger, mode=self.mode)

        # Create queueworker
        self.queueworker = QueueWorker(q=self.q, executable_path=self.executable,
                                       mode=self.mode, log=self.logger, rename_ext=self.rename_ext)

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

        self.logger.info(f"Running in mode {self.mode}")

        if self.mode == MODE_WATCH_DIR_FOR_RENAME or self.mode == MODE_WATCH_DIR_FOR_NEW:
            self.logger.info(f"Scanning {self.watch_dir} for files matching {'*' + self.watch_ext}...")

            # Setup thread for watching filesystem
            watcher_thread = threading.Thread(target=self.watch.start, daemon=True)

            # Start watcher
            watcher_thread.start()
        elif self.mode == MODE_PROCESS_DIR:
            # we don't need a watcher
            watcher_thread = None

            # Just loop through all files and add them to the queue
            pattern = os.path.join(self.watch_dir, "*" + self.watch_ext)
            self.logger.info(f"Scanning {self.watch_dir} for files matching {'*' + self.watch_ext}...")

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
