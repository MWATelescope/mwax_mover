from mwax_mover import mwax_queue_worker
from mwax_mover import mwax_watcher
import argparse
import logging
import logging.handlers
import os
import queue
import signal
import subprocess
from tenacity import *
import threading
import time

FILE_REPLACEMENT_TOKEN = "__FILE__"
FILENOEXT_REPLACEMENT_TOKEN = "__FILENOEXT__"

MODE_WATCH_DIR_FOR_RENAME = "WATCH_DIR_FOR_RENAME"
MODE_WATCH_DIR_FOR_NEW = "WATCH_DIR_FOR_NEW"
MODE_PROCESS_DIR = "PROCESS_DIR"


def run_command(command: str, command_timeout_seconds: float) -> bool:
    try:
        # launch the process
        subprocess.run(f"{command}",
                       shell=True, check=True, timeout=command_timeout_seconds)
        return True

    except subprocess.CalledProcessError as process_error:
        raise Exception(f"Error launching {command}. "
                        f"Return code {process_error.returncode},"
                        f"Output {process_error.output} {process_error.stderr}")

    except Exception as unknown_exception:
        raise Exception(f"Unknown error launching {command}. {unknown_exception}")


@retry(stop=stop_after_attempt(5), wait=wait_fixed(10))
def remove_file(logger, filename: str) -> bool:
    try:
        os.remove(filename)
        logger.info(f"{filename}- file deleted")
        return True

    except Exception as delete_exception:
        logger.error(f"{filename}- Error deleting: {delete_exception}. Retrying up to 5 times.")
        raise delete_exception


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
        parser.add_argument("-x", "--watchext", required=True, help="Extension to watch for e.g. .sub")
        parser.add_argument("-e", "--executablepath", required=True,
                            help=f"Absolute path to executable to launch. {FILE_REPLACEMENT_TOKEN} "
                                 f"will be substituted with the abs path of the filename being processed."
                                 f"{FILENOEXT_REPLACEMENT_TOKEN} will be replaced with the filename but not extenson.")
        parser.add_argument("-m", "--mode", required=True, default=None,
                            choices=[MODE_WATCH_DIR_FOR_NEW, MODE_WATCH_DIR_FOR_RENAME, MODE_PROCESS_DIR, ],
                            help=f"Mode to run:\n"
                            f"{MODE_WATCH_DIR_FOR_NEW}: Watch watchdir for new files forever. Launch executable.\n" 
                            f"{MODE_WATCH_DIR_FOR_RENAME}: Watch watchdir for renamed files forever. Launch executable.\n" 
                            f"{MODE_PROCESS_DIR}: For each file in watchdir, launch executable. Exit.\n")
        args = vars(parser.parse_args())

        # Check args
        self.watch_dir = args["watchdir"]
        self.watch_ext = args["watchext"]
        self.executable = args["executablepath"]
        self.mode = args["mode"]

        if not os.path.isdir(self.watch_dir):
            print(f"Error: --watchdir '{self.watch_dir}' does not exist or you don't have permission")
            exit(1)

        if not self.watch_ext[0] == ".":
            print(f"Error: --watchext '{self.watch_ext}' should start with a '.' e.g. '.sub'")
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
        self.watch = mwax_watcher.Watcher(path=self.watch_dir, q=self.q,
                                          pattern=f"{self.watch_ext}", log=self.logger, mode=self.mode)

        # Create queueworker
        self.queueworker = mwax_queue_worker.QueueWorker(label="queue", q=self.q, executable_path=self.executable,
                                                         mode=self.mode, log=self.logger, event_handler=None)

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

            mwax_watcher.scan_directory(self.logger, self.watch_dir, self.watch_ext, self.q)
        else:
            # Unsupported modes
            watcher_thread = None

        # Setup thread for processing items
        queueworker_thread = threading.Thread(target=self.queueworker.start, daemon=True)

        # Start queue worker
        queueworker_thread.start()

        while self.running:
            if queueworker_thread.isAlive():
                time.sleep(0.001)
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


def main():
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


if __name__ == '__main__':
    main()
