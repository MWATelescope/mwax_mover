"""Standalong mwax_mover module"""
import argparse
import logging
import logging.handlers
import os
import queue
import signal
import sys
import threading
import time
from tenacity import retry, stop_after_attempt, wait_fixed
from mwax_mover import mwax_queue_worker, mwax_watcher, utils

# The full filename with path
FILE_REPLACEMENT_TOKEN = "__FILE__"

# Full filename/path but with no extension
FILENOEXT_REPLACEMENT_TOKEN = "__FILENOEXT__"

MODE_WATCH_DIR_FOR_RENAME = "WATCH_DIR_FOR_RENAME"
MODE_WATCH_DIR_FOR_NEW = "WATCH_DIR_FOR_NEW"
MODE_WATCH_DIR_FOR_RENAME_OR_NEW = "WATCH_DIR_FOR_RENAME_OR_NEW"
MODE_PROCESS_DIR = "PROCESS_DIR"


@retry(stop=stop_after_attempt(5), wait=wait_fixed(10))
def remove_file(logger, filename: str, raise_error: bool) -> bool:
    """Deletes a file from the filesystem"""
    try:
        os.remove(filename)
        logger.info(f"{filename}- file deleted")
        return True

    except Exception as delete_exception:  # pylint: disable=broad-except
        if raise_error:
            logger.error(
                f"{filename}- Error deleting: {delete_exception}. Retrying up"
                " to 5 times."
            )
            raise delete_exception
        else:
            logger.warning(
                f"{filename}- Error deleting: {delete_exception}. File may"
                " have been moved or removed."
            )
            return True


class Processor:
    """Class to process items"""

    def __init__(self):
        # init the logging subsystem
        self.logger = logging.getLogger("mwax_mover")

        # init vars
        self.watch_dir = None
        self.watch_ext = None
        self.rename_ext = None
        self.executable = None
        self.mode = None
        self.recursive = False
        self.queue = None
        self.watch = None
        self.queueworker = None
        self.running = False

    def initialise(self):
        """Initialisation/setup"""
        # Get command line args
        parser = argparse.ArgumentParser()
        parser.description = (
            "mwax_mover: a command line tool which is part of the mwax"
            " correlator for the MWA.\n"
        )
        parser.add_argument(
            "-w",
            "--watchdir",
            required=True,
            help="Directory to watch for files with watchext extension",
        )
        parser.add_argument(
            "-x",
            "--watchext",
            required=True,
            help="Extension to watch for e.g. .sub",
        )
        parser.add_argument(
            "-e",
            "--executablepath",
            required=True,
            help=(
                "Absolute path to executable to launch."
                f" {FILE_REPLACEMENT_TOKEN} will be substituted with the abs"
                " path of the filename being"
                f" processed.{FILENOEXT_REPLACEMENT_TOKEN} will be replaced"
                " with the filename but not extenson."
            ),
        )
        parser.add_argument(
            "-m",
            "--mode",
            required=True,
            default=None,
            choices=[
                MODE_WATCH_DIR_FOR_NEW,
                MODE_WATCH_DIR_FOR_RENAME,
                MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
                MODE_PROCESS_DIR,
            ],
            help=(
                f"Mode to run:\n{MODE_WATCH_DIR_FOR_NEW}: Watch watchdir for"
                " new files forever. Launch"
                f" executable.\n{MODE_WATCH_DIR_FOR_RENAME}: Watch watchdir"
                " for renamed files forever. Launch"
                f" executable.\n{MODE_WATCH_DIR_FOR_RENAME_OR_NEW}: Watch"
                " watchdir for new OR renamed files forever. Launch"
                f" executable.\n{MODE_PROCESS_DIR}: For each file in watchdir,"
                " launch executable. Exit.\n"
            ),
        )
        parser.add_argument(
            "-r",
            "--recursive",
            required=False,
            default=False,
            help=(
                "Recurse subdirectories of the watchdir. Omitting this option"
                " is the default and only the watchdir will be monitored."
            ),
        )
        args = vars(parser.parse_args())

        # Check args
        self.watch_dir = args["watchdir"]
        self.watch_ext = args["watchext"]
        self.executable = args["executablepath"]
        self.mode = args["mode"]

        if self.mode == MODE_PROCESS_DIR:
            exit_once_queue_empty = True
        else:
            exit_once_queue_empty = False

        if args["recursive"]:
            self.recursive = args["recursive"]

        if not os.path.isdir(self.watch_dir):
            print(
                f"Error: --watchdir '{self.watch_dir}' does not exist or you"
                " don't have permission"
            )
            exit(1)

        if not self.watch_ext[0] == ".":
            print(
                f"Error: --watchext '{self.watch_ext}' should start with a '.'"
                " e.g. '.sub'"
            )
            exit(1)

        # start logging
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(
            logging.Formatter("%(asctime)s, %(levelname)s, %(message)s")
        )
        self.logger.addHandler(handler)

        self.logger.info("Starting mwax_mover processor...")

        # Create a queue for dealing with files
        self.queue = queue.Queue()

        # Create watcher
        self.watch = mwax_watcher.Watcher(
            path=self.watch_dir,
            dest_queue=self.queue,
            pattern=f"{self.watch_ext}",
            log=self.logger,
            mode=self.mode,
            recursive=False,
        )

        # Create queueworker
        self.queueworker = mwax_queue_worker.QueueWorker(
            label="queue",
            source_queue=self.queue,
            executable_path=self.executable,
            exit_once_queue_empty=exit_once_queue_empty,
            log=self.logger,
            event_handler=None,
        )

        self.running = True
        self.logger.info("Processor Initialised...")

    def signal_handler(self, _signum, _frame):
        """Handles SIGINT and SIGTERM"""
        self.logger.warning("Interrupted. Shutting down...")
        self.running = False
        self.watch.stop()
        self.queueworker.stop()

    def start(self):
        """Start processing"""
        # Make sure we can Ctrl-C / kill out of this
        signal.signal(signal.SIGINT, self.signal_handler)

        self.logger.info(f"Running in mode {self.mode}")

        if (
            self.mode == MODE_WATCH_DIR_FOR_RENAME
            or self.mode == MODE_WATCH_DIR_FOR_NEW
        ):
            self.logger.info(
                f"Scanning {self.watch_dir} for files matching"
                f" {'*' + self.watch_ext}..."
            )

            # Setup thread for watching filesystem
            watcher_thread = threading.Thread(
                target=self.watch.start, daemon=True
            )

            # Start watcher
            watcher_thread.start()
        elif self.mode == MODE_PROCESS_DIR:
            # we don't need a watcher
            watcher_thread = None

            utils.scan_for_existing_files_and_add_to_queue(
                self.logger,
                self.watch_dir,
                self.watch_ext,
                self.recursive,
                self.queue,
            )
        else:
            # Unsupported modes
            watcher_thread = None

        # Setup thread for processing items
        queueworker_thread = threading.Thread(
            target=self.queueworker.start, daemon=True
        )

        # Start queue worker
        queueworker_thread.start()

        while self.running:
            if queueworker_thread.is_alive():
                time.sleep(0.001)
            else:
                self.running = False
                break

        # Wait for threads to finish
        if watcher_thread:
            watcher_thread.join()

        if queueworker_thread.is_alive():
            queueworker_thread.join()

        # Finished
        self.logger.info("Completed Successfully")


def main():
    """Main function"""
    processor = Processor()

    try:
        processor.initialise()
        processor.start()
        sys.exit(0)
    except Exception as catch_all_exception:  # pylint: disable=broad-except
        if processor.logger:
            processor.logger.exception(str(catch_all_exception))
        else:
            print(str(catch_all_exception))


if __name__ == "__main__":
    main()