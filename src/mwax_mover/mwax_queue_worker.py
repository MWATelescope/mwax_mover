"""Module for the QueueWorker class"""

import os
import queue
import time
import threading
from mwax_mover import mwax_mover, mwax_command


class QueueWorker(object):
    """This class represents a worker process, processing items off a queue"""

    # Either pass an event handler or pass an executable path to run
    #
    # requeue_to_eoq_on_failure: if work fails, True  = requeue to back of
    #                                                   queue, try the next
    #                                                   item (order does not
    #                                                   matter)
    #                                         , False = keep retrying this
    #                                                   item (i.e. order
    #                                                   of items matters)
    def __init__(
        self,
        name: str,
        source_queue: queue.Queue,
        executable_path,
        log,
        event_handler,
        exit_once_queue_empty,
        requeue_to_eoq_on_failure: bool = True,
        backoff_initial_seconds: int = 1,
        backoff_factor: int = 2,
        backoff_limit_seconds: int = 60,
    ):
        self.name = name
        self.source_queue = source_queue

        if (event_handler is None and executable_path is None) or (
            event_handler is not None and executable_path is not None
        ):
            raise Exception("QueueWorker requires event_handler OR executable_path not" " both and not neither!")

        self._executable_path = executable_path
        self._event_handler = event_handler
        self._running = False
        self._paused = False
        self.exit_once_queue_empty = exit_once_queue_empty
        self.requeue_to_eoq_on_failure = requeue_to_eoq_on_failure
        self.logger = log
        self.current_item = None
        self.consecutive_error_count = 0
        self.backoff_initial_seconds = backoff_initial_seconds
        self.backoff_factor = backoff_factor
        self.backoff_limit_seconds = backoff_limit_seconds
        # Use threading event instead of time.sleep to backoff
        self.event = threading.Event()

    def start(self):
        """Start working on the queue"""
        self.logger.info(f"QueueWorker {self.name} starting...")
        self._running = True
        self.current_item = None
        self.consecutive_error_count = 0
        backoff = 0

        while self._running:
            if not self._paused:
                try:
                    success = False

                    if self.current_item is None:
                        self.current_item = self.source_queue.get(block=True, timeout=0.5)
                    self.logger.info(f"Processing {self.current_item}...")

                    start_time = time.time()

                    # Check file exists (maybe someone deleted it?)
                    if os.path.exists(self.current_item):
                        if self._executable_path:
                            success = self.run_command(self.current_item)
                        else:
                            success = self._event_handler(self.current_item)

                        if success:
                            # Dequeue the item, but requeue if it was not
                            # successful
                            self.source_queue.task_done()
                            self.current_item = None
                    else:
                        # Dequeue the item
                        self.logger.warning(
                            f"Processing {self.current_item } Complete... file"
                            " was moved or deleted. Queue size:"
                            f" {self.source_queue.qsize()}"
                        )
                        self.current_item = None
                        self.source_queue.task_done()
                        continue

                    elapsed = time.time() - start_time
                    self.logger.info(
                        "Complete. Queue size:" f" {self.source_queue.qsize()} Elapsed:" f" {elapsed:.2f} sec"
                    )

                    if success:
                        # reset our error count and backoffs
                        self.consecutive_error_count = 0
                    else:
                        self.consecutive_error_count += 1
                        backoff = self.backoff_initial_seconds * self.backoff_factor * self.consecutive_error_count
                        if backoff > self.backoff_limit_seconds:
                            backoff = self.backoff_limit_seconds

                        self.logger.info(
                            f"{self.consecutive_error_count} consecutive"
                            f" failures. Backing off for {backoff} seconds."
                        )
                        self.event.wait(backoff)

                        # If this option is set, add item back to the end of
                        # the queue
                        # If not set, just keep retrying the operation
                        if self.requeue_to_eoq_on_failure:
                            self.source_queue.task_done()
                            self.source_queue.put(self.current_item)
                            self.current_item = None

                except queue.Empty:
                    if self.exit_once_queue_empty:
                        # Queue is complete. Stop now
                        self.logger.info("Finished processing queue.")
                        self.stop()
                        return

    def pause(self, paused: bool):
        """Pause the processing"""
        self._paused = paused

    def stop(self):
        """Stop the queue worker"""
        self._running = False
        # cancel a wait if we are in one
        self.event.set()

    def run_command(self, filename: str) -> bool:
        """Execute a command"""
        command = f"{self._executable_path}"

        # Substitute the filename into the command
        command = command.replace(mwax_mover.FILE_REPLACEMENT_TOKEN, filename)

        filename_no_ext = os.path.splitext(filename)[0]
        command = command.replace(mwax_mover.FILENOEXT_REPLACEMENT_TOKEN, filename_no_ext)

        return mwax_command.run_command_ext(self.logger, command, -1, 60, True)

    def get_status(self) -> dict:
        """Return the status as a dictionary"""
        return {
            "Unix timestamp": time.time(),
            "current": self.current_item,
            "queue_size": self.source_queue.qsize(),
        }
