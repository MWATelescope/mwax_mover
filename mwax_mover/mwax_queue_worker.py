from mwax_mover import mwax_mover, mwax_command
import os
import queue
import time


class QueueWorker(object):
    # Either pass an event handler or pass an executable path to run
    #
    # requeue_to_eoq_on_failure: if work fails, True  = requeue to back of queue, try the next item
    #                                                   (order does not matter)
    #                                         , False = keep retrying this item (i.e. order of items matters)
    def __init__(self,
                 label: str,
                 q,
                 executable_path,
                 log,
                 event_handler,
                 exit_once_queue_empty,
                 requeue_to_eoq_on_failure: bool = True,
                 backoff_initial_seconds: int = 1,
                 backoff_factor: int = 2,
                 backoff_limit_seconds: int = 60):
        self.label = label
        self.q = q

        if (event_handler is None and executable_path is None) or \
           (event_handler is not None and executable_path is not None):
            raise Exception("QueueWorker requires event_handler OR executable_path not both and not neither!")

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

    def start(self):
        self.logger.info(f"QueueWorker {self.label} starting...")
        self._running = True
        self.current_item = None
        self.consecutive_error_count = 0
        backoff = 0

        while self._running:
            if not self._paused:
                try:
                    success = False

                    if self.current_item is None:
                        self.current_item = self.q.get(block=True, timeout=0.5)
                    self.logger.info(f"Processing {self.current_item}...")

                    start_time = time.time()

                    # Check file exists (maybe someone deleted it?)
                    if os.path.exists(self.current_item):
                        if self._executable_path:
                            success = self.run_command(self.current_item)
                        else:
                            success = self._event_handler(self.current_item)

                        if success:
                            # Dequeue the item, but requeue if it was not successful
                            self.q.task_done()
                            self.current_item = None
                        else:
                            # If this option is set, add item back to the end of the queue
                            # If not set, just keep retrying the operation
                            if self.requeue_to_eoq_on_failure:
                                self.q.task_done()
                                self.q.put(self.current_item)
                                self.current_item = None
                    else:
                        # Dequeue the item
                        self.q.task_done()
                        self.logger.warning(f"Processing {self.current_item } Complete... file was moved or deleted. "
                                            f"Queue size: {self.q.qsize()}")
                        self.current_item = None

                    elapsed = time.time() - start_time
                    self.logger.info(f"Complete. Queue size: {self.q.qsize()} Elapsed: {elapsed:.2f} sec")

                    if success:
                        # reset our error count and backoffs
                        self.consecutive_error_count = 0
                    else:
                        self.consecutive_error_count += 1
                        backoff = self.backoff_initial_seconds * self.backoff_factor * self.consecutive_error_count
                        if backoff > self.backoff_limit_seconds:
                            backoff = self.backoff_limit_seconds

                        self.logger.info(f"{self.consecutive_error_count} consecutive failures. Backing off "
                                         f"for {backoff} seconds.")
                        time.sleep(backoff)

                except queue.Empty:
                    if self.exit_once_queue_empty:
                        # Queue is complete. Stop now
                        self.logger.info("Finished processing queue.")
                        self.stop()
                        return

    def pause(self, paused: bool):
        self._paused = paused

    def stop(self):
        self._running = False

    def run_command(self, filename: str) -> bool:
        command = f"{self._executable_path}"

        # Substitute the filename into the command
        command = command.replace(mwax_mover.FILE_REPLACEMENT_TOKEN, filename)

        filename_no_ext = os.path.splitext(filename)[0]
        command = command.replace(mwax_mover.FILENOEXT_REPLACEMENT_TOKEN, filename_no_ext)

        return mwax_command.run_shell_command(self.logger, command)

    def get_status(self) -> dict:
        return {"current": self.current_item,
                "queue_size": self.q.qsize()}
