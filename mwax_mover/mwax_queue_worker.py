from mwax_mover import mwax_mover, mwax_command
import os
import queue
import time


class QueueWorker(object):
    # Either pass an event handler or pass an executable path to run
    def __init__(self, label, q, executable_path, mode, log, event_handler):
        self.label = label
        self.q = q

        if (event_handler is None and executable_path is None) or \
           (event_handler is not None and executable_path is not None):
            raise Exception("QueueWorker requires event_handler OR executable_path not both and not neither!")

        self._executable_path = executable_path
        self._event_handler = event_handler
        self._running = False
        self._paused = False
        self._mode = mode
        self.logger = log
        self.current_item = None

    def start(self):
        self.logger.info(f"QueueWorker {self.label} starting...")
        self._running = True

        while self._running:
            if not self._paused:
                try:
                    item = self.q.get(block=True, timeout=0.1)
                    self.current_item = item
                    self.logger.info(f"Processing {item}...")

                    start_time = time.time()

                    # Check file exists (maybe someone deleted it?)
                    if os.path.exists(item):
                        if self._executable_path:
                            success = self.run_command(item)
                        else:
                            success = self._event_handler(item)

                        # Dequeue the item, but requeue if it was not successful
                        self.q.task_done()

                        if not success:
                            self.q.put(item)
                    else:
                        # Dequeue the item
                        self.q.task_done()
                        self.logger.warning(f"Processing {item} Complete... file was moved or deleted. "
                                            f"Queue size: {self.q.qsize()}")

                    self.current_item = None
                    self.logger.info(f"Complete {item}. Queue size: {self.q.qsize()} Elapsed: {time.time() - start_time} sec")

                except queue.Empty:
                    if self._mode == mwax_mover.MODE_PROCESS_DIR:
                        # Queue is complete. Stop now
                        self.logger.info("Finished processing queue.")
                        self.stop()
                        return

    def pause(self, paused):
        self._paused = paused

    def stop(self):
        self._running = False

    def run_command(self, filename):
        command = f"{self._executable_path}"

        # Substitute the filename into the command
        command = command.replace(mwax_mover.FILE_REPLACEMENT_TOKEN, filename)

        filename_no_ext = os.path.splitext(filename)[0]
        command = command.replace(mwax_mover.FILENOEXT_REPLACEMENT_TOKEN, filename_no_ext)

        return mwax_command.run_shell_command(self.logger, command)

    def get_status(self):
        return {"current": self.current_item,
                "queue_size": self.q.qsize()}
