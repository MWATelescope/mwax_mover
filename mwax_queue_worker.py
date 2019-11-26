import mwax_mover
import os
import queue
import subprocess
import time


class QueueWorker(object):
    # Either pass an event handler or pass an executable path to run
    def __init__(self, label, q, executable_path, mode, log, event_handler):
        self.label = label
        self._q = q

        if (event_handler is None and executable_path is None) or \
           (event_handler is not None and executable_path is not None):
            raise Exception("QueueWorker requires event_handler OR executable_path not both and not neither!")

        self._executable_path = executable_path
        self._event_handler = event_handler
        self._running = False
        self._mode = mode
        self.logger = log

    def start(self):
        self.logger.info(f"QueueWorker {self.label} starting...")
        self._running = True

        while self._running:
            try:
                item = self._q.get(block=True, timeout=2)
                self.logger.info(f"Processing {item}...")

                if self._executable_path:
                    self.run_command(item)
                else:
                    self._event_handler(item, self._q)

                self._q.task_done()
                self.logger.info(f"Processing {item} Complete... Queue size: {self._q.qsize()}")
            except queue.Empty:
                if self._mode == mwax_mover.MODE_PROCESS_DIR:
                    # Queue is complete. Stop now
                    self.logger.info("Finished processing queue.")
                    self.stop()
                    return

            # Sleep for a couple of seconds
            time.sleep(2)

    def stop(self):
        self._running = False

    def run_command(self, filename):
        command = f"{self._executable_path}"

        # Substitute the filename into the command
        command = command.replace(mwax_mover.FILE_REPLACEMENT_TOKEN, filename)

        filename_no_ext = os.path.splitext(filename)[0]
        command = command.replace(mwax_mover.FILENOEXT_REPLACEMENT_TOKEN, filename_no_ext)

        # Example: "dada_diskdb -k 1234 -f 1216447872_02_256_201.sub -s"
        stderror = ""

        try:
            self.logger.info(f"Executing {command}...")
            # Execute the command
            completed_process = subprocess.run(command, shell=True)

            return_code = completed_process.returncode
            stderror = completed_process.stderr

            if return_code != 0:
                self.logger.error(f"Error executing {command}. Return code: {return_code} StdErr: {stderror}")

        except subprocess.CalledProcessError:
            self.logger.error(f"Error executing {command} StdErr: {stderror}")

        except Exception as command_exception:
            self.logger.error(f"Error executing {command}: {str(command_exception)}")