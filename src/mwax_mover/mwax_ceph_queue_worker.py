"""Module for the CephQueueWorker"""
import os
import queue
import time
from mwax_mover.mwax_queue_worker import QueueWorker
from mwax_mover.mwa_archiver import ceph_get_s3_object


class CephQueueWorker(QueueWorker):
    """Subclass of a queue worker which houses a Boto3 session object so we
    can take advantage of Session connection pooling"""

    def __init__(
        self,
        label: str,
        source_queue,
        executable_path,
        log,
        event_handler,
        exit_once_queue_empty,
        requeue_to_eoq_on_failure: bool = True,
        backoff_initial_seconds: int = 1,
        backoff_factor: int = 2,
        backoff_limit_seconds: int = 60,
        ceph_profile: str = "",
        ceph_endpoint: str = "",
    ):
        # Call Default QueueWorker contstructor
        super().__init__(
            label,
            source_queue,
            executable_path,
            log,
            event_handler,
            exit_once_queue_empty,
            requeue_to_eoq_on_failure,
            backoff_initial_seconds,
            backoff_factor,
            backoff_limit_seconds,
        )

        self.ceph_profile = ceph_profile
        self.ceph_endpoint = ceph_endpoint
        self.ceph_session = None

    def start(self):
        """Overrride this method from QueueWorker so we can initiate a boto3
        session"""

        self.logger.info(f"CephQueueWorker {self.label} starting...")
        #
        # Init the Boto3 session
        #
        # get s3 object
        try:
            self.ceph_session = ceph_get_s3_object(
                self.ceph_profile, self.ceph_endpoint
            )
        except Exception as e:
            self.logger.error(
                "Error creating Ceph Session: Profile:"
                f" {self.ceph_profile} Endpoint: {self.ceph_endpoint}."
                f" Error {e}"
            )
            return

        self._running = True
        self.current_item = None
        self.consecutive_error_count = 0
        backoff = 0

        while self._running:
            if not self._paused:
                try:
                    success = False

                    if self.current_item is None:
                        self.current_item = self.source_queue.get(
                            block=True, timeout=0.5
                        )
                    self.logger.info(f"Processing {self.current_item}...")

                    start_time = time.time()

                    # Check file exists (maybe someone deleted it?)
                    if os.path.exists(self.current_item):
                        if self._executable_path:
                            success = self.run_command(self.current_item)
                        else:
                            success = self._event_handler(
                                self.current_item, self.ceph_session
                            )

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
                        "Complete. Queue size:"
                        f" {self.source_queue.qsize()} Elapsed:"
                        f" {elapsed:.2f} sec"
                    )

                    if success:
                        # reset our error count and backoffs
                        self.consecutive_error_count = 0
                    else:
                        self.consecutive_error_count += 1
                        backoff = (
                            self.backoff_initial_seconds
                            * self.backoff_factor
                            * self.consecutive_error_count
                        )
                        if backoff > self.backoff_limit_seconds:
                            backoff = self.backoff_limit_seconds

                        self.logger.info(
                            f"{self.consecutive_error_count} consecutive"
                            f" failures. Backing off for {backoff} seconds."
                        )
                        self.event.wait(backoff)

                        # If this option is set, add item back to the end of
                        # the queue. If not set, just keep retrying the
                        # operation
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
