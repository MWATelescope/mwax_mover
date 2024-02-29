"""Module for the CephQueueWorker"""

import os
import queue
import time
from boto3 import Session
from mwax_mover.mwax_priority_queue_worker import PriorityQueueWorker
from mwax_mover.mwax_priority_queue_data import MWAXPriorityQueueData
from mwax_mover.mwa_archiver import ceph_get_s3_session


class CephPriorityQueueWorker(PriorityQueueWorker):
    """Subclass of a queue worker which houses a Boto3 session object so we
    can take advantage of Session connection pooling"""

    def __init__(
        self,
        name: str,
        source_queue: queue.Queue,
        executable_path,
        log,
        event_handler,
        exit_once_queue_empty,
        ceph_profile: str,
        requeue_to_eoq_on_failure: bool = True,
    ):
        # Call Default PriorityQueueWorker contstructor
        # but we don't use the backoff params at all
        # They are used in PriorityQueueWorker.start() but
        # we fully override it anway. Ceph itself
        # handles some forms of retry and backoff.
        super().__init__(
            name,
            source_queue,
            executable_path,
            log,
            event_handler,
            exit_once_queue_empty,
            requeue_to_eoq_on_failure,
            backoff_initial_seconds=0,
            backoff_factor=0,
            backoff_limit_seconds=0,
        )

        self.ceph_profile = ceph_profile
        self.ceph_session: Session = None

    def start(self):
        """Overrride this method from QueueWorker so we can initiate a boto3
        session"""

        self.logger.info(f"CephPriorityQueueWorker {self.name} starting...")
        #
        # Init the Boto3 session
        #
        # get s3 object
        try:
            self.ceph_session = ceph_get_s3_session(self.ceph_profile)
        except Exception as catch_all_exception:  # pylint: disable=broad-except
            self.logger.error(f"Error getting Ceph Session: {catch_all_exception}")
            return

        self._running = True
        self.current_item = None
        self.consecutive_error_count = 0

        while self._running:
            if not self._paused:
                try:
                    success = False

                    if self.current_item is None:
                        self.current_item = self.source_queue.get(block=True, timeout=0.5)
                    self.logger.info(f"Processing {self.current_item}...")

                    start_time = time.time()

                    filename_priority = self.current_item[0]
                    filename = str(self.current_item[1])

                    # Check file exists (maybe someone deleted it?)
                    if os.path.exists(filename):
                        if self._executable_path:
                            success = self.run_command(filename)
                        else:
                            success = self._event_handler(filename, self.ceph_session)

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

                    if not success:
                        # If this option is set, add item back to the end of
                        # the queue by making the priority larger
                        # If not set, just keep retrying the operation
                        if self.requeue_to_eoq_on_failure:
                            # increment the priority- otherwise it
                            # will go back and be in the same position
                            # in the queue
                            filename_priority += 1

                            self.source_queue.task_done()
                            self.source_queue.put(
                                (
                                    filename_priority,
                                    MWAXPriorityQueueData(filename),
                                )
                            )
                            self.current_item = None

                except queue.Empty:
                    if self.exit_once_queue_empty:
                        # Queue is complete. Stop now
                        self.logger.info("Finished processing queue.")
                        self.stop()
                        return
