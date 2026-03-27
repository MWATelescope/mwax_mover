"""Abstract base classes composing a directory watcher with a queue worker.

MWAXWatchQueueWorker combines one or more Watcher instances (plain queue) with a
QueueWorker. MWAXPriorityWatchQueueWorker does the same using PriorityWatcher and
PriorityQueueWorker, so that high-priority MWA project files are processed first.

On start(), all watcher threads perform their initial directory scan before the
queue worker thread begins, ensuring the full backlog is prioritised before any
processing starts. Concrete subclasses implement only the handler() method.
"""

import logging
from queue import PriorityQueue, Queue
from mwax_mover.mwax_watcher import Watcher
from mwax_mover.mwax_queue_worker import QueueWorker
from mwax_mover.mwax_priority_watcher import PriorityWatcher
from mwax_mover.mwax_priority_queue_worker import PriorityQueueWorker
from mwax_mover import utils
from abc import ABC, abstractmethod
import time
from typing import Optional
from threading import Thread

THREAD_JOIN_WAIT_TIMEOUT = 10

logger = logging.getLogger(__name__)


class MWAXWatchQueueWorker(ABC):
    """
    This class is responsible for watching a set of paths and putting any files that are found into an ordinary Python queue for processing.

    watch_path_exts: a list of tuples, where each tuple contains a path to watch and a pattern to match files against.
                     For example: [("/data/level7", ".fits"), ("/data/level8", ".txt")]
    """

    def __init__(
        self,
        name: str,
        watch_paths_exts: list[tuple[str, str]],
        mode,
        exclude_pattern: Optional[str] = None,
        recursive=False,
        exit_once_queue_empty: bool = False,
        requeue_to_eoq_on_failure: bool = True,
    ):
        self.name = name
        self.threads: list[Thread] = []

        # watch
        self.watchers: list[Watcher] = []
        self.watcher_threads: list[Thread] = []

        # queue
        self.queue = Queue()

        # queue
        self.queue_worker = QueueWorker(
            f"{self.name}_worker",
            self.queue,
            None,
            self.handler,
            exit_once_queue_empty,
            requeue_to_eoq_on_failure,
        )

        self.queue_worker_thread = Thread(
            name=f"{self.queue_worker.name}_thread", target=self.queue_worker.start, daemon=True
        )
        self.threads.append(self.queue_worker_thread)

        # Create a watcher and watcher thread for each path we're watching
        for p in watch_paths_exts:
            watch_path = p[0]
            pattern = p[1]

            new_watcher = Watcher(
                f"{name}_{watch_path.replace('/', '_')}",
                watch_path,
                self.queue,
                pattern,
                mode,
                recursive,
                exclude_pattern,
            )
            # Store the new watcher
            self.watchers.append(new_watcher)

            # Create and store the new thread
            new_thread = Thread(name=f"{new_watcher.name}_thread", target=new_watcher.start, daemon=True)
            self.watcher_threads.append(new_thread)
            self.threads.append(new_thread)

    def start(self):
        for w in self.watcher_threads:
            w.start()

        logger.info("Waiting for all watchers to finish scanning....")
        count_of_watchers_still_scanning = len(self.watchers)
        while count_of_watchers_still_scanning > 0:
            count_of_watchers_still_scanning = 0
            for watcher in self.watchers:
                if not watcher.scan_completed:
                    logger.debug(f"{watcher.name} still scanning!")
                    count_of_watchers_still_scanning += 1
            time.sleep(1)  # hold off for another second
        logger.info("Watchers are finished scanning.")

        self.queue_worker_thread.start()

        logger.info(f"MWAXWatchQueueWorker {self.name} started.")

    def is_running(self) -> bool:
        for thread in self.threads:
            if not thread.is_alive():
                return False
        return True

    def stop(self):
        logger.info(f"MWAXWatchQueueWorker {self.name} stopping.")
        for w in self.watchers:
            w.stop()
        self.queue_worker.stop()

        # Wait for threads to finish
        for watcher_thread in self.watcher_threads:
            if watcher_thread:
                thread_name = watcher_thread.name
                logger.debug(f"Watcher {thread_name} Stopping...")
                if watcher_thread.is_alive():
                    watcher_thread.join(THREAD_JOIN_WAIT_TIMEOUT)
                logger.debug(f"Watcher {thread_name} Stopped")

        if self.queue_worker_thread:
            logger.debug(f"Queue Worker {self.queue_worker_thread.name} Stopping...")
            if self.queue_worker_thread.is_alive():
                self.queue_worker_thread.join(THREAD_JOIN_WAIT_TIMEOUT)
            logger.debug(f"Queue Worker {self.queue_worker_thread.name} Stopped")

        logger.info(f"MWAXWatchQueueWorker {self.name} stopped.")

    def pause(self, pause: bool):
        self.queue_worker.pause(pause)

    def get_status(self) -> dict:
        status = {
            "name": self.name,
            "watchers": [],
            "queue_worker": self.queue_worker.get_status(),
        }
        for watcher in self.watchers:
            status["watchers"].append(watcher.get_status())
        return status

    def scan_completed(self) -> bool:
        for watcher in self.watchers:
            if not watcher.scan_completed:
                return False
        return True

    @abstractmethod
    def handler(self, item: str) -> bool:
        pass


class MWAXPriorityWatchQueueWorker(ABC):
    """
    This class is responsible for watching a set of paths and putting any files that are found into a Python priority queue for processing.

    watch_path_exts: a list of tuples, where each tuple contains a path to watch and a pattern to match files against.
                     For example: [("/data/level7", ".fits"), ("/data/level8", ".txt")]
    """

    def __init__(
        self,
        name: str,
        metafits_path: str,
        watch_path_exts: list[tuple[str, str]],
        mode,
        corr_hi_priority_projects: list[str],
        vcs_hi_priority_projects: list[str],
        exclude_pattern: Optional[str] = None,
        recursive=False,
        exit_once_queue_empty: bool = False,
        requeue_to_eoq_on_failure: bool = True,
    ):
        self.name = name
        self.metafits_path = metafits_path
        self.hostname = utils.get_hostname()
        self.threads: list[Thread] = []

        # Watch
        self.pwatchers: list[PriorityWatcher] = []
        self.pwatcher_threads: list[Thread] = []

        # queue
        self.pqueue = PriorityQueue()

        # queue worker
        self.pqueue_worker = PriorityQueueWorker(
            f"{self.name}_worker",
            self.pqueue,
            None,
            self.handler,
            exit_once_queue_empty,
            requeue_to_eoq_on_failure,
        )

        self.pqueue_worker_thread = Thread(
            name=f"{self.pqueue_worker.name}_thread", target=self.pqueue_worker.start, daemon=True
        )
        self.threads.append(self.pqueue_worker_thread)

        # Create a watcher and watcher thread for each path we're watching
        for p in watch_path_exts:
            watch_path = p[0]
            pattern = p[1]

            new_watcher = PriorityWatcher(
                f"{name}_{watch_path.replace('/', '_')}",
                watch_path,
                self.pqueue,
                pattern,
                mode,
                recursive,
                metafits_path,
                corr_hi_priority_projects,
                vcs_hi_priority_projects,
                exclude_pattern,
            )
            # Store the new watcher
            self.pwatchers.append(new_watcher)

            # Create and store the new thread
            new_thread = Thread(target=new_watcher.start, daemon=True)
            self.pwatcher_threads.append(new_thread)
            self.threads.append(new_thread)

    def is_running(self) -> bool:
        for thread in self.threads:
            if not thread.is_alive():
                return False
        return True

    def start(self):
        for w in self.pwatcher_threads:
            w.start()

        logger.info("Waiting for all watchers to finish scanning....")
        count_of_watchers_still_scanning = len(self.pwatchers)
        while count_of_watchers_still_scanning > 0:
            count_of_watchers_still_scanning = 0
            for watcher in self.pwatchers:
                if not watcher.scan_completed:
                    logger.debug(f"{watcher.name} still scanning!")
                    count_of_watchers_still_scanning += 1
            time.sleep(1)  # hold off for another second
        logger.info("Watchers are finished scanning.")

        self.pqueue_worker_thread.start()

        logger.info(f"MWAXPriorityWatchQueueWorker {self.name} started.")

    def stop(self):
        logger.info(f"MWAXPriorityWatchQueueWorker {self.name} stopping.")
        for w in self.pwatchers:
            w.stop()
        self.pqueue_worker.stop()

        # Wait for threads to finish
        for watcher_thread in self.pwatcher_threads:
            if watcher_thread:
                thread_name = watcher_thread.name
                logger.debug(f"Watcher {thread_name} Stopping...")
                if watcher_thread.is_alive():
                    watcher_thread.join(THREAD_JOIN_WAIT_TIMEOUT)
                logger.debug(f"Watcher {thread_name} Stopped")

        if self.pqueue_worker_thread:
            logger.debug(f"Queue Worker {self.pqueue_worker_thread.name} Stopping...")
            if self.pqueue_worker_thread.is_alive():
                self.pqueue_worker_thread.join(THREAD_JOIN_WAIT_TIMEOUT)
            logger.debug(f"Queue Worker {self.pqueue_worker_thread.name} Stopped")

        logger.info(f"MWAXPriorityWatchQueueWorker {self.name} stopped.")

    def get_status(self) -> dict:
        status = {
            "name": self.name,
            "watchers": [],
            "queue_worker": self.pqueue_worker.get_status(),
        }
        for watcher in self.pwatchers:
            status["watchers"].append(watcher.get_status())
        return status

    def scan_completed(self) -> bool:
        for watcher in self.pwatchers:
            if not watcher.scan_completed:
                return False
        return True

    def pause(self, pause: bool):
        self.pqueue_worker.pause(pause)

    @abstractmethod
    def handler(self, item: str) -> bool:
        pass
