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
from pathlib import Path

THREAD_JOIN_WAIT_TIMEOUT = 10

logger = logging.getLogger(__name__)


def get_last_two_dirs(path: str) -> str:
    """Get the last two directory components from a path.

    Args:
        path: The filesystem path.

    Returns:
        The last two directory components joined with '/'. If fewer than two
        components exist, returns all available components.

    Examples:
        >>> get_last_two_dirs("/a/b/c")
        'b/c'
        >>> get_last_two_dirs("/a/b/c/")
        'b/c'
        >>> get_last_two_dirs("/a/")
        'a'
    """
    n = 2
    parts = [p for p in Path(path).parts if p != "/"]
    return "/".join(parts[-n:])


def get_watcher_name(wqw_name: str, watch_path: str, pattern: str) -> str:
    """Generate a descriptive name for a watcher instance.

    Args:
        wqw_name: The base name of the watch queue worker.
        watch_path: The path being watched.
        pattern: The file extension pattern being matched.

    Returns:
        A formatted watcher name combining the base name and watch path.
        Returns 'unknown_watcher' if name generation fails.
    """
    try:
        return f"{wqw_name}_{watch_path.replace('/', '_')}".replace("__", "_")
    except Exception:
        return "unknown_watcher"


def get_watcher_thread_name(watch_path: str, pattern: str) -> str:
    """Generate a descriptive name for a watcher thread.

    Args:
        watch_path: The path being watched.
        pattern: The file extension pattern being matched.

    Returns:
        A formatted thread name derived from the watch path and pattern.
        Returns 'unknown_watcher_thread' if name generation fails.
    """
    try:
        return f"watch_{get_last_two_dirs(watch_path).replace('/', '_')}_{pattern.replace('.', '')}_thread"
    except Exception:
        return "unknown_watcher_thread"


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
        """Initialize a watch queue worker with watchers and a queue worker.

        Args:
            name: A descriptive name for this worker instance.
            watch_paths_exts: List of (path, extension_pattern) tuples to watch.
            mode: The watch mode (NEW, RENAME, or RENAME_OR_NEW).
            exclude_pattern: File extension to exclude from matching. Defaults to None.
            recursive: Whether to watch subdirectories recursively. Defaults to False.
            exit_once_queue_empty: Exit the worker once the queue is empty. Defaults to False.
            requeue_to_eoq_on_failure: Requeue failed items to end of queue. Defaults to True.
        """
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

        self.queue_worker_thread = Thread(name="worker_thread", target=self.queue_worker.start, daemon=True)
        self.threads.append(self.queue_worker_thread)

        # Create a watcher and watcher thread for each path we're watching
        for p in watch_paths_exts:
            watch_path = p[0]
            pattern = p[1]

            new_watcher = Watcher(
                get_watcher_name(self.name, watch_path, pattern),
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
            new_thread = Thread(
                name=get_watcher_thread_name(watch_path, pattern), target=new_watcher.start, daemon=True
            )
            self.watcher_threads.append(new_thread)
            self.threads.append(new_thread)

    def start(self):
        """Start all watcher and worker threads.

        Waits for all watchers to complete their initial directory scan before
        starting the queue worker thread. This ensures all existing files are
        enqueued before processing begins.
        """
        for w in self.watcher_threads:
            w.start()

        logger.info(f"{self.name} Waiting for all watchers to finish scanning....")
        count_of_watchers_still_scanning = len(self.watchers)
        while count_of_watchers_still_scanning > 0:
            count_of_watchers_still_scanning = 0
            for watcher in self.watchers:
                if not watcher.scan_completed:
                    logger.debug(f"{watcher.name} still scanning!")
                    count_of_watchers_still_scanning += 1
            time.sleep(1)  # hold off for another second
        logger.info(f"{self.name} Watchers are finished scanning.")

        self.queue_worker_thread.start()

        logger.info(f"{self.name} started.")

    def is_running(self) -> bool:
        """Check if all threads are running.

        Returns:
            True if all threads are alive, False otherwise.
        """
        for thread in self.threads:
            if not thread.is_alive():
                return False
        return True

    def stop(self):
        """Stop all watcher and worker threads gracefully.

        Signals all watchers and the queue worker to stop, then waits for their
        threads to finish execution.
        """
        logger.info(f"{self.name} stopping.")
        for w in self.watchers:
            w.stop()
        self.queue_worker.stop()

        # Wait for threads to finish
        for watcher_thread in self.watcher_threads:
            if watcher_thread:
                thread_name = watcher_thread.name
                logger.debug(f"{thread_name} Stopping...")
                if watcher_thread.is_alive():
                    watcher_thread.join(THREAD_JOIN_WAIT_TIMEOUT)
                logger.debug(f"{thread_name} Stopped")

        if self.queue_worker_thread:
            logger.debug(f"{self.queue_worker_thread.name} Stopping...")
            if self.queue_worker_thread.is_alive():
                self.queue_worker_thread.join(THREAD_JOIN_WAIT_TIMEOUT)
            logger.debug(f"{self.queue_worker_thread.name} Stopped")

        logger.info(f"{self.name} stopped.")

    def pause(self, pause: bool):
        """Pause or resume queue processing.

        Args:
            pause: True to pause processing, False to resume.
        """
        self.queue_worker.pause(pause)

    def get_status(self) -> dict:
        """Get the current status of all watchers and the queue worker.

        Returns:
            A dictionary containing worker name, watcher statuses, and queue worker status.
        """
        status = {
            "name": self.name,
            "watchers": [],
            "queue_worker": self.queue_worker.get_status(),
        }
        for watcher in self.watchers:
            status["watchers"].append(watcher.get_status())
        return status

    def scan_completed(self) -> bool:
        """Check if all watchers have completed their initial directory scan.

        Returns:
            True if all watchers have completed scanning, False otherwise.
        """
        for watcher in self.watchers:
            if not watcher.scan_completed:
                return False
        return True

    @abstractmethod
    def handler(self, item: str) -> bool:
        """Handle a dequeued item.

        Subclasses must implement this method to process items from the queue.

        Args:
            item: The item from the queue to process.

        Returns:
            True if processing succeeded, False otherwise.
        """
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
        """Initialize a priority watch queue worker with priority watchers and queue worker.

        Args:
            name: A descriptive name for this worker instance.
            metafits_path: Path to metafits files for priority determination.
            watch_path_exts: List of (path, extension_pattern) tuples to watch.
            mode: The watch mode (NEW, RENAME, or RENAME_OR_NEW).
            corr_hi_priority_projects: Correlator projects with high priority.
            vcs_hi_priority_projects: VCS projects with high priority.
            exclude_pattern: File extension to exclude from matching. Defaults to None.
            recursive: Whether to watch subdirectories recursively. Defaults to False.
            exit_once_queue_empty: Exit the worker once the queue is empty. Defaults to False.
            requeue_to_eoq_on_failure: Requeue failed items to end of queue. Defaults to True.
        """
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

        self.pqueue_worker_thread = Thread(name="worker_thread", target=self.pqueue_worker.start, daemon=True)
        self.threads.append(self.pqueue_worker_thread)

        # Create a watcher and watcher thread for each path we're watching
        for p in watch_path_exts:
            watch_path = p[0]
            pattern = p[1]

            new_watcher = PriorityWatcher(
                get_watcher_name(self.name, watch_path, pattern),
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
            new_thread = Thread(
                name=get_watcher_thread_name(watch_path, pattern), target=new_watcher.start, daemon=True
            )
            self.pwatcher_threads.append(new_thread)
            self.threads.append(new_thread)

    def is_running(self) -> bool:
        """Check if all threads are running.

        Returns:
            True if all threads are alive, False otherwise.
        """
        for thread in self.threads:
            if not thread.is_alive():
                return False
        return True

    def start(self):
        """Start all priority watcher and worker threads.

        Waits for all watchers to complete their initial directory scan before
        starting the priority queue worker thread. This ensures all existing files
        are enqueued with correct priorities before processing begins.
        """
        for w in self.pwatcher_threads:
            w.start()

        logger.info(f"{self.name}: Waiting for all watchers to finish scanning....")
        count_of_watchers_still_scanning = len(self.pwatchers)
        while count_of_watchers_still_scanning > 0:
            count_of_watchers_still_scanning = 0
            for watcher in self.pwatchers:
                if not watcher.scan_completed:
                    logger.debug(f"{watcher.name}: still scanning!")
                    count_of_watchers_still_scanning += 1
            time.sleep(1)  # hold off for another second
        logger.info(f"{self.name}: Watchers are finished scanning.")

        self.pqueue_worker_thread.start()

        logger.info(f"{self.name} started.")

    def stop(self):
        """Stop all priority watcher and worker threads gracefully.

        Signals all priority watchers and the queue worker to stop, then waits
        for their threads to finish execution.
        """
        logger.info(f"{self.name} stopping.")
        for w in self.pwatchers:
            w.stop()
        self.pqueue_worker.stop()

        # Wait for threads to finish
        for watcher_thread in self.pwatcher_threads:
            if watcher_thread:
                thread_name = watcher_thread.name
                logger.debug(f"{thread_name} Stopping...")
                if watcher_thread.is_alive():
                    watcher_thread.join(THREAD_JOIN_WAIT_TIMEOUT)
                logger.debug(f"{thread_name} Stopped")

        if self.pqueue_worker_thread:
            logger.debug(f"{self.pqueue_worker_thread.name} Stopping...")
            if self.pqueue_worker_thread.is_alive():
                self.pqueue_worker_thread.join(THREAD_JOIN_WAIT_TIMEOUT)
            logger.debug(f"{self.pqueue_worker_thread.name} Stopped")

        logger.info(f"{self.name} stopped.")

    def get_status(self) -> dict:
        """Get the current status of all priority watchers and the queue worker.

        Returns:
            A dictionary containing worker name, watcher statuses, and queue worker status.
        """
        status = {
            "name": self.name,
            "watchers": [],
            "queue_worker": self.pqueue_worker.get_status(),
        }
        for watcher in self.pwatchers:
            status["watchers"].append(watcher.get_status())
        return status

    def scan_completed(self) -> bool:
        """Check if all priority watchers have completed their initial directory scan.

        Returns:
            True if all watchers have completed scanning, False otherwise.
        """
        for watcher in self.pwatchers:
            if not watcher.scan_completed:
                return False
        return True

    def pause(self, pause: bool):
        """Pause or resume queue processing.

        Args:
            pause: True to pause processing, False to resume.
        """
        self.pqueue_worker.pause(pause)

    @abstractmethod
    def handler(self, item: str) -> bool:
        """Handle a dequeued item.

        Subclasses must implement this method to process items from the priority queue.

        Args:
            item: The item from the queue to process.

        Returns:
            True if processing succeeded, False otherwise.
        """
        pass
