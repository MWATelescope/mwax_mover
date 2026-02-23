import os
from logging import Logger
from queue import PriorityQueue, Queue
from mwax_mover.mwax_mover_mode import MWAXMoverMode
from mwax_mover.mwax_watcher import Watcher
from mwax_mover.mwax_queue_worker import QueueWorker
from mwax_mover.mwax_priority_watcher import PriorityWatcher
from mwax_mover.mwax_priority_queue_worker import PriorityQueueWorker
from abc import ABC, abstractmethod
from typing import Optional
from threading import Thread


class WatchQueueWorker(ABC):
    def __init__(
        self,
        name: str,
        logger: Logger,
        watch_paths: list[str],
        pattern: str,
        mode: MWAXMoverMode,
        exclude_pattern: Optional[str] = None,
        recursive=False,
        exit_once_queue_empty: bool = False,
        requeue_to_eoq_on_failure: bool = True,
    ):
        self.logger = logger
        self.name = name

        # watch
        self.watch_paths: list[str] = watch_paths
        self.watchers: list[Watcher] = []
        self.watcher_threads: list[Thread] = []

        # queue
        self.queue = Queue()

        # queue
        self.queue_worker = QueueWorker(
            f"{self.name}_worker",
            self.queue,
            None,
            self.logger,
            self.handler,
            exit_once_queue_empty,
            requeue_to_eoq_on_failure,
        )

        self.queue_worker_thread = Thread(
            name=f"{self.queue_worker.name}_thread", target=self.queue_worker.start, daemon=True
        )

        # Create a watcher and watcher thread for each path we're watching
        for p in watch_paths:
            new_watcher = Watcher(
                f"{name}_{os.path.basename(p)}",
                p,
                self.queue,
                pattern,
                self.logger,
                mode,
                recursive,
                exclude_pattern,
            )
            # Store the new watcher
            self.watchers.append(new_watcher)

            # Create and store the new thread
            new_thread = Thread(name=f"{new_watcher.name}_thread", target=new_watcher.start, daemon=True)
            self.watcher_threads.append(new_thread)

    def start(self):
        for w in self.watcher_threads:
            w.start()
        self.queue_worker_thread.start()

    @abstractmethod
    def hanlder(self, item: str) -> bool:
        pass


class PriorityWatchQueueWorker(ABC):
    def __init__(
        self,
        name: str,
        logger: Logger,
        metafits_path: str,
        watch_paths: list[str],
        pattern: str,
        mode: MWAXMoverMode,
        corr_hi_priority_projects: list[str],
        vcs_hi_priority_projects: list[str],
        exclude_pattern: Optional[str],
        recursive=False,
        exit_once_queue_empty: bool = False,
        requeue_to_eoq_on_failure: bool = True,
    ):
        self.logger = logger
        self.name = name

        # Watch
        self.watch_paths: list[str] = watch_paths
        self.pwatchers: list[PriorityWatcher] = []
        self.pwatcher_threads: list[Thread] = []

        # queue
        self.pqueue = PriorityQueue()

        # queue worker
        self.pqueue_worker = PriorityQueueWorker(
            f"{self.name}_worker",
            self.pqueue,
            None,
            self.logger,
            self.handler,
            exit_once_queue_empty,
            requeue_to_eoq_on_failure,
        )

        self.pqueue_worker_thread = Thread(
            name=f"{self.pqueue_worker.name}_thread", target=self.pqueue_worker.start, daemon=True
        )

        # Create a watcher and watcher thread for each path we're watching
        for p in watch_paths:
            new_watcher = PriorityWatcher(
                f"{name}_{os.path.basename(p)}",
                p,
                self.pqueue,
                pattern,
                self.logger,
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

    def start(self):
        for w in self.pwatcher_threads:
            w.start()
        self.pqueue_worker_thread.start()

    @abstractmethod
    def hanlder(self, item: str) -> bool:
        pass
