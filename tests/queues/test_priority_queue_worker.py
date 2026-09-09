"""Tests for the mwax_mover.queues.priority_queue_worker module.

PriorityQueueWorker had no dedicated test file before this one (docs/CLEANUP.md
4.2 flagged the gap while introducing ArchivePriority). Patterned after the
sibling QueueWorker's tests in tests/queues/test_watch_queue_worker.py, which
cover the same event_handler/executable_path constructor contract, backoff, and
start()/stop() lifecycle.
"""

import queue

import pytest

from mwax_mover.filesystem.naming import ArchivePriority
from mwax_mover.queues.priority_queue_data import MWAXPriorityQueueData
from mwax_mover.queues.priority_queue_worker import PriorityQueueWorker


def _make_worker(
    event_handler,
    name: str = "test_worker",
    requeue_to_eoq_on_failure: bool = True,
) -> PriorityQueueWorker:
    """Build a PriorityQueueWorker with fast (zero-wait) backoff, for a test's own queue.

    Args:
        event_handler: The handler callable to process each dequeued item.
        name: Worker name, defaulted since most tests don't care about it.
        requeue_to_eoq_on_failure: Passed straight through; see PriorityQueueWorker.

    Returns:
        A PriorityQueueWorker ready to have items put on its source_queue.
    """
    return PriorityQueueWorker(
        name=name,
        source_queue=queue.PriorityQueue(),
        executable_path=None,
        event_handler=event_handler,
        exit_once_queue_empty=True,
        requeue_to_eoq_on_failure=requeue_to_eoq_on_failure,
        backoff_initial_seconds=0,
        backoff_factor=1,
        backoff_limit_seconds=0,
    )


#
# Constructor validation
#


def test_requires_event_handler_or_executable_path_not_both():
    """Passing both event_handler and executable_path must raise."""
    with pytest.raises(Exception, match="not both and not neither"):
        PriorityQueueWorker(
            name="test",
            source_queue=queue.PriorityQueue(),
            executable_path="/bin/true",
            event_handler=lambda _item: True,
            exit_once_queue_empty=True,
        )


def test_requires_event_handler_or_executable_path_not_neither():
    """Passing neither event_handler nor executable_path must raise."""
    with pytest.raises(Exception, match="not both and not neither"):
        PriorityQueueWorker(
            name="test",
            source_queue=queue.PriorityQueue(),
            executable_path=None,
            event_handler=None,
            exit_once_queue_empty=True,
        )


#
# Basic processing
#


def test_processes_item_via_event_handler(tmp_path):
    """A successfully-handled item is passed to the handler and dequeued."""
    tmp_file = tmp_path / "item.dat"
    tmp_file.write_text("x")

    handled = []
    worker = _make_worker(lambda item: handled.append(item) or True)
    worker.source_queue.put((5, MWAXPriorityQueueData(str(tmp_file))))
    worker.start()

    assert handled == [str(tmp_file)]
    assert worker.source_queue.empty()


def test_missing_file_is_dequeued_without_calling_handler(tmp_path):
    """A file that no longer exists (e.g. moved or deleted) is skipped, not processed.

    Mirrors the file-exists check every watch_queue_worker subclass relies on:
    a race between enqueueing and processing must not be treated as a handler
    failure.
    """
    missing_file = tmp_path / "does_not_exist.dat"

    handled = []
    worker = _make_worker(lambda item: handled.append(item) or True)
    worker.source_queue.put((5, MWAXPriorityQueueData(str(missing_file))))
    worker.start()

    assert handled == [], "the handler must not be called for a file that no longer exists"
    assert worker.source_queue.empty()


#
# Retry / requeue on failure
#


def test_failed_item_is_requeued_with_incremented_priority(tmp_path):
    """On failure with requeue_to_eoq_on_failure=True, the item is put back with priority + 1.

    Confirmed by observing worker.current_item[0] (the raw priority the item
    was last dequeued with) across both attempts, rather than relying on
    queue ordering, which incrementing-then-requeueing can otherwise obscure.
    """
    tmp_file = tmp_path / "item.dat"
    tmp_file.write_text("x")

    observed_priorities = []

    def handler(_item):
        assert worker.current_item is not None, "handler is only called while processing an item"
        observed_priorities.append(worker.current_item[0])
        # Fail the first attempt to force a requeue, then succeed so the
        # worker drains the queue and start() returns on its own.
        return len(observed_priorities) > 1

    worker = _make_worker(handler)
    worker.source_queue.put((5, MWAXPriorityQueueData(str(tmp_file))))
    worker.start()

    assert observed_priorities == [5, 6], "the retried item must carry priority + 1, not the original priority"
    assert worker.source_queue.empty()


def test_requeue_to_eoq_on_failure_false_retries_in_place(tmp_path):
    """With requeue_to_eoq_on_failure=False, a failed item is retried in place, not requeued.

    The priority must stay unchanged between attempts (no requeue happens at
    all), unlike the default True behaviour above.
    """
    tmp_file = tmp_path / "item.dat"
    tmp_file.write_text("x")

    observed_priorities = []

    def handler(_item):
        assert worker.current_item is not None, "handler is only called while processing an item"
        observed_priorities.append(worker.current_item[0])
        return len(observed_priorities) > 1

    worker = _make_worker(handler, requeue_to_eoq_on_failure=False)
    worker.source_queue.put((5, MWAXPriorityQueueData(str(tmp_file))))
    worker.start()

    assert observed_priorities == [5, 5], "retrying in place must not change the item's priority"
    assert worker.source_queue.empty()


def test_none_priority_falls_back_to_archive_priority_default(tmp_path):
    """A queue tuple with priority=None must use ArchivePriority.DEFAULT (100) as
    the basis for the requeue priority, not a bare 99.

    Regression test for docs/CLEANUP.md 4.2: a fallback item must sort behind
    (not ahead of) an item explicitly assigned the default priority. Only
    reachable when a queue tuple has None as its priority, which no current
    producer does -- but the fallback must still be correct if one ever does.
    """
    tmp_file = tmp_path / "item.dat"
    tmp_file.write_text("x")

    observed_priorities = []

    def handler(_item):
        assert worker.current_item is not None, "handler is only called while processing an item"
        observed_priorities.append(worker.current_item[0])
        return len(observed_priorities) > 1

    worker = _make_worker(handler)
    worker.source_queue.put((None, MWAXPriorityQueueData(str(tmp_file))))
    worker.start()

    assert observed_priorities == [None, ArchivePriority.DEFAULT + 1], (
        "the requeued priority must be ArchivePriority.DEFAULT + 1 (101), not 99 + 1 (100)"
    )
    assert worker.source_queue.empty()


#
# start()/stop()/pause() lifecycle
#


def test_start_clears_backoff_event(tmp_path):
    """start() must reset the event stop() sets.

    Regression test, mirroring test_queue_worker_start_clears_backoff_event in
    tests/queues/test_watch_queue_worker.py: nothing ever cleared this event,
    so after the first stop() every subsequent event.wait(backoff) returned
    immediately and backoff was silently disabled for the rest of the
    process's life.
    """
    tmp_file = tmp_path / "item.dat"
    tmp_file.write_text("x")

    worker = _make_worker(lambda _item: True)
    worker.stop()
    assert worker.event.is_set(), "stop() should set the event to break an in-flight wait"

    observed = []

    def handler(_item):
        observed.append(worker.event.is_set())
        return True

    worker._event_handler = handler
    worker.source_queue.put((5, MWAXPriorityQueueData(str(tmp_file))))
    worker.start()

    assert observed == [False], f"event should be cleared by start(), saw is_set()={observed}"


def test_pause_sets_flag_without_starting():
    """pause()/resume() just toggle the flag start()'s loop checks."""
    worker = _make_worker(lambda _item: True)

    worker.pause(True)
    assert worker._paused is True

    worker.pause(False)
    assert worker._paused is False


def test_get_status_reports_name_current_item_and_queue_size(tmp_path):
    """get_status() must reflect the worker's name, current item, and queue size."""
    tmp_file = tmp_path / "item.dat"
    tmp_file.write_text("x")

    worker = _make_worker(lambda _item: True, name="status_test")
    worker.source_queue.put((5, MWAXPriorityQueueData(str(tmp_file))))

    status_before = worker.get_status()
    assert status_before == {"name": "status_test", "current_item": None, "queue_size": 1}

    worker.start()

    status_after = worker.get_status()
    assert status_after == {"name": "status_test", "current_item": None, "queue_size": 0}
