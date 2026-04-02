"""Tests for mwax_mover.mwax_priority_watcher.PriorityWatcher
and mwax_mover.mwax_priority_queue_data.MWAXPriorityQueueData.

Coverage:
  MWAXPriorityQueueData:
    - str / repr
    - All six comparison operators (filename-only, cross-directory)
    - Unhashability
    - Priority queue ordering with mixed paths (the motivating use case)

  PriorityWatcher:
    - Constructor validation (path existence, mask assignment per mode)
    - Mask logic bug regression (| vs &)
    - get_status()
    - do_watch_loop() event filtering (pattern, exclude_pattern, wrong event type)
    - Live IN_MOVED_TO integration test on /dev/shm
    - Live IN_CLOSE_WRITE integration test on /dev/shm

Filesystem note:
  Live inotify tests use /dev/shm (tmpfs) to match the production environment
  and are skipped automatically when /dev/shm is unavailable or on an
  inotify-unreliable filesystem (e.g. WSL v9fs /mnt/c paths).
"""

import os
import queue
import subprocess
import tempfile
import threading
import time
import unittest.mock as mock

import inotify.constants
import pytest

import mwax_mover.mwax_mover as mwax_mover
from mwax_mover.mwax_priority_queue_data import MWAXPriorityQueueData
from mwax_mover.mwax_priority_watcher import PriorityWatcher

# ── Helpers ───────────────────────────────────────────────────────────────────


def _fs_type(path: str) -> str:
    """Return the filesystem type string for *path* (e.g. 'tmpfs', 'v9fs')."""
    try:
        result = subprocess.run(
            ["stat", "-f", "-c", "%T", path],
            capture_output=True,
            text=True,
            timeout=5,
        )
        return result.stdout.strip()
    except Exception:
        return "unknown"


def _inotify_reliable(path: str) -> bool:
    """Return True if *path* is on a filesystem where inotify events are reliable."""
    reliable_fs = {"tmpfs", "ext2/ext3", "ext4", "ext2", "ext3"}
    return _fs_type(path) in reliable_fs


def _make_fake_event(mask: int, path: str, filename: str):
    """Build a fake inotify event tuple: (header, type_names, path, filename)."""
    header = mock.Mock()
    header.mask = mask
    return (header, [], path, filename)


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def dest_queue() -> queue.PriorityQueue:
    """A fresh priority queue for each test."""
    return queue.PriorityQueue()


@pytest.fixture()
def shm_watch_dir():
    """A temporary directory inside /dev/shm for live inotify tests.

    Skips the test automatically if /dev/shm is unavailable or its filesystem
    is not inotify-reliable (catches WSL /mnt/c scenarios).
    """
    shm_base = "/dev/shm"
    if not os.path.exists(shm_base):
        pytest.skip("/dev/shm not available on this system")
    if not _inotify_reliable(shm_base):
        pytest.skip(f"/dev/shm filesystem ({_fs_type(shm_base)}) is not inotify-reliable")
    with tempfile.TemporaryDirectory(dir=shm_base) as tmpdir:
        yield tmpdir


@pytest.fixture()
def make_watcher(tmp_path, dest_queue):
    """Factory fixture that constructs a PriorityWatcher with sensible defaults.

    Calling the fixture with no arguments produces a watcher using all defaults.
    Any argument accepted by PriorityWatcher can be overridden as a keyword arg:

        def test_something(make_watcher):
            w = make_watcher()                          # all defaults
            w = make_watcher(pattern=".*")              # override one arg
            w = make_watcher(mode=MODE_WATCH_DIR_FOR_NEW, exclude_pattern=".sub")
    """

    def _make(
        name: str = "test",
        path: str = str(tmp_path),
        watcher_dest_queue: queue.PriorityQueue = dest_queue,
        pattern: str = ".fits",
        mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
        recursive: bool = False,
        metafits_path: str = "/dummy/metafits",
        list_of_correlator_high_priority_projects: list[str] = [],  # noqa: B006
        list_of_vcs_high_priority_projects: list[str] = [],  # noqa: B006
        exclude_pattern: str | None = None,
    ) -> PriorityWatcher:
        return PriorityWatcher(
            name=name,
            path=path,
            dest_queue=watcher_dest_queue,
            pattern=pattern,
            mode=mode,
            recursive=recursive,
            metafits_path=metafits_path,
            list_of_correlator_high_priority_projects=list(list_of_correlator_high_priority_projects),
            list_of_vcs_high_priority_projects=list(list_of_vcs_high_priority_projects),
            exclude_pattern=exclude_pattern,
        )

    return _make


# ═══════════════════════════════════════════════════════════════════════════════
# MWAXPriorityQueueData tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestMWAXPriorityQueueDataStrRepr:
    def test_str_returns_full_path(self):
        item = MWAXPriorityQueueData("/path/to/1234567890_file.fits")
        assert str(item) == "/path/to/1234567890_file.fits"

    def test_repr_returns_full_path(self):
        item = MWAXPriorityQueueData("/path/to/1234567890_file.fits")
        assert repr(item) == "/path/to/1234567890_file.fits"


class TestMWAXPriorityQueueDataComparisons:
    """All six comparison operators use filename only, ignoring directory."""

    # Items with the same filename but different directories
    A_PATH1 = MWAXPriorityQueueData("/dir1/1234567890_aa.fits")
    A_PATH2 = MWAXPriorityQueueData("/dir2/1234567890_aa.fits")  # same filename as A_PATH1
    B = MWAXPriorityQueueData("/dir1/1234567890_bb.fits")  # later filename

    def test_eq_same_filename_different_dirs(self):
        assert self.A_PATH1 == self.A_PATH2

    def test_ne_different_filenames(self):
        assert self.A_PATH1 != self.B

    def test_ne_same_filename_different_dirs_is_false(self):
        assert not (self.A_PATH1 != self.A_PATH2)

    def test_lt_earlier_filename(self):
        assert self.A_PATH1 < self.B

    def test_lt_same_filename_is_false(self):
        assert not (self.A_PATH1 < self.A_PATH2)

    def test_le_earlier_filename(self):
        assert self.A_PATH1 <= self.B

    def test_le_same_filename(self):
        assert self.A_PATH1 <= self.A_PATH2

    def test_gt_later_filename(self):
        assert self.B > self.A_PATH1

    def test_gt_same_filename_is_false(self):
        assert not (self.A_PATH1 > self.A_PATH2)

    def test_ge_later_filename(self):
        assert self.B >= self.A_PATH1

    def test_ge_same_filename(self):
        assert self.A_PATH1 >= self.A_PATH2

    def test_directory_name_does_not_affect_ordering(self):
        """A file in /zzz/... should not sort after a later obsid in /aaa/..."""
        early = MWAXPriorityQueueData("/zzz/1234567890_file.fits")
        late = MWAXPriorityQueueData("/aaa/1234567891_file.fits")
        assert early < late

    def test_obsid_ordering(self):
        """Files are sorted by their 10-digit obsid prefix in the filename."""
        items = [
            MWAXPriorityQueueData(f"/some/path/{obsid}_file.fits")
            for obsid in ["1234567892", "1234567890", "1234567891"]
        ]
        assert sorted(items)[0] == MWAXPriorityQueueData("/x/1234567890_file.fits")
        assert sorted(items)[2] == MWAXPriorityQueueData("/x/1234567892_file.fits")


class TestMWAXPriorityQueueDataUnhashable:
    def test_instance_is_not_hashable(self):
        item = MWAXPriorityQueueData("/path/1234567890_file.fits")
        with pytest.raises(TypeError):
            hash(item)

    def test_cannot_be_added_to_set(self):
        item = MWAXPriorityQueueData("/path/1234567890_file.fits")
        with pytest.raises(TypeError):
            {item}


class TestMWAXPriorityQueueDataQueueOrdering:
    """Integration test: PriorityQueue sorts by filename, not full path.

    This is the exact scenario described in the module docstring.
    """

    def test_priority_queue_ordering_ignores_directory(self):
        q = queue.PriorityQueue()
        q.put((1, MWAXPriorityQueueData("path1/1234567891_file.dat")))
        q.put((1, MWAXPriorityQueueData("path2/1234567892_file.dat")))
        q.put((1, MWAXPriorityQueueData("path1/1234567890_file.dat")))

        results = [q.get()[1].value for _ in range(3)]
        assert results == [
            "path1/1234567890_file.dat",
            "path1/1234567891_file.dat",
            "path2/1234567892_file.dat",
        ]

    def test_higher_priority_dequeued_before_lower(self):
        """Lower priority number = higher priority = dequeued first."""
        q = queue.PriorityQueue()
        q.put((2, MWAXPriorityQueueData("/path/1234567890_low.fits")))
        q.put((1, MWAXPriorityQueueData("/path/1234567891_high.fits")))

        first = q.get()[1].value
        assert first == "/path/1234567891_high.fits"


# ═══════════════════════════════════════════════════════════════════════════════
# PriorityWatcher tests
# ═══════════════════════════════════════════════════════════════════════════════


class TestPriorityWatcherInit:
    """PriorityWatcher.__init__ validation."""

    def test_raises_when_path_does_not_exist(self, dest_queue):
        with pytest.raises(FileNotFoundError):
            PriorityWatcher(
                name="test",
                path="/nonexistent/path/xyz",
                dest_queue=dest_queue,
                pattern=".fits",
                mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
                recursive=False,
                metafits_path="/dummy",
                list_of_correlator_high_priority_projects=[],
                list_of_vcs_high_priority_projects=[],
            )

    def test_mask_new_mode(self, make_watcher):
        w = make_watcher(mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW)
        assert w.mask == inotify.constants.IN_CLOSE_WRITE

    def test_mask_rename_mode(self, make_watcher):
        w = make_watcher()
        assert w.mask == inotify.constants.IN_MOVED_TO

    def test_mask_rename_or_new_mode(self, make_watcher):
        w = make_watcher(mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW)
        expected = inotify.constants.IN_MOVED_TO | inotify.constants.IN_CLOSE_WRITE
        assert w.mask == expected

    def test_exclude_pattern_stored(self, make_watcher):
        w = make_watcher(exclude_pattern=".metafits")
        assert w.exclude_pattern == ".metafits"

    def test_exclude_pattern_defaults_to_none(self, make_watcher):
        w = make_watcher()
        assert w.exclude_pattern is None

    def test_watching_is_false_before_start(self, make_watcher):
        w = make_watcher()
        assert w.watching is False

    def test_scan_completed_is_false_before_start(self, make_watcher):
        w = make_watcher()
        assert w.scan_completed is False


class TestPriorityWatcherGetStatus:
    def test_returns_name_and_path(self, make_watcher, tmp_path):
        w = make_watcher(name="my_priority_watcher")
        status = w.get_status()
        assert status["name"] == "my_priority_watcher"
        assert status["watch_path"] == str(tmp_path)


class TestPriorityWatcherMaskLogic:
    """Regression tests for the | vs & mask check bug in do_watch_loop.

    The original code used:
        if header.mask | self.mask == self.mask:
    which is always True regardless of the event type received.

    The correct check is:
        if header.mask & self.mask:
    which only passes when the incoming event type overlaps with the desired mask.
    """

    def test_correct_operator_passes_matching_event(self):
        desired = inotify.constants.IN_MOVED_TO
        incoming = inotify.constants.IN_MOVED_TO
        assert bool(incoming & desired) is True

    def test_correct_operator_rejects_non_matching_event(self):
        desired = inotify.constants.IN_MOVED_TO
        incoming = inotify.constants.IN_OPEN
        assert bool(incoming & desired) is False

    def test_correct_operator_passes_combined_mask_event(self):
        desired = inotify.constants.IN_MOVED_TO | inotify.constants.IN_CLOSE_WRITE
        assert bool(inotify.constants.IN_MOVED_TO & desired) is True
        assert bool(inotify.constants.IN_CLOSE_WRITE & desired) is True

    def test_correct_operator_rejects_unrelated_event_against_combined_mask(self):
        desired = inotify.constants.IN_MOVED_TO | inotify.constants.IN_CLOSE_WRITE
        assert bool(inotify.constants.IN_OPEN & desired) is False


# ── do_watch_loop unit tests (mocked inotify) ─────────────────────────────────


def _run_priority_watcher_with_events(watcher: PriorityWatcher, fake_events: list, priority: int = 5):
    """Patch inotify and utils on *watcher* and drive do_watch_loop with *fake_events*.

    utils.get_priority is mocked to return *priority* so tests are not coupled
    to metafits lookup logic.
    """
    mock_inotify = mock.Mock()

    def event_gen(timeout_s, yield_nones):
        yield from fake_events
        watcher.watching = False

    mock_inotify.event_gen = event_gen
    watcher.inotify_tree = mock_inotify
    watcher.watching = True

    with (
        mock.patch("mwax_mover.mwax_priority_watcher.utils.scan_for_existing_files_and_add_to_priority_queue"),
        mock.patch(
            "mwax_mover.mwax_priority_watcher.utils.get_priority",
            return_value=priority,
        ),
    ):
        watcher.do_watch_loop()


class TestPriorityWatcherDoWatchLoopFiltering:
    def test_matching_extension_enqueued(self, make_watcher, dest_queue, tmp_path):
        w = make_watcher()
        event = _make_fake_event(
            inotify.constants.IN_MOVED_TO,
            str(tmp_path),
            "1234567890_file.fits",
        )
        _run_priority_watcher_with_events(w, [event])
        assert dest_queue.qsize() == 1
        priority, item = dest_queue.get()
        assert item.value == os.path.join(str(tmp_path), "1234567890_file.fits")

    def test_enqueued_item_carries_correct_priority(self, make_watcher, dest_queue):
        w = make_watcher()
        event = _make_fake_event(
            inotify.constants.IN_MOVED_TO,
            w.path,
            "1234567890_file.fits",
        )
        _run_priority_watcher_with_events(w, [event], priority=3)
        priority, _ = dest_queue.get()
        assert priority == 3

    def test_non_matching_extension_not_enqueued(self, make_watcher, dest_queue):
        w = make_watcher()
        event = _make_fake_event(
            inotify.constants.IN_MOVED_TO,
            w.path,
            "1234567890_file.metafits",
        )
        _run_priority_watcher_with_events(w, [event])
        assert dest_queue.empty()

    def test_wildcard_pattern_enqueues_any_extension(self, make_watcher, dest_queue):
        w = make_watcher(pattern=".*")
        event = _make_fake_event(
            inotify.constants.IN_MOVED_TO,
            w.path,
            "1234567890_file.metafits",
        )
        _run_priority_watcher_with_events(w, [event])
        assert dest_queue.qsize() == 1

    def test_excluded_extension_not_enqueued(self, make_watcher, dest_queue):
        w = make_watcher(pattern=".*", exclude_pattern=".metafits")
        event = _make_fake_event(
            inotify.constants.IN_MOVED_TO,
            w.path,
            "1234567890_file.metafits",
        )
        _run_priority_watcher_with_events(w, [event])
        assert dest_queue.empty()

    def test_excluded_extension_does_not_block_other_extensions(self, make_watcher, dest_queue):
        w = make_watcher(pattern=".*", exclude_pattern=".metafits")
        events = [
            _make_fake_event(inotify.constants.IN_MOVED_TO, w.path, "1234567890_file.metafits"),
            _make_fake_event(inotify.constants.IN_MOVED_TO, w.path, "1234567890_file.fits"),
        ]
        _run_priority_watcher_with_events(w, events)
        assert dest_queue.qsize() == 1
        _, item = dest_queue.get()
        assert item.value.endswith(".fits")

    def test_wrong_event_type_not_enqueued(self, make_watcher, dest_queue):
        """IN_OPEN arriving on an IN_MOVED_TO watcher must be ignored."""
        w = make_watcher()
        event = _make_fake_event(
            inotify.constants.IN_OPEN,
            w.path,
            "1234567890_file.fits",
        )
        _run_priority_watcher_with_events(w, [event])
        assert dest_queue.empty()

    def test_multiple_matching_events_all_enqueued(self, make_watcher, dest_queue):
        w = make_watcher()
        events = [
            _make_fake_event(
                inotify.constants.IN_MOVED_TO,
                w.path,
                f"123456789{i}_file.fits",
            )
            for i in range(5)
        ]
        _run_priority_watcher_with_events(w, events)
        assert dest_queue.qsize() == 5

    def test_queue_ordering_by_filename(self, make_watcher, dest_queue):
        """Items enqueued with equal priority are dequeued in filename order."""
        w = make_watcher()
        events = [
            _make_fake_event(inotify.constants.IN_MOVED_TO, w.path, "1234567892_file.fits"),
            _make_fake_event(inotify.constants.IN_MOVED_TO, w.path, "1234567890_file.fits"),
            _make_fake_event(inotify.constants.IN_MOVED_TO, w.path, "1234567891_file.fits"),
        ]
        _run_priority_watcher_with_events(w, events, priority=1)

        results = [dest_queue.get()[1].value for _ in range(3)]
        filenames = [os.path.basename(r) for r in results]
        assert filenames == [
            "1234567890_file.fits",
            "1234567891_file.fits",
            "1234567892_file.fits",
        ]


# ── Live integration tests ────────────────────────────────────────────────────


class TestPriorityWatcherLiveInotify:
    """Integration tests using real inotify events on /dev/shm.

    utils.get_priority is mocked so tests do not require real metafits files.
    """

    TIMEOUT = 5.0

    def _start_watcher_thread(self, watcher: PriorityWatcher) -> threading.Thread:
        t = threading.Thread(target=watcher.start, daemon=True)
        t.start()
        deadline = time.time() + self.TIMEOUT
        while not watcher.scan_completed and time.time() < deadline:
            time.sleep(0.05)
        return t

    def _stop_watcher(self, watcher: PriorityWatcher, thread: threading.Thread):
        watcher.stop()
        thread.join(timeout=3.0)
        try:
            del watcher.inotify_tree
        except AttributeError:
            pass

    def test_rename_into_watch_dir_detected(self, dest_queue, shm_watch_dir):
        """A file renamed into the watch dir from the same filesystem is detected."""
        with (
            mock.patch("mwax_mover.mwax_priority_watcher.utils.get_priority", return_value=1),
            mock.patch("mwax_mover.mwax_priority_watcher.utils.scan_for_existing_files_and_add_to_priority_queue"),
        ):
            watcher = PriorityWatcher(
                name="test_rename",
                path=shm_watch_dir,
                dest_queue=dest_queue,
                pattern=".fits",
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                recursive=False,
                metafits_path="/dummy",
                list_of_correlator_high_priority_projects=[],
                list_of_vcs_high_priority_projects=[],
            )
            thread = self._start_watcher_thread(watcher)

            pid = os.getpid()
            src = os.path.join("/dev/shm", f"_src_{pid}.fits")
            dst = os.path.join(shm_watch_dir, f"_dst_{pid}.fits")
            try:
                with open(src, "w") as f:
                    f.write("test")
                os.rename(src, dst)

                priority, item = dest_queue.get(timeout=self.TIMEOUT)
                assert item.value == dst
                assert priority == 1
            finally:
                self._stop_watcher(watcher, thread)
                for p in [dst]:
                    try:
                        os.unlink(p)
                    except FileNotFoundError:
                        pass

    def test_close_write_detected(self, dest_queue, shm_watch_dir):
        """A file written and closed in the watch dir is detected in NEW mode."""
        with (
            mock.patch("mwax_mover.mwax_priority_watcher.utils.get_priority", return_value=2),
            mock.patch("mwax_mover.mwax_priority_watcher.utils.scan_for_existing_files_and_add_to_priority_queue"),
        ):
            watcher = PriorityWatcher(
                name="test_new",
                path=shm_watch_dir,
                dest_queue=dest_queue,
                pattern=".fits",
                mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
                recursive=False,
                metafits_path="/dummy",
                list_of_correlator_high_priority_projects=[],
                list_of_vcs_high_priority_projects=[],
            )
            thread = self._start_watcher_thread(watcher)

            dst = os.path.join(shm_watch_dir, f"_new_{os.getpid()}.fits")
            try:
                with open(dst, "w") as f:
                    f.write("test")

                priority, item = dest_queue.get(timeout=self.TIMEOUT)
                assert item.value == dst
                assert priority == 2
            finally:
                self._stop_watcher(watcher, thread)
                try:
                    os.unlink(dst)
                except FileNotFoundError:
                    pass

    def test_non_matching_extension_not_detected(self, dest_queue, shm_watch_dir):
        """A renamed file with a non-matching extension is not enqueued."""
        with (
            mock.patch("mwax_mover.mwax_priority_watcher.utils.get_priority", return_value=1),
            mock.patch("mwax_mover.mwax_priority_watcher.utils.scan_for_existing_files_and_add_to_priority_queue"),
        ):
            watcher = PriorityWatcher(
                name="test_no_match",
                path=shm_watch_dir,
                dest_queue=dest_queue,
                pattern=".fits",
                mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
                recursive=False,
                metafits_path="/dummy",
                list_of_correlator_high_priority_projects=[],
                list_of_vcs_high_priority_projects=[],
            )
            thread = self._start_watcher_thread(watcher)

            pid = os.getpid()
            src = os.path.join("/dev/shm", f"_src_{pid}.metafits")
            dst = os.path.join(shm_watch_dir, f"_dst_{pid}.metafits")
            try:
                with open(src, "w") as f:
                    f.write("test")
                os.rename(src, dst)

                time.sleep(1.0)
                assert dest_queue.empty()
            finally:
                self._stop_watcher(watcher, thread)
                try:
                    os.unlink(dst)
                except FileNotFoundError:
                    pass
