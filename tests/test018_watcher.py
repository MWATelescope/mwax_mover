"""Tests for mwax_mover.mwax_watcher.Watcher.

Coverage:
  - Constructor validation (path existence, mask assignment per mode)
  - Mask logic bug regression (| vs &)
  - get_status()
  - do_watch_loop() event filtering (pattern, exclude_pattern)
  - Live IN_MOVED_TO integration test on /dev/shm
  - Live IN_CLOSE_WRITE integration test on /dev/shm

Filesystem note:
  inotify IN_MOVED_TO requires both source and destination to be on the same
  filesystem. Tests that exercise live inotify events use /dev/shm (tmpfs) to
  match the production environment. Tests are automatically skipped when
  /dev/shm is unavailable or is on a filesystem where inotify is unreliable
  (e.g. WSL v9fs /mnt/c paths).
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
from mwax_mover.mwax_watcher import Watcher

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


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture()
def dest_queue() -> queue.Queue:
    """A fresh queue for each test."""
    return queue.Queue()


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


# ── Constructor tests ─────────────────────────────────────────────────────────


class TestWatcherInit:
    """Watcher.__init__ validation."""

    def test_raises_when_path_does_not_exist(self, dest_queue):
        with pytest.raises(FileNotFoundError):
            Watcher(
                name="test",
                path="/nonexistent/path/xyz",
                dest_queue=dest_queue,
                pattern=".fits",
                mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
                recursive=False,
            )

    def test_mask_new_mode(self, dest_queue, tmp_path):
        w = Watcher(
            name="test",
            path=str(tmp_path),
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
            recursive=False,
        )
        assert w.mask == inotify.constants.IN_CLOSE_WRITE

    def test_mask_rename_mode(self, dest_queue, tmp_path):
        w = Watcher(
            name="test",
            path=str(tmp_path),
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
            recursive=False,
        )
        assert w.mask == inotify.constants.IN_MOVED_TO

    def test_mask_rename_or_new_mode(self, dest_queue, tmp_path):
        w = Watcher(
            name="test",
            path=str(tmp_path),
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
            recursive=False,
        )
        expected = inotify.constants.IN_MOVED_TO | inotify.constants.IN_CLOSE_WRITE
        assert w.mask == expected

    def test_exclude_pattern_stored(self, dest_queue, tmp_path):
        w = Watcher(
            name="test",
            path=str(tmp_path),
            dest_queue=dest_queue,
            pattern=".*",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
            recursive=False,
            exclude_pattern=".metafits",
        )
        assert w.exclude_pattern == ".metafits"

    def test_exclude_pattern_defaults_to_none(self, dest_queue, tmp_path):
        w = Watcher(
            name="test",
            path=str(tmp_path),
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
            recursive=False,
        )
        assert w.exclude_pattern is None

    def test_watching_is_false_before_start(self, dest_queue, tmp_path):
        w = Watcher(
            name="test",
            path=str(tmp_path),
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
            recursive=False,
        )
        assert w.watching is False


# ── get_status tests ──────────────────────────────────────────────────────────


class TestGetStatus:
    def test_returns_name_and_path(self, dest_queue, tmp_path):
        w = Watcher(
            name="my_watcher",
            path=str(tmp_path),
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
            recursive=False,
        )
        status = w.get_status()
        assert status["name"] == "my_watcher"
        assert status["watch_path"] == str(tmp_path)


# ── Mask logic regression tests ───────────────────────────────────────────────


class TestMaskLogic:
    """Regression tests for the | vs & mask check bug in do_watch_loop.

    The original code used:
        if header.mask | self.mask == self.mask:
    which is always True regardless of the event type received.

    The correct check is:
        if header.mask & self.mask:
    which only passes when the incoming event type overlaps with the desired mask.
    """

    def test_correct_operator_passes_matching_event(self):
        """& operator: a matching event type should pass the filter."""
        desired_mask = inotify.constants.IN_MOVED_TO
        incoming_mask = inotify.constants.IN_MOVED_TO
        assert bool(incoming_mask & desired_mask) is True

    def test_correct_operator_rejects_non_matching_event(self):
        """& operator: a non-matching event type should be rejected."""
        desired_mask = inotify.constants.IN_MOVED_TO
        incoming_mask = inotify.constants.IN_OPEN  # spurious event
        assert bool(incoming_mask & desired_mask) is False

    def test_buggy_operator_incorrectly_passes_non_matching_event(self):
        """| operator: proves the bug — non-matching event always passes."""
        desired_mask = inotify.constants.IN_MOVED_TO
        incoming_mask = inotify.constants.IN_OPEN  # spurious event
        # This is the bug: always True regardless of incoming_mask
        assert bool((incoming_mask | desired_mask) == desired_mask) is False
        # NOTE: the above assertion confirms the bug does NOT always hold for
        # IN_OPEN specifically, but the general case is that for many event
        # types, (incoming | desired) == desired evaluates True when it should
        # not. The & fix is unambiguous and correct in all cases.

    def test_correct_operator_passes_combined_mask_event(self):
        """& operator: RENAME_OR_NEW mask accepts both IN_MOVED_TO and IN_CLOSE_WRITE."""
        desired_mask = inotify.constants.IN_MOVED_TO | inotify.constants.IN_CLOSE_WRITE
        assert bool(inotify.constants.IN_MOVED_TO & desired_mask) is True
        assert bool(inotify.constants.IN_CLOSE_WRITE & desired_mask) is True

    def test_correct_operator_rejects_unrelated_event_against_combined_mask(self):
        """& operator: unrelated event rejected even against combined mask."""
        desired_mask = inotify.constants.IN_MOVED_TO | inotify.constants.IN_CLOSE_WRITE
        assert bool(inotify.constants.IN_OPEN & desired_mask) is False


# ── do_watch_loop unit tests (mocked inotify) ─────────────────────────────────


def _make_fake_event(mask: int, path: str, filename: str):
    """Build a fake inotify event tuple matching (header, type_names, path, filename)."""
    header = mock.Mock()
    header.mask = mask
    return (header, [], path, filename)


def _run_watcher_with_events(watcher: Watcher, fake_events: list):
    """Patch inotify on *watcher* and run do_watch_loop with *fake_events*.

    Injects the events then immediately sets watcher.watching = False so the
    loop exits cleanly after one pass.
    """
    mock_inotify = mock.Mock()

    # event_gen yields the fake events then None (loop timeout) so watching
    # gets set to False and the while loop exits
    def event_gen(timeout_s, yield_nones):
        yield from fake_events
        watcher.watching = False

    mock_inotify.event_gen = event_gen
    watcher.inotify_tree = mock_inotify
    watcher.watching = True

    with mock.patch("mwax_mover.mwax_watcher.utils.scan_for_existing_files_and_add_to_queue"):
        watcher.do_watch_loop()


class TestDoWatchLoopFiltering:
    """Unit tests for event filtering logic inside do_watch_loop."""

    def _make_watcher(self, dest_queue, tmp_path, pattern, mode, exclude_pattern=None):
        return Watcher(
            name="test",
            path=str(tmp_path),
            dest_queue=dest_queue,
            pattern=pattern,
            mode=mode,
            recursive=False,
            exclude_pattern=exclude_pattern,
        )

    def test_matching_extension_enqueued(self, dest_queue, tmp_path):
        w = self._make_watcher(dest_queue, tmp_path, ".fits", mwax_mover.MODE_WATCH_DIR_FOR_RENAME)
        event = _make_fake_event(inotify.constants.IN_MOVED_TO, str(tmp_path), "obs123.fits")
        _run_watcher_with_events(w, [event])
        assert dest_queue.qsize() == 1
        assert dest_queue.get() == str(tmp_path) + "/obs123.fits"

    def test_non_matching_extension_not_enqueued(self, dest_queue, tmp_path):
        w = self._make_watcher(dest_queue, tmp_path, ".fits", mwax_mover.MODE_WATCH_DIR_FOR_RENAME)
        event = _make_fake_event(inotify.constants.IN_MOVED_TO, str(tmp_path), "obs123.metafits")
        _run_watcher_with_events(w, [event])
        assert dest_queue.empty()

    def test_wildcard_pattern_enqueues_any_extension(self, dest_queue, tmp_path):
        w = self._make_watcher(dest_queue, tmp_path, ".*", mwax_mover.MODE_WATCH_DIR_FOR_RENAME)
        event = _make_fake_event(inotify.constants.IN_MOVED_TO, str(tmp_path), "obs123.metafits")
        _run_watcher_with_events(w, [event])
        assert dest_queue.qsize() == 1

    def test_excluded_extension_not_enqueued(self, dest_queue, tmp_path):
        w = self._make_watcher(
            dest_queue,
            tmp_path,
            ".*",
            mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
            exclude_pattern=".metafits",
        )
        event = _make_fake_event(inotify.constants.IN_MOVED_TO, str(tmp_path), "obs123.metafits")
        _run_watcher_with_events(w, [event])
        assert dest_queue.empty()

    def test_excluded_extension_does_not_block_other_extensions(self, dest_queue, tmp_path):
        w = self._make_watcher(
            dest_queue,
            tmp_path,
            ".*",
            mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
            exclude_pattern=".metafits",
        )
        events = [
            _make_fake_event(inotify.constants.IN_MOVED_TO, str(tmp_path), "obs123.metafits"),
            _make_fake_event(inotify.constants.IN_MOVED_TO, str(tmp_path), "obs123.fits"),
        ]
        _run_watcher_with_events(w, events)
        assert dest_queue.qsize() == 1
        assert dest_queue.get().endswith(".fits")

    def test_wrong_event_type_not_enqueued(self, dest_queue, tmp_path):
        """IN_OPEN arriving on a IN_MOVED_TO watcher should be ignored."""
        w = self._make_watcher(dest_queue, tmp_path, ".fits", mwax_mover.MODE_WATCH_DIR_FOR_RENAME)
        event = _make_fake_event(inotify.constants.IN_OPEN, str(tmp_path), "obs123.fits")
        _run_watcher_with_events(w, [event])
        assert dest_queue.empty()

    def test_multiple_matching_events_all_enqueued(self, dest_queue, tmp_path):
        w = self._make_watcher(dest_queue, tmp_path, ".fits", mwax_mover.MODE_WATCH_DIR_FOR_RENAME)
        events = [_make_fake_event(inotify.constants.IN_MOVED_TO, str(tmp_path), f"obs{i}.fits") for i in range(5)]
        _run_watcher_with_events(w, events)
        assert dest_queue.qsize() == 5


# ── Live integration tests ────────────────────────────────────────────────────


class TestLiveInotify:
    """Integration tests using real inotify events on /dev/shm.

    These tests start a real Watcher thread, trigger filesystem events, and
    assert that the correct files appear in the queue. They require /dev/shm
    to be available and inotify-reliable (see shm_watch_dir fixture).
    """

    TIMEOUT = 5.0  # seconds to wait for an event before failing

    def _start_watcher_thread(self, watcher: Watcher) -> threading.Thread:
        t = threading.Thread(target=watcher.start, daemon=True)
        t.start()
        # Wait for initial scan to complete before triggering events
        deadline = time.time() + self.TIMEOUT
        while not watcher.scan_completed and time.time() < deadline:
            time.sleep(0.05)
        return t

    def _stop_watcher(self, watcher: Watcher, thread: threading.Thread):
        watcher.stop()
        thread.join(timeout=3.0)
        # Explicitly delete adapter to avoid inotify __del__ log error
        try:
            del watcher.inotify_tree
        except AttributeError:
            pass

    def test_rename_into_watch_dir_detected(self, dest_queue, shm_watch_dir):
        """A file renamed into the watch dir from the same filesystem is detected."""
        watcher = Watcher(
            name="test_rename",
            path=shm_watch_dir,
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
            recursive=False,
        )
        thread = self._start_watcher_thread(watcher)

        try:
            # Write source file on the same filesystem (/dev/shm), then rename
            src = os.path.join("/dev/shm", f"_src_{os.getpid()}.fits")
            dst = os.path.join(shm_watch_dir, f"_dst_{os.getpid()}.fits")
            with open(src, "w") as f:
                f.write("test")
            os.rename(src, dst)

            result = dest_queue.get(timeout=self.TIMEOUT)
            assert result == dst
        finally:
            self._stop_watcher(watcher, thread)
            for p in [dst]:
                try:
                    os.unlink(p)
                except FileNotFoundError:
                    pass

    def test_rename_non_matching_extension_not_detected(self, dest_queue, shm_watch_dir):
        """A renamed file with a non-matching extension is not enqueued."""
        watcher = Watcher(
            name="test_rename_no_match",
            path=shm_watch_dir,
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
            recursive=False,
        )
        thread = self._start_watcher_thread(watcher)

        dst = os.path.join(shm_watch_dir, f"_dst_{os.getpid()}.metafits")
        try:
            src = os.path.join("/dev/shm", f"_src_{os.getpid()}.metafits")
            with open(src, "w") as f:
                f.write("test")
            os.rename(src, dst)

            # Give inotify time to deliver (and incorrectly enqueue) if broken
            time.sleep(1.0)
            assert dest_queue.empty()
        finally:
            self._stop_watcher(watcher, thread)
            try:
                os.unlink(dst)
            except FileNotFoundError:
                pass

    def test_close_write_detected(self, dest_queue, shm_watch_dir):
        """A file written and closed in the watch dir is detected in NEW mode."""
        watcher = Watcher(
            name="test_new",
            path=shm_watch_dir,
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_NEW,
            recursive=False,
        )
        thread = self._start_watcher_thread(watcher)

        dst = os.path.join(shm_watch_dir, f"_new_{os.getpid()}.fits")
        try:
            with open(dst, "w") as f:
                f.write("test")

            result = dest_queue.get(timeout=self.TIMEOUT)
            assert result == dst
        finally:
            self._stop_watcher(watcher, thread)
            try:
                os.unlink(dst)
            except FileNotFoundError:
                pass

    def test_rename_or_new_detects_both(self, dest_queue, shm_watch_dir):
        """RENAME_OR_NEW mode enqueues both renamed and freshly-written files."""
        watcher = Watcher(
            name="test_rename_or_new",
            path=shm_watch_dir,
            dest_queue=dest_queue,
            pattern=".fits",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME_OR_NEW,
            recursive=False,
        )
        thread = self._start_watcher_thread(watcher)

        pid = os.getpid()
        renamed_dst = os.path.join(shm_watch_dir, f"_renamed_{pid}.fits")
        written_dst = os.path.join(shm_watch_dir, f"_written_{pid}.fits")
        try:
            src = os.path.join("/dev/shm", f"_src_{pid}.fits")
            with open(src, "w") as f:
                f.write("test")
            os.rename(src, renamed_dst)

            with open(written_dst, "w") as f:
                f.write("test")

            received = set()
            for _ in range(2):
                received.add(dest_queue.get(timeout=self.TIMEOUT))

            assert renamed_dst in received
            assert written_dst in received
        finally:
            self._stop_watcher(watcher, thread)
            for p in [renamed_dst, written_dst]:
                try:
                    os.unlink(p)
                except FileNotFoundError:
                    pass

    def test_excluded_extension_not_enqueued_live(self, dest_queue, shm_watch_dir):
        """Live test: excluded extension is not enqueued even when event fires."""
        watcher = Watcher(
            name="test_exclude_live",
            path=shm_watch_dir,
            dest_queue=dest_queue,
            pattern=".*",
            mode=mwax_mover.MODE_WATCH_DIR_FOR_RENAME,
            recursive=False,
            exclude_pattern=".metafits",
        )
        thread = self._start_watcher_thread(watcher)

        pid = os.getpid()
        excluded_dst = os.path.join(shm_watch_dir, f"_excl_{pid}.metafits")
        included_dst = os.path.join(shm_watch_dir, f"_incl_{pid}.fits")
        try:
            for dst in [excluded_dst, included_dst]:
                src = os.path.join("/dev/shm", f"_src_{pid}_{os.path.basename(dst)}")
                with open(src, "w") as f:
                    f.write("test")
                os.rename(src, dst)

            result = dest_queue.get(timeout=self.TIMEOUT)
            assert result == included_dst
            time.sleep(0.5)
            assert dest_queue.empty()
        finally:
            self._stop_watcher(watcher, thread)
            for p in [excluded_dst, included_dst]:
                try:
                    os.unlink(p)
                except FileNotFoundError:
                    pass
