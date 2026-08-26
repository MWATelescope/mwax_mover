import logging
import os
import subprocess
import threading
from pathlib import Path
from unittest.mock import patch

import pytest
import tests_common
from tests_fakedb import FakeMWAXDBHandler

from mwax_mover.cli.mwax_calvin_controller import (
    DEFAULT_PLOT_UPLOAD_MAX_FITS_PER_PASS,
    PLOT_UPLOAD_BACKLOG_DELAY_SECS,
    MWAXCalvinController,
    fit_dir_sort_key,
)

logger = logging.getLogger(__name__)


def test_mwax_calvin_controller():
    """Tests that mwax_calvin reads a config file ok"""
    # Setup all the paths
    base_dir = tests_common.setup_test_directories("test016")

    # Start mwax_subfile_distributor using our test config
    mcal = MWAXCalvinController()

    # Override the hostname
    mcal.hostname = "calvin99"

    # Determine config file location
    config_filename = tests_common.render_test_config("test016")

    # Override db_handler with a fake one
    fake_db_handler = FakeMWAXDBHandler()
    # Add any select results (in order in the code below-or keep commented if none)
    # e.g. fake_db_handler.select_results = [[{"observation_num": 123, "size": 1024, "checksum": "abc123"}]]

    # Call to read config <-- this is what we're testing!
    mcal.initialise(
        config_filename,
        fake_db_handler,
    )

    #
    # Now confirm the params all match the config file
    #

    # mwax_mover section
    assert mcal.log_path == os.path.join(base_dir, "logs"), (
        f"log path mismatch: {mcal.log_path} {os.path.join(base_dir, 'logs')}"
    )

    assert mcal.health_multicast_interface_name == "lo"
    assert mcal.health_multicast_ip == "127.0.0.1"
    assert mcal.health_multicast_port == 8012
    assert mcal.health_multicast_hops == 1
    assert mcal.max_in_progress_asvo_jobs == 10
    assert mcal.s3_profile == "mwa-calvin-s3"
    assert mcal.s3_bucket == "mwa_calvin_solutions"
    assert mcal.plot_upload_paths == [
        os.path.join(base_dir, "shared/data/calvin11/plots"),
        os.path.join(base_dir, "shared/data/calvin12/plots"),
    ]
    # Not present in the test config, so the default applies
    assert mcal.plot_upload_max_fits_per_pass == DEFAULT_PLOT_UPLOAD_MAX_FITS_PER_PASS


class TestFitDirSortKey:
    """Tests for fit_dir_sort_key: newest (largest) fitid must sort first."""

    def test_orders_newest_fitid_first(self):
        """Fit dirs sort by descending fitid, not lexicographically."""
        # Deliberately mixed digit counts: sorted() on the raw names would put
        # "999999" after "1768401673707300" because "9" > "1".
        names = ["1768401673707300", "999999", "1768401673707301", "1768401673707299"]
        dirs = [Path("/plots") / n for n in names]

        dirs.sort(key=fit_dir_sort_key)

        assert [d.name for d in dirs] == [
            "1768401673707301",
            "1768401673707300",
            "1768401673707299",
            "999999",
        ]

    def test_non_numeric_dirs_sort_last(self):
        """A dir that is not named for a fitid never displaces a real fit."""
        dirs = [Path("/plots") / n for n in ["not-a-fitid", "1768401673707300", "abc"]]

        dirs.sort(key=fit_dir_sort_key)

        assert [d.name for d in dirs] == ["1768401673707300", "abc", "not-a-fitid"]


class TestUploadPublishedFitDirs:
    """Tests for upload_published_fit_dirs ordering, batching and error handling."""

    @staticmethod
    def make_controller(max_fits_per_pass: int = 100) -> MWAXCalvinController:
        """Build a controller with only the fields the upload path needs."""
        mcal = MWAXCalvinController()
        mcal.s3_profile = "test-profile"
        mcal.s3_bucket = "test-bucket"
        mcal.plot_upload_max_fits_per_pass = max_fits_per_pass
        return mcal

    @staticmethod
    def make_fit_dirs(base: Path, fit_ids: list[int]) -> None:
        """Create a published fit dir per fitid, each holding one plot file."""
        for fit_id in fit_ids:
            fit_dir = base / str(fit_id)
            fit_dir.mkdir(parents=True)
            (fit_dir / "phase_fits.png").write_text("dummy plot")

    @staticmethod
    def fake_rclone_move(path, profile, bucket, dest_subpath=None, min_file_age_secs=0):
        """Stand in for utils.rclone_move, emptying the source dir as rclone does.

        rclone move deletes each source file once it is transferred and leaves
        the empty directory behind, which is what lets the caller rmdir it. A
        mock that skipped this would make every dir look like a partial upload.
        """
        for src_file in Path(path).iterdir():
            src_file.unlink()
        return (1, 1024)

    def test_uploads_newest_fitid_first(self, tmp_path):
        """rclone_move is called in descending fitid order."""
        base = tmp_path / "plots"
        # Created in ascending order so a passing test cannot be an artefact of
        # creation order or of the filesystem's iteration order.
        self.make_fit_dirs(base, [1768401673707299, 1768401673707300, 1768401673707301])

        mcal = self.make_controller()

        with patch(
            "mwax_mover.cli.mwax_calvin_controller.utils.rclone_move",
            side_effect=self.fake_rclone_move,
        ) as mock_move:
            mcal.upload_published_fit_dirs(str(base))

        uploaded_order = [call.kwargs["dest_subpath"] for call in mock_move.call_args_list]
        assert uploaded_order == [
            "1768401673707301",
            "1768401673707300",
            "1768401673707299",
        ]

    def test_only_uploads_one_batch_of_newest_dirs(self, tmp_path):
        """With a backlog, only the newest max_fits_per_pass dirs go this pass."""
        base = tmp_path / "plots"
        fit_ids = [1768401673707000 + i for i in range(10)]
        self.make_fit_dirs(base, fit_ids)

        mcal = self.make_controller(max_fits_per_pass=3)

        with patch(
            "mwax_mover.cli.mwax_calvin_controller.utils.rclone_move",
            side_effect=self.fake_rclone_move,
        ) as mock_move:
            mcal.upload_published_fit_dirs(str(base))

        uploaded_order = [call.kwargs["dest_subpath"] for call in mock_move.call_args_list]
        assert uploaded_order == ["1768401673707009", "1768401673707008", "1768401673707007"]

        # The older dirs are untouched and still there for the next pass
        assert sorted(int(d.name) for d in base.iterdir()) == fit_ids[:7]
        assert len(list(base.iterdir())) == 7

    def test_staging_and_non_dirs_are_skipped(self, tmp_path):
        """Staging dirs and stray files are never uploaded."""
        base = tmp_path / "plots"
        self.make_fit_dirs(base, [1768401673707300])
        (base / ".staging-1768401673707301").mkdir()
        (base / "stray.txt").write_text("not a fit dir")

        mcal = self.make_controller()

        with patch(
            "mwax_mover.cli.mwax_calvin_controller.utils.rclone_move",
            side_effect=self.fake_rclone_move,
        ) as mock_move:
            mcal.upload_published_fit_dirs(str(base))

        uploaded_order = [call.kwargs["dest_subpath"] for call in mock_move.call_args_list]
        assert uploaded_order == ["1768401673707300"]

    def test_one_failing_dir_does_not_block_older_dirs(self, tmp_path):
        """A failure on the newest dir must not starve the ones behind it."""
        base = tmp_path / "plots"
        self.make_fit_dirs(base, [1768401673707299, 1768401673707300, 1768401673707301])

        mcal = self.make_controller()

        def fake_move(path, profile, bucket, dest_subpath=None, min_file_age_secs=0):
            if dest_subpath == "1768401673707301":
                raise subprocess.CalledProcessError(1, "rclone", stderr="boom")
            return self.fake_rclone_move(path, profile, bucket, dest_subpath, min_file_age_secs)

        with patch(
            "mwax_mover.cli.mwax_calvin_controller.utils.rclone_move",
            side_effect=fake_move,
        ) as mock_move:
            # Some dirs uploaded, so no exception should escape and the path
            # should not be backed off.
            mcal.upload_published_fit_dirs(str(base))

        attempted = [call.kwargs["dest_subpath"] for call in mock_move.call_args_list]
        assert attempted == [
            "1768401673707301",
            "1768401673707300",
            "1768401673707299",
        ]

        # The failed dir is kept for a retry; the successful ones are gone
        assert [d.name for d in base.iterdir()] == ["1768401673707301"]

    def test_raises_when_nothing_in_batch_uploads(self, tmp_path):
        """A total failure still propagates so the caller applies backoff."""
        base = tmp_path / "plots"
        self.make_fit_dirs(base, [1768401673707300, 1768401673707301])

        mcal = self.make_controller()

        with patch(
            "mwax_mover.cli.mwax_calvin_controller.utils.rclone_move",
            side_effect=subprocess.CalledProcessError(1, "rclone", stderr="s3 down"),
        ):
            with pytest.raises(subprocess.CalledProcessError):
                mcal.upload_published_fit_dirs(str(base))

        # Nothing was removed, so everything is retried next pass
        assert sorted(d.name for d in base.iterdir()) == [
            "1768401673707300",
            "1768401673707301",
        ]

    def test_missing_upload_path_is_ignored(self, tmp_path):
        """A non-existent base path is a warning, not an exception."""
        mcal = self.make_controller()

        with patch("mwax_mover.cli.mwax_calvin_controller.utils.rclone_move") as mock_move:
            mcal.upload_published_fit_dirs(str(tmp_path / "does_not_exist"))

        mock_move.assert_not_called()

    def test_returns_true_when_backlog_remains(self, tmp_path):
        """A truncated batch reports a backlog so the caller shortens its sleep."""
        base = tmp_path / "plots"
        self.make_fit_dirs(base, [1768401673707000 + i for i in range(5)])

        mcal = self.make_controller(max_fits_per_pass=2)

        with patch(
            "mwax_mover.cli.mwax_calvin_controller.utils.rclone_move",
            side_effect=self.fake_rclone_move,
        ):
            assert mcal.upload_published_fit_dirs(str(base)) is True

    def test_returns_false_when_path_fully_drained(self, tmp_path):
        """No backlog means the caller sleeps for the full interval."""
        base = tmp_path / "plots"
        self.make_fit_dirs(base, [1768401673707300, 1768401673707301])

        mcal = self.make_controller(max_fits_per_pass=10)

        with patch(
            "mwax_mover.cli.mwax_calvin_controller.utils.rclone_move",
            side_effect=self.fake_rclone_move,
        ):
            assert mcal.upload_published_fit_dirs(str(base)) is False

    def test_returns_false_when_batch_exactly_empties_path(self, tmp_path):
        """A full batch that leaves nothing behind is not a backlog."""
        base = tmp_path / "plots"
        self.make_fit_dirs(base, [1768401673707300, 1768401673707301])

        mcal = self.make_controller(max_fits_per_pass=2)

        with patch(
            "mwax_mover.cli.mwax_calvin_controller.utils.rclone_move",
            side_effect=self.fake_rclone_move,
        ):
            assert mcal.upload_published_fit_dirs(str(base)) is False

    def test_stop_event_aborts_batch_between_fit_dirs(self, tmp_path):
        """Shutdown stops the batch instead of working through every dir."""
        base = tmp_path / "plots"
        self.make_fit_dirs(base, [1768401673707000 + i for i in range(5)])

        mcal = self.make_controller()
        stop_event = threading.Event()

        def stop_after_first(path, profile, bucket, dest_subpath=None, min_file_age_secs=0):
            # Simulate shutdown arriving while the first transfer is running
            stop_event.set()
            return self.fake_rclone_move(path, profile, bucket, dest_subpath, min_file_age_secs)

        with patch(
            "mwax_mover.cli.mwax_calvin_controller.utils.rclone_move",
            side_effect=stop_after_first,
        ) as mock_move:
            result = mcal.upload_published_fit_dirs(str(base), stop_event)

        # Only the newest dir was uploaded; the rest were left alone
        assert mock_move.call_count == 1
        assert mock_move.call_args_list[0].kwargs["dest_subpath"] == "1768401673707004"
        assert sorted(int(d.name) for d in base.iterdir()) == [1768401673707000 + i for i in range(4)]

        # Stopping early must not ask the caller to come back sooner
        assert result is False

    def test_stop_event_already_set_uploads_nothing(self, tmp_path):
        """An already-set stop event means no transfers are started at all."""
        base = tmp_path / "plots"
        self.make_fit_dirs(base, [1768401673707300])

        mcal = self.make_controller()
        stop_event = threading.Event()
        stop_event.set()

        with patch("mwax_mover.cli.mwax_calvin_controller.utils.rclone_move") as mock_move:
            assert mcal.upload_published_fit_dirs(str(base), stop_event) is False

        mock_move.assert_not_called()
        assert [d.name for d in base.iterdir()] == ["1768401673707300"]


class RecordingStopEvent(threading.Event):
    """A stop event that records wait() timeouts and ends the loop after N passes.

    Subclasses threading.Event rather than duck-typing it so the handler still
    receives the type it declares.
    """

    def __init__(self, passes: int = 1):
        super().__init__()
        self.waits: list[float | None] = []
        self.passes_remaining = passes

    def wait(self, timeout: float | None = None) -> bool:
        """Record the timeout, then stop the loop once the pass budget is spent."""
        self.waits.append(timeout)
        self.passes_remaining -= 1
        if self.passes_remaining <= 0:
            self.set()
        return self.is_set()


class TestPlotUploadHandler:
    """Tests for how plot_upload_handler paces its passes."""

    @staticmethod
    def make_controller(tmp_path) -> MWAXCalvinController:
        """Build a controller with one upload path and a production-like interval."""
        mcal = MWAXCalvinController()
        mcal.s3_profile = "test-profile"
        mcal.s3_bucket = "test-bucket"
        mcal.plot_upload_paths = [str(tmp_path)]
        mcal.plot_upload_interval_secs = 600
        mcal.plot_upload_max_fits_per_pass = 100
        return mcal

    def test_uses_short_delay_when_backlog_pending(self, tmp_path):
        """A backlog must not drain at one batch per full interval."""
        mcal = self.make_controller(tmp_path)
        stop_event = RecordingStopEvent()

        with patch.object(mcal, "upload_published_fit_dirs", return_value=True):
            mcal.plot_upload_handler(stop_event)

        assert stop_event.waits == [PLOT_UPLOAD_BACKLOG_DELAY_SECS]

    def test_uses_full_interval_when_no_backlog(self, tmp_path):
        """With nothing pending, the normal interval applies."""
        mcal = self.make_controller(tmp_path)
        stop_event = RecordingStopEvent()

        with patch.object(mcal, "upload_published_fit_dirs", return_value=False):
            mcal.plot_upload_handler(stop_event)

        assert stop_event.waits == [600]

    def test_backoff_still_applies_on_failure(self, tmp_path):
        """A total failure backs the path off and does not report a backlog."""
        mcal = self.make_controller(tmp_path)
        stop_event = RecordingStopEvent()

        with patch.object(
            mcal,
            "upload_published_fit_dirs",
            side_effect=subprocess.CalledProcessError(1, "rclone", stderr="s3 down"),
        ):
            mcal.plot_upload_handler(stop_event)

        # Falls back to the full interval rather than hot-looping on the failure
        assert stop_event.waits == [600]

    def test_stop_event_prevents_further_paths(self, tmp_path):
        """Once shutdown is requested, remaining paths in the pass are skipped."""
        mcal = self.make_controller(tmp_path)
        mcal.plot_upload_paths = [str(tmp_path / "a"), str(tmp_path / "b"), str(tmp_path / "c")]
        stop_event = RecordingStopEvent()

        def stop_during_first_path(plot_upload_path, event=None):
            stop_event.set()
            return False

        with patch.object(
            mcal,
            "upload_published_fit_dirs",
            side_effect=stop_during_first_path,
        ) as mock_upload:
            mcal.plot_upload_handler(stop_event)

        # Only the first path was attempted, not all three
        assert mock_upload.call_count == 1
