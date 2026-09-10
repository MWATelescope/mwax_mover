"""Unit tests for MWAXDaemon, the shared lifecycle base class in mwax_mover.processors.daemon.

Tests exercise the base class in isolation via a minimal concrete subclass
implementing only the four abstract methods (get_extra_status, initialise,
start, stop). No real daemon inherits from MWAXDaemon yet -- see
docs/CLEANUP.md 6.4, step 1.
"""

import sys
from unittest.mock import MagicMock, patch

import pytest

from mwax_mover.processors.daemon import SECS_PER_INTERVAL, MWAXDaemon


class _FakeDaemon(MWAXDaemon):
    """Minimal concrete MWAXDaemon, for testing the base class in isolation."""

    DESCRIPTION = "fake daemon for testing"

    def __init__(self):
        """Initialize the fake daemon, recording calls for assertions."""
        super().__init__()
        self.initialise_calls = []
        self.start_called = False
        self.stop_called = False

    def get_extra_status(self) -> dict:
        """No daemon-specific status keys by default."""
        return {}

    def initialise(self, config_filename, *args, **kwargs):
        """Record the call instead of actually reading a config file."""
        self.initialise_calls.append((config_filename, args, kwargs))

    def start(self):
        """Record that start() was called."""
        self.start_called = True

    def stop(self):
        """Record that stop() was called."""
        self.stop_called = True


def test_cannot_instantiate_mwaxdaemon_directly():
    """MWAXDaemon is an ABC with abstract methods; direct instantiation must fail."""
    with pytest.raises(TypeError):
        MWAXDaemon()


def test_concrete_subclass_shares_base_attributes():
    """The base __init__ sets the attributes every daemon shares, all to their defaults."""
    daemon = _FakeDaemon()

    assert daemon.running is False
    assert daemon.hostname == ""
    assert daemon.fatal_exit_code == 0
    assert daemon.fatal_reason == ""
    assert daemon.health_multicast_interface_ip == ""


class TestRequestFatalShutdown:
    def test_sets_exit_code_reason_and_stops_running(self):
        """A fatal shutdown request records the cause and clears running."""
        daemon = _FakeDaemon()
        daemon.running = True

        daemon.request_fatal_shutdown(3, "disk full")

        assert daemon.fatal_exit_code == 3
        assert daemon.fatal_reason == "disk full"
        assert daemon.running is False

    def test_first_caller_wins(self):
        """A second call after the first must not overwrite the original cause."""
        daemon = _FakeDaemon()
        daemon.running = True

        daemon.request_fatal_shutdown(3, "disk full")
        daemon.request_fatal_shutdown(7, "knock-on failure")

        assert daemon.fatal_exit_code == 3
        assert daemon.fatal_reason == "disk full"

    def test_second_caller_logs_a_warning(self, caplog):
        """The knock-on failure is still logged, just doesn't become the cause."""
        daemon = _FakeDaemon()
        daemon.running = True
        daemon.request_fatal_shutdown(3, "disk full")

        with caplog.at_level("WARNING"):
            daemon.request_fatal_shutdown(7, "knock-on failure")

        assert "knock-on failure" in caplog.text


class TestSleep:
    def test_returns_immediately_when_not_running(self):
        """sleep() is a no-op once the daemon has already stopped running."""
        daemon = _FakeDaemon()
        daemon.running = False

        with patch("mwax_mover.processors.daemon.time.sleep") as mock_sleep:
            daemon.sleep(100)

        mock_sleep.assert_not_called()

    def test_short_duration_sleeps_once(self):
        """A duration at or under SECS_PER_INTERVAL sleeps in one call, no interval loop."""
        daemon = _FakeDaemon()
        daemon.running = True

        with patch("mwax_mover.processors.daemon.time.sleep") as mock_sleep:
            daemon.sleep(SECS_PER_INTERVAL)

        mock_sleep.assert_called_once_with(SECS_PER_INTERVAL)

    def test_long_duration_calls_during_sleep_interval_hook_once_per_interval(self):
        """A duration over SECS_PER_INTERVAL is chopped up, calling the hook once per full interval."""
        daemon = _FakeDaemon()
        daemon.running = True
        during_sleep_interval_mock = MagicMock()
        daemon.during_sleep_interval = during_sleep_interval_mock

        with patch("mwax_mover.processors.daemon.time.sleep") as mock_sleep:
            daemon.sleep(SECS_PER_INTERVAL * 3 + 2)

        # 3 full intervals (each followed by the hook), then the 2-second remainder.
        assert mock_sleep.call_count == 4
        assert during_sleep_interval_mock.call_count == 3

    def test_stops_early_if_running_cleared_mid_wait(self):
        """Clearing running partway through a long sleep interrupts it immediately."""
        daemon = _FakeDaemon()
        daemon.running = True

        def fake_sleep(_seconds):
            daemon.running = False

        with patch("mwax_mover.processors.daemon.time.sleep", side_effect=fake_sleep) as mock_sleep:
            daemon.sleep(SECS_PER_INTERVAL * 3)

        mock_sleep.assert_called_once_with(SECS_PER_INTERVAL)


class TestSignalHandler:
    def test_calls_stop(self):
        """The signal handler always stops the daemon."""
        daemon = _FakeDaemon()

        daemon.signal_handler(None, None)

        assert daemon.stop_called is True

    def test_includes_shutdown_log_detail_in_message(self, caplog):
        """A subclass's shutdown_log_detail() override is woven into the log message."""
        daemon = _FakeDaemon()
        daemon.shutdown_log_detail = MagicMock(return_value=" 3 workers")

        with caplog.at_level("WARNING"):
            daemon.signal_handler(None, None)

        assert "Shutting down 3 workers..." in caplog.text

    def test_default_detail_is_empty(self, caplog):
        """With no override, the message has no extra detail inserted."""
        daemon = _FakeDaemon()

        with caplog.at_level("WARNING"):
            daemon.signal_handler(None, None)

        assert "Shutting down..." in caplog.text


class TestGetStatus:
    def test_reports_the_five_common_keys_plus_cmdline_under_main(self):
        """Every daemon's status nests these six keys under "main" regardless of subclass."""
        daemon = _FakeDaemon()
        daemon.running = True
        daemon.hostname = "testhost"

        status = daemon.get_status()
        main_status = status["main"]

        assert main_status["process"] == "_FakeDaemon"
        assert main_status["host"] == "testhost"
        assert main_status["running"] is True
        assert "unix_timestamp" in main_status
        assert "version" in main_status
        assert "cmdline" in main_status

    def test_omits_workers_key_by_default(self):
        """A daemon with no worker pool must not report a (misleading) workers key at all."""
        daemon = _FakeDaemon()

        status = daemon.get_status()

        assert "workers" not in status

    def test_merges_get_extra_status_into_main(self):
        """A subclass's daemon-specific status keys are merged into "main", alongside the common keys."""
        daemon = _FakeDaemon()
        daemon.get_extra_status = MagicMock(return_value={"custom_key": 42})

        status = daemon.get_status()

        assert status["main"]["custom_key"] == 42

    def test_includes_workers_key_when_worker_status_present(self):
        """A daemon with a worker pool reports it under a top-level workers key."""
        daemon = _FakeDaemon()
        daemon.get_worker_status = MagicMock(return_value=[{"name": "worker1"}])

        status = daemon.get_status()

        assert status["workers"] == [{"name": "worker1"}]


class TestHealthLoop:
    def test_sends_multicast_with_the_health_config_and_exits_when_stopped(self):
        """One health iteration sends one multicast packet using the configured health settings."""
        daemon = _FakeDaemon()
        daemon.running = True
        daemon.cfg_health_multicast_ip = "224.0.0.1"
        daemon.cfg_health_multicast_port = 1234
        daemon.cfg_health_multicast_hops = 1
        daemon.health_multicast_interface_ip = "127.0.0.1"

        before_health_send_mock = MagicMock()
        daemon.before_health_send = before_health_send_mock

        def fake_sleep(_seconds):
            daemon.running = False

        with (
            patch("mwax_mover.processors.daemon.send_multicast") as mock_send_multicast,
            patch.object(daemon, "sleep", side_effect=fake_sleep),
        ):
            daemon.health_loop()

        before_health_send_mock.assert_called_once()
        mock_send_multicast.assert_called_once()
        call_args = mock_send_multicast.call_args[0]
        assert call_args[0] == "127.0.0.1"
        assert call_args[1] == "224.0.0.1"
        assert call_args[2] == 1234
        assert call_args[4] == 1

    def test_survives_a_send_multicast_exception(self, caplog):
        """A multicast send failure is logged and the loop continues, not raises."""
        daemon = _FakeDaemon()
        daemon.running = True

        def fake_sleep(_seconds):
            daemon.running = False

        with (
            patch("mwax_mover.processors.daemon.send_multicast", side_effect=RuntimeError("network down")),
            patch.object(daemon, "sleep", side_effect=fake_sleep),
            caplog.at_level("ERROR"),
        ):
            daemon.health_loop()

        assert "Failed to send health information" in caplog.text


class TestInitialiseFromCommandLine:
    def test_parses_cfg_argument_and_calls_initialise(self, monkeypatch):
        """The -c/--cfg argument is parsed and passed straight to initialise()."""
        daemon = _FakeDaemon()
        monkeypatch.setattr(sys, "argv", ["prog", "-c", "/some/config.cfg"])

        daemon.initialise_from_command_line()

        assert daemon.initialise_calls == [("/some/config.cfg", (), {})]

    def test_requires_the_cfg_argument(self, monkeypatch):
        """Omitting -c/--cfg is a command-line error, not a call to initialise()."""
        daemon = _FakeDaemon()
        monkeypatch.setattr(sys, "argv", ["prog"])

        with pytest.raises(SystemExit):
            daemon.initialise_from_command_line()


class TestDefaultHooks:
    def test_all_default_to_their_documented_no_ops(self):
        """Every hook's base implementation is a no-op, ready for a subclass to override."""
        daemon = _FakeDaemon()

        assert daemon.before_health_send() is None
        assert daemon.during_sleep_interval() is None
        assert daemon.get_worker_status() is None
        assert daemon.shutdown_log_detail() == ""
