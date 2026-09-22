from threading import Event
from unittest.mock import patch

from mwax_mover.core import timing


def test_interruptible_sleep_caps_timeout_under_pytest():
    event = Event()

    with (
        patch.object(timing, "running_under_pytest", return_value=True),
        patch.object(event, "wait", return_value=False) as mock_wait,
    ):
        result = timing.interruptible_sleep(event, 30)

    assert result is False
    mock_wait.assert_called_once_with(timeout=0.01)


def test_interruptible_sleep_preserves_timeout_in_production():
    event = Event()

    with (
        patch.object(timing, "running_under_pytest", return_value=False),
        patch.object(event, "wait", return_value=False) as mock_wait,
    ):
        result = timing.interruptible_sleep(event, 30)

    assert result is False
    mock_wait.assert_called_once_with(timeout=30)


def test_interruptible_sleep_reports_set_event():
    event = Event()
    event.set()

    result = timing.interruptible_sleep(event, 30)

    assert result is True


def test_interruptible_sleep_does_not_clear_event():
    event = Event()
    event.set()

    timing.interruptible_sleep(event, 30)

    assert event.is_set()
