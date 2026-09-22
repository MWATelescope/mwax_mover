"""Timing and retry-delay helpers.

Centralises ordinary sleeps, interruptible waits, and Tenacity wait policies
so tests can minimise elapsed wall-clock time without changing production
behaviour.

Thread joins, subprocess timeouts, queue timeouts, and lock/condition waits
are deliberately outside this module because they represent synchronisation
or correctness boundaries rather than discretionary delays.
"""

import time
from typing import Final, Protocol

from tenacity import wait_fixed
from tenacity.wait import wait_base

from mwax_mover.core.env import running_under_pytest


class WaitableEvent(Protocol):
    """Minimal protocol required by interruptible_sleep()."""

    def wait(self, timeout: float | None = None) -> bool:
        """Wait until set or until timeout expires."""


# A non-zero default allows worker threads to yield under pytest and avoids
# turning retry loops into tight CPU loops.
_DEFAULT_PYTEST_MAX_SLEEP_SECONDS: Final[float] = 0.01


def effective_sleep_seconds(
    seconds: float,
    *,
    pytest_max_seconds: float = _DEFAULT_PYTEST_MAX_SLEEP_SECONDS,
) -> float:
    """Return the wall-clock delay appropriate to the current environment.

    Production preserves the requested delay exactly. Under pytest, the delay
    is capped so polling and retry loops still yield without slowing the test
    suite substantially.

    Args:
        seconds: Requested delay in seconds. Must not be negative.
        pytest_max_seconds: Maximum delay used under pytest. Must not be
            negative.

    Returns:
        The requested duration in production, or the capped duration under
        pytest.

    Raises:
        ValueError: If either duration is negative.
    """
    if seconds < 0:
        raise ValueError(f"seconds must be >= 0, got {seconds}")

    if pytest_max_seconds < 0:
        raise ValueError(f"pytest_max_seconds must be >= 0, got {pytest_max_seconds}")

    if running_under_pytest():
        return min(seconds, pytest_max_seconds)

    return seconds


def sleep(
    seconds: float,
    *,
    pytest_max_seconds: float = _DEFAULT_PYTEST_MAX_SLEEP_SECONDS,
) -> None:
    """Sleep, using a short capped delay under pytest.

    This is intended for discretionary delays such as polling, startup
    settling, and retry backoff. It is not a replacement for Event.wait(),
    Thread.join(), subprocess timeouts, or other synchronisation operations.
    """
    delay = effective_sleep_seconds(
        seconds,
        pytest_max_seconds=pytest_max_seconds,
    )

    if delay > 0:
        time.sleep(delay)


def interruptible_sleep(
    stop_event: WaitableEvent,
    seconds: float,
    *,
    pytest_max_seconds: float = _DEFAULT_PYTEST_MAX_SLEEP_SECONDS,
) -> bool:
    """Wait for a duration or until a stop event is set.

    Args:
        stop_event: Event-like object providing wait(timeout).
        seconds: Requested maximum delay in seconds.
        pytest_max_seconds: Maximum delay used under pytest.

    Returns:
        True if the event became set, or False if the effective timeout
        expired.

    This preserves the useful semantics of Event.wait(): production shutdown
    can interrupt a long backoff immediately.
    """
    delay = effective_sleep_seconds(
        seconds,
        pytest_max_seconds=pytest_max_seconds,
    )
    return stop_event.wait(timeout=delay)


def retry_wait_fixed(
    seconds: float,
    *,
    pytest_max_seconds: float = _DEFAULT_PYTEST_MAX_SLEEP_SECONDS,
) -> wait_base:
    """Return a fixed Tenacity wait policy appropriate to the environment."""
    return wait_fixed(
        effective_sleep_seconds(
            seconds,
            pytest_max_seconds=pytest_max_seconds,
        )
    )
