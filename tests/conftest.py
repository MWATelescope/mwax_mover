"""
tests/conftest.py

Session-level pytest configuration.

Suppresses DEBUG/INFO noise from third-party libraries so that only
mwax_mover log output is visible at DEBUG level during test runs.
"""

import logging


# ---------------------------------------------------------------------------
# Suppress noisy third-party loggers
# ---------------------------------------------------------------------------

_NOISY_LOGGERS = [
    "PIL",
    "matplotlib",
    "matplotlib.font_manager",
    "scipy",
    "pandas",
    "urllib3",
    "asyncio",
    "numexpr",
]


def pytest_configure(config):
    """Raise the log level for noisy third-party libraries to WARNING.

    This runs before any tests are collected, ensuring the levels are set
    before any module-level logging occurs.
    """
    for name in _NOISY_LOGGERS:
        logging.getLogger(name).setLevel(logging.WARNING)
