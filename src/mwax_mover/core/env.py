"""Local host/environment introspection helpers.

get_hostname() returns the machine's short (non-FQDN) hostname;
running_under_pytest() detects whether the current process is a test run.
"""

import logging
import os
import socket
import sys

logger = logging.getLogger(__name__)


def get_hostname() -> str:
    """
    Return the short hostname of the running machine in lowercase.

    Any domain suffix (everything after the first ``.``) is stripped so that
    the fully-qualified domain name is never returned.

    Returns:
        The lowercase short hostname string (e.g. ``'mwax01'``).
    """
    hostname = socket.gethostname()

    # ensure we remove anything after a . in case we got the fqdn
    split_hostname = hostname.split(".")[0]

    return split_hostname.lower()


def running_under_pytest() -> bool:
    """
    Detect whether the current process is running under pytest.

    Checks for the presence of the ``PYTEST_CURRENT_TEST`` environment variable,
    which pytest sets automatically during test execution.

    Returns:
        True if running inside a pytest session, False otherwise.
    """
    # Returns True if we are running as part of pytest
    return ("PYTEST_CURRENT_TEST" in os.environ) or ("pytest" in sys.modules)
