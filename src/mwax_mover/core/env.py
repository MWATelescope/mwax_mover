"""Local host/environment introspection helpers.

get_hostname() returns the machine's short (non-FQDN) hostname;
running_under_pytest() detects whether the current process is a test run;
_available_memory_bytes() is a best-effort estimate of memory available to
this process (checking cgroup limits before node-wide free memory), used
by calvin.plots.gains to budget concurrent page-rendering workers.
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

    Checks for the presence of the ``PYTEST_CURRENT_TEST`` environment variable
    (set by pytest during the execution of each individual test) or the
    presence of ``pytest`` itself in ``sys.modules`` (true for the whole
    process once pytest has been imported, including collection and fixture
    setup/teardown outside of any test). The second condition is broader and
    fires in most real cases -- ``PYTEST_CURRENT_TEST`` alone would miss
    anything that runs before/after the test body itself.

    Returns:
        True if running inside a pytest session, False otherwise.
    """
    # Returns True if we are running as part of pytest
    return ("PYTEST_CURRENT_TEST" in os.environ) or ("pytest" in sys.modules)


def available_memory_bytes() -> int | None:
    """Best effort estimate of memory this process may actually use.

    Checks the cgroup limit before the node's free memory, because Calvin runs
    these as Slurm jobs: the node can have hundreds of GB free while this job is
    confined to a small fraction of it, and sizing a process pool against the
    node would then be wildly over-optimistic.

    Returns:
        Bytes believed available, or None if nothing could be determined (in
        which case callers should fall back to a conservative fixed value
        rather than assuming plenty).
    """
    candidates: list[int] = []

    # cgroup v2, then v1. "max" (v2) or a sentinel near 2**63 (v1) means
    # unlimited, so it tells us nothing and is skipped.
    for limit_path, usage_path in (
        ("/sys/fs/cgroup/memory.max", "/sys/fs/cgroup/memory.current"),
        ("/sys/fs/cgroup/memory/memory.limit_in_bytes", "/sys/fs/cgroup/memory/memory.usage_in_bytes"),
    ):
        try:
            with open(limit_path, encoding="utf-8") as fd:
                raw = fd.read().strip()
            if raw == "max":
                continue
            limit = int(raw)
            if limit > 2**62:
                continue

            used = 0
            try:
                with open(usage_path, encoding="utf-8") as fd:
                    used = int(fd.read().strip())
            except (OSError, ValueError):
                pass

            candidates.append(max(limit - used, 0))
            break
        except (OSError, ValueError):
            continue

    # MemAvailable already accounts for what is currently in use, so it needs no
    # usage subtraction.
    try:
        with open("/proc/meminfo", encoding="utf-8") as fd:
            for line in fd:
                if line.startswith("MemAvailable:"):
                    candidates.append(int(line.split()[1]) * 1024)
                    break
    except (OSError, ValueError, IndexError):
        pass

    if not candidates:
        return None

    # The tightest constraint wins: being inside a small cgroup on a big node is
    # exactly the case that has to be respected.
    return min(candidates)
