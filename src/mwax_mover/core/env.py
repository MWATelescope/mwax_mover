"""Local host/environment introspection helpers.

get_hostname() returns the machine's short (non-FQDN) hostname;
running_under_pytest() detects whether the current process is a test run;
_available_memory_bytes() is a best-effort estimate of memory available to
this process (checking cgroup limits before node-wide free memory), used
by calvin.plots.gains to budget concurrent page-rendering workers;
is_ram_backed() says whether a directory is on a RAM-backed file system (e.g. tmpfs).
"""

import logging
import os
import socket
import sys

logger = logging.getLogger(__name__)

# File system types that keep their data in RAM, so files in them use memory.
RAM_BACKED_FILESYSTEM_TYPES = frozenset({"tmpfs", "ramfs"})

# Where Linux lists the mounted file systems.
PROC_MOUNTS_PATH = "/proc/mounts"


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


def is_ram_backed(path: str, mounts_path: str = PROC_MOUNTS_PATH) -> bool:
    """Say whether a directory is on a RAM-backed file system (tmpfs or ramfs).

    Files in such a directory use RAM, which then cannot be used by programs such as Birli
    and WSClean. This reads the Linux mount table. The mount with the longest mount point
    that contains the directory decides.

    Args:
        path: The directory (or file). It does not have to exist: the closest existing parent is used.
        mounts_path: The mount table. Only a test needs to change this.

    Returns:
        True if the directory is on tmpfs or ramfs. False if it is not, or if the mount table cannot
        be read (for example not on Linux), because a wrong "yes" would take memory away for nothing.
    """
    # Follow symbolic links in the part of the path that exists, and keep the rest as it is.
    existing = os.path.abspath(path)
    missing_tail: list[str] = []
    while not os.path.exists(existing) and os.path.dirname(existing) != existing:
        missing_tail.insert(0, os.path.basename(existing))
        existing = os.path.dirname(existing)
    real_path = os.path.join(os.path.realpath(existing), *missing_tail)

    try:
        with open(mounts_path, encoding="utf-8") as mounts_file:
            mount_lines = mounts_file.readlines()
    except OSError:
        return False

    best_mount_point = ""
    best_fs_type = ""
    for line in mount_lines:
        fields = line.split()
        if len(fields) < 3:  # noqa: PLR2004 - a mount table line is "device mount_point fs_type options ..."
            continue
        mount_point = fields[1].replace("\\040", " ")
        if (real_path == mount_point or real_path.startswith(mount_point.rstrip("/") + "/")) and len(
            mount_point
        ) >= len(best_mount_point):
            best_mount_point = mount_point
            best_fs_type = fields[2]

    return best_fs_type in RAM_BACKED_FILESYSTEM_TYPES
