"""Tests for core.env: running_under_pytest() and available_memory_bytes().

Split out of the former test005_utils.py and test023_calvin_plots.py
(docs/RESTRUCTURE.md test-tree reorg).
"""

from mwax_mover.core.env import available_memory_bytes, running_under_pytest


def test_running_under_pytest():
    assert running_under_pytest()


class TestAvailableMemoryBytes:
    """Tests for memory detection, which must respect a Slurm/cgroup limit."""

    def test_prefers_the_tightest_constraint(self, tmp_path, monkeypatch):
        """A small cgroup on a big node must win over the node's free memory.

        This is the case that matters on calvin: the node can have hundreds of
        GB free while the Slurm job is confined to a fraction of it.
        """
        cgroup_max = tmp_path / "memory.max"
        cgroup_max.write_text("4000000000")  # 4 GB limit
        cgroup_cur = tmp_path / "memory.current"
        cgroup_cur.write_text("1000000000")  # 1 GB already used
        meminfo = tmp_path / "meminfo"
        meminfo.write_text("MemTotal:  500000000 kB\nMemAvailable: 400000000 kB\n")  # ~400 GB free

        real_open = open

        def _open(path, *args, **kwargs):
            mapping = {
                "/sys/fs/cgroup/memory.max": cgroup_max,
                "/sys/fs/cgroup/memory.current": cgroup_cur,
                "/proc/meminfo": meminfo,
            }
            return real_open(mapping.get(str(path), path), *args, **kwargs)

        monkeypatch.setattr("builtins.open", _open)

        # 4 GB limit minus 1 GB in use, not the node's 400 GB
        assert available_memory_bytes() == 3_000_000_000

    def test_unlimited_cgroup_falls_through_to_meminfo(self, tmp_path, monkeypatch):
        """cgroup v2 "max" means no limit, so it must not be treated as a number."""
        cgroup_max = tmp_path / "memory.max"
        cgroup_max.write_text("max")
        meminfo = tmp_path / "meminfo"
        meminfo.write_text("MemAvailable: 8000000 kB\n")

        real_open = open

        def _open(path, *args, **kwargs):
            if str(path) == "/sys/fs/cgroup/memory.max":
                return real_open(cgroup_max, *args, **kwargs)
            if str(path) == "/proc/meminfo":
                return real_open(meminfo, *args, **kwargs)
            raise FileNotFoundError(path)

        monkeypatch.setattr("builtins.open", _open)

        assert available_memory_bytes() == 8_000_000 * 1024

    def test_returns_none_when_nothing_is_readable(self, monkeypatch):
        """Undetectable memory must be reported as such, not guessed as plenty."""

        def _open(path, *args, **kwargs):
            raise FileNotFoundError(path)

        monkeypatch.setattr("builtins.open", _open)

        assert available_memory_bytes() is None
