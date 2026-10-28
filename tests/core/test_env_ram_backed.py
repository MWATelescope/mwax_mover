"""Tests for core.env.is_ram_backed()."""

from mwax_mover.core.env import is_ram_backed

MOUNTS = """\
sysfs /sys sysfs rw,nosuid 0 0
/dev/sda1 / ext4 rw,relatime 0 0
tmpfs /tmp tmpfs rw,nosuid,nodev 0 0
/dev/sdb1 /tmp/disk ext4 rw 0 0
ramfs /ram ramfs rw 0 0
/dev/sdc1 /data ext4 rw 0 0
"""


def _mounts(tmp_path) -> str:
    path = tmp_path / "mounts"
    path.write_text(MOUNTS)
    return str(path)


def test_a_directory_on_tmpfs_is_ram_backed(tmp_path):
    assert is_ram_backed("/tmp/calvin/jobs", _mounts(tmp_path)) is True
    assert is_ram_backed("/tmp", _mounts(tmp_path)) is True


def test_a_directory_on_ramfs_is_ram_backed(tmp_path):
    assert is_ram_backed("/ram/x", _mounts(tmp_path)) is True


def test_a_directory_on_a_disk_is_not_ram_backed(tmp_path):
    assert is_ram_backed("/data/calvin", _mounts(tmp_path)) is False


def test_the_longest_matching_mount_point_decides(tmp_path):
    # /tmp/disk is a disk mounted inside the tmpfs at /tmp
    assert is_ram_backed("/tmp/disk/jobs", _mounts(tmp_path)) is False


def test_a_mount_point_that_only_starts_with_the_same_letters_does_not_match(tmp_path):
    assert is_ram_backed("/tmpfoo/x", _mounts(tmp_path)) is False


def test_an_unreadable_mount_table_means_not_ram_backed(tmp_path):
    assert is_ram_backed("/tmp/x", str(tmp_path / "does_not_exist")) is False


def test_a_directory_that_does_not_exist_uses_its_nearest_parent(tmp_path):
    assert is_ram_backed("/tmp/does/not/exist/yet", _mounts(tmp_path)) is True
