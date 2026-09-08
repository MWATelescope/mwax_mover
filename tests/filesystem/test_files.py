"""Tests for filesystem.files: generic file-level operations.

Split out of the former test005_utils.py (docs/RESTRUCTURE.md test-tree
reorg).
"""

import os
import shutil
import tarfile
import time
from pathlib import Path

import pytest

from tests_common import data_path

from mwax_mover.filesystem.files import delete_files_older_than, do_checksum_md5, extract_tar


def test_do_checksum_md5():
    """Tests that we can correctly get the MD5 of a file"""

    filename = os.path.join(
        os.getcwd(),
        data_path("1244973688", "1244973688_20190619100110_ch114_000.fits"),
    )

    numa_node = None
    timeout = 30

    #
    # Run test
    #
    md5sum = do_checksum_md5(filename, numa_node, timeout)

    assert md5sum == "c1024dd2184887bc293cffe07406046f"


def test_delete_files_older_than():
    test_content = "hello word"
    test_path = "/tmp/delete_files_older_than"

    # Remove test dir if it already exists
    if os.path.exists(test_path):
        shutil.rmtree(test_path)
    os.mkdir(test_path)

    # Create some test files which SHOULD be deleted
    filename_1 = os.path.join(test_path, "file_1.txt")
    with open(filename_1, "w") as f:
        f.write(test_content)

    filename_2 = os.path.join(test_path, "file_2.dat")
    with open(filename_2, "w") as f:
        f.write(test_content)

    filename_3 = os.path.join(test_path, "file_3.dat")
    with open(filename_3, "w") as f:
        f.write(test_content)

    # Create some test files which SHOULD NOT be deleted (wrong ext)
    filename_4 = os.path.join(test_path, "file_4.blah")
    with open(filename_4, "w") as f:
        f.write(test_content)

    filename_5 = os.path.join(test_path, "file_5.keep")
    with open(filename_5, "w") as f:
        f.write(test_content)

    time.sleep(10)

    # Create some test files which SHOULD NOT be deleted (not old enough)
    filename_6 = os.path.join(test_path, "file_6.txt")
    with open(filename_6, "w") as f:
        f.write(test_content)

    filename_7 = os.path.join(test_path, "file_7.dat")
    with open(filename_7, "w") as f:
        f.write(test_content)

    # Do test delete
    files_deleted = delete_files_older_than(test_path, 5, [".txt", ".dat"])

    # 3 files should be deleted
    assert len(files_deleted) == 3

    # They should be the first 3 files
    assert not os.path.exists(filename_1)
    assert not os.path.exists(filename_2)
    assert not os.path.exists(filename_3)

    # These should not be deleted
    assert os.path.exists(filename_4)
    assert os.path.exists(filename_5)
    assert os.path.exists(filename_6)
    assert os.path.exists(filename_7)

    # Clean up
    shutil.rmtree(test_path)


def test_extract_tar():
    #
    # 1. tar does not exist
    #
    with pytest.raises(FileNotFoundError):
        extract_tar("some_nonexistant_file", "/tmp")

    #
    # 2. destination does not exist
    #
    with pytest.raises(NotADirectoryError):
        extract_tar(__file__, "/some_nonexistent_path")

    #
    # 3. Successful tar
    #
    # create one first
    hello_world_text = "hello world"
    filename = "hello_world.txt"
    tmp_file: Path = Path(f"/tmp/{filename}")
    tmp_file.write_text(hello_world_text)
    tmp_tar: Path = Path("/tmp/test.tar")

    with tarfile.open(tmp_tar, mode="w") as tf:
        tf.add(tmp_file, arcname=filename)

    # Remove the tmp_file
    tmp_file.unlink(missing_ok=False)

    # now see is we extract it ok
    extract_tar(str(tmp_tar), "/tmp")

    # see if the file exists
    tmp_file: Path = Path(f"/tmp/{filename}")

    assert tmp_file.exists()
    assert hello_world_text == tmp_file.read_text()
