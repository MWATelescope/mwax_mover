"""Tests for filesystem.scan: directory scanning.

Split out of the former test005_utils.py (docs/RESTRUCTURE.md test-tree
reorg).
"""

import os
import queue

from tests_common import obs_data_dir

from mwax_mover.filesystem.scan import scan_directory, scan_for_existing_files_and_add_to_queue


def test_scan_for_existing_files_and_add_to_queue():
    """Test we can find files and add to a queue"""
    queue_target = queue.Queue()
    watch_dir = obs_data_dir(1244973688)
    pattern = ".fits"
    recursive = False

    #
    # Run test
    #
    scan_for_existing_files_and_add_to_queue(watch_dir, pattern, recursive, queue_target)

    assert queue_target.qsize() == 2
    assert queue_target.get() == os.path.join(
        os.getcwd(),
        os.path.join(watch_dir, "1244973688_20190619100110_ch114_000.fits"),
    )
    assert queue_target.get() == os.path.join(os.getcwd(), os.path.join(watch_dir, "1244973688_metafits.fits"))


def test_scan_directory():
    """Tests we can get a list of files in a dir"""
    watch_dir = obs_data_dir(1244973688)
    pattern = ".fits"
    recursive = False

    #
    # Run test
    #
    list_of_files = scan_directory(watch_dir, pattern, recursive, exclude_pattern=None)

    assert len(list_of_files) == 2
    assert (
        os.path.join(
            os.getcwd(),
            os.path.join(watch_dir, "1244973688_20190619100110_ch114_000.fits"),
        )
        in list_of_files
    )
    assert os.path.join(os.getcwd(), os.path.join(watch_dir, "1244973688_metafits.fits")) in list_of_files
