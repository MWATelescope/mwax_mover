"""
Tests for the mwax_mover.queues.priority_queue_data module
"""

import os
import queue

from tests_common import obs_data_dir

from mwax_mover.queues.priority_queue_data import (
    MWAXPriorityQueueData,
    scan_for_existing_files_and_add_to_priority_queue,
)


def test():
    """
    Check that the sort of items with the same priority
    is by the filename and not filename and path
    """
    test_queue = queue.PriorityQueue()
    test_queue.put((1, MWAXPriorityQueueData("path1/file3.dat")))
    test_queue.put((1, MWAXPriorityQueueData("path1/file1.dat")))
    test_queue.put((1, MWAXPriorityQueueData("path2/file2.dat")))

    assert "path1/file1.dat" == str(test_queue.get()[1])
    assert "path2/file2.dat" == str(test_queue.get()[1])
    assert "path1/file3.dat" == str(test_queue.get()[1])


def test_scan_for_existing_files_and_add_to_priority_queue():
    """Test we can find files and add to a priority queue"""
    queue_target = queue.PriorityQueue()
    watch_dir = obs_data_dir(1244973688)
    pattern = ".fits"
    recursive = False
    metafits_path = watch_dir

    #
    # Run test
    #
    scan_for_existing_files_and_add_to_priority_queue(
        metafits_path,
        watch_dir,
        pattern,
        recursive,
        queue_target,
        ["D0006"],
        ["C001"],
    )

    assert queue_target.qsize() == 2

    # Get first item
    item1 = queue_target.get()

    assert str(item1[1]) == os.path.join(os.getcwd(), os.path.join(watch_dir, "1244973688_metafits.fits"))
    assert item1[0] == 1  # metafits ppd file

    # get second item
    item2 = queue_target.get()

    assert str(item2[1]) == os.path.join(
        os.getcwd(),
        os.path.join(watch_dir, "1244973688_20190619100110_ch114_000.fits"),
    )
    assert item2[0] == 30  # Regular correlator obs
