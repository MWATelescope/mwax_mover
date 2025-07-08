"""
Tests for the mwax_priority_queue_data.py module
"""
import queue
from mwax_mover.mwax_priority_queue_data import MWAXPriorityQueueData


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
