"""
Class representing the data item within a PriorityQueue.
The comparison operators ignore the path and just compare the filename.
This allows us to have a PriorityQueue which sorts by the filename not
the full filepath.
E.g.
without this class:
q = PriorityQueue()
q.put(1, 'path1/file1.dat')
q.put(1, 'path2/file2.dat')
q.put(1, 'path1/file3.dat')
print(q.get()[1])
    path1/file1.dat
print(q.get()[1])
    path1/file3.dat
print(q.get()[1])
    path2/file2.dat

But correct order should be:
print(q.get()[1])
    path1/file1.dat
print(q.get()[1])
    path2/file2.dat
print(q.get()[1])
    path1/file3.dat
"""

import os


class MWAXPriorityQueueData:
    """
    Use an instance of this class where you normally specify
    data when interacting with a PriorityQueue
    """

    def __init__(self, full_filename: str):
        """Initialize a priority queue data item with a full file path.

        Args:
            full_filename: The full path and filename of the file.
        """
        self.value: str = full_filename

    def __repr__(self):
        """Return the official string representation of the object.

        Returns:
            The full filename/path.
        """
        return self.value

    def __str__(self):
        """Return the string representation of the object.

        Returns:
            The full filename/path.
        """
        return self.value

    def __f(self, comparison_value):
        # The sort key is the filename (without the path)
        return os.path.split(comparison_value)[1]

    def __lt__(self, obj):
        """Check if this object is less than another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if this object's filename is less than obj's filename.
        """
        return self.__f(self.value) < self.__f(obj.value)

    def __le__(self, obj):
        """Check if this object is less than or equal to another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if this object's filename is less than or equal to obj's filename.
        """
        return self.__f(self.value) <= self.__f(obj.value)

    def __eq__(self, obj):
        """Check if this object is equal to another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if both objects have the same filename.
        """
        return self.__f(self.value) == self.__f(obj.value)

    def __ne__(self, obj):
        """Check if this object is not equal to another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if the objects have different filenames.
        """
        return self.__f(self.value) != self.__f(obj.value)

    def __gt__(self, obj):
        """Check if this object is greater than another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if this object's filename is greater than obj's filename.
        """
        return self.__f(self.value) > self.__f(obj.value)

    def __ge__(self, obj):
        """Check if this object is greater than or equal to another (by filename only).

        Args:
            obj: Another MWAXPriorityQueueData instance to compare.

        Returns:
            True if this object's filename is greater than or equal to obj's filename.
        """
        return self.__f(self.value) >= self.__f(obj.value)
