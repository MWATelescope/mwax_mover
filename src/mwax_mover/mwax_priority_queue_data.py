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
        self.value: str = full_filename

    def __repr__(self):
        return self.value

    def __str__(self):
        return self.value

    def __f(self, comparison_value):
        # The sort key is the filename (without the path)
        return os.path.split(comparison_value)[1]

    def __lt__(self, obj):
        """self < obj."""
        return self.__f(self.value) < self.__f(obj.value)

    def __le__(self, obj):
        """self <= obj."""
        return self.__f(self.value) <= self.__f(obj.value)

    def __eq__(self, obj):
        """self == obj."""
        return self.__f(self.value) == self.__f(obj.value)

    def __ne__(self, obj):
        """self != obj."""
        return self.__f(self.value) != self.__f(obj.value)

    def __gt__(self, obj):
        """self > obj."""
        return self.__f(self.value) > self.__f(obj.value)

    def __ge__(self, obj):
        """self >= obj."""
        return self.__f(self.value) >= self.__f(obj.value)
