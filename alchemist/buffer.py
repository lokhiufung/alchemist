from collections import defaultdict
from typing import List, Any


class Buffer:
    def __init__(self):
        """
        Initialize a Buffer object to store data streams indexed by their names.
        """
        self._buffers = defaultdict(list)

    def add(self, name: str, data: Any):
        """
        Add data to the buffer under a specific stream name.

        :param name: Name of the buffer stream.
        :param data: Data to add to the buffer.
        """
        self._buffers[name].append(data)

    def get(self, name: str) -> List[Any]:
        """
        Retrieve all data from the buffer for a specific stream name without clearing it.

        :param name: Name of the buffer stream.
        :return: A list of all data in the specified buffer stream.
        """
        return self._buffers[name]

    def pop(self, name: str) -> List[Any]:
        """
        Retrieve and clear all data from the buffer for a specific stream name.

        :param name: Name of the buffer stream.
        :return: A list of all data that was in the specified buffer stream.
        """
        data = self._buffers[name]
        self._buffers[name] = []
        return data

    def clear(self, name: str):
        """
        Clear all data from the buffer for a specific stream name.

        :param name: Name of the buffer stream.
        """
        self._buffers[name] = []

    def clear_all(self):
        """
        Clear all data from all buffer streams.
        """
        self._buffers.clear()

    def has_data(self, name: str) -> bool:
        """
        Check if a buffer stream has any data.

        :param name: Name of the buffer stream.
        :return: True if the buffer stream contains data, otherwise False.
        """
        return len(self._buffers[name]) > 0