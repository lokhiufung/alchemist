import sys
from collections import deque


class DataLine:
    def __init__(self, name, element_type=float, memory_limit_mb=10):
        self.name = name
        self.element_size = sys.getsizeof(element_type())  # Estimate the size of one element
        self.memory_limit_bytes = memory_limit_mb * 1024 * 1024  # Convert memory limit to bytes
        self.max_len = self.calculate_max_len()  # Set max_len based on memory limit
        self.values = deque(maxlen=self.max_len)
        self.counts = 0  # REMINDER: this counts the total number of values

    def calculate_max_len(self):
        """ Calculate max_len based on memory limit and size of one element """
        return self.memory_limit_bytes // self.element_size

    def __len__(self):
        return self.counts

    def __getitem__(self, index):
        try:
            return list(self.values)[index]
        except IndexError:
            return float('nan')

    def append(self, value):
        self.counts += 1
        self.values.append(float(value))  # make sure the value is a float