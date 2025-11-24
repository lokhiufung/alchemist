import sys
import itertools
from collections import deque


DEFAULT_MAX_LEN = 60 * 24 * 365 * 10  # 10 years


class DataLine:

    def __init__(self, name, max_len=DEFAULT_MAX_LEN, require_float=float):
        self.name = name
        self.max_len = max_len
        self.require_float = require_float
        self.values = deque(maxlen=self.max_len)
        self.counts = 0  # REMINDER: this counts the total number of values

        self.listeners = []

    def __len__(self):
        return self.counts

    def __getitem__(self, index):
        if isinstance(index, slice):
            start, stop, step = index.indices(len(self.values))
            if step > 0:
                return list(itertools.islice(self.values, start, stop, step))
            else:
                start_r = len(self.values) - 1 - start
                stop_r = len(self.values) - 1 - stop
                step_r = -step
                return list(itertools.islice(reversed(self.values), start_r, stop_r, step_r))
        try:
            return self.values[index]
        except IndexError:
            return float('nan')

    def add_listener(self, listener):
        self.listeners.append(listener)

    def append(self, value):
        self.counts += 1
        if self.require_float:
            value = float(value)
        self.values.append(value)  # make sure the value is a float
        self.push()

    def push(self):
        for listener in self.listeners:
            listener.push()