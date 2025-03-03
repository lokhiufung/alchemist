from abc import abstractmethod
from collections import deque

from alchemist.datas.common import Tick, Bar
from alchemist.datas.frequency import Frequency


class BaseData:
    def __init__(self, max_len: int, freq: str, index=None):
        self.index = index
        self.max_len = max_len
        self.frequency = Frequency(freq=freq)
        self.freq = self.frequency.freq
        self.data = deque(maxlen=self.max_len)
        self.total_len = 0
        self.listeners = []
        self.unit, self.resolution = self.frequency.unit, self.frequency.resolution

    def _step(self):
        # increase the the total len of the data by 1
        self.total_len += 1
        
    def __len__(self):
        return self.total_len
    
    def __getitem__(self, index):
        return list(self.data)[index]  # need to reverse it

    def on_tick_update(self, tick: Tick):
        pass

    def on_bar_update(self, bar: Bar):
        pass

    def add_listener(self, listener):
        self.listeners.append(listener)

    def push(self):
        for listener in self.listeners:
            listener.push()
    
    def __str__(self):
        return f"Data(freq='{self.freq}', max_len={self.max_len}, total_len={self.total_len})"

    def __repr__(self):
        return (f"Data(index={self.index}, freq='{self.freq}', "
                f"max_len={self.max_len}, total_len={self.total_len})")