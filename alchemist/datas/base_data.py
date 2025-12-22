from abc import abstractmethod
from collections import deque
from datetime import datetime

from alchemist.datas.common import Tick, Bar
from alchemist.datas.frequency import Frequency
from alchemist.data_line import DataLine


class BaseData:

    TS_DATA_LINE_NAME = 'ts'
    DATA_LINES: tuple[str] = None

    def __init__(self, max_len: int, freq: str, index=None):
        self.index = index
        self.max_len = max_len
        self.frequency = Frequency(freq=freq)
        self.freq = self.frequency.freq
        self.total_len = 0

        # create data lines
        self._initialize_data_lines()
        
        self.unit, self.resolution = self.frequency.unit, self.frequency.resolution

    def _initialize_data_lines(self):
        for data_line_name in self.DATA_LINES:
            if data_line_name == self.TS_DATA_LINE_NAME:
                data_line = DataLine(name=data_line_name, max_len=self.max_len, require_float=False)
            else:
                data_line = DataLine(name=data_line_name, max_len=self.max_len, require_float=True)
            setattr(self, data_line_name, data_line)
    
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

    def __str__(self):
        return f"Data(freq='{self.freq}', max_len={self.max_len}, total_len={self.total_len})"

    def __repr__(self):
        return (f"Data(index={self.index}, freq='{self.freq}', "
                f"max_len={self.max_len}, total_len={self.total_len})")
    
    def print_latest(self):
        latest_index = -1
        if self.total_len > 0:
            latest_data = {data_line_name: getattr(self, data_line_name)[latest_index] for data_line_name in self.DATA_LINES}
            print(f"Latest Data @ {latest_data.get('ts', 'N/A')}: {latest_data}")