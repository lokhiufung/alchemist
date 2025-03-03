import typing
from abc import ABC

from alchemist.datas.bar_data import BarData
from alchemist.data_line import DataLine


class BaseIndicator(ABC):

    DATA_LINES: typing.Tuple[str] = ()

    def __init__(self, *datas, min_period=1, params=None):
        self.min_period = min_period
        self.datas = datas
        # TODO: current only support bar data
        self._check_if_data_supported()
        self.params = {} if params is None else params
        
        self._initialize_listeners()

        self._last_update_ts = None
        self._last_len = 0

        self._initialize_data_lines()

    def _initialize_listeners(self):
        for i, data in enumerate(self.datas):
            setattr(self, f'data_{i}', data)
            data.add_listener(listener=self)  # TODO: kind of weird

    def _initialize_data_lines(self):
        for data_line_name in self.DATA_LINES:
            setattr(self, data_line_name, DataLine(name=data_line_name))

    def _check_if_data_supported(self):
        for data in self.datas:
            if not isinstance(data, BarData):
                raise ValueError(f'Data {data} is not supported by indicator {self.__class__.__name__}')
            
    def push(self):
        if self.is_ready():
            self.advance()

    def is_ready(self):
        # 0. check if all datas have minperiod
        for data in self.datas:
            if len(data) < self.min_period:
                return False
        # 1. check synchronization by ts
        ts = self.datas[0].data[-1].ts
        for data in self.datas:
            if ts != data.data[-1].ts:
                return False
        # 2. check if the datas have ticked
        if self._last_update_ts is None or self._last_update_ts != ts:
            return True
        return False

    def next(self):
        pass

    def advance(self):
        # TODO: ignore cases where there are child indicators
        # for indicator in self.indicators:
        #     indicator.advance()
        self.next()
        # check if all datalines are added a new value
        for data_line_name in self.DATA_LINES:
            data_line = getattr(self, data_line_name)
            if len(data_line) == self._last_len:
                # make sure that the length of data_line is as same as the number of time the next() is called
                data_line.append(float('nan'))
    