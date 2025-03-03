from datetime import datetime

from alchemist.datas.base_data import BaseData
from alchemist.datas.common import Bar


class BarData(BaseData):

    def __init__(self, max_len: int, freq: str, index=None):
        super().__init__(max_len, freq, index)
        self.current_bar = None
        self.last_ts = None

    def on_bar_update(self, ts: datetime, open_: float, high: float, low: float, close: float, volume: float):
        bar = Bar(ts, open_, high, low, close, volume) # create a new bar
        self.data.append(bar)
        self._step()
        self.push()  # push the last bar to listeners
    
