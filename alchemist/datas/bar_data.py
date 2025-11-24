from datetime import datetime

from alchemist.datas.base_data import BaseData
from alchemist.datas.common import Bar


class BarData(BaseData):

    DATA_LINES = (
        "ts",
        "open",
        "high",
        "low",
        "close",
        "volume",
    )

    def on_bar_update(self, ts: datetime, open_: float, high: float, low: float, close: float, volume: float):
        self._step()

        bar = Bar(ts, open_, high, low, close, volume) # create a new bar

        self.open.append(open_)
        self.high.append(high)
        self.low.append(low)
        self.close.append(close)
        self.volume.append(volume)
        self.ts.append(ts)

        # self.push()  # push the last bar to listeners
    
    def __getitem__(self, index):
        if isinstance(index, slice):
            ts = self.ts[index]
            open_ = self.open[index]
            high = self.high[index]
            low = self.low[index]
            close = self.close[index]
            volume = self.volume[index]
            return [
                Bar(t, o, h, l, c, v) 
                for t, o, h, l, c, v in zip(ts, open_, high, low, close, volume)
            ]
        return Bar(
            self.ts[index],
            self.open[index],
            self.high[index],
            self.low[index],
            self.close[index],
            self.volume[index]
        )
    
