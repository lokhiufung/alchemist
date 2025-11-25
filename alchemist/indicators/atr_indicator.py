from alchemist.indicators.base_indicator import BaseIndicator
from collections import deque


class ATRIndicator(BaseIndicator):
    DATA_LINES = (
        'atr',
    )

    def __init__(self, high_line, low_line, close_line, min_period=14, ts_line=None):
        super().__init__(high_line, low_line, close_line, min_period=min_period, ts_line=ts_line)
        self.high_line = self.data_0
        self.low_line = self.data_1
        self.close_line = self.data_2
        self._tr_window = deque(maxlen=min_period)
        self._tr_sum = 0.0

    def next(self):
        if len(self.close_line) < 2:
            self.atr.append(float('nan'))
            return

        high = self.high_line[-1]
        low = self.low_line[-1]
        prev_close = self.close_line[-2]

        tr = max(
            high - low,
            abs(high - prev_close),
            abs(low - prev_close)
        )
        if len(self._tr_window) == self._tr_window.maxlen:
            self._tr_sum -= self._tr_window[0]
        self._tr_window.append(tr)
        self._tr_sum += tr

        self.atr.append(self._tr_sum / len(self._tr_window))
