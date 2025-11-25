import math

from alchemist.indicators.base_indicator import BaseIndicator


class BollingerbandIndicator(BaseIndicator):
    DATA_LINES = (
        'top',
        'bot',
        'width',
    )

    def __init__(self, close_line, min_period=20, num_std_dev=2, ts_line=None):
        super().__init__(close_line, min_period=min_period, ts_line=ts_line)
        self.close_line = self.data_0
        self.num_std_dev = num_std_dev
        self._sum = 0.0
        self._sum_sq = 0.0

    def next(self):
        price = self.close_line[-1]
        if len(self.top) == 0:
            # first output: seed sums with initial window
            window = self.close_line[-self.min_period:]
            self._sum = sum(window)
            self._sum_sq = sum(p * p for p in window)
        else:
            self._sum += price
            self._sum_sq += price * price
            old = self.close_line[-self.min_period - 1]
            self._sum -= old
            self._sum_sq -= old * old

        mean = self._sum / self.min_period
        variance = (self._sum_sq / self.min_period) - mean * mean
        std_dev = math.sqrt(variance) if variance > 0 else 0.0

        upper_band = mean + self.num_std_dev * std_dev
        lower_band = mean - self.num_std_dev * std_dev

        self.top.append(upper_band)
        self.bot.append(lower_band)
        self.width.append(upper_band - lower_band)
