import numpy as np

from alchemist.indicators.base_indicator import BaseIndicator
from alchemist.data_line import DataLine


class RsiIndicator(BaseIndicator):
    DATA_LINES = (
        'rsi',
    )
    def next(self):
        if len(self.time_bar) >= self.period + 1:
            # avg_loss and avg_gain are semi-positive
            avg_gain = sum([(bar_2.close - bar_1.close) / bar_1.close if bar_2.close - bar_1.close > 0 else 0 for bar_1, bar_2 in zip(self.data_0[self.min_period:], self.data_0[-(self.min_period + 1):-1])]) / self.min_period
            avg_loss = sum([abs(bar_2.close - bar_1.close) / bar_1.close if bar_2.close - bar_1.close < 0 else 0 for bar_1, bar_2 in zip(self.data_0[self.min_period:], self.data_0[-(self.min_period + 1):-1])]) / self.min_period
            rs = avg_gain / (avg_loss + 1e-10)
            rsi = 100 - (100 / (1 + rs))
            self.rsi.append(rsi)
