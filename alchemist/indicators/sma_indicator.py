from alchemist.indicators.base_indicator import BaseIndicator


class SmaIndicator(BaseIndicator):

    DATA_LINES = (
        'sma',
    )

    def __init__(self, close_line, min_period=1, ts_line=None):
        super().__init__(close_line, min_period=min_period, ts_line=ts_line)
        self.close_line = self.data_0
        self._sum = 0.0

    def next(self):
        val = self.close_line[-1]
        if len(self.sma) == 0:
            # first value: seed rolling sum with window
            self._sum = sum(self.close_line[-self.min_period:])
        else:
            # rolling update: add new, drop value that rolled out
            self._sum += val
            self._sum -= self.close_line[-self.min_period - 1]

        self.sma.append(self._sum / self.min_period)
            
