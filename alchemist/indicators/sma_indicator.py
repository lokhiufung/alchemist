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


class DailySmaIndicator(BaseIndicator):
    DATA_LINES = (
        'sma',
    )

    def __init__(self, close_line, period, ts_line):
        if ts_line is None:
            raise ValueError('DailySmaIndicator requires a ts_line for daily rollovers')

        super().__init__(
            close_line,
            min_period=1,  # process every bar and manage readiness manually
            ts_line=ts_line,
            update_mode='time',
            rollover_frequency='1d',
        )
        self.close_line = self.data_0
        self.period = period
        self.reset_state()

    def reset_state(self):
        self._window = []
        self._sum = 0.0

    def next(self):
        val = self.close_line[-1]
        self._window.append(val)
        self._sum += val

        if len(self._window) > self.period:
            self._sum -= self._window.pop(0)

        if len(self._window) < self.period:
            self.sma.append(float('nan'))
            return

        self.sma.append(self._sum / self.period)
            
