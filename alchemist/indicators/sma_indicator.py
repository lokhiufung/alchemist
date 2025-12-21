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

    def __init__(
        self,
        close_line,
        period=None,
        ts_line=None,
        min_period=None,
        start_hour=None,
        start_minute=0,
    ):
        if ts_line is None:
            raise ValueError('DailySmaIndicator requires a ts_line for daily rollovers')

        if period is None:
            period = min_period
        if period is None:
            raise ValueError('DailySmaIndicator requires `period` or `min_period`')

        super().__init__(
            close_line,
            min_period=1,  # process every bar and manage readiness manually
            ts_line=ts_line,
            update_mode='time',
            rollover_frequency='1d',
        )
        self.close_line = self.data_0
        self.period = period
        self.start_hour = start_hour
        self.start_minute = start_minute
        self.reset_state()

    def reset_state(self, active=None):
        self._window = []
        self._sum = 0.0
        if active is None:
            self._active = self.start_hour is None
        else:
            self._active = active

    def _maybe_start_new_session(self, ts):
        if self.start_hour is None:
            return
        if ts.hour == self.start_hour and ts.minute == self.start_minute:
            # reset and start accumulating for the new session
            self.reset_state(active=True)

    def next(self):
        ts = self.ts_line[-1]
        self._maybe_start_new_session(ts)

        if not self._active:
            self.sma.append(float('nan'))
            return

        val = self.close_line[-1]
        self._window.append(val)
        self._sum += val

        if len(self._window) > self.period:
            self._sum -= self._window.pop(0)

        if len(self._window) < self.period:
            self.sma.append(float('nan'))
            return

        self.sma.append(self._sum / self.period)
            
