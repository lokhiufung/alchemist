from typing import Optional

from alchemist.indicators.base_indicator import BaseIndicator


class DailyRunningMaxIndicator(BaseIndicator):
    DATA_LINES = (
        'daily_max',
    )

    def __init__(self, value_line, ts_line):
        if ts_line is None:
            raise ValueError('DailyRunningMaxIndicator requires a ts_line for daily rollovers')

        super().__init__(
            value_line,
            min_period=1,
            ts_line=ts_line,
            update_mode='time',
            rollover_frequency='1d',
        )
        self.value_line = self.data_0
        self._current_day = None
        self.reset_state()

    def reset_state(self):
        self._current_max: Optional[float] = None

    def next(self):
        ts = self.ts_line[-1]
        if self._current_day != ts.date():
            self.reset_state()
            self._current_day = ts.date()

        val = self.value_line[-1]
        if self._current_max is None or val > self._current_max:
            self._current_max = val
        self.daily_max.append(self._current_max)
