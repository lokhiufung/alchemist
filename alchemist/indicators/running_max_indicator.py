from typing import Optional

from alchemist.indicators.base_indicator import BaseIndicator


class DailyRunningMaxIndicator(BaseIndicator):
    DATA_LINES = (
        'daily_max',
    )

    def __init__(
        self,
        value_line,
        ts_line,
        start_hour=None,
        start_minute=0,
    ):
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
        self.start_hour = start_hour
        self.start_minute = start_minute
        self.reset_state()

    def reset_state(self, active=None):
        self._current_max: Optional[float] = None
        if active is None:
            self._active = self.start_hour is None
        else:
            self._active = active

    def _maybe_start_new_session(self, ts):
        if self.start_hour is None:
            return
        if ts.hour == self.start_hour and ts.minute == self.start_minute:
            self.reset_state(active=True)

    def next(self):
        ts = self.ts_line[-1]
        if self._current_day != ts.date():
            self.reset_state(active=self.start_hour is None)
            self._current_day = ts.date()

        self._maybe_start_new_session(ts)

        if not self._active:
            self.daily_max.append(float('nan'))
            return

        val = self.value_line[-1]
        if self._current_max is None or val > self._current_max:
            self._current_max = val
        self.daily_max.append(self._current_max)
