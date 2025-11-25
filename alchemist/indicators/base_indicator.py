import typing
from abc import ABC
from datetime import datetime

from alchemist.data_line import DataLine
from alchemist.datas.frequency import Frequency


class BaseIndicator(ABC):

    DATA_LINES: typing.Tuple[str, ...] = ()

    def __init__(
        self,
        *data_lines: DataLine,
        min_period: int = 1,
        params: dict = None,
        ts_line: typing.Optional[DataLine] = None,
        update_mode: str = 'bar',
        rollover_frequency: typing.Optional[str] = None,
    ):
        """
        All inputs must be DataLine instances. For bar-driven indicators, pass the
        associated ts DataLine via `ts_line` to drive updates once per bar.
        """
        self.min_period = min_period
        self.params = {} if params is None else params
        self.data_lines = data_lines
        self.ts_line = ts_line
        self.update_mode = update_mode  # 'bar' (default) or 'time'
        self.rollover_frequency = Frequency(freq=rollover_frequency) if rollover_frequency else None

        self._last_seen = {}
        self._period_start = None

        self._initialize_data_lines()
        self._initialize_listeners()

    def _initialize_listeners(self):
        # attach listeners only to the driver line to avoid multi-triggers per bar
        driver_lines = [self.ts_line] if self.ts_line is not None else list(self.data_lines)
        for driver_line in driver_lines:
            if driver_line is None:
                raise ValueError('Driver DataLine cannot be None')
            driver_line.add_listener(listener=self)
        # keep handy references for subclass readability
        for i, line in enumerate(self.data_lines):
            setattr(self, f'data_{i}', line)

    def _initialize_data_lines(self):
        for data_line_name in self.DATA_LINES:
            setattr(self, data_line_name, DataLine(name=data_line_name))

    def push(self):
        if self.is_ready():
            self.advance()

    def _should_rollover(self, ts: datetime) -> bool:
        if self.rollover_frequency is None:
            return False
        if self._period_start is None:
            self._period_start = self.rollover_frequency.to_start_index(ts)
            return False
        return self.rollover_frequency.to_start_index(ts) != self._period_start

    def is_ready(self):
        # require minimum period of values
        for line in self.data_lines:
            if len(line) < self.min_period:
                return False

        # time-driven indicators must have a ts line
        if self.update_mode == 'time' and self.ts_line is None:
            return False

        # bar-driven with ts_line: ensure new timestamp and optional sync
        if self.ts_line is not None:
            if len(self.ts_line) == 0:
                return False
            ts = self.ts_line[-1]
            if self._last_seen.get('ts') == ts:
                return False
        else:
            # driven by value lines; ensure at least one advanced
            advanced = False
            for i, line in enumerate(self.data_lines):
                last = self._last_seen.get(i, -1)
                if len(line) != last:
                    advanced = True
                    break
            if not advanced:
                return False

        return True

    def next(self):
        """Subclasses implement calculation logic."""
        pass

    def reset_state(self):
        """Hook for time-based indicators to clear accumulators."""
        ...

    def advance(self):
        ts = self.ts_line[-1] if self.ts_line is not None else None

        if self.update_mode == 'time' and ts is not None and self._should_rollover(ts):
            self.reset_state()
            self._period_start = self.rollover_frequency.to_start_index(ts) if self.rollover_frequency else None

        # Capture lengths before next
        lengths_before = {name: len(getattr(self, name)) for name in self.DATA_LINES}

        self.next()

        # ensure outputs advance once per call
        for data_line_name in self.DATA_LINES:
            data_line = getattr(self, data_line_name)
            if len(data_line) == lengths_before[data_line_name]:
                data_line.append(float('nan'))

        # Update last_seen
        if ts is not None:
            self._last_seen['ts'] = ts
        for i, line in enumerate(self.data_lines):
            self._last_seen[i] = len(line)
    
