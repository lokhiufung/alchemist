from datetime import datetime

from alchemist.datas.common import Bar
from alchemist.datas.frequency import Frequency


class TimeBarResampler:
    def __init__(self, frequency: Frequency):
        self.frequency = frequency
        self.unit, self.resolution = self.frequency.unit, self.frequency.resolution
        self.current_bar = None
        self.last_ts = None
        self._buffer = []

    def create(self, ts, open_, high, low, close, volume):
        self.current_bar = Bar(ts, open_, high, low, close, volume) # create a new bar
    
    def update(self, ts, open_, high, low, close, volume):
        self.current_bar.update(ts, open_, high, low, close, volume) 

    def flush(self):
        bar = None
        try:
            bar = self._buffer.pop()
        except IndexError:
            ...
        finally:
            return bar
    
    def reset_current_bar(self):
        self.current_bar = None

    def to_start_index(self, ts: datetime):
        if self.resolution == 's':
            # Calculate the seconds-based start index
            second = (ts.second // self.unit) * self.unit
            return ts.replace(second=second, microsecond=0)
        elif self.resolution == 'm':
            # Calculate the minutes-based start index
            minute = (ts.minute // self.unit) * self.unit
            return ts.replace(minute=minute, second=0, microsecond=0)
        elif self.resolution == 'h':
            # Calculate the hours-based start index
            hour = (ts.hour // self.unit) * self.unit
            return ts.replace(hour=hour, minute=0, second=0, microsecond=0)
        else:
            raise ValueError(f'Invalid resolution: {self.resolution}')
    
    def is_new_bar_from_tick(self, ts):
        return self.to_start_index(ts) != self.last_ts
    
    def is_close_bar_from_bar(self, ts: datetime, frequency: Frequency):
        """
        Determines if the given timestamp (ts) signals the end of the current bar 
        based on the bar's resolution and unit.

        **Bar Indexing Convention**:
        - **Start Convention**: The index represents the start of the bar's time window.
        For example, the bar indexed at `09:30:00` covers the interval from `09:30:00` to `09:30:59`.
        - **End Convention**: The index represents the end of the bar's time window.
        For example, the bar indexed at `09:30:00` covers the interval from `09:29:00` to `09:29:59`.

        **Implementation Note**:
        - For resampling to larger timeframes, such as `5m`, the method accounts for
        the range of `0-4` minutes within a 5-minute bar.
        - The "+1" adjustment ensures that the transition to a new bar is correctly detected.
        - **Unit Divisor Restriction**: Only unit divisors of 10 are allowed for the `unit` parameter 
        (e.g., 1, 2, 5, 10) to ensure proper alignment of bar boundaries.
        - The method uses the normalized value of the frequency in seconds to compare timestamps.

        **Behavior**:
        - It calculates if the sum of the timestamp (in seconds) and the frequency's normalized value
        is divisible by the current frequency's normalized value without a remainder.

        :param ts: The timestamp to evaluate (as a `datetime` object).
        :param frequency: The frequency to check against (as a `Frequency` object).
        :return: True if the timestamp signals the end of the current bar; False otherwise.
        """
        # just check if the ts is a multiple of the unit
        return (int(ts.timestamp()) + frequency.normalized_value) % self.frequency.normalized_value  == 0

    def on_bar_update(self, freq: str, ts: int, open_, high, low, close, volume):
        frequency = Frequency(freq=freq)
        # Case 1: the coming frequency is the same as the resampling target frequency
        if frequency == self.frequency:
            self.create(
                ts=self.to_start_index(ts),
                open_=open_,
                high=high,
                low=low,
                close=close,
                volume=volume,
            )
            self._buffer.append(self.current_bar)
        else:
            if self.last_ts is None or self.current_bar is None:
                self.create(
                    ts=self.to_start_index(ts),
                    open_=open_,
                    high=high,
                    low=low,
                    close=close,
                    volume=volume,
                )
            elif self.is_close_bar_from_bar(ts=ts, frequency=frequency):
                self.update(
                    ts=self.current_bar.ts,
                    open_=self.current_bar.open,
                    high=max(self.current_bar.high, high),
                    low=min(self.current_bar.low, low),
                    close=close,
                    volume=self.current_bar.volume + volume,
                )
                self._buffer.append(self.current_bar)
                self.reset_current_bar()
            else:
                self.update(
                    ts=self.current_bar.ts,
                    open_=self.current_bar.open,
                    high=max(self.current_bar.high, high),
                    low=min(self.current_bar.low, low),
                    close=close,
                    volume=self.current_bar.volume + volume,
                )
        # update last_ts
        self.last_ts = self.to_start_index(ts)
    
    def on_tick_update(self, freq: str, ts: datetime, price: float, size: float):
        if self.last_ts is None:
            self.create(
                ts=self.to_start_index(ts),
                open_=price,
                high=price,
                low=price,
                close=price,
                volume=size,
            )
        elif self.is_new_bar_from_tick(ts=ts):
            self._buffer.append(self.current_bar)
            self.create(
                ts=self.to_start_index(ts),
                open_=price,
                high=price,
                low=price,
                close=price,
                volume=size,
            )
        else:
            self.update(
                ts=self.current_bar.ts,
                open_=self.current_bar.open,
                high=max(self.current_bar.high, price),
                low=min(self.current_bar.low, price),
                close=price,
                volume=self.current_bar.volume + size,
            )
        # update last_ts
        self.last_ts = self.to_start_index(ts)
    