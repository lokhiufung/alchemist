from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from alchemist.calculations.calculation import Calculation
from alchemist.exchanges import ExchangeProfile
from alchemist.registry import register_calc


def require_period(period: int) -> None:
    if isinstance(period, bool) or not isinstance(period, int) or period < 1:
        raise ValueError(f"period must be a positive integer, got {period!r}")


@register_calc("sma")
class Sma(Calculation):
    def setup(self, period: int) -> None:
        require_period(period)
        self.period = period
        self.window: deque[float] = deque(maxlen=period)

    def step(self, close: float) -> float | None:
        self.window.append(close)
        return self.value()

    def warmup(self, rows) -> None:
        self.window = deque((row[0] for row in rows), maxlen=self.period)

    def value(self) -> float | None:
        if len(self.window) < self.period:
            return None
        return sum(self.window) / self.period


@register_calc("ema")
class Ema(Calculation):
    def setup(self, period: int) -> None:
        require_period(period)
        self.alpha = 2.0 / (period + 1)
        self.current: float | None = None

    def step(self, close: float) -> float:
        self.current = close if self.current is None else self.current + self.alpha * (close - self.current)
        return self.current

    def value(self) -> float | None:
        return self.current


@register_calc("atr")
class Atr(Calculation):
    def setup(self, period: int) -> None:
        require_period(period)
        self.period = period
        self.previous_close: float | None = None
        self.true_ranges: list[float] = []
        self.current: float | None = None

    def step(self, high: float, low: float, close: float) -> float | None:
        if self.previous_close is None:
            true_range = high - low
        else:
            true_range = max(high, self.previous_close) - min(low, self.previous_close)
        self.previous_close = close
        if self.current is not None:
            self.current = (self.current * (self.period - 1) + true_range) / self.period
        else:
            self.true_ranges.append(true_range)
            if len(self.true_ranges) == self.period:
                self.current = sum(self.true_ranges) / self.period
        return self.current

    def value(self) -> float | None:
        return self.current


@register_calc("vwap")
class Vwap(Calculation):
    bar_fields = ("end_ts",)

    def setup(self, profile: ExchangeProfile) -> None:
        self.zone = ZoneInfo(profile.tz)
        self.day = None
        self.price_volume = 0.0
        self.volume = 0.0

    def step(self, end_ts: float, price: float, volume: float) -> float | None:
        day = datetime.fromtimestamp(end_ts, self.zone).date()
        if day != self.day:
            self.day, self.price_volume, self.volume = day, 0.0, 0.0
        self.price_volume += price * volume
        self.volume += volume
        return self.value()

    def value(self) -> float | None:
        return self.price_volume / self.volume if self.volume else None


class RollingExtreme(Calculation):
    pick = min

    def setup(self, period: int) -> None:
        require_period(period)
        self.period = period
        self.window: deque[float] = deque(maxlen=period)

    def step(self, price: float) -> float | None:
        self.window.append(price)
        return self.value()

    def value(self) -> float | None:
        return self.pick(self.window) if len(self.window) == self.period else None


@register_calc("lowest")
class Lowest(RollingExtreme):
    pick = min


@register_calc("highest")
class Highest(RollingExtreme):
    pick = max


class SessionBarCount(Calculation):
    bar_fields = ("start_ts", "end_ts")

    def setup(self, session: str, profile: ExchangeProfile) -> None:
        profile.session(session)
        self.session, self.profile = session, profile

    def step(self, start_ts: float, end_ts: float) -> int | None:
        window = self.profile.session_window_at(self.session, datetime.fromtimestamp(start_ts, timezone.utc))
        if window is None:
            return None
        return self.count(window[0].timestamp(), window[1].timestamp(), end_ts)

    def count(self, open_ts: float, close_ts: float, end_ts: float) -> int:
        raise NotImplementedError


@register_calc("session_bars_elapsed")
class SessionBarsElapsed(SessionBarCount):
    def count(self, open_ts: float, close_ts: float, end_ts: float) -> int:
        return round(end_ts - open_ts)


@register_calc("session_bars_left")
class SessionBarsLeft(SessionBarCount):
    def count(self, open_ts: float, close_ts: float, end_ts: float) -> int:
        return round(close_ts - end_ts)
