from alchemist.calculations import Calculation, SessionFixedCalculation
from alchemist.registry import register_calc


@register_calc('rsi')
class Rsi(Calculation):

    def setup(self, period: int) -> None:
        self.period = period
        self.prev: float | None = None
        self.count = 0
        self.avg_gain = 0.0
        self.avg_loss = 0.0

    def step(self, src: float) -> float | None:
        if self.prev is None:
            self.prev = src
            return None
        change, self.prev = src - self.prev, src
        gain, loss = max(change, 0.0), max(-change, 0.0)

        self.count += 1
        weight = 1.0 / min(self.count, self.period)
        self.avg_gain += weight * (gain - self.avg_gain)
        self.avg_loss += weight * (loss - self.avg_loss)

        if self.count < self.period:
            return None
        if self.avg_loss == 0.0:
            return 100.0
        return 100.0 - 100.0 / (1.0 + self.avg_gain / self.avg_loss)


@register_calc('daily_atr')
class DailyAtr(SessionFixedCalculation):

    def setup(self, period: int) -> None:
        self.period = period
        self.atr: float | None = None

    def daily_months(self) -> int:
        return self.period // 20 + 2

    def warmup(self, daily_bars) -> None:
        rows = list(daily_bars)[-(self.period + 1):]
        if len(rows) <= self.period:
            return
        true_ranges = [
            max(cur["high"], prev["close"]) - min(cur["low"], prev["close"])
            for prev, cur in zip(rows, rows[1:])
        ]
        self.atr = sum(true_ranges) / self.period

    def value(self) -> float | None:
        return self.atr

    def snapshot_state(self) -> dict | None:
        return None if self.atr is None else {"atr": self.atr}

    def restore_state(self, state: dict) -> None:
        self.atr = state["atr"]
