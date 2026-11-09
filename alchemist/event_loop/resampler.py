from __future__ import annotations

import math


class Resampler:
    def __init__(self, resolution_s: int = 1) -> None:
        self.resolution_s = resolution_s
        self.open_bar: dict | None = None
        self.last_tick_ts: float | None = None
        self.last_emitted_start: float | None = None

    def bucket_start(self, ts: float) -> float:
        return math.floor(ts / self.resolution_s) * self.resolution_s

    def update_from_tick(self, ts: float, price: float, size: float) -> dict | None:
        if self.last_tick_ts is not None and ts < self.last_tick_ts:
            return None
        start = self.bucket_start(ts)
        if self.last_emitted_start is not None and start <= self.last_emitted_start:
            return None
        self.last_tick_ts = ts
        completed = None
        if self.open_bar is not None and start > self.open_bar["start_ts"]:
            completed = self.close_open_bar()
        if self.open_bar is None:
            self.open_bar = {"start_ts": start, "end_ts": start + self.resolution_s, "open": price, "high": price,
                             "low": price, "close": price, "volume": size, "resolution_s": self.resolution_s,
                             "broker_ts": ts}
        else:
            bar = self.open_bar
            bar["high"], bar["low"] = max(bar["high"], price), min(bar["low"], price)
            bar["close"], bar["broker_ts"] = price, ts
            bar["volume"] += size
        return completed

    def close_open_bar(self) -> dict | None:
        bar, self.open_bar = self.open_bar, None
        if bar is not None:
            self.last_emitted_start = bar["start_ts"]
        return bar

    def flush_due(self, now: float, grace_s: float) -> dict | None:
        if self.open_bar is not None and now >= self.open_bar["end_ts"] + grace_s:
            return self.close_open_bar()
        return None

    def flush(self) -> dict | None:
        return self.close_open_bar()
