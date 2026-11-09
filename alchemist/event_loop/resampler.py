from __future__ import annotations


class Resampler:
    """Event Loop behaviours 1-2, decision 7: ticks -> completed 1-second OHLCV bars; pure, no I/O."""

    def __init__(self, resolution_s: int = 1) -> None: ...

    def update_from_tick(self, ts: float, price: float, size: float) -> dict | None:
        """The completed bar when ts rolls the bucket; late or out-of-order ticks are dropped."""
        ...

    def flush_due(self, now: float, grace_s: float) -> dict | None:
        """The open bar when its bucket closed more than grace_s ago."""
        ...

    def flush(self) -> dict | None:
        """The open bar at end of data."""
        ...
