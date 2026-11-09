from __future__ import annotations

import logging
import math
import time
from collections import defaultdict
from typing import Callable

from alchemist.instruments import Instrument

logger = logging.getLogger("alchemist.latency")

FLUSH_INTERVAL_S = 60.0
SECOND_PRECISION_S = 1.0
FINE_PRECISION_S = 0.001
PRECISION_SAMPLES = 100


def percentile(samples: list[float], fraction: float) -> float | None:
    if not samples:
        return None
    ordered = sorted(samples)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


class LatencyMetrics:
    def __init__(self, wall: Callable[[], float] = time.time) -> None:
        self.wall = wall
        self.series: dict[str, dict[Instrument, list[float]]] = {
            name: defaultdict(list) for name in ("feed", "proc", "queue_wait", "dispatch", "slot_cost", "decode")}
        self.broker_stamps: dict[Instrument, list[float]] = defaultdict(list)
        self.records: list[dict] = []
        self.next_flush: float | None = None

    def precision(self, instrument: Instrument) -> float:
        stamps = self.broker_stamps.get(instrument, [])
        return SECOND_PRECISION_S if all(float(ts).is_integer() for ts in stamps) else FINE_PRECISION_S

    def record_feed(self, instrument: Instrument, broker_ts: float, recv_ts: float) -> None:
        self.series["feed"][instrument].append(recv_ts - broker_ts)
        stamps = self.broker_stamps[instrument]
        stamps.append(broker_ts)
        del stamps[:-PRECISION_SAMPLES]

    def record_decode(self, instrument: Instrument, decode_s: float) -> None:
        self.series["decode"][instrument].append(decode_s)

    def record_processing(self, instrument: Instrument, recv_ts: float, done_ts: float, queue_wait: float,
                          dispatch: float, slot_cost: float, kind: str = "bar", feed: float | None = None,
                          feed_precision_s: float | None = None) -> None:
        processing = done_ts - recv_ts
        for name, value in (("proc", processing), ("queue_wait", queue_wait), ("dispatch", dispatch),
                            ("slot_cost", slot_cost)):
            self.series[name][instrument].append(value)
        self.records.append({"kind": kind, "instrument": instrument.symbol, "processing": processing,
                             "queue_wait": queue_wait, "dispatch": dispatch, "slot_cost": slot_cost, "feed": feed,
                             "feed_precision_s": feed_precision_s})
        del self.records[:-1000]

    def instruments(self) -> list[Instrument]:
        seen: dict[Instrument, None] = {}
        for table in self.series.values():
            for instrument, samples in table.items():
                if samples:
                    seen[instrument] = None
        return list(seen)

    def flush(self, now: float) -> list[dict]:
        rows = []
        for instrument in self.instruments():
            def pick(name: str, fraction: float) -> float | None:
                return percentile(self.series[name][instrument], fraction)

            row = {"instrument": instrument.symbol, "event": "latency",
                   "feed_p50": pick("feed", 0.5), "feed_p99": pick("feed", 0.99),
                   "proc_p50": pick("proc", 0.5), "proc_p99": pick("proc", 0.99),
                   "queue_wait_p99": pick("queue_wait", 0.99), "dispatch_p99": pick("dispatch", 0.99),
                   "slot_cost_p50": pick("slot_cost", 0.5), "slot_cost_p99": pick("slot_cost", 0.99),
                   "decode_p50": pick("decode", 0.5), "decode_p99": pick("decode", 0.99),
                   "ts_precision_s": self.precision(instrument)}
            rows.append(row)
            logger.info("latency %s", " ".join(f"{k}={v}" for k, v in row.items()), extra={"latency_row": row})
        for table in self.series.values():
            table.clear()
        return rows

    def maybe_flush(self, now: float) -> None:
        if self.next_flush is None:
            self.next_flush = now + FLUSH_INTERVAL_S
        elif now >= self.next_flush:
            self.flush(now)
            self.next_flush = now + FLUSH_INTERVAL_S
