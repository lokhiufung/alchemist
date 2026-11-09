import asyncio
import os
import random
import threading
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import pytest

import alchemist.calculations  # noqa: F401
import alchemist.gateways  # noqa: F401
from alchemist.clock import WallClock
from alchemist.config import EventLoopConfig, StrategyConfig
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.buffer import EventBuffer
from alchemist.event_loop.loop import EventLoop
from alchemist.event_loop.resampler import Resampler
from alchemist.event_loop.recorders import NullRecorder
from alchemist.event_loop.shutdown import ShutdownCoordinator
from alchemist.instruments import ES, MES, NQ
from alchemist.loader import load_strategies
from alchemist.messages import make_event
from alchemist.registry import get_strategy
from alchemist.safety.null import NullSafety
from alchemist.stores import InMemoryCalcStateStore, InMemoryRoundTripStore
from alchemist.strategy_builder.strategy import Stores

pytestmark = pytest.mark.load

SECONDS = float(os.environ.get("LOAD_SECONDS", "60"))
TICKS_PER_SECOND_PER_STREAM = 500
BUDGET_S = 0.005
SESSION_MIDDAY = datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc).timestamp()


class OffsetClock(WallClock):
    def __init__(self, wakeup, offset: float) -> None:
        super().__init__(wakeup)
        self.offset = offset

    def now(self):
        return datetime.fromtimestamp(time.time() + self.offset, timezone.utc)


class OrderSinkGateway:
    def __init__(self) -> None:
        self.orders = 0
        self.next_id = 1

    def market_data_type(self, instrument):
        return ("tick_update", None)

    def on_bar(self, instrument, bar):
        return None

    def next_order_id(self):
        self.next_id += 1
        return self.next_id

    async def place_order(self, order):
        self.orders += 1

    async def cancel_order(self, order_id):
        return None

    async def get_open_orders(self, timeout=5.0):
        return []

    async def get_positions(self, timeout=5.0):
        return []


class StubAux:
    def annotate(self, instrument, bar):
        return None


def daily_history():
    return [{"date": str(i), "start_ts": float(i), "end_ts": float(i) + 1, "open": 5000.0, "high": 5010.0,
             "low": 4990.0, "close": 5000.0, "volume": 1.0, "resolution_s": 86400.0, "broker_ts": 1.0}
            for i in range(30)]


def produce(buffer, clock, instruments, stop, rng):
    prices = {i: 5000.0 for i in instruments}
    interval = 1.0 / TICKS_PER_SECOND_PER_STREAM
    started = time.perf_counter()
    count = 0
    while not stop.is_set():
        target = started + count * interval
        delay = target - time.perf_counter()
        if delay > 0:
            time.sleep(delay)
        broker_ts = clock.timestamp()
        for instrument in instruments:
            prices[instrument] += rng.choice((-0.25, 0.0, 0.25))
            buffer.push(make_event("tick_update", {"instrument": instrument, "broker_ts": broker_ts,
                                                   "recv_ts": time.time(), "price": prices[instrument],
                                                   "size": 1}, time.time()))
        count += 1


@pytest.mark.parametrize("strategies", [10, 15])
async def test_scenario_load_target_latency(strategies):
    load_strategies(Path("local/strategies"))
    config = EventLoopConfig()
    buffer = EventBuffer(config)
    clock = OffsetClock(buffer.drain_wakeup, SESSION_MIDDAY - time.time())
    instruments = [MES, ES, NQ]
    book, gateway = RuntimeBook(), OrderSinkGateway()
    stores = Stores(InMemoryCalcStateStore(), InMemoryRoundTripStore())
    strategy_class = get_strategy("macd_rsi_rth_long")
    for index in range(strategies):
        instrument = instruments[index % 3]
        slot = strategy_class(StrategyConfig(f"s{index}", "macd_rsi_rth_long", "ACC", (instrument,), (instrument,),
                                             snapshot_interval=int(os.environ.get("LOAD_SNAPSHOT_INTERVAL", "60"))), gateway, clock, stores)
        slot.warmup({"daily": daily_history(), "intraday": []})
        slot.on_seed_state(True)
        slot.on_aux_ready(True, {}) if slot.aux_names() else None
        book.commit(slot, [])
    for instrument in instruments:
        book.resamplers[instrument] = Resampler()
        book.bars[instrument] = deque(maxlen=5000)
        book.fed_instruments.add(instrument)
    shutdown = ShutdownCoordinator(book, gateway, clock, config)
    loop = EventLoop(buffer, book, gateway, StubAux(), NullSafety(), NullRecorder(), NullRecorder(), shutdown, clock,
                     config)
    samples = {instrument.symbol: [] for instrument in instruments}
    record = loop.metrics.record_processing

    def capture(instrument, *args, **kwargs):
        samples[instrument.symbol].append(args[1] - args[0])
        record(instrument, *args, **kwargs)

    loop.metrics.record_processing = capture
    stop = threading.Event()
    producer = threading.Thread(target=produce, args=(buffer, clock, instruments, stop, random.Random(7)), daemon=True)
    runner = asyncio.create_task(loop.run())
    producer.start()
    await asyncio.sleep(SECONDS)
    stop.set()
    producer.join()
    loop.stopping = True
    await runner
    report = {}
    for symbol, values in samples.items():
        ordered = sorted(values)
        assert len(ordered) >= SECONDS * 0.8, f"{symbol}: only {len(ordered)} bars were processed"
        report[symbol] = (ordered[len(ordered) // 2], ordered[min(len(ordered) - 1, int(len(ordered) * 0.99))])
    print(f"\nload {strategies} strategies, {SECONDS}s: " + ", ".join(
        f"{s} p50={p50 * 1000:.3f}ms p99={p99 * 1000:.3f}ms" for s, (p50, p99) in report.items()))
    assert all(p99 <= BUDGET_S for _, p99 in report.values()), report
