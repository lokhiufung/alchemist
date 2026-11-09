import asyncio
from collections import deque
from datetime import datetime, timezone

from alchemist.clock import Clock, ReplayClock
from alchemist.config import EventLoopConfig
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.buffer import EventBuffer
from alchemist.event_loop.loop import EventLoop
from alchemist.event_loop.resampler import Resampler
from alchemist.instruments import ES, MES
from alchemist.messages import make_event

OPEN = datetime(2026, 4, 15, 13, 30, tzinfo=timezone.utc).timestamp()


class FakeWallClock(Clock):
    def __init__(self, start: float = OPEN) -> None:
        self.current = start
        self.waits = 0

    def now(self):
        return datetime.fromtimestamp(self.current, timezone.utc)

    async def wait(self, timeout):
        self.waits += 1
        self.current += timeout
        await asyncio.sleep(0)


class RecordingStrategy:
    def __init__(self, name="s", instrument=MES, raises_on_bar=False, log=None, clock=None) -> None:
        self.name, self.signal_instrument = name, instrument
        self.bars: list[float] = []
        self.events: list[tuple] = []
        self.raises_on_bar = raises_on_bar
        self.quarantined = False
        self.closed_trades = 0
        self.log = log if log is not None else []
        self.clock = clock
        self.clock_at_bar: list[float] = []
        self.flatten_requests: list[tuple[str, float]] = []
        self.flat_idle = True

    def on_bar(self, bars, bar):
        self.log.append(("strategy", self.name, bar["start_ts"]))
        if self.clock is not None:
            self.clock_at_bar.append(self.clock.timestamp())
        if self.raises_on_bar:
            raise RuntimeError(f"{self.name} broke")
        self.bars.append(bar["start_ts"])
        self.events.append(("bar", bar["start_ts"], bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"]))

    def on_order_update(self, update):
        self.events.append(("order", update["order_id"], update["status"]))

    def on_balance_update(self, update):
        self.events.append(("balance", update["currency"], update["available"]))

    def on_position_update(self, update):
        self.events.append(("position", update["size"]))

    def consume_trade_closed(self):
        closed, self.closed_trades = self.closed_trades, 0
        return closed > 0

    def quarantine(self, reason):
        self.quarantined = True

    def is_quarantined(self):
        return self.quarantined

    def release_quarantine(self):
        self.quarantined = False

    def request_flatten(self, reason, bar_ts):
        self.flatten_requests.append((reason, bar_ts))

    def is_flat_idle(self):
        return self.flat_idle

    async def settle(self):
        return None


class StubGateway:
    def __init__(self, message_type="bar_update", log=None) -> None:
        self.message_type = message_type
        self.shown: list[float] = []
        self.log = log if log is not None else []

    def market_data_type(self, instrument):
        return (self.message_type, 1.0 if self.message_type == "bar_update" else None)

    def on_bar(self, instrument, bar):
        self.log.append(("gateway", bar["start_ts"]))
        self.shown.append(bar["start_ts"])


class StubAux:
    def __init__(self, log=None) -> None:
        self.log = log if log is not None else []

    def annotate(self, instrument, bar):
        self.log.append(("aux", bar["start_ts"]))


class StubRecorder:
    def __init__(self) -> None:
        self.bars = []
        self.updates = []
        self.flushes = 0
        self.finalized = False

    def record(self, instrument, bar):
        self.bars.append(bar["start_ts"])

    def record_update(self, update):
        self.updates.append(update["order_id"])

    async def flush(self):
        self.flushes += 1

    def finalize(self):
        self.finalized = True


class StubShutdown:
    shutdown_requested = False
    not_flat_at_shutdown = False

    async def process_due(self):
        return None

    def flattening_names(self):
        return set()

    async def flatten_on_bar(self, instrument, bar):
        return None

    def ready_to_exit(self):
        return True

    async def finalize(self, drain_remaining):
        await drain_remaining()


class StubSafety:
    def __init__(self) -> None:
        self.check_ins = 0

    def check_in(self):
        self.check_ins += 1


def bar_update(ts, close=100.0, instrument=MES, **fields):
    bar = {"instrument": instrument, "start_ts": ts, "end_ts": ts + 1.0, "open": close, "high": close, "low": close,
           "close": close, "volume": 1.0, "resolution_s": 1.0, "broker_ts": ts + 1.0, **fields}
    return make_event("bar_update", bar, 0.0)


def tick(ts, price, size, instrument=MES, recv_ts=0.0):
    return make_event("tick_update", {"instrument": instrument, "broker_ts": ts, "recv_ts": recv_ts,
                                      "price": price, "size": size}, 0.0)


def order_update(order_id=1, status="FILLED"):
    return make_event("order_update", {"order_id": order_id, "status": status, "filled": 1, "remaining": 0,
                                       "avg_fill_price": 1.0, "last_fill_price": 1.0, "ts": 0.0, "account_id": "A",
                                       "instrument": MES, "reason": None}, 0.0)


def balance_update(currency="USD", available=5.0):
    return make_event("balance_update", {"account_id": "A", "currency": currency, "available": available}, 0.0)


def end_of_data():
    return make_event("end_of_data", {}, 0.0)


class Harness:
    def __init__(self, strategies=None, message_type="bar_update", clock=None, resample=None, config=None,
                 log=None) -> None:
        self.log = log if log is not None else []
        self.clock = clock or ReplayClock(datetime.fromtimestamp(OPEN, timezone.utc))
        self.config = config or EventLoopConfig()
        self.buffer = EventBuffer(self.config)
        self.book = RuntimeBook()
        self.gateway = StubGateway(message_type, self.log)
        self.aux = StubAux(self.log)
        self.bar_recorder, self.trade_recorder = StubRecorder(), StubRecorder()
        self.safety = StubSafety()
        advance = self.clock.advance if isinstance(self.clock, ReplayClock) else (lambda moment: None)
        self.loop = EventLoop(self.buffer, self.book, self.gateway, self.aux, self.safety, self.bar_recorder,
                              self.trade_recorder, StubShutdown(), self.clock, self.config, clock_advance=advance)
        for strategy in strategies or []:
            self.add(strategy)
        if message_type == "tick_update":
            for instrument in {s.signal_instrument for s in self.book.strategies}:
                self.book.resamplers[instrument] = Resampler()
        for instrument in {s.signal_instrument for s in self.book.strategies}:
            self.book.bars.setdefault(instrument, deque(maxlen=500))

    def add(self, strategy):
        self.book.strategies.append(strategy)

    def push(self, *events):
        for event in events:
            self.buffer.push(event)

    async def run(self, timeout=5.0):
        await asyncio.wait_for(self.loop.run(), timeout)
