import asyncio
from collections import deque
from datetime import datetime, timezone

from alchemist.config import EventLoopConfig
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.buffer import CommandBuffer, EventBuffer
from alchemist.event_loop.loop import EventLoop
from alchemist.event_loop.recorders import Recorder
from alchemist.event_loop.resampler import Resampler
from alchemist.event_loop.watch_time import WatchTime
from alchemist.instruments import ES, MES
from alchemist.messages import make_event
from alchemist.requests import Requests

OPEN = datetime(2026, 4, 15, 13, 30, tzinfo=timezone.utc).timestamp()


class FakeSystemTime:
    def __init__(self, start: float = 0.0) -> None:
        self.current = start
        self.waits = 0

    def __call__(self) -> float:
        return self.current


class StubOrderBook:
    def __init__(self) -> None:
        self.orders: dict[int, dict] = {}

    def get_order(self, order_id):
        return self.orders.get(order_id)


class RecordingStrategy:
    def __init__(self, name="s", instrument=MES, raises_on_bar=False, log=None, watch_time=None) -> None:
        self.name, self.signal_instrument = name, instrument
        self.bars: list[float] = []
        self.events: list[tuple] = []
        self.raises_on_bar = raises_on_bar
        self.quarantined = False
        self.closed_trades = 0
        self.log = log if log is not None else []
        self.watch_time = watch_time
        self.watch_at_bar: list[float] = []
        self.flatten_requests: list[tuple[str, float]] = []
        self.flat_idle = True
        self.om = StubOrderBook()

    def on_bar(self, bars, bar):
        self.log.append(("strategy", self.name, bar["start_ts"]))
        if self.watch_time is not None:
            self.watch_at_bar.append(self.watch_time.now())
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


class StubAux:
    def __init__(self, log=None) -> None:
        self.log = log if log is not None else []

    def annotate(self, instrument, bar):
        self.log.append(("aux", bar["start_ts"]))


class StubRecorder(Recorder):
    def __init__(self) -> None:
        self.bars = []
        self.updates = []
        self.flushes = 0
        self.finalized = False

    def record_bar(self, instrument, bar, session):
        self.bars.append(bar["start_ts"])

    def record_order_update(self, update, strategy_key, order):
        self.updates.append(update["order_id"])

    def flush(self):
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

    async def finalize(self, settle):
        await settle()

    def request_shutdown(self, reason):
        self.shutdown_requested = True


class StubSafety:
    def __init__(self) -> None:
        self.check_ins = 0

    def check_in(self):
        self.check_ins += 1


class StubDeploy:
    def __init__(self) -> None:
        self.requests = []

    def handle(self, request):
        self.requests.append(request)


def bar_update(ts, close=100.0, instrument=MES, **fields):
    bar = {"instrument": instrument, "start_ts": ts, "end_ts": ts + 1.0, "open": close, "high": close, "low": close,
           "close": close, "volume": 1.0, "resolution_s": 1.0, "broker_ts": ts + 1.0, **fields}
    return make_event("bar_update", bar, ts + 1.0)


def tick(ts, price, size, instrument=MES, recv_ts=0.0):
    return make_event("tick_update", {"instrument": instrument, "broker_ts": ts, "recv_ts": recv_ts,
                                      "price": price, "size": size}, ts)


def order_update(order_id=1, status="FILLED"):
    return make_event("order_update", {"order_id": order_id, "status": status, "filled": 1, "remaining": 0,
                                       "avg_fill_price": 1.0, "last_fill_price": 1.0, "ts": 0.0, "account_id": "A",
                                       "instrument": MES, "reason": None}, 0.0)


def balance_update(currency="USD", available=5.0):
    return make_event("balance_update", {"account_id": "A", "currency": currency, "available": available}, 0.0)


def end_of_data():
    return make_event("end_of_data", {}, 0.0)


class Harness:
    def __init__(self, strategies=None, message_type="bar_update", system=None, resample=None, config=None,
                 log=None, shutdown=None) -> None:
        self.log = log if log is not None else []
        self.system = system or FakeSystemTime()
        self.watch_time = WatchTime(system_time=self.system)
        self.config = config or EventLoopConfig()
        self.buffer = EventBuffer(self.config)
        self.buffer.wait = self.wait
        self.outbound = CommandBuffer()
        self.requests = Requests(self.outbound)
        self.book = RuntimeBook()
        self.aux = StubAux(self.log)
        self.recorder = StubRecorder()
        self.safety = StubSafety()
        self.deploy = StubDeploy()
        self.shutdown = shutdown or StubShutdown()
        self.loop = EventLoop(self.buffer, self.requests, self.book, self.aux, [self.recorder],
                              self.shutdown, self.watch_time, self.deploy, self.safety.check_in,
                              self.config)
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

    async def wait(self):
        self.system.waits += 1
        self.system.current += self.config.drain_timeout_s
        await asyncio.sleep(0)

    async def run(self, timeout=5.0):
        await asyncio.wait_for(self.loop.run(), timeout)
