import asyncio
from datetime import datetime, timezone

from alchemist.clock import ReplayClock
from alchemist.config import EventLoopConfig
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.buffer import EventBuffer
from alchemist.event_loop.loop import EventLoop
from alchemist.event_loop.shutdown import ShutdownCoordinator
from alchemist.exchanges import exchange_profile
from alchemist.instruments import ES, FDAX, MES, NQ
from alchemist.messages import make_event

from el_helpers import RecordingStrategy, StubAux, StubRecorder, StubSafety


def at(instrument, session, hour, minute, second=0, day=15, month=4):
    from datetime import date, time, timedelta
    zone = exchange_profile(instrument).session(session)
    from zoneinfo import ZoneInfo
    return datetime(2026, month, day, hour, minute, second, tzinfo=ZoneInfo(zone.tz)).astimezone(timezone.utc)


class FlattenStrategy(RecordingStrategy):
    def __init__(self, name, instrument, flat_on_request=3, session="NY_RTH") -> None:
        super().__init__(name, instrument)
        self.execution_instrument = instrument
        self.session = session
        self.flat_on_request = flat_on_request
        self.request_times: list[float] = []
        self.bar_log: list[float] = []

    def on_bar(self, bars, bar):
        self.bar_log.append(bar["start_ts"])
        super().on_bar(bars, bar)

    def request_flatten(self, reason, bar_ts):
        self.request_times.append(bar_ts)
        super().request_flatten(reason, bar_ts)

    def is_flat_idle(self):
        return self.flat_on_request is not None and len(self.request_times) >= self.flat_on_request


class ShutdownGateway:
    def __init__(self, open_orders=None, positions=None, unreadable=False) -> None:
        self.open_orders = list(open_orders or [])
        self.positions = list(positions or [])
        self.unreadable = unreadable
        self.cancelled: list[int] = []
        self.placed: list[dict] = []
        self.event_buffer = None
        self.next_id = 1000

    def next_order_id(self):
        self.next_id += 1
        return self.next_id

    def market_data_type(self, instrument):
        return ("bar_update", 1.0)

    def on_bar(self, instrument, bar):
        return None

    async def start_feed(self):
        return None

    async def get_open_orders(self, timeout=5.0):
        if self.unreadable:
            raise TimeoutError("no answer")
        return [dict(o) for o in self.open_orders]

    async def get_positions(self, timeout=5.0):
        return [dict(p) for p in self.positions]

    async def cancel_order(self, order_id):
        self.cancelled.append(order_id)
        self.open_orders = [o for o in self.open_orders if o["order_id"] != order_id]

    async def place_order(self, order):
        self.placed.append(dict(order))
        for position in self.positions:
            if position["instrument"] == order["instrument"]:
                position["size"] += order["qty"] if order["side"] == "BUY" else -order["qty"]
        self.positions = [p for p in self.positions if p["size"] != 0]


class Rig:
    def __init__(self, strategies, start, gateway=None, config=None) -> None:
        self.config = config or EventLoopConfig()
        self.clock = ReplayClock(start)
        self.buffer = EventBuffer(self.config)
        self.book = RuntimeBook()
        self.gateway = gateway or ShutdownGateway()
        self.bar_recorder, self.trade_recorder = StubRecorder(), StubRecorder()
        self.shutdown = ShutdownCoordinator(self.book, self.gateway, self.clock, self.config)
        self.loop = EventLoop(self.buffer, self.book, self.gateway, StubAux(), StubSafety(), self.bar_recorder,
                              self.trade_recorder, self.shutdown, self.clock, self.config,
                              clock_advance=self.clock.advance)
        self.shutdown.attach(self.loop) if hasattr(self.shutdown, "attach") else None
        for strategy in strategies:
            self.book.strategies.append(strategy)
            self.book.sessions_by_strategy[strategy.name] = strategy.session
            self.book.bars.setdefault(strategy.signal_instrument, __import__("collections").deque(maxlen=100))

    def push_second_bars(self, instrument, begin: datetime, count: int, close=100.0):
        from el_helpers import bar_update
        for index in range(count):
            self.buffer.push(bar_update(begin.timestamp() + index, close, instrument))

    async def run(self, timeout=10.0):
        await asyncio.wait_for(self.loop.run(), timeout)
