import asyncio
from datetime import datetime, timezone

from alchemist.clock import ReplayClock
from alchemist.config import SafetyConfig
from alchemist.event_loop.book import RuntimeBook
from alchemist.exchanges import EXCHANGES
from alchemist.instruments import ES, FDAX, MES
from alchemist.safety.monitor import SafetyMonitor


def utc(day, hour, minute=0, second=0, month=4):
    return datetime(2026, month, day, hour, minute, second, tzinfo=timezone.utc)


class FakeGateway:
    def __init__(self, clock) -> None:
        self.clock = clock
        self.heartbeat_ok = lambda now: True
        self.last_tick = None
        self.reconnect_failures = 0
        self.calls: list[tuple] = []
        self.on_resubscribe = None
        self.resubscribe_request = False
        self.executions: list[dict] = []
        self.open_orders: list[dict] = []
        self.positions: list[dict] = []
        self.fail_reads = False
        self.recovered = False

    @property
    def last_tick_ts(self):
        return self.last_tick() if callable(self.last_tick) else self.last_tick

    def mark(self, name, *details):
        self.calls.append((name, self.clock.timestamp(), *details))

    def names(self):
        return [c[0] for c in self.calls]

    def before_first_reconnect(self):
        names = self.names()
        return names[:names.index("reconnect")] if "reconnect" in names else names

    async def heartbeat(self, timeout=3.0):
        self.mark("heartbeat", timeout)
        return self.heartbeat_ok(self.clock.timestamp())

    async def reconnect(self):
        self.mark("reconnect")
        if self.reconnect_failures > 0:
            self.reconnect_failures -= 1
            raise ConnectionError("still down")
        self.recovered = True

    async def subscribe_tick(self, contract):
        self.mark("subscribe_tick", contract["instrument"].symbol)
        if self.on_resubscribe:
            self.on_resubscribe()

    async def subscribe_order_update(self):
        self.mark("subscribe_order_update")

    async def subscribe_account_update(self):
        self.mark("subscribe_account_update")

    def consume_resubscribe_request(self):
        requested, self.resubscribe_request = self.resubscribe_request, False
        return requested

    async def get_executions(self, since=None):
        self.mark("get_executions")
        if self.fail_reads:
            raise TimeoutError
        return list(self.executions)

    async def get_open_orders(self, timeout=5.0):
        self.mark("get_open_orders")
        if self.fail_reads:
            raise TimeoutError
        return list(self.open_orders)

    async def get_positions(self, timeout=5.0):
        self.mark("get_positions")
        if self.fail_reads:
            raise TimeoutError("no answer")
        return list(self.positions)


class Buffer:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def push(self, event, wake=True):
        self.events.append(event)

    def of_type(self, event_type):
        return [e["data"] for e in self.events if e["event_type"] == event_type]


class Rig:
    def __init__(self, start, instruments=(MES,), config=None, strategies=()) -> None:
        self.clock = ReplayClock(start)
        self.config = config or SafetyConfig()
        self.gateway = FakeGateway(self.clock)
        self.book = RuntimeBook()
        for instrument in instruments:
            self.book.reserve_instrument(instrument, {"instrument": instrument}, fed=True)
        self.book.strategies.extend(strategies)
        self.buffer = Buffer()
        self.sleeps: list[float] = []
        self.restarts: list[float] = []
        self.monitor = SafetyMonitor(self.gateway, self.book, self.buffer, EXCHANGES, self.clock, self.config,
                                     restart_gateway=self.restart, sleep=self.sleep)

    async def sleep(self, delay):
        self.sleeps.append(delay)
        self.clock.advance(self.clock.now().fromtimestamp(self.clock.timestamp() + delay, timezone.utc))

    async def restart(self):
        self.restarts.append(self.clock.timestamp())

    def advance_to(self, moment):
        if moment > self.clock.now():
            self.clock.advance(moment)

    async def steps(self, begin, end, interval=None):
        interval = interval or self.config.heartbeat_interval_s
        moment = begin
        from datetime import timedelta
        while moment <= end:
            self.advance_to(moment)
            await self.monitor.step()
            moment += timedelta(seconds=interval)
