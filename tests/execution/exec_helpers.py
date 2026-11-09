import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from alchemist.clock import ReplayClock
from alchemist.config import ExecutionConfig
from alchemist.exchanges import exchange_profile
from alchemist.instruments import MES
from alchemist.managers.order_manager import OrderManager
from alchemist.stores import InMemoryRoundTripStore
from alchemist.strategy_builder.strategy_executions import make_execution

DAY = (2026, 4, 15)


def session_open_ts(instrument=MES, session="NY_RTH", day=DAY):
    from datetime import date
    return exchange_profile(instrument).session_window(session, date(*day))[0].timestamp()


def make_bar(index: int, close: float = 100.0, open_ts: float | None = None, **fields) -> dict:
    start = (session_open_ts() if open_ts is None else open_ts) + index
    return {"instrument": MES, "start_ts": start, "end_ts": start + 1.0, "open": close, "high": close, "low": close,
            "close": close, "volume": 1.0, "resolution_s": 1.0, "broker_ts": start + 1.0, **fields}


class FakeGateway:
    def __init__(self) -> None:
        self.next_id = 1
        self.placed: list[dict] = []
        self.cancelled: list[int] = []

    def next_order_id(self) -> int:
        self.next_id += 1
        return self.next_id - 1

    async def place_order(self, order: dict) -> None:
        self.placed.append(dict(order))

    async def cancel_order(self, order_id: int) -> None:
        self.cancelled.append(order_id)


class ScriptedSignal:
    def __init__(self, direction="long", cap=1, exit_names=("stop", "target")) -> None:
        self.params = SimpleNamespace(direction=direction, max_trades_per_day=cap, session="regular")
        self.index = 0
        self.entries: set[int] = set()
        self.exits: dict[int, dict[str, bool]] = {}
        self.exit_names = list(exit_names)
        self.levels: dict[str, float | None] = {}
        self.node_levels: dict[str, float | None] = {}
        self.calls: list[tuple] = []
        self.steps = 0

    def step(self) -> None:
        self.steps += 1
        self.calls.append(("step", self.index))

    def evaluate_entry(self) -> bool:
        self.calls.append(("entry?", self.index))
        return self.index in self.entries

    def entry_conditions(self):
        return {"entry": self.index in self.entries}

    def evaluate_exit_conditions(self) -> dict[str, bool]:
        scripted = self.exits.get(self.index, {})
        return {name: scripted.get(name, False) for name in self.exit_names}

    def evaluate_exit(self) -> bool:
        return any(self.evaluate_exit_conditions().values())

    def exit_condition_names(self):
        return list(self.exit_names)

    def exit_condition_level(self, name):
        return self.levels.get(name)

    def level(self, name):
        return self.node_levels.get(name)

    def has_level(self, name):
        return name in self.node_levels

    def condition_names(self):
        return {"entry", *self.exit_names}

    def freeze_exit_inputs(self, bar) -> None:
        self.calls.append(("freeze", self.index))

    def release_exit_inputs(self) -> None:
        self.calls.append(("release", self.index))


async def settle() -> None:
    for _ in range(5):
        await asyncio.sleep(0)


def status_update(order_id, status, filled=0, avg=None, reason=None):
    return {"order_id": order_id, "status": status, "filled": filled, "remaining": 1 - filled,
            "avg_fill_price": avg, "last_fill_price": avg, "ts": 0.0, "account_id": "SIM",
            "instrument": MES, "reason": reason}


class Slot:
    def __init__(self, signal=None, execution_config=None, store=None, session="NY_RTH", instrument=MES,
                 ready=True, cap=1, name="slot", reminder_interval_s=300.0, **make_kwargs) -> None:
        self.signal = signal or ScriptedSignal(cap=cap)
        self.gateway = FakeGateway()
        self.om = OrderManager(self.gateway, "SIM", name)
        self.clock = ReplayClock(datetime.fromtimestamp(session_open_ts(instrument, session), timezone.utc))
        self.open_ts = session_open_ts(instrument, session)
        self.store = store if store is not None else InMemoryRoundTripStore()
        self.name = name
        self.execution = make_execution(
            execution_config, self.signal, self.om, instrument=instrument, account_id="SIM", slot=name,
            session=session, clock=self.clock, roundtrip_store=self.store,
            config=ExecutionConfig(reminder_interval_s=reminder_interval_s), **make_kwargs)
        if ready:
            self.execution.set_warmup_ready()
            self.execution.set_aux_ready()

    async def bar(self, index, close=100.0, **fields):
        bar = make_bar(index, close, open_ts=self.open_ts, **fields)
        self.clock.advance(datetime.fromtimestamp(bar["end_ts"], timezone.utc))
        self.signal.index = index
        self.execution.on_bar(bar, self.signal.step)
        await settle()
        return bar

    async def raw_bar(self, bar):
        at = datetime.fromtimestamp(bar["end_ts"], timezone.utc)
        if at > self.clock.now():
            self.clock.advance(at)
        self.execution.on_bar(bar, self.signal.step)
        await settle()

    async def update(self, order_id, status, filled=0, avg=None, reason=None):
        message = status_update(order_id, status, filled, avg, reason)
        change = self.om.on_order_update(message)
        if change:
            self.execution.on_order_update(message)
        await settle()

    async def fill(self, order_id, price):
        await self.update(order_id, "SUBMITTED")
        await self.update(order_id, "FILLED", filled=1, avg=price)

    async def fill_last(self, price=100.0):
        await self.fill(self.gateway.placed[-1]["order_id"], price)

    @property
    def orders(self) -> list[dict]:
        return self.gateway.placed

    def sides(self):
        return [(o["side"], o["order_type"], o["qty"]) for o in self.gateway.placed]


def position(size: int, avg: float = 100.0) -> dict:
    return {"size": size, "avg_price": avg, "currency": "USD", "side": "LONG" if size > 0 else "FLAT"}


async def drive_to(slot: "Slot", phase: str) -> None:
    signal, execution = slot.signal, slot.execution
    if phase == "WARMUP":
        await slot.bar(1)
    elif phase == "WAITING_FOR_WORKING_HOURS":
        execution.quarantine("test")
        execution.release_quarantine()
    elif phase == "ENTRY_LOOP_ACTIVITY":
        await slot.bar(1)
    elif phase in ("PROCESSING_ENTRY_ORDER", "IN_POSITION_ACTIVITY", "PROCESSING_EXIT_ORDER"):
        signal.entries, signal.exits = {1}, {3: {"target": True}}
        await slot.bar(1)
        if phase != "PROCESSING_ENTRY_ORDER":
            await slot.fill_last()
            await slot.bar(2)
        if phase == "PROCESSING_EXIT_ORDER":
            await slot.bar(3)
    elif phase == "SESSION_END_FLATTENING":
        signal.entries = {1}
        await slot.bar(1)
        await slot.fill_last()
        execution.request_flatten(position(1), "session_end")
        await settle()
    elif phase == "DONE":
        signal.entries, signal.exits = {1}, {2: {"target": True}}
        await slot.bar(1)
        await slot.fill_last()
        await slot.bar(2)
        await slot.fill_last()
    elif phase == "ERROR":
        execution.quarantine("test")
    else:
        raise ValueError(phase)
