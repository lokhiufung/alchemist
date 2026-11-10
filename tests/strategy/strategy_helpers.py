import asyncio
from collections import deque
from datetime import datetime, timezone
from types import SimpleNamespace

from alchemist.clock import ReplayClock
from alchemist.config import StrategyConfig
from alchemist.instruments import FDAX, MES, NQ
from alchemist.logger import fields_of
from alchemist.registry import register_signal_dag
from alchemist.stores import InMemoryCalcStateStore, InMemoryRoundTripStore
from alchemist.strategy_builder.strategy import Stores, Strategy

SESSION_OPEN = datetime(2026, 4, 15, 13, 30, tzinfo=timezone.utc).timestamp()


def make_bar(index: int, close: float = 100.0, open_ts: float = SESSION_OPEN) -> dict:
    start = open_ts + index
    return {"instrument": MES, "start_ts": start, "end_ts": start + 1.0, "open": close, "high": close, "low": close,
            "close": close, "volume": 1.0, "resolution_s": 1.0, "broker_ts": start + 1.0}


class StubSignal:
    builds = 0
    aux = frozenset()
    warm_after_seed = True
    session_state: dict = {}

    def __init__(self, instrument) -> None:
        type(self).builds += 1
        self.params = SimpleNamespace(session="regular", direction="long", max_trades_per_day=1)
        self.steps: list[tuple[float, dict]] = []
        self.seeds: list[dict] = []
        self.restored: list[dict] = []
        self.warm = False
        self.current: dict = {}

    @classmethod
    def from_config(cls, strategy_config):
        return cls(strategy_config.signal_instrument)

    def warmup_requirements(self):
        from alchemist.strategy_builder.strategy_signal import WarmupRequirements
        return WarmupRequirements()

    def aux_names(self):
        return type(self).aux

    def seed(self, history):
        self.seeds.append(history)
        self.warm = type(self).warm_after_seed

    def is_warm(self):
        return self.warm

    def step(self, bars, strategy_data):
        self.steps.append((bars[-1]["start_ts"], dict(strategy_data)))
        self.current = {"close": bars[-1]["close"]}

    def values(self):
        return dict(self.current)

    def evaluate_entry(self):
        return False

    def entry_conditions(self):
        return {}

    def evaluate_exit_conditions(self):
        return {}

    def freeze_exit_inputs(self, bar):
        return None

    def release_exit_inputs(self):
        return None

    def snapshot_session_state(self):
        return dict(type(self).session_state)

    def restore_session_state(self, state):
        self.restored.append(state)
        self.warm = True


class StubExecution:
    def __init__(self) -> None:
        self.bars: list[float] = []
        self.updates: list[dict] = []
        self.flatten_calls: list[tuple[dict, str]] = []
        self.warmup_ready = False
        self.aux_ready = False
        self.wanted = False
        self.quarantined = False
        self.entry_order_bar_ts: float | None = None
        self.on_decide = None
        self.signal = None
        self.trade_closed = False

    def on_bar(self, bar, step_signal):
        step_signal()
        self.bars.append(bar["start_ts"])
        if self.on_decide:
            self.on_decide(bar)

    def on_order_update(self, update):
        self.updates.append(update)

    def set_warmup_ready(self, ready=True):
        self.warmup_ready = ready

    def set_aux_ready(self, ready=True):
        self.aux_ready = ready

    def request_flatten(self, position, reason):
        self.flatten_calls.append((dict(position), reason))

    def flatten_wanted(self):
        return self.wanted

    def is_processing_order(self):
        return False

    def is_quarantined(self):
        return self.quarantined

    def quarantine(self, reason):
        self.quarantined = True

    def release_quarantine(self):
        self.quarantined = False

    def consume_trade_closed(self):
        closed, self.trade_closed = self.trade_closed, False
        return closed

    async def settle(self):
        return None


class FakeGateway:
    def __init__(self) -> None:
        self.next_id = 1
        self.placed: list[dict] = []

    def next_order_id(self):
        self.next_id += 1
        return self.next_id - 1

    async def place_order(self, order):
        self.placed.append(dict(order))

    async def cancel_order(self, order_id):
        return None


register_stub_name = "stub_signal"


class StubStrategy(Strategy):
    signal_dag = register_stub_name


def install_stub_signal(signal_class=StubSignal):
    register_signal_dag(register_stub_name)(signal_class)


def make_strategy(instrument=MES, execution=True, calc_store=None, snapshot_interval=60, name="slot",
                  strategy_class=StubStrategy, execution_instrument=None) -> Strategy:
    config = StrategyConfig(name, "stub", "ACC", (instrument,), (execution_instrument or instrument,),
                            snapshot_interval=snapshot_interval)
    clock = ReplayClock(datetime.fromtimestamp(SESSION_OPEN, timezone.utc))
    stores = Stores(calc_state=calc_store or InMemoryCalcStateStore(), round_trips=InMemoryRoundTripStore())
    strategy = strategy_class(config, FakeGateway(), clock, stores)
    if execution:
        stub = StubExecution()
        stub.aux_ready = strategy.execution.aux_ready
        stub.signal = strategy.signal
        strategy.execution = stub
    return strategy


def fill_update(order_id, side_status="FILLED", filled=1, price=5001.0, instrument=MES):
    return {"order_id": order_id, "status": side_status, "filled": filled, "remaining": 1 - filled,
            "avg_fill_price": price if filled else None, "last_fill_price": price if filled else None, "ts": 0.0,
            "account_id": "ACC", "instrument": instrument, "reason": None}


async def place_and_fill(strategy, side="BUY", price=5001.0, instrument=None):
    order = await strategy.om.place_order(instrument or strategy.execution_instrument, side, 1, "MKT")
    strategy.on_order_update(fill_update(order["order_id"], "SUBMITTED", 0, price))
    strategy.on_order_update(fill_update(order["order_id"], "FILLED", 1, price))
    return order


async def settle():
    for _ in range(10):
        await asyncio.sleep(0)


def snapshot_rows(caplog):
    return [fields_of(r) for r in caplog.records if getattr(r, "event", None) == "signal_snapshot"]
