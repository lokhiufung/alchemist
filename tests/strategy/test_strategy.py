import logging
from collections import deque
from datetime import date, datetime, timezone

import pytest

import alchemist.gateways  # noqa: F401
from alchemist.exchanges import exchange_profile
from alchemist.gateways.tapes import tape_from_spec
from alchemist.instruments import FDAX, MES, NQ
from alchemist.loader import load_strategies
from alchemist.registry import get_strategy
from alchemist.clock import ReplayClock
from alchemist.config import GatewayConfig, ReplayConfig, StrategyConfig
from alchemist.gateways.simulation_gateway import SimulationGateway
from alchemist.stores import InMemoryCalcStateStore, InMemoryRoundTripStore
from alchemist.strategy_builder.strategy import Stores

from strategy_helpers import (SESSION_OPEN, StubSignal, StubStrategy, fill_update, make_bar, make_strategy,
                              place_and_fill, settle)

ENTRY_INDEX, TARGET_INDEX = 601, 618


class RecordingExecution:
    def __init__(self, strategy) -> None:
        self.strategy, self.bars, self.pending = strategy, [], []
        self.warmup_ready = self.aux_ready = False
        self.entry_order_bar_ts = None
        self.signal = strategy.signal

    def on_bar(self, bar, step_signal):
        step_signal()
        self.bars.append(bar["start_ts"])

    def on_order_update(self, update):
        pass

    def set_warmup_ready(self, ready=True):
        self.warmup_ready = ready

    def set_aux_ready(self, ready=True):
        self.aux_ready = ready

    def flatten_wanted(self):
        return False

    def request_flatten(self, position, reason):
        pass

    def is_processing_order(self):
        return False

    def is_quarantined(self):
        return False

    def consume_trade_closed(self):
        return False


async def test_golden_round_trip_books_position():
    load_strategies(__import__("pathlib").Path("local/strategies"))
    strategy_class = get_strategy("macd_rsi_rth_long")
    replay = ReplayConfig("synthetic:golden_session", date(2026, 4, 15), "NY_RTH")
    start, end = exchange_profile(MES).session_window("NY_RTH", replay.date)
    clock = ReplayClock(start.astimezone(timezone.utc))
    gateway = SimulationGateway(type("B", (), {"push": lambda self, e: None})(), GatewayConfig("simulation", replay=replay),
                                clock)
    config = StrategyConfig("macd_rsi_rth_long", "macd_rsi_rth_long", "SIM", (MES,), (MES,))
    stores = Stores(InMemoryCalcStateStore(), InMemoryRoundTripStore())
    strategy = strategy_class(config, gateway, clock, stores)
    assert strategy.execution.aux_ready is True

    steps = []
    execution = RecordingExecution(strategy)
    strategy.execution = execution

    daily = await gateway.get_daily_history(MES, date(2026, 1, 1), replay.date, "NY_RTH")
    seed_bars = await gateway.get_intraday_seed_bars(MES, "NY_RTH")
    strategy.warmup({"daily": daily, "intraday": seed_bars})
    await strategy.rebuild_intraday_state(seed_bars)
    strategy.on_seed_state(True)
    assert strategy.signal.is_warm() and execution.warmup_ready
    real_step = strategy.signal.step
    strategy.signal.step = lambda bars, data: (steps.append((bars[-1]["start_ts"], dict(data))), real_step(bars, data))[1]

    bars = list(tape_from_spec("synthetic:golden_session").bars(MES, start, end))
    window = deque(maxlen=500)
    positions = {}
    for index, bar in enumerate(bars):
        window.append(bar)
        strategy.on_bar(window, bar)
        assert len(steps) == len(execution.bars) == index + 1
        if index in (ENTRY_INDEX, TARGET_INDEX):
            side = "BUY" if index == ENTRY_INDEX else "SELL"
            execution.entry_order_bar_ts = bar["start_ts"] if side == "BUY" else execution.entry_order_bar_ts
            await place_and_fill(strategy, side, bar["close"])
            positions[side] = dict(strategy.position)
    assert positions["BUY"]["size"] == 1 and positions["BUY"]["currency"] == "USD"
    assert positions["SELL"]["size"] == 0 and strategy.position["size"] == 0
    after_buy = [data for ts, data in steps if bars[ENTRY_INDEX]["start_ts"] < ts <= bars[TARGET_INDEX]["start_ts"]]
    assert {d["entry_price"] for d in after_buy} == {bars[ENTRY_INDEX]["close"]}
    assert steps[-1][1]["entry_price"] is None


async def test_strategy_data_carries_entry_price_after_fill():
    strategy = make_strategy()
    bar = make_bar(1)
    strategy.on_bar(deque([bar]), bar)
    strategy.execution.entry_order_bar_ts = bar["start_ts"]
    await place_and_fill(strategy, "BUY", 5001.0)
    later = make_bar(11)
    strategy.on_bar(deque([later]), later)
    (_, flat), (_, long) = strategy.signal.steps
    assert flat == {"entry_price": None, "secs_since_entry": None}
    assert long == {"entry_price": 5001.0, "secs_since_entry": 10.0}


async def test_no_step_outside_session_or_when_quarantined_out_of_session():
    strategy = make_strategy(execution=False)
    strategy.warmup({"daily": [1]})
    await strategy.rebuild_intraday_state([])
    strategy.on_seed_state(True)
    before_open = make_bar(-7200)
    strategy.on_bar(deque([before_open]), before_open)
    strategy.quarantine("boom")
    strategy.on_bar(deque([before_open]), before_open)
    assert strategy.signal.steps == []
    in_session = make_bar(5)
    strategy.on_bar(deque([in_session]), in_session)
    assert len(strategy.signal.steps) == 1


@pytest.mark.parametrize("update_kind", ["owned_change", "not_owned", "duplicate"])
async def test_position_booked_only_from_order_manager_changes(update_kind):
    strategy = make_strategy()
    order = await strategy.om.place_order(MES, "BUY", 1, "MKT")
    if update_kind == "not_owned":
        strategy.on_order_update(fill_update(9999, "FILLED", 1, 5001.0))
        assert strategy.position["size"] == 0
        return
    strategy.on_order_update(fill_update(order["order_id"], "FILLED", 1, 5001.0))
    if update_kind == "duplicate":
        strategy.on_order_update(fill_update(order["order_id"], "FILLED", 1, 5001.0))
    assert strategy.position["size"] == 1
    assert len(strategy.execution.updates) == (1 if update_kind == "owned_change" else 1)


@pytest.mark.parametrize("execution_instrument,currency", [(NQ, "USD"), (FDAX, "EUR")])
async def test_position_booked_in_execution_instrument_currency(execution_instrument, currency):
    strategy = make_strategy(instrument=MES, execution_instrument=execution_instrument)
    await place_and_fill(strategy, "BUY", 100.0, execution_instrument)
    assert strategy.position["currency"] == currency


@pytest.mark.parametrize("target", ["own", "other_instrument"])
async def test_broker_position_update_never_books_position(target):
    strategy = make_strategy()
    instrument = MES if target == "own" else FDAX
    strategy.on_position_update({"account_id": "ACC", "instrument": instrument, "size": 3, "avg_cost": 1.0,
                                 "currency": "USD"})
    assert strategy.position["size"] == 0


def test_aux_ready_set_at_build_when_no_aux_name():
    assert make_strategy().execution.aux_ready is True


@pytest.mark.parametrize("late", ["warmup", "aux"])
async def test_late_data_in_position_kept_pending_then_rebuilt_when_flat(late):
    class AuxSignal(StubSignal):
        aux = frozenset({"X"})

    from alchemist.registry import register_signal_dag
    register_signal_dag("aux_signal2")(AuxSignal)
    strategy = make_strategy(strategy_class=type("S", (StubStrategy,), {"signal_dag": "aux_signal2"}))
    strategy.warmup({"daily": [1]})
    await strategy.rebuild_intraday_state([])
    strategy.on_seed_state(True)
    strategy.on_aux_ready(True, {"X": [1]})
    await place_and_fill(strategy, "BUY")
    builds = AuxSignal.builds
    if late == "warmup":
        strategy.warmup({"daily": [7]})
        assert strategy.execution.warmup_ready is False
    else:
        strategy.on_aux_ready(True, {"X": [8]})
        assert strategy.execution.aux_ready is False
    assert AuxSignal.builds == builds
    await place_and_fill(strategy, "SELL")
    await settle()
    assert AuxSignal.builds == builds + 1
    assert strategy.execution.warmup_ready and strategy.execution.aux_ready


@pytest.mark.parametrize("busy", ["position", "open_order", "submitted_order"])
async def test_rebuild_refused_unless_flat_idle(busy):
    strategy = make_strategy()
    strategy.warmup({"daily": [1]})
    await strategy.rebuild_intraday_state([])
    strategy.on_seed_state(True)
    if busy == "position":
        await place_and_fill(strategy, "BUY")
    elif busy == "open_order":
        order = await strategy.om.place_order(MES, "BUY", 1, "LMT", limit_price=1.0)
        strategy.on_order_update(fill_update(order["order_id"], "OPEN", 0))
    else:
        await strategy.om.place_order(MES, "BUY", 1, "MKT")
    builds = StubSignal.builds
    strategy.warmup({"daily": [2]})
    await settle()
    assert StubSignal.builds == builds and strategy.is_flat_idle() is False


async def test_rebuild_then_ready_when_flat_idle():
    StubSignal.warm_after_seed = False
    strategy = make_strategy()
    strategy.warmup({"daily": [1]})
    await strategy.rebuild_intraday_state([])
    strategy.on_seed_state(True)
    assert strategy.execution.warmup_ready is False
    StubSignal.warm_after_seed = True
    strategy.warmup({"daily": [1, 2]})
    assert strategy.execution.warmup_ready is False
    strategy.on_seed_state(True)
    assert strategy.execution.warmup_ready is True


async def test_seed_state_not_ready_keeps_slot_gated():
    strategy = make_strategy()
    strategy.warmup({"daily": [1]})
    strategy.on_seed_state(False)
    assert strategy.execution.warmup_ready is False


async def test_live_bars_held_during_replay_then_stepped_in_order():
    strategy = make_strategy()
    seed_bars = [make_bar(i) for i in range(-5, 0)]
    async with strategy.deferring_bars():
        for index in (1, 2, 3):
            bar = make_bar(index)
            strategy.on_bar(deque([bar]), bar)
        assert strategy.signal.steps == [] and len(strategy.deferred_bars) == 3
    assert [ts for ts, _ in strategy.signal.steps] == [SESSION_OPEN + i for i in (1, 2, 3)]
    assert strategy.deferred_bars is None


async def test_replay_skips_bars_outside_the_session():
    strategy = make_strategy()
    inside, outside = make_bar(5), make_bar(-7200)
    await strategy.rebuild_intraday_state([outside, inside])
    assert [ts for ts, _ in strategy.signal.steps] == [inside["start_ts"]]


@pytest.mark.parametrize("size", [0, 1])
async def test_flatten_closes_booked_position(size):
    strategy = make_strategy(execution=False)
    strategy.warmup({"daily": [1]})
    await strategy.rebuild_intraday_state([])
    strategy.on_seed_state(True)
    bar = make_bar(1)
    strategy.on_bar(deque([bar]), bar)
    if size:
        await place_and_fill(strategy, "BUY")
    placed = len(strategy.om.gateway.placed)
    strategy.request_flatten("session_end", bar["start_ts"])
    await settle()
    orders = strategy.om.gateway.placed[placed:]
    assert [(o["side"], o["order_type"], o["qty"]) for o in orders] == ([("SELL", "MKT", 1)] if size else [])


@pytest.mark.parametrize("record", ["valid", "missing", "corrupt"])
async def test_restore_session_values(record, tmp_path):
    from alchemist.stores import CalcStateStore
    StubSignal.session_state = {"atr": 20.0}
    store = CalcStateStore(tmp_path, "bot")
    if record != "missing":
        first = make_strategy(calc_store=store)
        first.warmup({"daily": [1]})
        first.on_seed_state(True)
        if record == "corrupt":
            next((tmp_path / "bot").glob("*")).write_text("{{")
    second = make_strategy(calc_store=store)
    assert second.signal.restored == ([{"atr": 20.0}] if record == "valid" else [])


async def test_quarantined_slot_never_persists():
    store = InMemoryCalcStateStore()
    StubSignal.session_state = {"atr": 20.0}
    strategy = make_strategy(calc_store=store)
    strategy.quarantine("boom")
    strategy.warmup({"daily": [1]})
    strategy.on_seed_state(True)
    assert store.records == {}


@pytest.mark.parametrize("interval,expected", [(0, 0), (10, 10)])
async def test_snapshot_every_n_bars(interval, expected, caplog):
    strategy = make_strategy(snapshot_interval=interval)
    with caplog.at_level(logging.INFO):
        for index in range(1, 101):
            bar = make_bar(index)
            strategy.on_bar(deque([bar]), bar)
    rows = [r.snapshot_row for r in caplog.records if hasattr(r, "snapshot_row")]
    assert len(rows) == expected and all("stale" not in row for row in rows)
