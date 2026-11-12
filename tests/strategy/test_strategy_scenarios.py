import json
import logging
from collections import deque

import pytest

from alchemist.instruments import FDAX, MES
from alchemist.registry import register_signal_dag
from alchemist.stores import FileDailyStore

from strategy_helpers import (snapshot_rows, SESSION_OPEN, StubSignal, StubStrategy, fill_update, install_stub_signal, make_bar,
                              make_strategy, place_and_fill, settle)


async def seed(strategy, history=None, ready=True):
    strategy.warmup(history or {"daily": [1]})
    await strategy.rebuild_intraday_state([])
    strategy.on_seed_state(ready)


def send_bar(strategy, index, close=100.0):
    bar = make_bar(index, close)
    strategy.on_bar(deque([bar]), bar)
    return bar


async def test_scenario_bar_order():
    strategy = make_strategy()
    execution = strategy.execution
    steps_seen_at_decision = []

    def decide(bar):
        steps_seen_at_decision.append(len(strategy.signal.steps))
        if bar["start_ts"] == SESSION_OPEN + 2:
            strategy.execution.entry_order_bar_ts = bar["start_ts"]

    execution.on_decide = decide
    for index in range(1, 6):
        send_bar(strategy, index)
        if index == 2:
            await place_and_fill(strategy, "BUY", 5001.0)
    assert steps_seen_at_decision == [1, 2, 3, 4, 5]
    data = [d for _, d in strategy.signal.steps]
    assert data[0] == data[1] == {"entry_price": None, "secs_since_entry": None}
    assert [d["entry_price"] for d in data[2:]] == [5001.0] * 3
    assert [d["secs_since_entry"] for d in data[2:]] == [1.0, 2.0, 3.0]


@pytest.mark.parametrize("instrument,currency", [(MES, "USD"), (FDAX, "EUR")])
async def test_scenario_position_from_fills(instrument, currency):
    strategy = make_strategy(instrument=instrument)
    buy = await place_and_fill(strategy, "BUY", 5001.0)
    assert (strategy.position["size"], strategy.position["avg_price"], strategy.position["currency"]) == (
        1, 5001.0, currency)
    strategy.on_order_update(fill_update(buy["order_id"], "FILLED", 1, 5001.0, instrument))
    strategy.on_position_update({"account_id": "ACC", "instrument": instrument, "size": 7, "avg_cost": 1.0,
                                 "currency": currency})
    strategy.on_balance_update({"account_id": "OTHER", "currency": currency, "available": 5.0})
    assert strategy.position["size"] == 1 and strategy.balance["available"] is None
    strategy.on_balance_update({"account_id": "ACC", "currency": currency, "available": 9.0})
    assert strategy.balance == {"currency": currency, "available": 9.0}
    await place_and_fill(strategy, "SELL", 5011.0, instrument)
    assert strategy.position["size"] == 0 and strategy.position["avg_price"] is None


async def test_scenario_late_data_pending_rebuild_flat_later():
    strategy = make_strategy()
    await seed(strategy)
    assert strategy.execution.warmup_ready is True
    await place_and_fill(strategy, "BUY")
    builds = StubSignal.builds
    strategy.warmup({"daily": [1, 2]})
    strategy.warmup({"daily": [1, 2, 3]})
    assert strategy.execution.warmup_ready is False and StubSignal.builds == builds
    await place_and_fill(strategy, "SELL")
    await settle()
    assert StubSignal.builds == builds + 1
    assert strategy.signal.seeds[-1]["daily"] == [1, 2, 3]
    assert strategy.execution.warmup_ready is True


async def test_scenario_late_data_pending_rebuild_session_ends_in_position(caplog):
    strategy = make_strategy()
    await seed(strategy)
    await place_and_fill(strategy, "BUY")
    builds = StubSignal.builds
    strategy.warmup({"daily": [9]})
    with caplog.at_level(logging.WARNING):
        strategy.request_flatten("session_end", SESSION_OPEN + 100)
    assert StubSignal.builds == builds
    assert len([r for r in caplog.records if r.levelno == logging.WARNING and "rebuild" in r.getMessage()]) == 1


async def test_scenario_rebuild_waits_for_orders():
    strategy = make_strategy()
    await seed(strategy)
    order = await strategy.om.place_order(MES, "BUY", 1, "MKT")
    builds = StubSignal.builds
    strategy.warmup({"daily": [5]})
    await settle()
    assert StubSignal.builds == builds
    strategy.on_order_update(fill_update(order["order_id"], "REJECTED", 0))
    await settle()
    assert StubSignal.builds == builds + 1


async def test_scenario_aux_ready_gate():
    class AuxSignal(StubSignal):
        aux = frozenset({"X"})

    register_signal_dag("aux_signal")(AuxSignal)
    plain = make_strategy(name="p")
    aux = make_strategy(name="q", strategy_class=type("AuxStrategy", (StubStrategy,), {"signal_dag": "aux_signal"}))
    assert plain.execution.aux_ready is True and aux.execution.aux_ready is False
    await seed(plain)
    await seed(aux)
    assert (plain.execution.warmup_ready, plain.execution.aux_ready) == (True, True)
    assert (aux.execution.warmup_ready, aux.execution.aux_ready) == (True, False)
    aux.on_aux_ready(True, {"X": [{"date": "d", "value": 1.0}]})
    assert aux.execution.aux_ready is True and aux.signal.seeds[-1]["aux"] == {"X": [{"date": "d", "value": 1.0}]}
    aux.on_aux_ready(False, {})
    assert aux.execution.aux_ready is False


async def test_scenario_flatten_hand_off_request():
    strategy = make_strategy()
    await place_and_fill(strategy, "BUY", 5001.0)
    strategy.request_flatten("shutdown", SESSION_OPEN + 5)
    assert [(c[0]["size"], c[1]) for c in strategy.execution.flatten_calls] == [(1, "shutdown")]


async def test_scenario_flatten_hand_off_wanted():
    strategy = make_strategy()
    await place_and_fill(strategy, "BUY", 5001.0)
    strategy.execution.wanted = True
    for index in (1, 2, 3):
        send_bar(strategy, index)
    assert [(c[0]["size"], c[1]) for c in strategy.execution.flatten_calls] == [(1, "exit_failed")] * 3


@pytest.mark.parametrize("case", ["normal", "corrupt", "quarantined"])
async def test_scenario_restart_restore(case, tmp_path):
    StubSignal.session_state = {"atr": 20.0}
    store = FileDailyStore(tmp_path, "bot", "calc_state")
    saves = []
    original = store.save
    store.save = lambda *args: (saves.append(args), original(*args))[1]
    first = make_strategy(calc_store=store)
    if case == "quarantined":
        first.quarantine("boom")
    await seed(first)
    first.on_seed_state(True)
    if case == "corrupt":
        next((tmp_path / "bot").glob("*")).write_text("{broken")
    assert len(saves) == (0 if case == "quarantined" else 1)
    second = make_strategy(calc_store=store)
    if case == "normal":
        assert second.signal.restored == [{"atr": 20.0}]
    else:
        assert second.signal.restored == []


async def test_scenario_snapshots(caplog):
    for interval, expected in ((0, 0), (60, 3)):
        strategy = make_strategy(snapshot_interval=interval)
        caplog.clear()
        with caplog.at_level(logging.INFO):
            for index in range(1, 181):
                send_bar(strategy, index)
        rows = snapshot_rows(caplog)
        assert len(rows) == expected
        assert all(row["instrument"] == "MES" and row["event"] == "signal_snapshot" and "stale" not in row for row in rows)
    strategy = make_strategy(snapshot_interval=60)
    strategy.quarantine("boom")
    caplog.clear()
    with caplog.at_level(logging.INFO):
        for index in range(1, 181):
            send_bar(strategy, index)
    assert [row["stale"] for row in snapshot_rows(caplog)] == [True] * 3


@pytest.mark.parametrize("case", ["flat_later_in_session", "session_ends_in_position"])
async def test_scenario_late_data_pending_rebuild(case, caplog):
    if case == "flat_later_in_session":
        await test_scenario_late_data_pending_rebuild_flat_later()
    else:
        await test_scenario_late_data_pending_rebuild_session_ends_in_position(caplog)


@pytest.mark.parametrize("case", ["flatten_request", "flatten_wanted"])
async def test_scenario_flatten_hand_off(case):
    if case == "flatten_request":
        await test_scenario_flatten_hand_off_request()
    else:
        await test_scenario_flatten_hand_off_wanted()
