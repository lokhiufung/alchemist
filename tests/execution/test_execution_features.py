import logging
import re
from pathlib import Path

import pytest

from alchemist.stores import InMemoryRoundTripStore
from alchemist.strategy_builder.strategy_executions import ExecutionConfigError, StrategyExecution, make_execution

from exec_helpers import ScriptedSignal, Slot, drive_to, make_bar, position, session_open_ts, settle

NEXT_DAY = 86_400


async def test_at_most_one_order_in_flight():
    signal = ScriptedSignal()
    signal.entries = set(range(1, 8))
    slot = Slot(signal)
    for index in range(1, 8):
        await slot.bar(index)
    assert len(slot.orders) == 1
    await slot.fill_last()
    signal.exits = {9: {"target": True}, 10: {"target": True}}
    for index in (8, 9, 10):
        await slot.bar(index)
    assert len(slot.orders) == 2


@pytest.mark.parametrize("cap", [1, 2])
async def test_daily_cap(cap):
    signal = ScriptedSignal(cap=cap)
    signal.entries = {1, 3, 5, 7, 9}
    signal.exits = {2: {"target": True}, 4: {"target": True}, 6: {"target": True}, 8: {"target": True}}
    slot = Slot(signal)
    for index in range(1, 11):
        await slot.bar(index)
        if slot.orders and slot.om.get_submitted_orders():
            await slot.fill_last()
    assert slot.store.count("slot", "2026-04-15") == cap and len(slot.orders) == 2 * cap


@pytest.mark.parametrize("ends_by", ["exit_fill", "entry_dies"])
async def test_freeze_before_entry_release_after(ends_by):
    signal = ScriptedSignal()
    signal.entries, signal.exits = {1}, {2: {"target": True}}
    slot = Slot(signal)
    log = []
    freeze, release = signal.freeze_exit_inputs, signal.release_exit_inputs
    signal.freeze_exit_inputs = lambda bar: (log.append(("freeze", len(slot.orders))), freeze(bar))[1]
    signal.release_exit_inputs = lambda: (log.append(("release", len(slot.orders))), release())[1]
    await slot.bar(1)
    if ends_by == "entry_dies":
        await slot.update(slot.orders[0]["order_id"], "REJECTED")
        assert log == [("freeze", 0), ("release", 1)]
        return
    await slot.fill_last()
    await slot.bar(2)
    assert log == [("freeze", 0)]
    await slot.fill_last()
    assert log == [("freeze", 0), ("release", 2)]


@pytest.mark.parametrize("phase", [p for p, _ in __import__("test_execution_scenarios").PHASES])
async def test_processing_order_answer_per_phase(phase):
    expected = dict(__import__("test_execution_scenarios").PHASES)[phase]
    slot = Slot(ready=phase != "WARMUP")
    await drive_to(slot, phase)
    assert slot.execution.is_processing_order() is expected


def test_no_caller_reads_state_names():
    root = Path("alchemist")
    offenders = [p for p in root.rglob("*.py") if "strategy_executions" not in p.parts
                 and re.search(r"\bPhase\b|execution\.state\b", p.read_text())]
    assert offenders == []


@pytest.mark.parametrize("case", ["not_warm", "aux_not_ready", "out_of_session", "half_session_holiday"])
async def test_no_entry_when_not_warm_or_out_of_session(case):
    signal = ScriptedSignal()
    signal.entries = set(range(0, 100_000))
    slot = Slot(signal, ready=case in ("out_of_session", "half_session_holiday"))
    if case == "aux_not_ready":
        slot.execution.set_warmup_ready()
    if case == "not_warm":
        slot.execution.set_aux_ready()
    if case == "out_of_session":
        await slot.raw_bar(make_bar(0, open_ts=slot.open_ts - 7200))
    elif case == "half_session_holiday":
        from datetime import date
        holiday_open = __import__("alchemist.exchanges", fromlist=["x"]).exchange_profile("CME").session_window(
            "NY_RTH", date(2026, 11, 27))[0].timestamp()
        await slot.raw_bar(make_bar(100, open_ts=holiday_open))
    else:
        await slot.bar(1)
    assert slot.orders == []


@pytest.mark.parametrize("phase", ["ENTRY_LOOP_ACTIVITY", "PROCESSING_ENTRY_ORDER", "IN_POSITION_ACTIVITY",
                                   "PROCESSING_EXIT_ORDER", "SESSION_END_FLATTENING", "DONE", "ERROR"])
async def test_signal_stepped_once_per_in_session_bar_in_every_phase(phase):
    slot = Slot()
    await drive_to(slot, phase)
    before = len(slot.signal.calls)
    await slot.bar(50)
    new_calls = slot.signal.calls[before:]
    assert [c for c in new_calls if c[0] == "step"] == [("step", 50)]
    assert new_calls[0] == ("step", 50)


async def test_daily_reset_at_overnight_session_roll():
    signal = ScriptedSignal(cap=1)
    signal.entries, signal.exits = {1}, {2: {"target": True}}
    slot = Slot(signal, session="NY_GLOBEX")
    await slot.bar(1)
    await slot.fill_last()
    await slot.bar(2)
    await slot.fill_last()
    assert slot.execution.state.value == "DONE"
    signal.entries = set(range(0, 200_000))
    await slot.bar(6 * 3600 + 30)
    assert slot.execution.state.value == "DONE"
    await slot.bar(NEXT_DAY + 5)
    assert slot.execution.state.value == "PROCESSING_ENTRY_ORDER"


@pytest.mark.parametrize("persisted,phase", [(0, "PROCESSING_ENTRY_ORDER"), (1, "PROCESSING_ENTRY_ORDER"), (2, "DONE")])
async def test_restart_restores_cap(persisted, phase):
    store = InMemoryRoundTripStore()
    for _ in range(persisted):
        store.record("slot", "2026-04-15")
    signal = ScriptedSignal(cap=2)
    signal.entries = {1}
    slot = Slot(signal, store=store)
    await slot.bar(1)
    assert slot.execution.state.value == phase


async def test_no_store_starts_from_zero():
    signal = ScriptedSignal(cap=1)
    signal.entries = {1}
    slot = Slot(signal, store=InMemoryRoundTripStore())
    await slot.bar(1)
    assert len(slot.orders) == 1


async def test_error_slot_sends_no_order():
    signal = ScriptedSignal()
    signal.entries = set(range(0, 100))
    slot = Slot(signal)
    slot.execution.quarantine("boom")
    for index in range(1, 20):
        await slot.bar(index)
    assert slot.orders == [] and slot.execution.is_quarantined() is True


@pytest.mark.parametrize("while_", ["trading", "done"])
async def test_release_from_quarantine(while_):
    slot = Slot()
    await drive_to(slot, "ENTRY_LOOP_ACTIVITY" if while_ == "trading" else "DONE")
    slot.execution.quarantine("boom")
    slot.execution.release_quarantine()
    assert slot.execution.is_quarantined() is False
    assert slot.execution.state.value == ("WAITING_FOR_WORKING_HOURS" if while_ == "trading" else "DONE")


@pytest.mark.parametrize("size", [0, 1])
@pytest.mark.parametrize("working", ["none", "open"])
async def test_flatten_waits_then_one_market(size, working):
    slot = Slot()
    await slot.bar(1)
    order_id = None
    if working == "open":
        order = await slot.om.place_order(__import__("alchemist.instruments", fromlist=["x"]).MES, "SELL", 1, "LMT",
                                          limit_price=500.0)
        order_id = order["order_id"]
        await slot.update(order_id, "OPEN")
    market_while_working = []
    slot.execution.request_flatten(position(size), "session_end")
    await settle()
    market_while_working += [o for o in slot.orders if o["order_type"] == "MKT"] if working == "open" else []
    if working == "open":
        await slot.update(order_id, "CANCELLED")
        slot.execution.request_flatten(position(size), "session_end")
        await settle()
    flatten_orders = [o for o in slot.orders if o["order_type"] == "MKT"]
    assert market_while_working == [] and len(flatten_orders) == size


async def test_rejected_flatten_not_resent():
    slot = Slot()
    await slot.bar(1)
    slot.execution.request_flatten(position(1), "session_end")
    await settle()
    await slot.update(slot.orders[-1]["order_id"], "REJECTED")
    for _ in range(3):
        slot.execution.request_flatten(position(1), "session_end")
        await settle()
        await slot.bar(5)
    assert len([o for o in slot.orders if o["order_type"] == "MKT"]) == 1


@pytest.mark.parametrize("ending", ["REJECTED", "CANCELLED"])
async def test_failed_exit_never_resent_flattens_next_bar(ending):
    signal = ScriptedSignal()
    signal.entries, signal.exits = {1}, {3: {"target": True}, 4: {"target": True}}
    slot = Slot(signal)
    await slot.bar(1)
    await slot.fill_last()
    await slot.bar(2)
    await slot.bar(3)
    await slot.update(slot.orders[-1]["order_id"], ending)
    await slot.bar(4)
    assert len(slot.orders) == 2 and slot.execution.flatten_wanted() is True


def test_make_execution_picks_base_class_and_refuses_bad_blocks():
    slot = Slot()
    assert type(slot.execution) is StrategyExecution
    kwargs = dict(instrument=None, account_id="SIM", slot="my_slot", session="NY_RTH", watch_time=slot.clock,
                  roundtrip_store=slot.store)
    with pytest.raises(ExecutionConfigError, match="my_slot"):
        make_execution({"class": "NoSuchExecution"}, slot.signal, slot.om, **kwargs)
    with pytest.raises(ExecutionConfigError, match="my_slot"):
        make_execution({"class": "StrategyExecution", "surprise": 1}, slot.signal, slot.om, **kwargs)
    assert type(make_execution({"class": "StrategyExecution"}, slot.signal, slot.om, **kwargs)) is StrategyExecution
