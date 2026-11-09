import logging
from datetime import date

import pytest

from alchemist.instruments import MES
from alchemist.stores import InMemoryRoundTripStore

from exec_helpers import ScriptedSignal, Slot, drive_to, make_bar, position, settle

NEXT_DAY = 86_400


@pytest.mark.parametrize("cap", [1, 2])
async def test_scenario_bar_close_round_trip(cap):
    signal = ScriptedSignal(cap=cap)
    signal.entries, signal.exits = {3, 8}, {7: {"target": True}}
    slot = Slot(signal)
    placed_at_freeze = []
    original = signal.freeze_exit_inputs
    signal.freeze_exit_inputs = lambda bar: (placed_at_freeze.append(len(slot.orders)), original(bar))[1]

    for index in range(1, 8):
        await slot.bar(index)
        if index == 3:
            await slot.fill_last(100.0)
        if index == 7:
            await slot.fill_last(110.0)
    assert slot.sides() == [("BUY", "MKT", 1), ("SELL", "MKT", 1)] and placed_at_freeze == [0]
    assert ("release", 7) in signal.calls and slot.store.count("slot", "2026-04-15") == 1
    await slot.bar(8)
    assert len(slot.orders) == (2 if cap == 1 else 3)
    assert [c for c in signal.calls if c[0] == "step"] == [("step", i) for i in range(1, 9)]
    assert slot.execution.state.value == ("DONE" if cap == 1 else "PROCESSING_ENTRY_ORDER")


async def test_scenario_dead_entry():
    signal = ScriptedSignal()
    signal.entries = {3, 5}
    slot = Slot(signal)
    for index in (1, 2, 3):
        await slot.bar(index)
    await slot.update(slot.orders[0]["order_id"], "REJECTED", reason="no margin")
    assert slot.execution.state.value == "ENTRY_LOOP_ACTIVITY" and ("release", 3) in signal.calls
    await slot.bar(4)
    assert len(slot.orders) == 1
    await slot.bar(5)
    assert len(slot.orders) == 2 and slot.orders[1]["side"] == "BUY"


@pytest.mark.parametrize("session", ["NY_RTH", "NY_GLOBEX"])
async def test_scenario_ready_and_session_gate(session):
    signal = ScriptedSignal(cap=1)
    signal.entries = set(range(0, 200_000))
    slot = Slot(signal, session=session, ready=False)
    await slot.bar(1)
    assert not [c for c in signal.calls if c[0] == "entry?"]
    slot.execution.set_warmup_ready()
    slot.execution.set_aux_ready()
    outside = make_bar(0, open_ts=slot.open_ts - 3600)
    steps_before = signal.steps
    await slot.raw_bar(outside)
    assert signal.steps == steps_before and not slot.orders
    await slot.bar(2)
    await slot.fill_last()
    slot.signal.exits = {3: {"target": True}}
    await slot.bar(3)
    await slot.fill_last()
    assert slot.execution.state.value == "DONE"
    midnight_for_globex = 6 * 3600 + 5
    await slot.bar(midnight_for_globex)
    assert slot.execution.state.value == "DONE"
    await slot.bar(NEXT_DAY + 10)
    assert slot.execution.state.value == "PROCESSING_ENTRY_ORDER"
    assert slot.store.count("slot", "2026-04-15") == 1


PHASES = [("WARMUP", False), ("WAITING_FOR_WORKING_HOURS", False), ("ENTRY_LOOP_ACTIVITY", False),
          ("PROCESSING_ENTRY_ORDER", True), ("IN_POSITION_ACTIVITY", False), ("PROCESSING_EXIT_ORDER", True),
          ("SESSION_END_FLATTENING", True), ("DONE", False), ("ERROR", False)]


async def test_scenario_processing_answer():
    answers = {}
    for phase, _ in PHASES:
        slot = Slot(ready=phase != "WARMUP")
        await drive_to(slot, phase)
        assert slot.execution.state.value == phase, phase
        answers[phase] = slot.execution.is_processing_order()
    assert answers == dict(PHASES)


async def test_flattening_is_not_processing_once_nothing_may_fill():
    slot = Slot()
    await drive_to(slot, "SESSION_END_FLATTENING")
    await slot.fill_last(100.0)
    assert slot.execution.state.value == "SESSION_END_FLATTENING"
    assert slot.execution.is_processing_order() is False


async def test_scenario_failed_exit(caplog):
    signal = ScriptedSignal()
    signal.entries, signal.exits = {1}, {3: {"target": True}}
    slot = Slot(signal)
    await slot.bar(1)
    await slot.fill_last()
    await slot.bar(2)
    with caplog.at_level(logging.INFO):
        await slot.bar(3)
        exit_id = slot.orders[-1]["order_id"]
        await slot.update(exit_id, "REJECTED")
        await slot.bar(4)
    assert [r for r in caplog.records if r.levelno == logging.CRITICAL]
    assert len(slot.orders) == 2 and slot.execution.flatten_wanted() is True
    criticals_after_exit = len([r for r in caplog.records if r.levelno == logging.CRITICAL])
    slot.execution.request_flatten(position(1), "exit_failed")
    await settle()
    assert len(slot.orders) == 3 and "oca_group" in slot.orders[2] and slot.orders[2]["oca_group"] is None
    with caplog.at_level(logging.INFO):
        await slot.update(slot.orders[2]["order_id"], "REJECTED")
        for index in (5, 6, 7):
            await slot.bar(index)
            slot.execution.request_flatten(position(1), "exit_failed")
            await settle()
    assert len(slot.orders) == 3
    assert len([r for r in caplog.records if r.levelno == logging.CRITICAL]) > criticals_after_exit
    assert slot.execution.flatten_wanted() is False


@pytest.mark.parametrize("case", ["fills", "rejected_then_quarantined"])
async def test_scenario_session_end_flatten(case):
    signal = ScriptedSignal()
    signal.entries = {1}
    slot = Slot(signal)
    await slot.bar(1)
    await slot.fill_last()
    working = await slot.om.place_order(MES, "SELL", 1, "LMT", limit_price=200.0)
    await slot.update(working["order_id"], "OPEN")
    count_before = slot.store.count("slot", "2026-04-15")

    slot.execution.request_flatten(position(1), "session_end")
    await settle()
    assert slot.gateway.cancelled == [working["order_id"]] and len(slot.orders) == 2
    slot.execution.request_flatten(position(1), "session_end")
    await settle()
    assert len(slot.orders) == 2
    await slot.update(working["order_id"], "CANCELLED")
    slot.execution.request_flatten(position(1), "session_end")
    await settle()
    assert slot.sides()[-1] == ("SELL", "MKT", 1) and slot.orders[-1]["oca_group"] is None
    flatten_id = slot.orders[-1]["order_id"]

    if case == "fills":
        await slot.fill(flatten_id, 100.0)
        slot.execution.request_flatten(position(0), "session_end")
        await slot.bar(2)
        assert slot.execution.state.value == "DONE"
        assert slot.store.count("slot", "2026-04-15") == count_before
        return
    await slot.update(flatten_id, "REJECTED")
    slot.execution.quarantine("boom")
    slot.execution.release_quarantine()
    slot.execution.request_flatten(position(1), "session_end")
    await settle()
    await slot.bar(3)
    assert len(slot.orders) == 3


@pytest.mark.parametrize("case", ["seeking", "done"])
async def test_scenario_quarantine(case, caplog):
    signal = ScriptedSignal()
    signal.entries = set(range(0, 20_000))
    signal.exits = {2: {"target": True}}
    slot = Slot(signal)
    if case == "done":
        signal.entries = {1}
        await slot.bar(1)
        await slot.fill_last()
        await slot.bar(2)
        await slot.fill_last()
        signal.entries = set(range(0, 20_000))
        assert slot.execution.state.value == "DONE"
    placed = len(slot.orders)
    slot.execution.quarantine("test")
    with caplog.at_level(logging.WARNING):
        for step in range(70):
            await slot.bar(10 + step * 10)
    assert len(slot.orders) == placed
    reminders = [r for r in caplog.records if "quarantine" in r.getMessage().lower()]
    assert 1 <= len(reminders) <= 3
    slot.execution.release_quarantine()
    assert slot.execution.state.value == ("WAITING_FOR_WORKING_HOURS" if case == "seeking" else "DONE")


@pytest.mark.parametrize("case", ["same_day", "next_day", "no_store"])
async def test_scenario_count_restart(case):
    store = InMemoryRoundTripStore()
    if case != "no_store":
        store.record("slot", "2026-04-15")
    signal = ScriptedSignal(cap=1)
    signal.entries = set(range(0, 100_000))
    slot = Slot(signal, store=store)
    await slot.bar(1 if case != "next_day" else NEXT_DAY + 1)
    if case == "same_day":
        assert slot.execution.state.value == "DONE" and not slot.orders
    else:
        assert slot.execution.state.value == "PROCESSING_ENTRY_ORDER" and len(slot.orders) == 1
