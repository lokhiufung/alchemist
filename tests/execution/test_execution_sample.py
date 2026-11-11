from datetime import date, datetime, timezone

from alchemist.exchanges import exchange_profile
from alchemist.gateways.simulation_gateway.tapes import tape_from_spec
from alchemist.instruments import MES

import alchemist.gateways  # noqa: F401
from exec_helpers import ScriptedSignal, Slot, settle

ENTRY_INDEX, TARGET_INDEX = 601, 618


async def test_golden_macd_entry_then_target_exit():
    signal = ScriptedSignal(cap=1, exit_names=("stop", "target", "end_of_day"))
    signal.entries = {ENTRY_INDEX}
    signal.exits = {TARGET_INDEX: {"target": True}}
    slot = Slot(signal)
    start, end = exchange_profile(MES).session_window("NY_RTH", date(2026, 4, 15))
    bars = list(tape_from_spec("synthetic:golden_session").bars(MES, start, end))
    assert len(bars) == 23_400

    processing = []
    for index, bar in enumerate(bars):
        slot.clock.advance(bar["end_ts"])
        signal.index = index
        slot.execution.on_bar(bar, signal.step)
        await settle()
        if slot.orders and index in (ENTRY_INDEX, TARGET_INDEX):
            processing.append(slot.execution.is_processing_order())
            await slot.fill_last(bar["close"])
            processing.append(slot.execution.is_processing_order())
        elif index > TARGET_INDEX + 5:
            assert not slot.execution.is_processing_order()

    assert signal.steps == 23_400
    assert slot.sides() == [("BUY", "MKT", 1), ("SELL", "MKT", 1)]
    assert processing == [True, False, True, False]
    assert slot.execution.state.value == "DONE"
    assert slot.execution.consume_trade_closed() is True


import logging

import pytest

from exec_helpers import (PROTECTED_BLOCK, STOP_LOSS_BLOCK, Trade, enter_protected, position, protected_slot)


@pytest.mark.parametrize("case", ["A", "B", "B2", "C", "D", "E", "stop_loss"])
async def test_protected_sample(case, caplog):
    if case == "stop_loss":
        slot = protected_slot(STOP_LOSS_BLOCK)
        trade = await enter_protected(slot)
        assert [o["order_type"] for o in slot.orders if o["oca_group"]] == ["STP"]
        assert slot.orders[1]["stop_price"] == 93.0 and slot.orders[1]["side"] == "SELL"
        slot.signal.exits = {3: {"stop": True}}
        await slot.bar(3)
        assert slot.sides()[-1] == ("SELL", "MKT", 1) and slot.orders[-1]["oca_group"] == trade.group
        return
    slot = protected_slot()
    ack = case != "D"
    trade = await enter_protected(slot, ack_legs=ack)
    group, legs = trade.group, trade.legs
    assert group and set(legs) == {"STP", "LMT"} and len(slot.orders) == 3
    assert slot.orders[1]["stop_price"] == 98.0 and slot.orders[2]["limit_price"] == 110.0
    market_after_group = lambda: [o for o in slot.orders[3:] if o["order_type"] == "MKT"]

    if case == "A":
        await slot.update(legs["STP"], "FILLED", filled=1, avg=98.0)
        await slot.update(legs["LMT"], "CANCELLED")
        assert not market_after_group()
    elif case == "B":
        slot.signal.exits = {3: {"end_of_day": True}}
        await slot.bar(3)
        assert slot.gateway.cancelled == []
        assert [o["oca_group"] for o in market_after_group()] == [group]
        await slot.update(legs["STP"], "CANCELLED")
        await slot.update(legs["LMT"], "CANCELLED")
        await slot.fill_last(99.0)
    elif case == "B2":
        slot.signal.exits = {3: {"end_of_day": True}}
        await slot.bar(3)
        await slot.update(legs["STP"], "FILLED", filled=1, avg=98.0)
        await slot.update(slot.orders[-1]["order_id"], "REJECTED")
        await slot.bar(4)
        assert len(market_after_group()) == 1
    elif case == "C":
        await slot.update(legs["STP"], "REJECTED")
        assert slot.execution.state.value == "IN_POSITION_ACTIVITY"
        slot.signal.exits = {3: {"stop": True}}
        await slot.bar(3)
        assert [o["oca_group"] for o in market_after_group()] == [group]
        assert len([o for o in slot.orders if o["oca_group"] and o["order_type"] != "MKT"]) == 2
        await slot.fill_last(98.0)
    elif case == "D":
        await slot.update(legs["STP"], "FILLED", filled=1, avg=98.0)
        await slot.update(legs["LMT"], "CANCELLED")
        assert not market_after_group()
    elif case == "E":
        slot.execution.request_flatten(position(1), "session_end")
        await settle()
        assert sorted(slot.gateway.cancelled) == sorted(legs.values()) and not market_after_group()
        await slot.update(legs["STP"], "CANCELLED")
        slot.execution.request_flatten(position(1), "session_end")
        await settle()
        assert not market_after_group()
        await slot.update(legs["LMT"], "CANCELLED")
        slot.execution.request_flatten(position(1), "session_end")
        await settle()
        assert [o["oca_group"] for o in market_after_group()] == [None]
        await slot.fill_last(100.0)
        slot.execution.request_flatten(position(0), "session_end")
        await settle()
        assert slot.execution.state.value == "DONE" and slot.store.count("slot", "2026-04-15") == 0
        return

    assert slot.store.count("slot", "2026-04-15") == 1
    assert slot.execution.state.value == "DONE"
    groups = {o["oca_group"] for o in slot.orders if o["oca_group"]}
    assert groups == {group}
    for market in market_after_group():
        assert market["oca_group"] == group
    assert not slot.gateway.cancelled
