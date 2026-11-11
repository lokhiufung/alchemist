import logging

import pytest

from alchemist.strategy_builder.strategy_executions import ExecutionConfigError, make_execution

from exec_helpers import (PROTECTED_BLOCK, STOP_LOSS_BLOCK, Slot, enter_protected, protected_signal, protected_slot,
                          settle)


async def entered(slot, **kw):
    return await enter_protected(slot, **kw)


def market_orders(slot):
    return [o for o in slot.orders if o["order_type"] == "MKT"][1:]


@pytest.mark.parametrize("case", ["crossed_stop", "missing_level", "zero_fill"])
async def test_no_group_for_crossed_stop_missing_level_or_zero_fill(case):
    slot = protected_slot()
    slot.signal.entries = {1}
    if case == "missing_level":
        slot.signal.levels = {"stop": None, "target": 110.0}
    await slot.bar(1)
    if case == "zero_fill":
        await slot.update(slot.orders[0]["order_id"], "REJECTED")
    else:
        await slot.fill_last(100.0)
    await slot.bar(2, close=97.0 if case == "crossed_stop" else 100.0)
    await slot.bar(3, close=97.0)
    assert [o for o in slot.orders if o["oca_group"]] == []


async def test_group_sized_to_filled_quantity_once_per_trade():
    slot = protected_slot()
    trade = await entered(slot)
    for index in range(3, 8):
        await slot.bar(index)
    legs = [o for o in slot.orders if o["oca_group"]]
    assert len(legs) == 2 and {o["qty"] for o in legs} == {1}


@pytest.mark.parametrize("direction,price,exit_side", [("long", 93.0, "SELL"), ("short", 107.0, "BUY")])
async def test_stop_loss_level_moved_by_offset_against_trade(direction, price, exit_side):
    slot = protected_slot(STOP_LOSS_BLOCK, direction=direction)
    slot.signal.node_levels = {"swing_low": 95.0 if direction == "long" else 105.0}
    await entered(slot)
    leg = slot.orders[1]
    assert (leg["order_type"], leg["stop_price"], leg["side"]) == ("STP", price, exit_side)


@pytest.mark.parametrize("phase,sent", [("IN_POSITION_PROTECTED", False), ("PROCESSING_PROTECTION_ORDERS", True),
                                        ("IN_POSITION_ACTIVITY", True)])
async def test_selected_condition_masked_only_when_every_leg_open(phase, sent):
    slot = protected_slot()
    trade = await entered(slot, ack_legs=phase != "PROCESSING_PROTECTION_ORDERS")
    if phase == "IN_POSITION_ACTIVITY":
        await slot.update(trade.legs["LMT"], "REJECTED")
    assert slot.execution.state.value == phase
    slot.signal.exits = {3: {"stop": True}}
    await slot.bar(3)
    assert bool(market_orders(slot)) is sent


async def test_selected_true_with_other_true_sends_no_exit_and_logs_skip(caplog):
    slot = protected_slot()
    await entered(slot)
    slot.signal.exits = {3: {"stop": True, "end_of_day": True}}
    with caplog.at_level(logging.INFO):
        await slot.bar(3)
    assert market_orders(slot) == [] and any("skipped" in r.getMessage() for r in caplog.records)


async def test_non_selected_alone_sends_exit_when_every_leg_open():
    slot = protected_slot()
    await entered(slot)
    slot.signal.exits = {3: {"end_of_day": True}}
    await slot.bar(3)
    assert len(market_orders(slot)) == 1


async def test_stop_loss_class_masks_nothing():
    slot = protected_slot(STOP_LOSS_BLOCK)
    await entered(slot)
    slot.signal.exits = {3: {"stop": True}}
    await slot.bar(3)
    assert len(market_orders(slot)) == 1


@pytest.mark.parametrize("alive,expect_group", [(2, True), (1, True), (0, False)])
async def test_market_exit_joins_group_while_leg_alive(alive, expect_group):
    slot = protected_slot()
    trade = await entered(slot)
    if alive == 1:
        await slot.update(trade.legs["STP"], "REJECTED")
    if alive == 0:
        await slot.update(trade.legs["STP"], "REJECTED")
        await slot.update(trade.legs["LMT"], "CANCELLED")
    slot.signal.exits = {3: {"end_of_day": True}}
    await slot.bar(3)
    exit_order = market_orders(slot)[-1]
    assert (exit_order["oca_group"] == trade.group) is expect_group
    assert (exit_order["oca_type"] == 2) is expect_group
    assert slot.gateway.cancelled == []


@pytest.mark.parametrize("phase", ["PROCESSING_PROTECTION_ORDERS", "IN_POSITION_PROTECTED", "PROCESSING_EXIT_ORDER"])
async def test_leg_fill_completes_one_round_trip(phase):
    slot = protected_slot()
    trade = await entered(slot, ack_legs=phase != "PROCESSING_PROTECTION_ORDERS")
    if phase == "PROCESSING_EXIT_ORDER":
        slot.signal.exits = {3: {"end_of_day": True}}
        await slot.bar(3)
    orders_before = len(slot.orders)
    await slot.update(trade.legs["STP"], "FILLED", filled=1, avg=98.0)
    await slot.update(trade.legs["LMT"], "CANCELLED")
    assert slot.store.count("slot", "2026-04-15") == 1 and len(slot.orders) == orders_before
    assert slot.execution.consume_trade_closed() is True


@pytest.mark.parametrize("ending", ["REJECTED", "CANCELLED"])
async def test_lost_leg_falls_back_without_replacement(ending, caplog):
    slot = protected_slot()
    trade = await entered(slot)
    groups_before = len([o for o in slot.orders if o["oca_group"]])
    with caplog.at_level(logging.INFO):
        await slot.update(trade.legs["STP"], ending)
    assert slot.execution.state.value == "IN_POSITION_ACTIVITY"
    await slot.bar(3)
    await slot.bar(4)
    assert len([o for o in slot.orders if o["oca_group"]]) == groups_before
    assert len([r for r in caplog.records if r.levelno == logging.CRITICAL]) == 1


async def test_surviving_leg_is_left_working_after_fallback():
    slot = protected_slot()
    trade = await entered(slot)
    await slot.update(trade.legs["STP"], "REJECTED")
    assert slot.gateway.cancelled == []
    assert [o["order_id"] for o in slot.om.get_open_orders()] == [trade.legs["LMT"]]


async def test_failed_joined_exit_returns_to_protected_when_legs_still_open():
    slot = protected_slot()
    trade = await entered(slot)
    slot.signal.exits = {3: {"end_of_day": True}}
    await slot.bar(3)
    await slot.update(market_orders(slot)[-1]["order_id"], "REJECTED")
    assert slot.execution.state.value == "IN_POSITION_PROTECTED"
    await slot.bar(4)
    assert slot.execution.flatten_wanted() is True and len(market_orders(slot)) == 1


@pytest.mark.parametrize("options,message", [
    ({"class": "ProtectedStrategyExecution"}, "resting_exits"),
    ({"class": "ProtectedStrategyExecution", "resting_exits": {}}, "resting_exits"),
    ({"class": "ProtectedStrategyExecution", "resting_exits": {"stop": "STP", "end_of_day": "STP"}}, "one"),
    ({"class": "ProtectedStrategyExecution", "resting_exits": {"stop": "MKT"}}, "STP"),
    ({"class": "ProtectedStrategyExecution", "resting_exits": {"nope": "STP"}}, "nope"),
    ({"class": "ProtectedStrategyExecution", "resting_exits": {"stop": "STP"}, "extra": 1}, "extra"),
    ({"class": "StopLossProtectedStrategyExecution", "level_name": "swing_low"}, "offset_points"),
    ({"class": "StopLossProtectedStrategyExecution", "level_name": "stop", "offset_points": 1.0}, "stop"),
    ({"class": "StopLossProtectedStrategyExecution", "level_name": "missing", "offset_points": 1.0}, "missing"),
    ({"class": "StopLossProtectedStrategyExecution", "level_name": "swing_low", "offset_points": True}, "offset_points"),
])
def test_make_execution_refuses_bad_protected_blocks(options, message):
    slot = Slot(protected_signal(), ready=False)
    with pytest.raises(ExecutionConfigError, match=message):
        make_execution(options, slot.signal, slot.om, instrument=None, account_id="SIM", slot="my_slot",
                       session="NY_RTH", watch_time=slot.clock, roundtrip_store=slot.store)


def test_make_execution_builds_both_protected_classes():
    slot = Slot(protected_signal(), ready=False)
    common = dict(instrument=None, account_id="SIM", slot="s", session="NY_RTH", watch_time=slot.clock, roundtrip_store=slot.store)
    one = make_execution(PROTECTED_BLOCK, slot.signal, slot.om, **common)
    two = make_execution(STOP_LOSS_BLOCK, slot.signal, slot.om, **common)
    assert (type(one).__name__, type(two).__name__) == ("ProtectedStrategyExecution",
                                                      "StopLossProtectedStrategyExecution")
