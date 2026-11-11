import math

import pytest

from alchemist.exchanges import exchange_profile
from alchemist.instruments import FDAX, MES, AssetClass, Instrument

from gateway_helpers import Driver, driver, make_order

TICK = 0.25
NO_TICK = Instrument(AssetClass.FUTURE, "ZZ", "CME", "USD", 1.0, 0.0)


@pytest.mark.parametrize("slippage", [0, 2])
async def test_scenario_market_order_fills(slippage):
    d = driver({"slippage_ticks": slippage})
    d.show(5000.0)
    buy = make_order(d.gateway, "MKT", "BUY")
    await d.gateway.place_order(buy)
    end_of_bar = d.gateway.tape_time
    d.show(5002.0)
    sell = make_order(d.gateway, "MKT", "SELL")
    await d.gateway.place_order(sell)

    assert d.statuses(buy["order_id"])[-1] == "FILLED" and d.statuses(sell["order_id"])[-1] == "FILLED"
    fills = {u["order_id"]: u for u in d.updates() if u["status"] == "FILLED"}
    assert fills[buy["order_id"]]["avg_fill_price"] == 5000.0 + slippage * TICK
    assert fills[buy["order_id"]]["ts"] == end_of_bar
    assert fills[sell["order_id"]]["avg_fill_price"] == 5002.0 - slippage * TICK
    assert sell["order_id"] > buy["order_id"]
    rows = await d.gateway.get_executions()
    assert [(r["order_id"], r["side"], r["qty"], r["currency"]) for r in rows] == [
        (buy["order_id"], "BUY", 1, "USD"), (sell["order_id"], "SELL", 1, "USD")]
    sizes = [p["size"] for p in d.buffer.of_type("position_update")]
    assert sizes == [1, 0]


async def test_scenario_limit_order_rests_rounded():
    d = driver()
    d.show(5000.0)
    buy = make_order(d.gateway, "LMT", "BUY", limit_price=4900.10)
    sell = make_order(d.gateway, "LMT", "SELL", limit_price=5100.10)
    await d.gateway.place_order(buy)
    await d.gateway.place_order(sell)
    for _ in range(3):
        d.show(5000.0)
    assert buy["limit_price"] == 4900.0 and sell["limit_price"] == 5100.25
    assert d.statuses(buy["order_id"]) == ["OPEN"] and d.statuses(sell["order_id"]) == ["OPEN"]
    listed = {o["order_id"]: o["limit_price"] for o in await d.gateway.get_open_orders()}
    assert listed == {buy["order_id"]: 4900.0, sell["order_id"]: 5100.25}


@pytest.mark.parametrize("case,next_bar,expected", [
    ("touch", dict(close=4999.0, high=5000.5, low=4995.0, open_=5000.0), 4996.0),
    ("gap", dict(close=4992.0, high=4995.0, low=4990.0, open_=4994.0), 4994.0)])
async def test_scenario_limit_order_fills(case, next_bar, expected):
    d = driver()
    d.show(5000.0)
    order = make_order(d.gateway, "LMT", "BUY", limit_price=4996.0)
    await d.gateway.place_order(order)
    assert d.statuses(order["order_id"]) == ["OPEN"]
    d.show(**next_bar)
    assert d.statuses(order["order_id"]) == ["OPEN", "FILLED"]
    assert d.updates(order["order_id"])[-1]["avg_fill_price"] == expected


@pytest.mark.parametrize("case,next_bar,expected", [
    ("touch", dict(close=4996.0, high=5000.0, low=4994.0, open_=5000.0), 4995.0),
    ("gap", dict(close=4992.0, high=4993.0, low=4990.0, open_=4993.0), 4993.0)])
async def test_scenario_stop_order(case, next_bar, expected):
    d = driver()
    d.show(5000.0)
    await d.gateway.place_order(make_order(d.gateway, "MKT", "BUY"))
    stop = make_order(d.gateway, "STP", "SELL", stop_price=4995.0)
    await d.gateway.place_order(stop)
    d.show(**next_bar)
    assert d.statuses(stop["order_id"]) == ["OPEN", "FILLED"]
    assert d.updates(stop["order_id"])[-1]["avg_fill_price"] == expected


async def test_scenario_cancel_resting_order():
    d = driver()
    d.show(5000.0)
    order = make_order(d.gateway, "LMT", "BUY", limit_price=4900.0)
    await d.gateway.place_order(order)
    await d.gateway.cancel_order(order["order_id"])
    d.show(4800.0, high=5000.0, low=4700.0)
    assert d.statuses(order["order_id"]) == ["OPEN", "CANCELLED"]
    assert await d.gateway.get_open_orders() == []
    assert await d.gateway.get_executions() == []


async def test_scenario_cancel_filled_order():
    d = driver()
    d.show(5000.0)
    order = make_order(d.gateway, "MKT", "BUY")
    await d.gateway.place_order(order)
    before = len(d.buffer.events)
    await d.gateway.cancel_order(order["order_id"])
    assert len(d.buffer.events) == before


async def long_one(d):
    d.show(5000.0)
    await d.gateway.place_order(make_order(d.gateway, "MKT", "BUY"))


def group_legs(d, group="G"):
    stop = make_order(d.gateway, "STP", "SELL", stop_price=4900.0, oca_group=group, oca_type=2)
    limit = make_order(d.gateway, "LMT", "SELL", limit_price=5100.0, oca_group=group, oca_type=2)
    return stop, limit


@pytest.mark.parametrize("case", ["lmt_touched", "both_touched", "mkt_into_group", "order_into_filled_group"])
async def test_scenario_oca_group(case):
    d = driver()
    await long_one(d)
    stop, limit = group_legs(d)
    await d.gateway.place_order(stop)
    await d.gateway.place_order(limit)
    if case == "lmt_touched":
        d.show(5100.0, high=5110.0, low=5000.0)
        assert d.statuses(limit["order_id"]) == ["OPEN", "FILLED"]
        assert d.statuses(stop["order_id"]) == ["OPEN", "CANCELLED"]
    elif case == "both_touched":
        d.show(5000.0, high=5110.0, low=4890.0)
        assert d.statuses(stop["order_id"]) == ["OPEN", "FILLED"]
        assert d.statuses(limit["order_id"]) == ["OPEN", "CANCELLED"]
    elif case == "mkt_into_group":
        exit_order = make_order(d.gateway, "MKT", "SELL", oca_group="G", oca_type=2)
        await d.gateway.place_order(exit_order)
        order_of_events = [(u["order_id"], u["status"]) for u in d.updates()
                           if u["order_id"] in (stop["order_id"], limit["order_id"], exit_order["order_id"])]
        cancelled_at = [i for i, (_, status) in enumerate(order_of_events) if status == "CANCELLED"]
        filled_at = order_of_events.index((exit_order["order_id"], "FILLED"))
        assert len(cancelled_at) == 2 and max(cancelled_at) < filled_at
    else:
        d.show(5100.0, high=5110.0, low=5000.0)
        late = make_order(d.gateway, "LMT", "SELL", limit_price=5200.0, oca_group="G", oca_type=2)
        await d.gateway.place_order(late)
        assert d.statuses(late["order_id"]) == ["REJECTED"]


def invalid_orders(gateway):
    bad = [make_order(gateway, order_type, limit_price=price) if order_type == "LMT"
           else make_order(gateway, order_type, stop_price=price)
           for order_type in ("LMT", "STP") for price in (None, 0, -1.0, math.nan)]
    bad.append(make_order(gateway, "LMT", limit_price=5000.0, oca_group="G", oca_type=None))
    return bad


async def test_scenario_invalid_order_refused_locally(tmp_path):
    d = driver(journal=tmp_path / "journal.jsonl")
    await d.gateway.connect()
    d.show(5000.0)
    for order in invalid_orders(d.gateway):
        with pytest.raises(ValueError):
            await d.gateway.place_order(order)
    with pytest.raises(ValueError, match="tick"):
        await d.gateway.place_order(make_order(d.gateway, "MKT", instrument=NO_TICK))
    assert d.updates() == [] and await d.gateway.get_open_orders() == []
    assert not (tmp_path / "journal.jsonl").exists() or (tmp_path / "journal.jsonl").read_text() == ""


async def test_scenario_positions_and_balances():
    d = driver()
    d.show(5000.0)
    await d.gateway.place_order(make_order(d.gateway, "MKT", "BUY"))
    positions = await d.gateway.get_positions()
    assert [(p["account_id"], p["instrument"], p["size"], p["avg_cost"]) for p in positions] == [
        ("SIM", MES, 1, 5000.0)]
    balances = d.buffer.of_type("balance_update")
    assert balances and all(b["account_id"] == "SIM" and b["currency"] == "USD" for b in balances)
