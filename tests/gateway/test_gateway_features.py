import pytest

from alchemist.instruments import FDAX, MES

from gateway_helpers import driver, make_order


@pytest.mark.parametrize("slippage", [0, 1, 2])
@pytest.mark.parametrize("side,sign", [("BUY", 1), ("SELL", -1)])
async def test_sim_market_fill_at_reference_price_plus_slippage(slippage, side, sign):
    d = driver({"slippage_ticks": slippage})
    d.show(5000.0)
    order = make_order(d.gateway, "MKT", side)
    await d.gateway.place_order(order)
    assert d.updates(order["order_id"])[-1]["avg_fill_price"] == 5000.0 + sign * slippage * 0.25


@pytest.mark.parametrize("case,bar,price", [
    ("touch", dict(close=5100.0, high=5101.0, low=5000.0, open_=5050.0), 5100.0),
    ("gap", dict(close=5120.0, high=5130.0, low=5110.0, open_=5115.0), 5115.0)])
async def test_sim_resting_order_fills_on_touch_or_gap(case, bar, price):
    d = driver()
    d.show(5000.0)
    order = make_order(d.gateway, "LMT", "SELL", limit_price=5100.0)
    await d.gateway.place_order(order)
    d.show(**bar)
    assert d.updates(order["order_id"])[-1]["avg_fill_price"] == price


async def test_sim_stop_wins_when_both_legs_touch():
    d = driver()
    d.show(5000.0)
    stop = make_order(d.gateway, "STP", "SELL", stop_price=4900.0, oca_group="G", oca_type=2)
    limit = make_order(d.gateway, "LMT", "SELL", limit_price=5100.0, oca_group="G", oca_type=2)
    await d.gateway.place_order(limit)
    await d.gateway.place_order(stop)
    d.show(5000.0, high=5200.0, low=4800.0)
    assert d.updates(stop["order_id"])[-1]["status"] == "FILLED"
    assert d.updates(limit["order_id"])[-1]["status"] == "CANCELLED"


async def test_sim_order_into_filled_group_rejected():
    d = driver()
    d.show(5000.0)
    first = make_order(d.gateway, "LMT", "SELL", limit_price=5100.0, oca_group="G", oca_type=2)
    await d.gateway.place_order(first)
    d.show(5100.0, high=5110.0, low=5000.0)
    late = make_order(d.gateway, "LMT", "SELL", limit_price=5200.0, oca_group="G", oca_type=2)
    await d.gateway.place_order(late)
    assert d.statuses(late["order_id"]) == ["REJECTED"]


async def test_sim_cancel_of_non_resting_order_emits_nothing():
    d = driver()
    d.show(5000.0)
    await d.gateway.cancel_order(987)
    assert d.buffer.events == []


async def test_every_execution_row_carries_order_id_and_currency():
    d = driver()
    d.show(5000.0)
    await d.gateway.place_order(make_order(d.gateway, "MKT", "BUY"))
    rows = await d.gateway.get_executions()
    assert rows and all(r["order_id"] and r["currency"] and r["exec_id"] for r in rows)
    assert await d.gateway.get_executions(since=rows[0]["ts"]) == []


async def test_balance_update_per_currency():
    d = driver()
    d.show(5000.0)
    await d.gateway.place_order(make_order(d.gateway, "MKT", "BUY"))
    d.show(15000.0, instrument=FDAX)
    await d.gateway.place_order(make_order(d.gateway, "MKT", "BUY", instrument=FDAX))
    currencies = [b["currency"] for b in d.buffer.of_type("balance_update")]
    assert sorted(set(currencies)) == ["EUR", "USD"]
    latest = {b["currency"]: b["available"] for b in d.buffer.of_type("balance_update")}
    assert latest["USD"] == latest["EUR"] and len(latest) == 2


async def test_next_order_id_starts_at_one_per_run_and_rises():
    d = driver()
    ids = [d.gateway.next_order_id() for _ in range(3)]
    assert ids == [1, 2, 3]


async def test_pnl_moves_available_funds_by_multiplier():
    d = driver()
    d.show(5000.0)
    await d.gateway.place_order(make_order(d.gateway, "MKT", "BUY"))
    d.show(5010.0)
    await d.gateway.place_order(make_order(d.gateway, "MKT", "SELL"))
    first, last = d.buffer.of_type("balance_update")[0], d.buffer.of_type("balance_update")[-1]
    assert last["available"] - first["available"] == 10.0 * MES.multiplier
    assert await d.gateway.get_positions() == []
