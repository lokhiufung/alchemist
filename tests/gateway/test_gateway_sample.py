import json
from datetime import timedelta, timezone

from alchemist.instruments import MES

from gateway_helpers import REPLAY_DATE, Driver, make_gateway, make_order

ENTRY_INDEX, TARGET_INDEX = 601, 618


def atr(daily, period=14):
    rows = daily[-(period + 1):]
    return sum(max(c["high"], p["close"]) - min(c["low"], p["close"]) for p, c in zip(rows, rows[1:])) / period


async def test_golden_replay_delivers_every_bar_then_end_of_data_and_fills_two_orders(tmp_path):
    journal = tmp_path / "broker_journal.jsonl"
    gateway, buffer = make_gateway(journal=journal)
    await gateway.connect()
    daily = await gateway.get_daily_history(MES, REPLAY_DATE - timedelta(days=120), REPLAY_DATE, "NY_RTH")
    assert atr(daily) == 20.0
    await gateway.subscribe_tick(await gateway.resolve_contract(MES))
    await gateway.start_replay()
    bars = list(buffer.events)
    assert [e["event_type"] for e in bars] == ["bar_update"] * 23_400 + ["end_of_data"]
    starts = [e["data"]["start_ts"] for e in bars[:-1]]
    assert starts == sorted(starts)

    orders = {}
    for index, event in enumerate(bars[:-1]):
        bar = event["data"]
        gateway.clock.advance(__import__("datetime").datetime.fromtimestamp(bar["end_ts"], timezone.utc))
        gateway.on_bar(MES, bar)
        if index in (ENTRY_INDEX, TARGET_INDEX):
            side = "BUY" if index == ENTRY_INDEX else "SELL"
            order = make_order(gateway, "MKT", side)
            await gateway.place_order(order)
            orders[side] = (order["order_id"], bar)

    fills = [u for u in buffer.of_type("order_update") if u["status"] == "FILLED"]
    assert len(fills) == 2
    for fill in fills:
        side = "BUY" if fill["order_id"] == orders["BUY"][0] else "SELL"
        assert fill["avg_fill_price"] == orders[side][1]["close"]
    assert (orders["BUY"][1]["close"], orders["SELL"][1]["close"]) == (5001.0, 5011.0)
    rows = await gateway.get_executions()
    assert [r["order_id"] for r in rows] == [orders["BUY"][0], orders["SELL"][0]]

    lines = journal.read_text().splitlines()
    assert len(lines) == 4
    buy_order, buy_fill, sell_order, sell_fill = lines
    assert buy_order.startswith('{"instrument":"MES","order_id":1,"order_type":"MKT","qty":1,"side":"BUY","time":"')
    assert buy_fill.startswith('{"order_id":1,"price":5001.00,"qty":1,"time":"')
    assert sell_fill.startswith('{"order_id":2,"price":5011.00,"qty":1,"time":"')
    assert [json.loads(l)["order_id"] for l in lines] == [1, 1, 2, 2]
    assert json.loads(buy_fill)["time"].endswith(".000Z")
