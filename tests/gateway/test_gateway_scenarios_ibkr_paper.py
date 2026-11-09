"""Gateway scenarios on an IBKR paper account. Run by hand while the instrument's session is open:
uv run pytest -m ibkr_paper tests/gateway/test_gateway_scenarios_ibkr_paper.py
The paper account comes from tests/.env (IBKR_HOST, IBKR_PORT, IBKR_CLIENT_ID, IBKR_ACCOUNT)."""
import asyncio
import os
from pathlib import Path

import pytest
import yaml

import alchemist.gateways  # noqa: F401
from alchemist.clock import WallClock
from alchemist.config import GatewayConfig
from alchemist.exchanges import exchange_profile
from alchemist.gateways.ibkr.gateway import IbkrGateway
from alchemist.gateways.utils import round_price
from alchemist.instruments import AssetClass, Instrument, register_instrument

from gateway_helpers import RecordingBuffer

pytestmark = pytest.mark.ibkr_paper

SETTINGS = yaml.safe_load((Path(__file__).parents[1] / "ibkr_paper_instruments.yaml").read_text())
WAITS = SETTINGS["waits"]


def instruments():
    found = {}
    for row in SETTINGS["instruments"]:
        item = Instrument(AssetClass(row["asset_class"]), row["symbol"], row["exchange"], row["currency"],
                          row["multiplier"], row["tick_size"])
        register_instrument(item)
        found[item.symbol] = item
    return found


def load_env():
    env_file = Path(__file__).parents[1] / ".env"
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                key, value = line.split("=", 1)
                os.environ.setdefault(key.strip(), value.strip())


@pytest.fixture
async def gateway():
    load_env()
    buffer = RecordingBuffer()
    config = GatewayConfig("ibkr", options={"host": os.environ["IBKR_HOST"], "port": int(os.environ["IBKR_PORT"]),
                                            "client_id": int(os.environ["IBKR_CLIENT_ID"]),
                                            "accounts": [os.environ["IBKR_ACCOUNT"]]})
    live = IbkrGateway(buffer, config, WallClock())
    await live.connect()
    await live.subscribe_order_update()
    await live.subscribe_account_update()
    live.buffer = buffer
    yield live
    await live.disconnect()


async def wait_until(predicate, seconds):
    deadline = asyncio.get_running_loop().time() + seconds
    while asyncio.get_running_loop().time() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.2)
    return predicate()


def order_for(gateway, instrument, order_type, side, **fields):
    return {"order_id": gateway.next_order_id(), "account_id": os.environ["IBKR_ACCOUNT"], "instrument": instrument,
            "side": side, "qty": 1, "order_type": order_type, "stop_price": None, "limit_price": None,
            "oca_group": None, "oca_type": None, "tif": "DAY", "outside_rth": True, "slot": "paper", **fields}


@pytest.mark.parametrize("symbol", ["MES", "AAPL"])
async def test_scenario_subscribe_stream(gateway, symbol):
    instrument = instruments()[symbol]
    contract = await gateway.resolve_contract(instrument)
    gateway.set_instrument_sessions({instrument: exchange_profile(instrument).data_session})
    await gateway.subscribe_tick(contract)
    assert await wait_until(lambda: gateway.buffer.of_type("tick_update"), WAITS["first_message_s"])
    ticks = gateway.buffer.of_type("tick_update")
    assert all(t["instrument"] == instrument and t["size"] > 0 for t in ticks)
    assert all(abs(t["price"] / contract["min_tick"] - round(t["price"] / contract["min_tick"])) < 1e-6 for t in ticks)
    assert [t["recv_ts"] for t in ticks] == sorted(t["recv_ts"] for t in ticks)
    assert gateway.last_tick_ts is not None


async def test_scenario_resolve_contracts(gateway):
    found = instruments()
    contracts = {s: await gateway.resolve_contract(found[s]) for s in ("MES", "FDAX", "FDXM", "AAPL")}
    assert contracts["MES"]["contract_month"] == exchange_profile(found["MES"]).front_contract(
        found["MES"], __import__("datetime").date.today())
    assert contracts["FDAX"]["con_id"] != contracts["FDXM"]["con_id"]
    assert all(c["min_tick"] > 0 for c in contracts.values())
    assert contracts["AAPL"].get("contract_month") is None


async def test_scenario_heartbeat_and_reconnect(gateway):
    mes = instruments()["MES"]
    contract = await gateway.resolve_contract(mes)
    await gateway.subscribe_tick(contract)
    last_id = gateway.next_order_id()
    assert await gateway.heartbeat(timeout=3.0) is True
    await gateway.reconnect()
    assert await wait_until(lambda: gateway.buffer.of_type("tick_update"), WAITS["first_message_s"])
    assert gateway.next_order_id() > last_id
    await gateway.disconnect()
    assert await gateway.heartbeat(timeout=3.0) is False


async def test_scenario_market_order_fills(gateway):
    mes = instruments()["MES"]
    await gateway.resolve_contract(mes)
    buy = order_for(gateway, mes, "MKT", "BUY")
    await gateway.place_order(buy)
    assert await wait_until(lambda: any(u["order_id"] == buy["order_id"] and u["status"] == "FILLED"
                                        for u in gateway.buffer.of_type("order_update")), WAITS["fill_s"])
    sell = order_for(gateway, mes, "MKT", "SELL")
    await gateway.place_order(sell)
    assert await wait_until(lambda: any(u["order_id"] == sell["order_id"] and u["status"] == "FILLED"
                                        for u in gateway.buffer.of_type("order_update")), WAITS["fill_s"])
    rows = await gateway.get_executions()
    assert sorted(r["order_id"] for r in rows if r["order_id"] in (buy["order_id"], sell["order_id"])) == sorted(
        [buy["order_id"], sell["order_id"]])
    assert sell["order_id"] > buy["order_id"]


async def test_scenario_limit_order_rests_rounded(gateway):
    mes = instruments()["MES"]
    contract = await gateway.resolve_contract(mes)
    await gateway.subscribe_tick(contract)
    assert await wait_until(lambda: gateway.buffer.of_type("tick_update"), WAITS["first_message_s"])
    last = gateway.buffer.of_type("tick_update")[-1]["price"]
    distance = round(last * 0.02 / contract["min_tick"]) * contract["min_tick"]
    buy = order_for(gateway, mes, "LMT", "BUY", limit_price=last - distance + 0.4 * contract["min_tick"])
    sell = order_for(gateway, mes, "LMT", "SELL", limit_price=last + distance + 0.4 * contract["min_tick"])
    await gateway.place_order(buy)
    await gateway.place_order(sell)
    await asyncio.sleep(WAITS["rest_check_s"])
    open_ids = {o["order_id"]: o for o in await gateway.get_open_orders()}
    assert buy["limit_price"] == round_price(buy["limit_price"], contract["min_tick"], "BUY", "LMT")
    assert {buy["order_id"], sell["order_id"]} <= set(open_ids)
    for order in (buy, sell):
        await gateway.cancel_order(order["order_id"])


async def test_scenario_limit_order_fills(gateway):
    mes = instruments()["MES"]
    contract = await gateway.resolve_contract(mes)
    await gateway.subscribe_tick(contract)
    assert await wait_until(lambda: gateway.buffer.of_type("tick_update"), WAITS["first_message_s"])
    last = gateway.buffer.of_type("tick_update")[-1]["price"]
    buy = order_for(gateway, mes, "LMT", "BUY", limit_price=last + 5 * contract["min_tick"])
    await gateway.place_order(buy)
    assert await wait_until(lambda: any(u["order_id"] == buy["order_id"] and u["status"] == "FILLED"
                                        for u in gateway.buffer.of_type("order_update")), WAITS["fill_s"])
    await gateway.place_order(order_for(gateway, mes, "MKT", "SELL"))


async def test_scenario_stop_order(gateway):
    mes = instruments()["MES"]
    contract = await gateway.resolve_contract(mes)
    await gateway.subscribe_tick(contract)
    assert await wait_until(lambda: gateway.buffer.of_type("tick_update"), WAITS["first_message_s"])
    last = gateway.buffer.of_type("tick_update")[-1]["price"]
    distance = round(last * 0.02 / contract["min_tick"]) * contract["min_tick"]
    sell_stop = order_for(gateway, mes, "STP", "SELL", stop_price=last - distance - 0.4 * contract["min_tick"])
    buy_stop = order_for(gateway, mes, "STP", "BUY", stop_price=last + distance + 0.4 * contract["min_tick"])
    for order in (sell_stop, buy_stop):
        await gateway.place_order(order)
    await asyncio.sleep(WAITS["rest_check_s"])
    assert sell_stop["stop_price"] <= last - distance and buy_stop["stop_price"] >= last + distance
    for order in (sell_stop, buy_stop):
        await gateway.cancel_order(order["order_id"])
    assert await wait_until(lambda: len([u for u in gateway.buffer.of_type("order_update")
                                         if u["status"] == "CANCELLED"]) == 2, WAITS["fill_s"])


async def test_scenario_cancel_resting_order(gateway):
    mes = instruments()["MES"]
    contract = await gateway.resolve_contract(mes)
    await gateway.subscribe_tick(contract)
    assert await wait_until(lambda: gateway.buffer.of_type("tick_update"), WAITS["first_message_s"])
    last = gateway.buffer.of_type("tick_update")[-1]["price"]
    resting = order_for(gateway, mes, "LMT", "BUY", limit_price=round(last * 0.9 / 0.25) * 0.25)
    await gateway.place_order(resting)
    await asyncio.sleep(2)
    await gateway.cancel_order(resting["order_id"])
    assert await wait_until(lambda: any(u["order_id"] == resting["order_id"] and u["status"] == "CANCELLED"
                                        for u in gateway.buffer.of_type("order_update")), WAITS["fill_s"])
    assert resting["order_id"] not in {o["order_id"] for o in await gateway.get_open_orders()}


async def test_scenario_cancel_filled_order(gateway):
    mes = instruments()["MES"]
    await gateway.resolve_contract(mes)
    filled = order_for(gateway, mes, "MKT", "BUY")
    await gateway.place_order(filled)
    assert await wait_until(lambda: any(u["order_id"] == filled["order_id"] and u["status"] == "FILLED"
                                        for u in gateway.buffer.of_type("order_update")), WAITS["fill_s"])
    await gateway.cancel_order(filled["order_id"])
    assert await wait_until(lambda: any(u["order_id"] == filled["order_id"] and u["status"] == "CANCEL_REJECTED"
                                        for u in gateway.buffer.of_type("order_update")), WAITS["fill_s"])
    await gateway.place_order(order_for(gateway, mes, "MKT", "SELL"))


async def test_scenario_oca_group(gateway):
    mes = instruments()["MES"]
    contract = await gateway.resolve_contract(mes)
    await gateway.subscribe_tick(contract)
    assert await wait_until(lambda: gateway.buffer.of_type("tick_update"), WAITS["first_message_s"])
    last = gateway.buffer.of_type("tick_update")[-1]["price"]
    await gateway.place_order(order_for(gateway, mes, "MKT", "BUY"))
    await asyncio.sleep(3)
    stop = order_for(gateway, mes, "STP", "SELL", stop_price=last * 0.9, oca_group="paper-oca", oca_type=2)
    limit = order_for(gateway, mes, "LMT", "SELL", limit_price=last * 1.1, oca_group="paper-oca", oca_type=2)
    for order in (stop, limit):
        await gateway.place_order(order)
    await asyncio.sleep(WAITS["rest_check_s"])
    for order in (stop, limit):
        await gateway.cancel_order(order["order_id"])
    await gateway.place_order(order_for(gateway, mes, "MKT", "SELL"))


async def test_scenario_invalid_order_refused_locally(gateway):
    mes = instruments()["MES"]
    await gateway.resolve_contract(mes)
    before = await gateway.get_open_orders()
    for bad in (order_for(gateway, mes, "LMT", "BUY"), order_for(gateway, mes, "LMT", "BUY", limit_price=0),
                order_for(gateway, mes, "LMT", "BUY", limit_price=-1.0),
                order_for(gateway, mes, "LMT", "BUY", limit_price=float("nan")),
                order_for(gateway, mes, "LMT", "BUY", limit_price=100.0, oca_group="g")):
        with pytest.raises(ValueError):
            await gateway.place_order(bad)
    assert await gateway.get_open_orders() == before


async def test_scenario_positions_and_balances(gateway):
    mes = instruments()["MES"]
    await gateway.resolve_contract(mes)
    await gateway.place_order(order_for(gateway, mes, "MKT", "BUY"))
    await asyncio.sleep(WAITS["fill_s"] / 3)
    positions = await gateway.get_positions()
    assert any(p["instrument"] == mes and p["size"] == 1 for p in positions)
    assert all(b["account_id"] and b["currency"] for b in gateway.buffer.of_type("balance_update"))
    await gateway.place_order(order_for(gateway, mes, "MKT", "SELL"))


@pytest.mark.parametrize("window", ["months", "dates"])
async def test_scenario_history_and_seed_bars(gateway, window):
    from datetime import date, timedelta
    mes = instruments()["MES"]
    await gateway.resolve_contract(mes)
    start = date.today() - timedelta(days=61 if window == "months" else 20)
    daily = await gateway.get_daily_history(mes, start, date.today(), "NY_RTH")
    assert [d["start_ts"] for d in daily] == sorted(d["start_ts"] for d in daily) and daily
    seed = await gateway.get_intraday_seed_bars(mes, "NY_RTH")
    assert seed and all(b["resolution_s"] == 1.0 for b in seed)


async def test_scenario_add_instrument_while_streaming(gateway):
    found = instruments()
    mes = await gateway.resolve_contract(found["MES"])
    await gateway.subscribe_tick(mes)
    es = await gateway.resolve_contract(found["ES"])
    await gateway.subscribe_tick(es)
    assert await wait_until(lambda: any(t["instrument"] == found["ES"] for t in gateway.buffer.of_type("tick_update")),
                            WAITS["first_message_s"])
    await gateway.unsubscribe_tick(es)
    await asyncio.sleep(2)
    count = len([t for t in gateway.buffer.of_type("tick_update") if t["instrument"] == found["ES"]])
    await asyncio.sleep(3)
    assert len([t for t in gateway.buffer.of_type("tick_update") if t["instrument"] == found["ES"]]) == count
