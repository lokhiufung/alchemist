import asyncio
import threading
import time
from datetime import date, datetime, timedelta, timezone

import pytest

import alchemist.gateways  # noqa: F401
from alchemist.exchanges import exchange_profile
from alchemist.gateways.ibkr.errors import CANCEL_REFUSAL_CODES, ORDER_REJECT_CODES
from alchemist.instruments import ES, FDAX, FDXM, MES, AssetClass, Instrument
from alchemist.registry import get_gateway

from ibkr_helpers import StubTransport, connected, make_ibkr, mkt_order


def updates(buffer):
    return buffer.of_type("order_update")


def test_ibkr_gateway_picked_by_registered_name():
    from alchemist.gateways.ibkr.gateway import IbkrGateway
    assert get_gateway("ibkr") is IbkrGateway


async def test_reconnect_restores_order_account_and_tick_streams():
    gateway, buffer, transport, contracts = await connected(instruments=(MES, ES))
    for contract in contracts.values():
        await gateway.subscribe_tick(contract)
    await gateway.subscribe_order_update()
    await gateway.subscribe_account_update()
    transport.calls.clear()
    await gateway.reconnect()
    names = transport.names()
    assert names.count("req_tick_by_tick") == 2 and "req_order_updates" in names and "req_account_updates" in names
    assert names.index("connect") < names.index("req_order_updates")


async def test_stale_subscription_dropped_after_reconnect():
    gateway, buffer, transport, contracts = await connected()
    await gateway.subscribe_tick(contracts[MES])
    old_generation = gateway.generation
    req_id = next(iter(gateway.tick_requests))
    await gateway.reconnect()
    gateway.on_tick(old_generation, req_id, time.time(), 5000.0, 1)
    assert buffer.of_type("tick_update") == []


async def test_order_ids_never_regress_on_reconnect():
    gateway, buffer, transport, _ = await connected()
    last = gateway.next_order_id()
    transport.first_order_id = 1
    await gateway.reconnect()
    assert gateway.next_order_id() > last


async def test_first_order_id_follows_the_brokers_next_valid_id():
    gateway, *_ = await connected()
    assert gateway.next_order_id() == 100


async def test_market_data_appended_in_time_order():
    gateway, buffer, transport, contracts = await connected()
    await gateway.subscribe_tick(contracts[MES])
    req_id = next(iter(gateway.tick_requests))
    for index in range(5):
        gateway.on_tick(gateway.generation, req_id, 1000.0 + index, 5000.0 + index, 2)
    ticks = buffer.of_type("tick_update")
    assert [t["broker_ts"] for t in ticks] == sorted(t["broker_ts"] for t in ticks) and len(ticks) == 5
    assert all(t["instrument"] == MES and t["size"] == 2 and t["recv_ts"] > 0 for t in ticks)


async def test_declared_message_type_matches_messages_sent():
    gateway, buffer, transport, contracts = await connected()
    assert gateway.market_data_type(MES) == ("tick_update", None)
    await gateway.subscribe_tick(contracts[MES])
    gateway.on_tick(gateway.generation, next(iter(gateway.tick_requests)), 1.0, 1.0, 1)
    assert {e["event_type"] for e in buffer.events} == {"tick_update"}


async def test_reader_thread_wakes_loop_through_thread_safe_call():
    gateway, buffer, transport, contracts = await connected()
    await gateway.subscribe_tick(contracts[MES])
    req_id = next(iter(gateway.tick_requests))
    seen_threads = []
    original = buffer.push
    buffer.push = lambda event, wake=True: (seen_threads.append(threading.current_thread().name), original(event))[1]
    worker = threading.Thread(target=lambda: gateway.on_tick(gateway.generation, req_id, 1.0, 2.0, 1), name="reader")
    worker.start()
    worker.join()
    assert seen_threads == ["reader"] and len(buffer.of_type("tick_update")) == 1


async def test_last_tick_ts_ignores_instrument_whose_data_session_is_closed():
    gateway, buffer, transport, contracts = await connected(instruments=(MES, FDAX))
    for contract in contracts.values():
        await gateway.subscribe_tick(contract)
    gateway.set_instrument_sessions({MES: "NY_GLOBEX", FDAX: "FRA_DAY"})
    ids = {instrument: req for req, instrument in gateway.tick_requests.items()}
    saturday = datetime(2026, 4, 18, 12, 0, tzinfo=timezone.utc).timestamp()
    gateway.on_tick(gateway.generation, ids[FDAX], saturday, 1.0, 1,
                    now=saturday)
    assert gateway.last_tick_ts is None
    monday = datetime(2026, 4, 20, 14, 0, tzinfo=timezone.utc).timestamp()
    gateway.on_tick(gateway.generation, ids[MES], monday, 5000.0, 1, now=monday)
    assert gateway.last_tick_ts == monday


@pytest.mark.parametrize("instrument,expected", [
    (MES, ("CME", "USD", "MES")), (FDAX, ("EUREX", "EUR", "FDAX")),
    (Instrument(AssetClass.STOCK, "AAPL", "NASDAQ", "USD", 1.0, 0.01), ("SMART", "USD", "AAPL"))])
async def test_contract_resolved_from_exchange_profile(instrument, expected):
    gateway, buffer, transport, contracts = await connected(instruments=(instrument,))
    contract = contracts[instrument]
    assert (contract["venue"], contract["currency"], contract["trading_class"]) == expected
    spec = next(c[2] for c in transport.calls if c[0] == "req_contract_details")
    assert spec == {**spec, **exchange_profile(instrument).broker_identity(instrument)}


async def test_future_contract_month_follows_the_roll_rule():
    gateway, buffer, transport, contracts = await connected()
    expected = exchange_profile(MES).front_contract(MES, datetime.now(timezone.utc).date())
    assert contracts[MES]["contract_month"] == expected


async def test_stock_contract_has_no_month():
    stock = Instrument(AssetClass.STOCK, "AAPL", "NASDAQ", "USD", 1.0, 0.01)
    gateway, buffer, transport, contracts = await connected(instruments=(stock,))
    assert contracts[stock].get("contract_month") is None


@pytest.mark.parametrize("pair", [(FDAX, FDXM), (FDAX, Instrument(AssetClass.FUTURE, "FDXS", "EUREX", "EUR", 1.0, 1.0))])
async def test_shared_symbol_instruments_keyed_apart(pair):
    gateway, buffer, transport, contracts = await connected(instruments=pair)
    for contract in contracts.values():
        await gateway.subscribe_tick(contract)
    assert contracts[pair[0]]["trading_class"] != contracts[pair[1]]["trading_class"]
    assert len(gateway.tick_requests) == 2 and len(set(gateway.tick_requests.values())) == 2


@pytest.mark.parametrize("code", sorted(ORDER_REJECT_CODES))
async def test_order_reject_codes_become_rejected_by_order_id(code):
    gateway, buffer, transport, contracts = await connected()
    order = mkt_order(gateway)
    await gateway.place_order(order)
    gateway.on_error(order["order_id"], code, "rejected")
    rejected = [u for u in updates(buffer) if u["status"] == "REJECTED"]
    assert [u["order_id"] for u in rejected] == [order["order_id"]]
    before = len(updates(buffer))
    gateway.on_error(gateway.next_request_id(), code, "data request failed")
    assert len(updates(buffer)) == before


async def test_data_farm_recovery_raises_one_shot_resubscribe():
    gateway, *_ = await connected()
    gateway.on_error(-1, 2103, "farm broken")
    assert gateway.consume_resubscribe_request() is False
    gateway.on_error(-1, 2104, "farm ok")
    assert gateway.consume_resubscribe_request() is True
    assert gateway.consume_resubscribe_request() is False


async def test_connectivity_restored_codes_raise_a_resubscribe_request():
    gateway, *_ = await connected()
    gateway.on_error(-1, 1102, "restored")
    assert gateway.consume_resubscribe_request() is True


@pytest.mark.parametrize("answer_delay,expected", [(0.1, True), (0.4, False), (None, False)])
async def test_heartbeat_true_iff_answer_within_timeout(answer_delay, expected):
    gateway, buffer, transport, _ = await connected()
    transport.answer_time_after = answer_delay
    assert await gateway.heartbeat(timeout=0.2) is expected


async def test_heartbeat_is_false_when_disconnected():
    gateway, buffer, transport, _ = await connected()
    await gateway.disconnect()
    assert await gateway.heartbeat(timeout=0.2) is False


def test_real_transport_builds_without_connecting():
    pytest.importorskip("ibapi")
    from alchemist.gateways.ibkr.client import create_transport
    gateway, buffer, transport = make_ibkr()
    real = create_transport(gateway)
    assert not real.isConnected()
