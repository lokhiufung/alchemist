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


import math

from alchemist.clock import ReplayClock
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.seeder import Seeder
from alchemist.config import EventLoopConfig

from seed_helpers import SeedStrategy


def sent_orders(transport):
    return [c for c in transport.calls if c[0] == "place_order"]


@pytest.mark.parametrize("order_type,side,expected", [("STP", "BUY", 5000.25), ("LMT", "SELL", 5000.25),
                                                      ("STP", "SELL", 5000.0), ("LMT", "BUY", 5000.0)])
async def test_ibkr_prices_rounded_by_side_and_type_before_sending(order_type, side, expected):
    gateway, buffer, transport, _ = await connected()
    field = "stop_price" if order_type == "STP" else "limit_price"
    order = mkt_order(gateway, side=side, order_type=order_type, **{field: 5000.10})
    await gateway.place_order(order)
    spec = sent_orders(transport)[0][3]
    assert spec[field] == expected and order[field] == expected and spec["order_type"] == order_type


@pytest.mark.parametrize("price", [None, 0, -1.0, math.nan, "text"])
async def test_ibkr_priced_order_without_valid_price_never_reaches_the_broker(price):
    gateway, buffer, transport, _ = await connected()
    with pytest.raises(ValueError):
        await gateway.place_order(mkt_order(gateway, order_type="LMT", limit_price=price))
    assert sent_orders(transport) == [] and updates(buffer) == []


@pytest.mark.parametrize("oca_type,sent", [(None, False), (1, True), (2, True), (3, True)])
async def test_ibkr_oca_group_needs_a_valid_type(oca_type, sent):
    gateway, buffer, transport, _ = await connected()
    order = mkt_order(gateway, order_type="LMT", limit_price=5000.0, oca_group="g", oca_type=oca_type)
    if sent:
        await gateway.place_order(order)
        assert sent_orders(transport)[0][3]["oca_group"] == "g"
    else:
        with pytest.raises(ValueError):
            await gateway.place_order(order)


async def test_ibkr_order_ids_distinct_and_increasing():
    gateway, *_ = await connected()
    ids = [gateway.next_order_id() for _ in range(1000)]
    assert ids == sorted(set(ids))


async def test_orders_are_sent_from_one_send_thread():
    gateway, buffer, transport, _ = await connected()
    threads = []
    original = transport.place_order
    transport.place_order = lambda *a: (threads.append(threading.current_thread().name), original(*a))[1]
    for _ in range(3):
        await gateway.place_order(mkt_order(gateway))
    assert set(threads) == {"ibkr-send"}


@pytest.mark.parametrize("code", sorted(CANCEL_REFUSAL_CODES))
async def test_refused_cancel_becomes_cancel_rejected_never_cancelled(code):
    gateway, buffer, transport, _ = await connected()
    order = mkt_order(gateway)
    await gateway.place_order(order)
    await gateway.cancel_order(order["order_id"])
    gateway.on_error(order["order_id"], code, "cannot cancel")
    statuses = [u["status"] for u in updates(buffer)]
    assert statuses == ["CANCEL_REJECTED"] and ("cancel_order", order["order_id"]) in transport.calls


async def test_pending_cancel_status_never_forwarded():
    gateway, buffer, transport, _ = await connected()
    order = mkt_order(gateway)
    await gateway.place_order(order)
    gateway.on_order_status(order["order_id"], "PendingCancel", 0, 1, 0.0, 0.0)
    gateway.on_order_status(order["order_id"], "Cancelled", 0, 1, 0.0, 0.0)
    assert [u["status"] for u in updates(buffer)] == ["CANCELLED"]


async def test_order_status_translation_and_partial_fills():
    gateway, buffer, transport, _ = await connected()
    order = mkt_order(gateway)
    order["qty"] = 3
    await gateway.place_order(order)
    gateway.on_order_status(order["order_id"], "Submitted", 0, 3, 0.0, 0.0)
    gateway.on_order_status(order["order_id"], "Submitted", 1, 2, 5000.0, 5000.0)
    gateway.on_order_status(order["order_id"], "Filled", 3, 0, 5001.0, 5002.0)
    got = [(u["status"], u["filled"], u["avg_fill_price"]) for u in updates(buffer)]
    assert got == [("OPEN", 0, None), ("PARTIALLY_FILLED", 1, 5000.0), ("FILLED", 3, 5001.0)]


def exec_row(order_id=5, **fields):
    return {"sec_type": "future", "trading_class": "MES", "venue": "CME", "order_id": order_id, "exec_id": "e1",
            "ts": 1000.0, "side": "BOT", "qty": 1, "price": 5000.0, "account": "DU1", "currency": "USD", **fields}


async def test_every_execution_row_carries_order_id_and_currency():
    gateway, buffer, transport, _ = await connected()
    transport.executions = [exec_row(5), exec_row(6, side="SLD", exec_id="e2"), exec_row(None, exec_id="e3"),
                            exec_row(7, currency="", exec_id="e4")]
    rows = await gateway.get_executions()
    assert [(r["order_id"], r["side"], r["currency"]) for r in rows] == [(5, "BUY", "USD"), (6, "SELL", "USD")]
    assert all(r["instrument"] == MES and r["exec_id"] for r in rows)


async def test_open_orders_rows_carry_the_instrument():
    gateway, buffer, transport, _ = await connected()
    transport.open_orders = [{"sec_type": "future", "trading_class": "MES", "venue": "CME", "order_id": 9,
                              "side": "BUY", "qty": 1, "order_type": "LMT", "limit_price": 4000.0}]
    assert [(o["order_id"], o["instrument"]) for o in await gateway.get_open_orders()] == [(9, MES)]


async def history_gateway(now):
    gateway, buffer, transport = make_ibkr()
    gateway.clock = ReplayClock(now)
    await gateway.connect()
    return gateway, transport


@pytest.mark.parametrize("instrument,session", [(MES, "NY_RTH"), (FDAX, "FRA_DAY")])
async def test_daily_frame_has_no_look_ahead_past_last_completed_close(instrument, session):
    now = datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc)
    gateway, transport = await history_gateway(now)
    await gateway.resolve_contract(instrument)
    profile = exchange_profile(instrument)
    last_close = gateway.last_completed_close(profile, session, now)
    day_open, _ = profile.session_window_at(session, last_close - timedelta(seconds=1))
    transport.history_rows = (
        [{"start_ts": day_open.timestamp() + 60 * i, "open": 10.0, "high": 12.0, "low": 9.0, "close": 11.0,
          "volume": 5.0} for i in range(3)]
        + [{"start_ts": last_close.timestamp() + 60, "open": 99.0, "high": 99.0, "low": 99.0, "close": 99.0,
            "volume": 1.0}])
    frame = await gateway.get_daily_history(instrument, date(2026, 4, 1), date(2026, 4, 16), session)
    assert len(frame) == 1 and frame[0]["high"] == 12.0 and frame[0]["end_ts"] <= last_close.timestamp()
    assert frame[0]["volume"] == 15.0


@pytest.mark.parametrize("start", [date(2026, 2, 14), date(2026, 3, 27)])
async def test_history_window_in_months_or_dates(start):
    now = datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc)
    gateway, transport = await history_gateway(now)
    await gateway.resolve_contract(MES)
    await gateway.get_daily_history(MES, start, date(2026, 4, 16), "NY_RTH")
    request = next(c for c in transport.calls if c[0] == "req_historical_data")
    close = gateway.last_completed_close(exchange_profile(MES), "NY_RTH", now)
    assert request[4] == f"{(close.date() - start).days + 1} D" and request[5] == "1 min"


async def test_roll_history_from_previous_contract():
    now = datetime(2026, 3, 13, 12, 0, tzinfo=timezone.utc)
    gateway, transport = await history_gateway(now)
    contract = await gateway.resolve_contract(MES)
    assert contract["contract_month"] == "202606"
    await gateway.get_daily_history(MES, date(2026, 3, 1), date(2026, 3, 12), "NY_RTH")
    assert next(c for c in transport.calls if c[0] == "req_historical_data")[2] == "202603"
    assert "2026-03-10" in gateway.roll_mismatch_dates()
    later, later_transport = await history_gateway(datetime(2026, 3, 25, 12, 0, tzinfo=timezone.utc))
    await later.resolve_contract(MES)
    await later.get_daily_history(MES, date(2026, 3, 15), date(2026, 3, 24), "NY_RTH")
    assert next(c for c in later_transport.calls if c[0] == "req_historical_data")[2] == "202606"


async def test_seed_bars_are_requested_in_one_second_chunks_inside_the_prior_session():
    now = datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc)
    gateway, transport = await history_gateway(now)
    await gateway.resolve_contract(MES)
    profile = exchange_profile(MES)
    close = gateway.last_completed_close(profile, "NY_RTH", now)
    open_at = profile.session_window_at("NY_RTH", close - timedelta(seconds=1))[0]
    transport.history_rows = [{"start_ts": open_at.timestamp() + i, "open": 1.0, "high": 1.0, "low": 1.0,
                               "close": 1.0, "volume": 1.0} for i in range(5)]
    bars = await gateway.get_intraday_seed_bars(MES, "NY_RTH")
    requests = [c for c in transport.calls if c[0] == "req_historical_data"]
    assert len(requests) >= 12 and {r[5] for r in requests} == {"1 secs"}
    assert bars and all(b["resolution_s"] == 1.0 and b["instrument"] == MES for b in bars)


async def test_balance_update_per_currency():
    gateway, buffer, transport, _ = await connected()
    gateway.on_account_value("DU1", "USD", "AvailableFunds", "1000.5")
    gateway.on_account_value("DU1", "EUR", "AvailableFunds", "700")
    gateway.on_account_value("DU1", "USD", "NetLiquidation", "9999")
    balances = buffer.of_type("balance_update")
    assert [(b["currency"], b["available"]) for b in balances] == [("USD", 1000.5), ("EUR", 700.0)]


async def test_positions_snapshot_carries_account_size_and_currency():
    gateway, buffer, transport, _ = await connected()
    transport.positions = [{"sec_type": "future", "trading_class": "MES", "venue": "CME", "account": "DU1",
                            "size": 1, "avg_cost": 25000.0}]
    assert await gateway.get_positions() == [{"account_id": "DU1", "instrument": MES, "size": 1,
                                               "avg_cost": 25000.0, "currency": "USD"}]


async def test_position_snapshot_timeout_leaves_no_waiter():
    gateway, buffer, transport, _ = await connected()
    transport.answer_positions = False
    with pytest.raises(TimeoutError):
        await gateway.get_positions(timeout=0.1)
    assert gateway.waiters == {}


async def test_new_instrument_subscribed_without_touching_running_feeds():
    gateway, buffer, transport, contracts = await connected(instruments=(MES, ES))
    await gateway.subscribe_tick(contracts[MES])
    await gateway.subscribe_tick(contracts[ES])
    await gateway.unsubscribe_tick(contracts[ES])
    ticks = [c for c in transport.calls if c[0] in ("req_tick_by_tick", "cancel_tick_by_tick")]
    assert [(c[0], c[2] if c[0] == "req_tick_by_tick" else None) for c in ticks if c[0] == "req_tick_by_tick"] == [
        ("req_tick_by_tick", "MES"), ("req_tick_by_tick", "ES")]
    assert len([c for c in ticks if c[0] == "cancel_tick_by_tick"]) == 1
    await gateway.subscribe_tick(contracts[MES])
    assert len([c for c in transport.calls if c[0] == "req_tick_by_tick"]) == 2


async def test_sessions_declared_again_for_new_instrument():
    gateway, buffer, transport = make_ibkr()
    gateway.clock = ReplayClock(datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc))
    await gateway.connect()
    seeder = Seeder(gateway, gateway.clock, EventLoopConfig())
    book = RuntimeBook()
    book.attach(gateway, seeder)
    seeder.attempt = lambda strategy: asyncio.sleep(0)
    await book.admit(SeedStrategy("cme", MES))
    await book.admit(SeedStrategy("eurex", FDAX))
    assert gateway.instrument_sessions == {MES: "NY_GLOBEX", FDAX: "FRA_DAY"}
    frankfurt_morning = datetime(2026, 4, 15, 8, 0, tzinfo=timezone.utc).timestamp()
    req_id = next(r for r, i in gateway.tick_requests.items() if i == FDAX)
    gateway.on_tick(gateway.generation, req_id, frankfurt_morning, 1.0, 1, now=frankfurt_morning)
    assert gateway.last_tick_ts == frankfurt_morning
