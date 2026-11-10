import json
import logging

import pytest

from gateway_helpers import driver, make_order
from ibkr_helpers import connected, mkt_order


def gateway_records(caplog):
    return [r for r in caplog.records if r.name == "gateway"]


def events(caplog):
    return [r.event for r in gateway_records(caplog)]


def by_event(caplog, event):
    return [r for r in gateway_records(caplog) if r.event == event]


@pytest.fixture(autouse=True)


def capture_info(caplog):
    caplog.set_level(logging.INFO, logger="gateway")


async def test_simulation_logs_order_sent_and_every_broker_reply(caplog):
    d = driver()
    d.show(5000.0)
    order = make_order(d.gateway)
    await d.gateway.place_order(order)
    assert events(caplog) == ["order_sent", "broker_order_update", "broker_order_update",
                              "broker_position_update", "broker_balance_update"]
    sent = by_event(caplog, "order_sent")[0]
    assert (sent.order_id, sent.instrument, sent.side, sent.qty, sent.order_type) == (
        order["order_id"], "MES", "BUY", 1, "MKT")
    assert [r.status for r in by_event(caplog, "broker_order_update")] == ["SUBMITTED", "FILLED"]
    assert by_event(caplog, "broker_order_update")[1].avg_fill_price == 5000.0


async def test_simulation_logs_cancel_sent(caplog):
    d = driver()
    d.show(5000.0)
    order = make_order(d.gateway, order_type="LMT", limit_price=4990.0)
    await d.gateway.place_order(order)
    await d.gateway.cancel_order(order["order_id"])
    assert by_event(caplog, "cancel_sent")[0].order_id == order["order_id"]
    assert [r.status for r in by_event(caplog, "broker_order_update")] == ["OPEN", "CANCELLED"]


async def test_market_data_is_not_logged(caplog):
    d = driver()
    d.show(5000.0)
    d.show(5001.0)
    assert events(caplog) == []


async def test_broker_log_fields_are_json_safe(caplog):
    d = driver()
    d.show(5000.0)
    await d.gateway.place_order(make_order(d.gateway))
    for record in gateway_records(caplog):
        json.dumps({k: v for k, v in vars(record).items() if k in ("instrument", "order_id", "status")})
        assert getattr(record, "instrument", "MES") == "MES"


async def test_ibkr_logs_raw_order_status_even_when_ignored(caplog):
    gateway, buffer, transport, _ = await connected()
    order = mkt_order(gateway)
    await gateway.place_order(order)
    gateway.on_order_status(order["order_id"], "PendingCancel", 0, 1, 0.0, 0.0)
    gateway.on_order_status(999, "Filled", 1, 0, 5000.0, 5000.0)
    raw = by_event(caplog, "broker_order_status")
    assert [(r.order_id, r.status) for r in raw] == [(order["order_id"], "PendingCancel"), (999, "Filled")]
    assert by_event(caplog, "order_sent")[0].order_id == order["order_id"]
    assert by_event(caplog, "broker_order_update") == []


async def test_ibkr_logs_broker_errors_executions_and_positions(caplog):
    gateway, buffer, transport, _ = await connected()
    gateway.on_error(-1, 2104, "farm ok")
    transport.executions = [{"order_id": 100, "exec_id": "e1", "ts": 1.0, "side": "BOT", "qty": 1, "price": 5000.0,
                             "account": "DU1", "currency": "USD", "sec_type": "future", "trading_class": "MES",
                             "venue": "CME"}]
    await gateway.get_executions()
    transport.positions = [{"account": "DU1", "size": 1, "avg_cost": 5000.0, "sec_type": "future",
                            "trading_class": "MES", "venue": "CME"}]
    await gateway.get_positions()
    error = by_event(caplog, "broker_error")[0]
    assert (error.code, error.error_message) == (2104, "farm ok")
    assert by_event(caplog, "broker_execution")[0].exec_id == "e1"
    assert by_event(caplog, "broker_position")[0].size == 1


async def test_simulation_broker_lines_name_the_method_that_made_them(caplog):
    d = driver()
    d.show(5000.0)
    order = make_order(d.gateway, order_type="LMT", limit_price=4990.0)
    await d.gateway.place_order(order)
    await d.gateway.cancel_order(order["order_id"])
    await d.gateway.place_order(make_order(d.gateway))
    assert [(r.event, r.funcName) for r in gateway_records(caplog)] == [
        ("order_sent", "place_order"), ("broker_order_update", "place_order"),
        ("cancel_sent", "cancel_order"), ("broker_order_update", "cancel_order"),
        ("order_sent", "place_order"), ("broker_order_update", "place_market"), ("broker_order_update", "fill"),
        ("broker_position_update", "book_position"), ("broker_balance_update", "book_position")]


async def test_ibkr_broker_lines_name_the_callback(caplog):
    gateway, _, _, _ = await connected()
    order = mkt_order(gateway)
    await gateway.place_order(order)
    await gateway.cancel_order(order["order_id"])
    gateway.on_order_status(order["order_id"], "Filled", 1, 0, 5000.0, 5000.0)
    gateway.on_error(order["order_id"], 202, "cancelled")
    gateway.on_account_value("DU1", "USD", "AvailableFunds", "100")
    assert [(r.event, r.funcName) for r in gateway_records(caplog)] == [
        ("order_sent", "place_order"), ("cancel_sent", "cancel_order"),
        ("broker_order_status", "on_order_status"), ("broker_order_update", "on_order_status"),
        ("broker_error", "on_error"), ("broker_balance_update", "on_account_value")]
