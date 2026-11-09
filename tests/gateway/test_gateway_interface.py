import math
import threading

import pytest

from alchemist.config import GatewayConfig
from alchemist.gateways import Gateway, OrderIdSequence, check_order, round_price
from alchemist.instruments import MES
from alchemist.registry import get_gateway, register_gateway


class RecordingBuffer:
    def __init__(self) -> None:
        self.events: list = []

    def push(self, event) -> None:
        self.events.append(event)


COMPLETE = {
    "connect": None, "disconnect": None, "reconnect": None, "subscribe_order_update": None,
    "resolve_contract": None, "subscribe_tick": None, "next_order_id": 1, "place_order": None,
    "cancel_order": None, "get_executions": [], "get_open_orders": [], "heartbeat": True,
    "get_daily_history": [], "get_intraday_seed_bars": [], "subscribe_account_update": None,
    "get_positions": [],
}


def gateway_class(missing: str | None = None):
    namespace = {}
    for name, result in COMPLETE.items():
        if name == missing:
            continue
        if name == "next_order_id":
            namespace[name] = lambda self: 1
        else:
            async def method(self, *args, _result=result, **kwargs):
                return _result
            namespace[name] = method
    return type("StubGateway", (Gateway,), namespace)


@pytest.mark.parametrize("side,order_type,expected", [
    ("BUY", "STP", 5000.25), ("SELL", "LMT", 5000.25), ("SELL", "STP", 5000.00), ("BUY", "LMT", 5000.00)])
def test_stop_and_limit_prices_rounded_by_side_and_type(side, order_type, expected):
    assert round_price(5000.10, 0.25, side, order_type) == expected


@pytest.mark.parametrize("side,order_type", [("BUY", "STP"), ("SELL", "LMT"), ("SELL", "STP"), ("BUY", "LMT")])
def test_price_on_tick_is_left_alone(side, order_type):
    assert round_price(5000.25, 0.25, side, order_type) == 5000.25
    assert round_price(100.07, 0.01, side, order_type) == pytest.approx(100.07)


def test_rounded_price_is_a_whole_multiple_of_the_tick():
    for tick in (0.25, 1.0, 0.01):
        price = round_price(123.456, tick, "BUY", "STP")
        assert abs(price / tick - round(price / tick)) < 1e-9


def order(order_type="LMT", **fields):
    base = {"order_id": 1, "instrument": MES, "side": "BUY", "qty": 1, "order_type": order_type,
            "stop_price": None, "limit_price": None, "oca_group": None, "oca_type": None,
            "tif": "DAY", "outside_rth": True, "slot": "T"}
    return {**base, **fields}


@pytest.mark.parametrize("order_type,field", [("STP", "stop_price"), ("LMT", "limit_price")])
@pytest.mark.parametrize("price", [None, 0, -1.0, math.nan, "text"], ids=["missing", "zero", "negative", "nan", "text"])
def test_priced_order_without_valid_price_rejected_locally(order_type, field, price):
    reason = check_order(order(order_type, **{field: price}), 0.25)
    assert reason is not None and field in reason


def test_valid_orders_pass_the_check():
    assert check_order(order("MKT"), 0.25) is None
    assert check_order(order("LMT", limit_price=5000.0), 0.25) is None
    assert check_order(order("STP", stop_price=4990.0), 0.25) is None


@pytest.mark.parametrize("oca_type,rejected", [(None, True), (1, False), (2, False), (3, False), (4, True)])
def test_oca_group_needs_valid_type(oca_type, rejected):
    reason = check_order(order("LMT", limit_price=5000.0, oca_group="g", oca_type=oca_type), 0.25)
    assert (reason is not None) is rejected
    if rejected:
        assert "oca_type" in reason


@pytest.mark.parametrize("tick", [None, 0.0])
def test_contract_without_known_tick_rejected(tick):
    reason = check_order(order("MKT"), tick)
    assert reason is not None and "tick" in reason


def test_order_ids_distinct_and_increasing():
    sequence = OrderIdSequence(1)
    ids = [sequence.next() for _ in range(1000)]
    assert ids == sorted(set(ids)) and len(ids) == 1000


def test_order_ids_are_unique_across_threads():
    sequence = OrderIdSequence(1)
    taken: list[int] = []
    workers = [threading.Thread(target=lambda: taken.extend(sequence.next() for _ in range(500))) for _ in range(4)]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join()
    assert len(set(taken)) == 2000


def test_order_ids_never_fall_back_when_the_floor_is_lower():
    sequence = OrderIdSequence(10)
    sequence.raise_floor(5)
    assert sequence.next() == 10
    sequence.raise_floor(50)
    assert sequence.next() == 50


def test_missing_required_operation_fails_at_build():
    with pytest.raises(TypeError, match="heartbeat"):
        gateway_class(missing="heartbeat")(RecordingBuffer(), GatewayConfig("stub"), None)


def test_complete_gateway_builds_with_collaborators():
    buffer, config = RecordingBuffer(), GatewayConfig("stub")
    gateway = gateway_class()(buffer, config, "clock")
    assert (gateway.event_buffer, gateway.config, gateway.clock) == (buffer, config, "clock")


def test_capabilities_have_safe_defaults():
    gateway = gateway_class()(RecordingBuffer(), GatewayConfig("stub"), None)
    assert gateway.last_tick_ts is None
    assert gateway.market_data_type(MES) == ("tick_update", None)
    assert gateway.market_data_ready() is True
    assert gateway.consume_resubscribe_request() is False
    assert gateway.roll_mismatch_dates() == frozenset()
    assert gateway.intraday_seed_failure_is_fatal() is False
    assert gateway.set_instrument_sessions({MES: "NY_RTH"}) is None
    assert gateway.on_bar(MES, {}) is None


async def test_async_capability_defaults():
    gateway = gateway_class()(RecordingBuffer(), GatewayConfig("stub"), None)
    assert await gateway.get_commissions() == {}
    assert await gateway.unsubscribe_tick(object()) is None


def test_gateway_picked_by_registered_name():
    from alchemist.gateways import SimulationGateway
    assert get_gateway("simulation") is SimulationGateway
    register_gateway("stub")(gateway_class())
    assert get_gateway("stub").__name__ == "StubGateway"


def test_unknown_gateway_name_lists_known():
    import alchemist.gateways  # noqa: F401
    with pytest.raises(KeyError, match="simulation"):
        get_gateway("nope")
