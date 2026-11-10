import logging

import pytest

from alchemist.instruments import MES
from alchemist.managers.order_manager import CANCEL_REJECTED_INTERNALLY, OCA_GROUP_MAX_LEN

from om_helpers import FakeGateway, group_leg, make_manager, open_order, update


async def test_golden_two_market_orders():
    manager, gateway = make_manager("macd_rsi_rth_long")
    owned_when_sent = []
    gateway.on_place = lambda order: owned_when_sent.append(manager.get_order(order["order_id"]) is not None)

    buy = await manager.place_order(MES, "BUY", 1, "MKT")
    manager.on_order_update(update(buy["order_id"], "SUBMITTED"))
    manager.on_order_update(update(buy["order_id"], "FILLED", filled=1, avg=5001.0, last=5001.0))
    sell = await manager.place_order(MES, "SELL", 1, "MKT")
    manager.on_order_update(update(sell["order_id"], "SUBMITTED"))
    manager.on_order_update(update(sell["order_id"], "FILLED", filled=1, avg=5011.0, last=5011.0))

    assert [o["order_id"] for o in gateway.placed] == [buy["order_id"], sell["order_id"]] == [1, 2]
    assert owned_when_sent == [True, True]
    assert [manager.get_order(i)["status"] for i in (1, 2)] == ["FILLED", "FILLED"]
    assert [manager.get_order(i)["filled"] for i in (1, 2)] == [1, 1]
    assert manager.get_submitted_orders() == manager.get_open_orders() == manager.get_cancelling_orders() == []


@pytest.mark.parametrize("slot_length", [1, 40, 100])
async def test_group_cancel_race_fill_wins(slot_length):
    manager, gateway = make_manager("s" * slot_length)
    entry = await manager.place_order(MES, "BUY", 1, "MKT")
    manager.on_order_update(update(entry["order_id"], "SUBMITTED"))
    manager.on_order_update(update(entry["order_id"], "FILLED", filled=1, avg=5000.0, last=5000.0))

    group = await manager.place_group([group_leg("STP", stop_price=4990.0), group_leg("LMT", limit_price=5010.0)])
    stop_id, limit_id = (o["order_id"] for o in manager.get_order_group(group))
    for order_id in (stop_id, limit_id):
        manager.on_order_update(update(order_id, "OPEN"))
    legs = [o for o in gateway.placed if o["oca_group"] == group]
    assert len(gateway.placed) == 3 and len(legs) == 2
    assert {(o["oca_type"], o["tif"]) for o in legs} == {(2, "DAY")}

    await manager.cancel_group(group)
    assert {o["order_id"] for o in manager.get_cancelling_orders(group)} == {stop_id, limit_id}
    assert not manager.is_group_closed(group)
    manager.on_order_update(update(limit_id, "CANCELLED"))
    assert not manager.is_group_closed(group)
    manager.on_order_update(update(stop_id, "FILLED", filled=1, avg=4990.0, last=4990.0))
    assert manager.is_group_closed(group)
    assert (manager.get_order(stop_id)["filled"], manager.get_order(limit_id)["filled"]) == (1, 0)
    assert manager.get_open_orders() == manager.get_cancelling_orders() == []

    sent_before = len(gateway.cancelled)
    assert await manager.cancel_order(entry["order_id"]) == CANCEL_REJECTED_INTERNALLY
    assert len(gateway.cancelled) == sent_before

    names = {group}
    for _ in range(3):
        name = await manager.place_group([group_leg("LMT", limit_price=5010.0)])
        assert len(name) <= OCA_GROUP_MAX_LEN and name not in names
        names.add(name)


async def test_scenario_market_order_life(caplog):
    manager, gateway = make_manager("T")
    seen_when_sent = []
    gateway.on_place = lambda order: seen_when_sent.append(manager.get_submitted_orders()[0]["order_id"])
    order = await manager.place_order(MES, "BUY", 1, "MKT")
    assert gateway.placed == [{**gateway.placed[0], "order_id": 1, "account_id": "SIM", "instrument": MES, "slot": "T"}]
    assert seen_when_sent == [1] and len(gateway.placed) == 1

    assert manager.on_order_update(update(1, "SUBMITTED"))["fill_delta"] == 0
    change = manager.on_order_update(update(1, "FILLED", filled=1, avg=5001.0, last=5001.0))
    assert (change["status"], change["filled"], change["avg_fill_price"], change["fill_delta"]) == (
        "FILLED", 1, 5001.0, 1)
    assert manager.on_order_update(update(1, "FILLED", filled=1, avg=5001.0, last=5001.0)) is None
    stored = manager.get_order(1)
    assert (stored["status"], stored["filled"], stored["avg_fill_price"]) == ("FILLED", 1, 5001.0)
    assert order["order_id"] == 1


@pytest.mark.parametrize("case", ["cancelled", "refused"])
async def test_scenario_limit_order_cancel(case):
    manager, gateway = make_manager()
    order_id = await open_order(manager, "LMT", limit_price=5000.0)
    assert await manager.cancel_order(order_id) is None
    assert gateway.cancelled == [order_id]
    assert [o["order_id"] for o in manager.get_cancelling_orders()] == [order_id]
    assert manager.get_open_orders() == []
    if case == "cancelled":
        manager.on_order_update(update(order_id, "CANCELLED"))
        assert (manager.get_order(order_id)["status"], manager.get_order(order_id)["filled"]) == ("CANCELLED", 0)
    else:
        manager.on_order_update(update(order_id, "CANCEL_REJECTED"))
        assert [o["order_id"] for o in manager.get_open_orders()] == [order_id]
        assert manager.get_cancelling_orders() == []
    assert gateway.cancelled == [order_id]


@pytest.mark.parametrize("state", ["unknown id", "submitted", "cancelling", "closed"])
async def test_scenario_cancel_not_open(state, caplog):
    manager, gateway = make_manager()
    if state == "unknown id":
        order_id = 999
    elif state == "submitted":
        order_id = (await manager.place_order(MES, "BUY", 1, "LMT", limit_price=1.0))["order_id"]
    elif state == "cancelling":
        order_id = await open_order(manager, "LMT", limit_price=1.0)
        await manager.cancel_order(order_id)
    else:
        order_id = await open_order(manager, "LMT", limit_price=1.0)
        manager.on_order_update(update(order_id, "CANCELLED"))
    before = (manager.get_order(order_id), len(gateway.cancelled))
    caplog.clear()
    with caplog.at_level(logging.INFO):
        assert await manager.cancel_order(order_id) == CANCEL_REJECTED_INTERNALLY
    assert (manager.get_order(order_id), len(gateway.cancelled)) == before
    assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1


async def test_scenario_unrequested_cancel():
    manager, _ = make_manager()
    group = await manager.place_group([group_leg("STP", stop_price=1.0), group_leg("LMT", limit_price=2.0)])
    stop_id, limit_id = (o["order_id"] for o in manager.get_order_group(group))
    manager.on_order_update(update(stop_id, "OPEN"))
    manager.on_order_update(update(limit_id, "OPEN"))
    manager.on_order_update(update(stop_id, "FILLED", filled=1, avg=1.0, last=1.0))
    change = manager.on_order_update(update(limit_id, "CANCELLED"))
    assert (change["status"], change["filled"]) == ("CANCELLED", 0)
    assert manager.get_open_orders() == manager.get_cancelling_orders() == []


async def test_scenario_foreign_update(caplog):
    shared = FakeGateway()
    first, _ = make_manager("slot-1", shared)
    second, _ = make_manager("slot-2", shared)
    await open_order(first, "LMT", limit_price=1.0)
    foreign_id = await open_order(second, "LMT", limit_price=1.0)
    before = (first.get_open_orders(), first.get_submitted_orders())
    caplog.clear()
    with caplog.at_level(logging.DEBUG):
        assert first.on_order_update(update(foreign_id, "FILLED", filled=1, avg=1.0, last=1.0)) is None
    assert caplog.records == []
    assert (first.get_open_orders(), first.get_submitted_orders()) == before
    assert first.get_order(foreign_id) is None and second.get_order(foreign_id) is not None


async def test_scenario_rejected_order(caplog):
    shared = FakeGateway()
    owner, _ = make_manager("owner", shared)
    other, _ = make_manager("other", shared)
    order = await owner.place_order(MES, "BUY", 1, "MKT")
    with caplog.at_level(logging.INFO):
        owner.on_order_update(update(order["order_id"], "REJECTED", reason="margin"))
        other.on_order_update(update(order["order_id"], "REJECTED", reason="margin"))
    stored = owner.get_order(order["order_id"])
    assert (stored["status"], stored["filled"]) == ("REJECTED", 0)
    rejected = [(r.event, r.strategy, r.reason) for r in caplog.records]
    assert rejected == [("order_rejected", "owner", "margin")]
    assert len(shared.placed) == 1
