import logging

import pytest

from alchemist.instruments import MES
from alchemist.constants.order_manager import CANCEL_REJECTED_INTERNALLY, OCA_GROUP_MAX_LEN

from om_helpers import FakeGateway, group_leg, make_manager, open_order, update


@pytest.mark.parametrize("order_type,prices", [("MKT", {}), ("STP", {"stop_price": 4990.0}), ("LMT", {"limit_price": 5010.0})])
async def test_order_sent_once_and_owned(order_type, prices):
    manager, gateway = make_manager()
    order = await manager.place_order(MES, "SELL", 1, order_type, **prices)
    assert [o["order_id"] for o in gateway.placed].count(order["order_id"]) == 1
    assert manager.get_order(order["order_id"]) is not None
    placed = gateway.placed[0]
    assert (placed["order_type"], placed["qty"], placed["tif"], placed["outside_rth"]) == (order_type, 1, "DAY", True)
    assert (placed["stop_price"], placed["limit_price"]) == (prices.get("stop_price"), prices.get("limit_price"))


@pytest.mark.parametrize("slot_length", [8, 100])
async def test_group_name_fresh_and_at_most_48_chars(slot_length):
    manager, _ = make_manager("x" * slot_length)
    names = [await manager.place_group([group_leg("LMT", limit_price=1.0)]) for _ in range(1000)]
    assert max(len(n) for n in names) <= OCA_GROUP_MAX_LEN
    assert len(set(names)) == 1000


@pytest.mark.parametrize("size", [1, 2])
async def test_group_orders_share_name_oca_type_and_day(size):
    manager, gateway = make_manager()
    legs = [group_leg("STP", stop_price=1.0), group_leg("LMT", limit_price=2.0)][:size]
    group = await manager.place_group(legs)
    assert {o["oca_group"] for o in gateway.placed} == {group}
    assert {o["oca_type"] for o in gateway.placed} == {2}
    assert {o["tif"] for o in gateway.placed} == {"DAY"}
    assert len(gateway.placed) == size


async def test_market_order_joining_a_group_counts_as_member():
    manager, _ = make_manager()
    group = await manager.place_group([group_leg("STP", stop_price=1.0)])
    exit_order = await manager.place_order(MES, "SELL", 1, "MKT", oca_group=group)
    assert exit_order["order_id"] in {o["order_id"] for o in manager.get_order_group(group)}


async def test_partial_then_full_fill_accumulates_and_duplicate_ignored():
    manager, _ = make_manager()
    order_id = await open_order(manager, "LMT", limit_price=100.0)
    first = manager.on_order_update(update(order_id, "PARTIALLY_FILLED", filled=1, avg=100.0, last=100.0))
    second = manager.on_order_update(update(order_id, "FILLED", filled=3, avg=102.0, last=103.0))
    duplicate = manager.on_order_update(update(order_id, "FILLED", filled=3, avg=102.0, last=103.0))
    assert (first["fill_delta"], second["fill_delta"], duplicate) == (1, 2, None)
    stored = manager.get_order(order_id)
    assert (stored["filled"], stored["avg_fill_price"]) == (3, 102.0)


async def test_average_price_derived_from_last_fill_when_not_reported():
    manager, _ = make_manager()
    order_id = await open_order(manager, "LMT", limit_price=100.0)
    manager.on_order_update(update(order_id, "PARTIALLY_FILLED", filled=1, last=100.0))
    manager.on_order_update(update(order_id, "FILLED", filled=3, last=103.0))
    assert manager.get_order(order_id)["avg_fill_price"] == pytest.approx((100.0 + 2 * 103.0) / 3)


@pytest.mark.parametrize("status", ["FILLED", "CANCELLED", "REJECTED"])
@pytest.mark.parametrize("origin", ["submitted", "open", "cancelling"])
async def test_closed_status_closes_from_any_store(status, origin):
    manager, _ = make_manager()
    if origin == "submitted":
        order_id = (await manager.place_order(MES, "BUY", 1, "LMT", limit_price=1.0))["order_id"]
    else:
        order_id = await open_order(manager, "LMT", limit_price=1.0)
        if origin == "cancelling":
            await manager.cancel_order(order_id)
    filled = 1 if status == "FILLED" else 0
    manager.on_order_update(update(order_id, status, filled=filled, avg=1.0 if filled else None, last=1.0 if filled else None))
    assert manager.get_order(order_id)["status"] == status
    assert manager.get_submitted_orders() == manager.get_open_orders() == manager.get_cancelling_orders() == []


async def test_partial_fill_before_cancel_is_kept_on_close():
    manager, _ = make_manager()
    order_id = await open_order(manager, "LMT", limit_price=1.0)
    manager.on_order_update(update(order_id, "PARTIALLY_FILLED", filled=1, avg=1.0, last=1.0))
    manager.on_order_update(update(order_id, "CANCELLED", filled=1))
    assert manager.get_order(order_id)["filled"] == 1


async def test_filled_quantity_never_decreases():
    manager, _ = make_manager()
    order_id = await open_order(manager, "LMT", limit_price=1.0)
    manager.on_order_update(update(order_id, "PARTIALLY_FILLED", filled=2, avg=1.0, last=1.0))
    assert manager.on_order_update(update(order_id, "PARTIALLY_FILLED", filled=1, avg=1.0, last=1.0)) is None
    assert manager.get_order(order_id)["filled"] == 2


async def test_cancel_rejected_returns_order_to_open():
    manager, _ = make_manager()
    order_id = await open_order(manager, "LMT", limit_price=1.0)
    await manager.cancel_order(order_id)
    manager.on_order_update(update(order_id, "CANCEL_REJECTED"))
    assert [o["order_id"] for o in manager.get_open_orders()] == [order_id]


async def test_unrequested_cancel_closes_open_order():
    manager, _ = make_manager()
    order_id = await open_order(manager, "LMT", limit_price=1.0)
    manager.on_order_update(update(order_id, "CANCELLED"))
    assert manager.get_order(order_id)["status"] == "CANCELLED" and manager.get_open_orders() == []


async def test_cancel_or_reject_logged_only_by_owning_slot(caplog):
    shared = FakeGateway()
    owner, _ = make_manager("owner", shared)
    bystander, _ = make_manager("bystander", shared)
    order_id = await open_order(owner, "LMT", limit_price=1.0)
    with caplog.at_level(logging.DEBUG):
        bystander.on_order_update(update(order_id, "CANCELLED"))
    assert caplog.records == []


async def test_update_for_unowned_order_returns_none():
    manager, _ = make_manager()
    assert manager.on_order_update(update(12345, "FILLED", filled=1, avg=1.0, last=1.0)) is None


@pytest.mark.parametrize("closed_count", [0, 1, 2])
async def test_group_closed_only_when_all_closed(closed_count):
    manager, _ = make_manager()
    group = await manager.place_group([group_leg("STP", stop_price=1.0), group_leg("LMT", limit_price=2.0)])
    ids = [o["order_id"] for o in manager.get_order_group(group)]
    for order_id in ids:
        manager.on_order_update(update(order_id, "OPEN"))
    for order_id in ids[:closed_count]:
        manager.on_order_update(update(order_id, "CANCELLED"))
    assert manager.is_group_closed(group) is (closed_count == 2)


async def test_unknown_group_is_not_closed():
    manager, _ = make_manager()
    assert manager.is_group_closed("nope") is False and manager.get_order_group("nope") == []


async def test_closed_order_stays_queryable():
    manager, _ = make_manager()
    order_id = await open_order(manager, "LMT", limit_price=1.0)
    manager.on_order_update(update(order_id, "FILLED", filled=1, avg=7.0, last=7.0))
    stored = manager.get_order(order_id)
    assert (stored["status"], stored["filled"], stored["avg_fill_price"]) == ("FILLED", 1, 7.0)


@pytest.mark.parametrize("state", ["unknown", "submitted", "cancelling", "closed"])
async def test_cancel_of_non_open_order_rejected_internally(state):
    manager, gateway = make_manager()
    order_id = 4242
    if state == "submitted":
        order_id = (await manager.place_order(MES, "BUY", 1, "LMT", limit_price=1.0))["order_id"]
    elif state in ("cancelling", "closed"):
        order_id = await open_order(manager, "LMT", limit_price=1.0)
        await manager.cancel_order(order_id)
        gateway.cancelled.clear()
        if state == "closed":
            manager.on_order_update(update(order_id, "CANCELLED"))
    assert await manager.cancel_order(order_id) == CANCEL_REJECTED_INTERNALLY
    assert gateway.cancelled == []


async def test_group_cancel_skips_non_open_orders():
    manager, gateway = make_manager()
    group = await manager.place_group([group_leg("STP", stop_price=1.0), group_leg("LMT", limit_price=2.0)])
    stop_id, limit_id = (o["order_id"] for o in manager.get_order_group(group))
    manager.on_order_update(update(stop_id, "OPEN"))
    await manager.cancel_group(group)
    assert gateway.cancelled == [stop_id]
    assert [o["order_id"] for o in manager.get_submitted_orders(group)] == [limit_id]


async def test_cancel_all_cancels_each_open_order_once():
    manager, gateway = make_manager()
    ids = [await open_order(manager, "LMT", limit_price=float(p)) for p in (1, 2)]
    await manager.cancel_all()
    await manager.cancel_all()
    assert sorted(gateway.cancelled) == sorted(ids)


async def test_cancel_request_never_closes_order():
    manager, _ = make_manager()
    order_id = await open_order(manager, "LMT", limit_price=1.0)
    await manager.cancel_order(order_id)
    assert manager.get_order(order_id)["status"] == "CANCELLING"
    assert [o["order_id"] for o in manager.get_cancelling_orders()] == [order_id]


async def test_order_record_carries_its_submission_time_but_the_gateway_order_does_not():
    manager, gateway = make_manager()
    order = await manager.place_order(MES, "BUY", 1, "MKT")
    assert isinstance(manager.get_order(order["order_id"])["submitted_at"], float)
    assert "submitted_at" not in gateway.placed[0]
