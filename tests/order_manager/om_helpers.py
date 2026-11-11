from alchemist.instruments import MES
from alchemist.managers.order_manager import OrderManager


from request_helpers import OrderSink as FakeGateway


def update(order_id: int, status: str, filled: int = 0, avg: float | None = None,
           last: float | None = None, reason: str | None = None) -> dict:
    return {"order_id": order_id, "status": status, "filled": filled, "remaining": 1 - filled,
            "avg_fill_price": avg, "last_fill_price": last, "ts": 0.0, "account_id": "SIM",
            "instrument": MES, "reason": reason}


def make_manager(slot: str = "T", gateway: FakeGateway | None = None, first_id: int = 1):
    gateway = gateway or FakeGateway(first_id)
    return OrderManager(gateway, "SIM", slot), gateway


def group_leg(order_type: str, side: str = "SELL", **prices) -> dict:
    return {"instrument": MES, "side": side, "qty": 1, "order_type": order_type, **prices}


async def open_order(manager, order_type="LMT", **prices):
    order = await manager.place_order(MES, "BUY", 1, order_type, **prices)
    manager.on_order_update(update(order["order_id"], "OPEN"))
    return order["order_id"]
