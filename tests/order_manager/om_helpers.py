from alchemist.instruments import MES
from alchemist.managers.order_manager import OrderManager


class FakeGateway:
    def __init__(self, first_id: int = 1) -> None:
        self.next_id = first_id
        self.placed: list[dict] = []
        self.cancelled: list[int] = []
        self.on_place = None

    def next_order_id(self) -> int:
        self.next_id += 1
        return self.next_id - 1

    async def place_order(self, order: dict) -> None:
        if self.on_place is not None:
            self.on_place(order)
        self.placed.append(dict(order))

    async def cancel_order(self, order_id: int) -> None:
        self.cancelled.append(order_id)


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
