from __future__ import annotations

from alchemist.messages import make_event


def filled_update(entry: dict, rows: list[dict]) -> dict:
    order = entry["order"]
    qty = sum(r["qty"] for r in rows)
    average = sum(r["price"] * r["qty"] for r in rows) / qty
    return {"order_id": order["order_id"], "status": "FILLED" if qty >= order["qty"] else "PARTIALLY_FILLED",
            "filled": qty, "remaining": max(order["qty"] - qty, 0), "avg_fill_price": average,
            "last_fill_price": rows[-1]["price"], "ts": max(r["ts"] for r in rows),
            "account_id": order["account_id"], "instrument": order["instrument"], "reason": None,
            "strategy": entry["strategy"]}


def rejected_update(entry: dict, event_time: float) -> dict:
    order = entry["order"]
    return {"order_id": order["order_id"], "status": "REJECTED", "filled": 0, "remaining": order["qty"],
            "avg_fill_price": None, "last_fill_price": None, "ts": event_time, "account_id": order["account_id"],
            "instrument": order["instrument"], "reason": "absent at the broker after the minimum age",
            "strategy": entry["strategy"]}


def repair_orders(pending: list[dict], executions: list[dict], open_orders: list[dict], now: float,
                  event_time: float, min_age_s: float) -> list[dict]:
    by_order: dict[int, list[dict]] = {}
    for row in executions:
        by_order.setdefault(row["order_id"], []).append(row)
    open_ids = {o["order_id"] for o in open_orders}
    events = []
    for entry in pending:
        order_id = entry["order"]["order_id"]
        rows = by_order.get(order_id)
        if rows:
            update = filled_update(entry, rows)
            events.append(make_event("repaired_fill", update, update["ts"]))
        elif order_id not in open_ids and now - entry["sent_at"] >= min_age_s:
            events.append(make_event("order_update", rejected_update(entry, event_time), event_time))
    return events
