from datetime import datetime, timezone

from alchemist.config import SafetyConfig
from alchemist.event_loop.buffer import CommandBuffer, EventBuffer
from alchemist.exchanges import EXCHANGES
from alchemist.instruments import FDAX, MES
from alchemist.messages import make_command, make_event, make_reply
from alchemist.safety.monitor import SafetyMonitor

MONDAY_OPEN = datetime(2026, 4, 20, 14, 0, tzinfo=timezone.utc).timestamp()
FRIDAY_CLOSED = datetime(2026, 4, 17, 21, 0, 30, tzinfo=timezone.utc).timestamp()


def order(order_id, strategy="A", instrument=MES, qty=1):
    return {"order_id": order_id, "account_id": "ACC", "instrument": instrument, "side": "BUY", "qty": qty,
            "order_type": "MKT", "stop_price": None, "limit_price": None, "oca_group": None, "oca_type": None,
            "tif": "DAY", "outside_rth": True, "slot": strategy}


def execution(order_id, price, qty=1, ts=MONDAY_OPEN, instrument=MES):
    return {"order_id": order_id, "exec_id": f"e{order_id}", "ts": ts, "side": "BUY", "qty": qty, "price": price,
            "instrument": instrument, "account_id": "ACC", "currency": "USD"}


class Rig:
    def __init__(self, start=MONDAY_OPEN, config=None) -> None:
        self.now = start
        self.config = config or SafetyConfig()
        self.inbound, self.outbound = EventBuffer(), CommandBuffer()
        self.monitor = SafetyMonitor(self.inbound, self.outbound, EXCHANGES, self.config,
                                     system_time=lambda: self.now)
        self.mine: set[int] = set()
        self.answered: set[int] = set()
        self.seen_inbound = 0
        self.broker_executions: list[dict] = []
        self.broker_open_orders: list[dict] = []
        self.auto_reply = False

    def event(self, kind, data=None, event_time=None):
        event = make_event(kind, data or {}, self.now if event_time is None else event_time)
        event["pushed_at"] = self.now
        self.inbound.push(event)

    def feed(self, instrument=MES, event_time=None):
        self.event("bar_update", {"instrument": instrument, "start_ts": self.now}, event_time)

    def command(self, kind, **data):
        command = make_command(kind, data)
        self.mine.add(command["correlation_id"])
        self.outbound.push(command)

    def subscribe(self, instrument):
        self.command("subscribe", contract={"instrument": instrument})

    def unsubscribe(self, instrument):
        self.command("unsubscribe", contract={"instrument": instrument})

    def place(self, order_id, strategy="A", instrument=MES):
        self.command("place_order", order=order(order_id, strategy, instrument))

    def requests(self, kind=None):
        return [c for c in self.outbound.queue
                if c["correlation_id"] not in self.mine and (kind is None or c["command_type"] == kind)]

    def kinds(self):
        return [c["command_type"] for c in self.requests() if c["command_type"] != "alert"]

    def alerts(self):
        return self.requests("alert")

    def reply_all(self):
        for command in self.requests():
            if command["correlation_id"] in self.answered:
                continue
            result = {"get_executions": self.broker_executions, "get_open_orders": self.broker_open_orders}.get(
                command["command_type"])
            if result is None:
                continue
            self.answered.add(command["correlation_id"])
            reply = make_reply(command, self.now, result=result)
            reply["pushed_at"] = self.now
            self.inbound.push(reply)

    def step(self):
        self.monitor.step(self.now)
        if self.auto_reply:
            self.reply_all()
            self.monitor.step(self.now)

    def run(self, seconds, every=1.0, on_tick=None):
        end = self.now + seconds
        while self.now < end - 1e-9:
            self.now += every
            if on_tick is not None:
                on_tick(self)
            self.step()

    def repairs(self):
        self.inbound.begin_pass()
        events = []
        while (event := self.inbound.pop()) is not None:
            if event["event_type"] in ("repaired_fill", "order_update") and "strategy" in event["data"]:
                events.append(event)
        return events
