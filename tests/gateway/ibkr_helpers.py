import asyncio
import threading
import time
from datetime import datetime, timezone

from alchemist.event_loop.buffer import CommandBuffer
from alchemist.config import GatewayConfig
from alchemist.gateways.ib_gateway.gateway import IbGateway
from alchemist.instruments import FDAX, MES

from gateway_helpers import RecordingBuffer


class StubTransport:
    """Plays recorded EWrapper callbacks for the requests the gateway makes."""

    def __init__(self) -> None:
        self.gateway = None
        self.calls: list[tuple] = []
        self.answer_time_after: float | None = 0.0
        self.min_tick = 0.25
        self.executions: list[dict] = []
        self.open_orders: list[dict] = []
        self.positions: list[dict] = []
        self.history_rows: list[dict] = []
        self.answer_positions = True
        self.first_order_id = 100

    def names(self):
        return [c[0] for c in self.calls]

    def connect(self, host, port, client_id):
        self.calls.append(("connect", host, port, client_id))
        self.gateway.on_next_valid_id(self.first_order_id)

    def disconnect(self):
        self.calls.append(("disconnect",))

    def req_current_time(self):
        self.calls.append(("req_current_time",))
        if self.answer_time_after is not None:
            delay = self.answer_time_after
            threading.Timer(delay, lambda: self.gateway.on_current_time(time.time())).start()

    def req_contract_details(self, req_id, spec):
        self.calls.append(("req_contract_details", req_id, spec))
        details = {**spec, "con_id": hash((spec["trading_class"], spec.get("contract_month"))) % 10**6,
                   "min_tick": self.min_tick}
        self.gateway.on_contract_details(req_id, details)
        self.gateway.on_contract_details_end(req_id)

    def req_tick_by_tick(self, req_id, contract):
        self.calls.append(("req_tick_by_tick", req_id, contract["trading_class"]))

    def cancel_tick_by_tick(self, req_id):
        self.calls.append(("cancel_tick_by_tick", req_id))

    def req_order_updates(self):
        self.calls.append(("req_order_updates",))

    def req_account_updates(self, accounts):
        self.calls.append(("req_account_updates", tuple(accounts)))

    def place_order(self, order_id, contract, spec):
        self.calls.append(("place_order", order_id, contract["trading_class"], spec))

    def cancel_order(self, order_id):
        self.calls.append(("cancel_order", order_id))

    def req_executions(self, req_id, since):
        self.calls.append(("req_executions", req_id, since))
        for row in self.executions:
            self.gateway.on_exec_details(req_id, row)
        self.gateway.on_exec_details_end(req_id)

    def req_open_orders(self):
        self.calls.append(("req_open_orders",))
        for row in self.open_orders:
            self.gateway.on_open_order(row)
        self.gateway.on_open_order_end()

    def req_positions(self):
        self.calls.append(("req_positions",))
        if self.answer_positions:
            for row in self.positions:
                self.gateway.on_position(row)
            self.gateway.on_position_end()

    def req_historical_data(self, req_id, contract, end, duration, bar_size):
        self.calls.append(("req_historical_data", req_id, contract["contract_month"], end, duration, bar_size))
        for row in self.history_rows:
            self.gateway.on_historical_bar(req_id, row)
        self.gateway.on_historical_end(req_id)


def make_ibkr(options=None, transport=None):
    transport = transport or StubTransport()
    buffer = RecordingBuffer()
    config = GatewayConfig("ibkr", options={"host": "127.0.0.1", "port": 7497, "client_id": 7,
                                            "accounts": ["DU1"], **(options or {})})
    gateway = IbGateway(buffer, CommandBuffer(), config, transport=transport)
    transport.gateway = gateway
    return gateway, buffer, transport


def mkt_order(gateway, instrument=MES, side="BUY", **fields):
    return {"order_id": gateway.next_order_id(), "account_id": "DU1", "instrument": instrument, "side": side,
            "qty": 1, "order_type": "MKT", "stop_price": None, "limit_price": None, "oca_group": None,
            "oca_type": None, "tif": "DAY", "outside_rth": True, "slot": "T", **fields}


async def connected(options=None, instruments=(MES,)):
    gateway, buffer, transport = make_ibkr(options)
    await gateway.connect()
    contracts = {}
    for instrument in instruments:
        contracts[instrument] = await gateway.resolve_contract(instrument)
    return gateway, buffer, transport, contracts
