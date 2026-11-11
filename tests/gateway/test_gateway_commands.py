import asyncio

import pytest

from alchemist.config import GatewayConfig
from alchemist.event_loop.buffer import CommandBuffer
from alchemist.gateways import Gateway
from alchemist.instruments import MES
from alchemist.messages import make_command

from gateway_helpers import RecordingBuffer, ListTape, REPLAY_DATE, make_gateway, session_seconds


class StubGateway(Gateway):
    def __init__(self, inbound, outbound, config, **kwargs) -> None:
        super().__init__(inbound, outbound, config, **kwargs)
        self.calls: list[str] = []
        self.alive = True
        self.connect_failures = 0

    def order_id_floor(self):
        return 7

    async def restart_broker_process(self):
        self.calls.append("restart")

    async def connect(self):
        self.calls.append("connect")
        if self.connect_failures:
            self.connect_failures -= 1
            raise ConnectionError("refused")

    async def disconnect(self):
        self.calls.append("disconnect")

    async def subscribe_order_update(self):
        self.calls.append("orders")

    async def subscribe_account_update(self):
        self.calls.append("account")

    async def resolve_contract(self, instrument):
        return {"instrument": instrument}

    async def subscribe_tick(self, contract):
        self.calls.append(f"tick:{contract['instrument'].symbol}")

    def next_order_id(self):
        return 1

    async def place_order(self, order):
        if order["qty"] <= 0:
            raise ValueError("qty must be positive")
        self.calls.append(f"order:{order['order_id']}")

    async def cancel_order(self, order_id):
        self.calls.append(f"cancel:{order_id}")

    async def get_executions(self, since=None):
        return [{"order_id": 1}]

    async def get_open_orders(self, timeout=5.0):
        raise TimeoutError("no answer")

    async def ping(self, timeout):
        return self.alive

    async def get_daily_history(self, instrument, start, end, session):
        return []

    async def get_intraday_seed_bars(self, instrument, session):
        return []

    async def get_positions(self, timeout=5.0):
        return []


def stub(sleeps=None, **config):
    slept = sleeps if sleeps is not None else []

    async def sleep(seconds):
        slept.append(seconds)
        await asyncio.sleep(0)

    inbound, outbound = RecordingBuffer(), CommandBuffer()
    return StubGateway(inbound, outbound, GatewayConfig("stub", **config), sleep=sleep), inbound, outbound


def replies(inbound):
    from alchemist.constants.messages import REPLY_EVENT_TYPES
    return [e["data"] for e in inbound.events if e["event_type"] in REPLY_EVENT_TYPES]


async def run_all(gateway, commands):
    for command in commands:
        gateway.outbound.push(command)
    consumer = asyncio.create_task(gateway.run_commands())
    await asyncio.sleep(0)
    await gateway.outbound.wait_idle()
    consumer.cancel()


@pytest.mark.parametrize("request_type,data", [("get_executions", {"since": None}), ("get_open_orders", {}),
                                               ("poll_positions", {})])
async def test_every_request_gets_one_reply_with_its_id(request_type, data):
    gateway, inbound, _ = stub()
    command = make_command(request_type, data)
    await run_all(gateway, [command])
    assert [r["correlation_id"] for r in replies(inbound)] == [command["correlation_id"]]


async def test_commands_run_in_push_order():
    gateway, inbound, _ = stub()
    commands = [make_command("place_order", {"order": {"order_id": i, "qty": 1}}) if i % 3 else
                make_command("cancel_order", {"order_id": i}) for i in range(100)]
    await run_all(gateway, commands)
    assert gateway.calls == [f"order:{i}" if i % 3 else f"cancel:{i}" for i in range(100)]


async def test_failed_order_ends_rejected_and_its_reply_carries_the_error():
    gateway, inbound, _ = stub()
    order = {"order_id": 9, "qty": 0, "account_id": "A", "instrument": MES}
    command = make_command("place_order", {"order": order})
    await run_all(gateway, [command])
    updates = [e["data"] for e in inbound.events if e["event_type"] == "order_update"]
    assert [(u["order_id"], u["status"]) for u in updates] == [(9, "REJECTED")]
    assert replies(inbound)[0]["correlation_id"] == command["correlation_id"] and replies(inbound)[0]["error"]


async def test_non_broker_commands_go_to_their_sink():
    gateway, inbound, _ = stub()
    written = []
    gateway.sinks["deploy_result"] = written.append
    await run_all(gateway, [make_command("deploy_result", {"request_id": 1, "line": {}, "final": True})])
    assert [c["command_type"] for c in written] == ["deploy_result"] and replies(inbound) == []


@pytest.mark.parametrize("answer_delay", [1.5, 6.0, None], ids=["half_timeout", "twice_timeout", "never"])
async def test_heartbeat_event_iff_answer_within_timeout(answer_delay):
    gateway, inbound, _ = stub(heartbeat_interval_s=5.0, heartbeat_timeout_s=3.0)

    async def ping(timeout):
        return answer_delay is not None and answer_delay <= timeout

    gateway.ping = ping
    task = asyncio.create_task(gateway.run_heartbeat())
    for _ in range(3):
        await asyncio.sleep(0)
    task.cancel()
    kinds = [e["event_type"] for e in inbound.events]
    answered = answer_delay == 1.5
    assert ("heartbeat" in kinds) is answered
    assert ("connection_lost" in kinds) is (not answered)


@pytest.mark.parametrize("interval", [5.0])
async def test_heartbeat_every_interval(interval):
    slept = []
    gateway, inbound, _ = stub(slept, heartbeat_interval_s=interval)
    task = asyncio.create_task(gateway.run_heartbeat())
    while sum(slept) < 60.0:
        await asyncio.sleep(0)
    task.cancel()
    beats = [e for e in inbound.events if e["event_type"] == "heartbeat"]
    assert abs(len(beats) - 60.0 / interval) <= 1


@pytest.mark.parametrize("failures", [1, 50])
async def test_reconnect_never_gives_up(failures):
    slept = []
    gateway, inbound, _ = stub(slept)
    gateway.connect_failures = failures
    await gateway.reconnect()
    assert len(slept) == failures and gateway.connect_failures == 0
    assert inbound.events[-1]["event_type"] == "connection_restored"


@pytest.mark.parametrize("base,cap", [(15.0, 120.0)])
def test_backoff_delays(base, cap):
    from alchemist.gateways.gateway import reconnect_delay
    assert [reconnect_delay(n, base, cap) for n in range(1, 9)] == [min(cap, base * 2 ** (n - 1)) for n in range(1, 9)]


@pytest.mark.parametrize("subclass", ["ibkr", "simulated"])
async def test_connection_events_around_reconnect(subclass):
    if subclass == "ibkr":
        from ibkr_helpers import connected
        gateway, inbound, transport, contracts = await connected()
        await gateway.subscribe(contracts[MES])
        streams_active = lambda: "req_tick_by_tick" in transport.names()
        transport.calls.clear()
    else:
        gateway, inbound = make_gateway()
        await gateway.connect()
        await gateway.subscribe(await gateway.resolve_contract(MES))
        gateway.day_bars.clear()
        streams_active = lambda: MES in gateway.day_bars
    seen = []
    original_push = inbound.push

    def push(event, *args):
        seen.append((event["event_type"], streams_active()))
        original_push(event)

    inbound.push = push
    await gateway.reconnect()
    connection = [(kind, active) for kind, active in seen if kind.startswith("connection")]
    assert connection == [("connection_lost", False), ("connection_restored", True)]


@pytest.mark.parametrize("base,cap", [(15.0, 120.0)])
async def test_reconnect_backs_off_to_the_cap_and_restores_every_stream_first(base, cap):
    slept = []
    gateway, inbound, _ = stub(slept, reconnect_base_s=base, reconnect_cap_s=cap)
    await gateway.subscribe({"instrument": MES})
    gateway.calls.clear()
    gateway.connect_failures = 6
    await gateway.reconnect()
    assert slept == [15.0, 30.0, 60.0, 120.0, 120.0, 120.0]
    assert gateway.calls[-3:] == ["orders", "account", "tick:MES"]
    kinds = [e["event_type"] for e in inbound.events]
    assert kinds == ["connection_lost", "connection_restored"]
    assert inbound.events[-1]["data"]["next_order_id"] == 7


async def test_reconnect_command_while_reconnecting_runs_one_loop():
    gateway, inbound, _ = stub()
    gateway.reconnecting = True
    await gateway.reconnect()
    assert inbound.events == []


async def test_simulated_broker_waits_until_every_command_is_answered_before_the_next_bar():
    gateway, inbound = make_gateway(tape=ListTape(session_seconds(MES, "NY_RTH", REPLAY_DATE, 2)))
    await gateway.connect()
    await gateway.subscribe_tick(await gateway.resolve_contract(MES))
    gateway.outbound.push(make_command("get_positions"))
    await gateway.start_feed()
    for _ in range(5):
        await asyncio.sleep(0)
    assert [e["event_type"] for e in inbound.events] == []
    taken = await gateway.outbound.get()
    gateway.outbound.done()
    await gateway.feed_task
    assert taken["command_type"] == "get_positions"
    assert [e["event_type"] for e in inbound.events] == ["bar_update", "bar_update", "end_of_data"]


async def test_a_slow_command_finishes_before_the_next_one_starts():
    gateway, inbound, _ = stub()
    trace = []

    async def slow_history(instrument, start, end, session):
        trace.append("history started")
        await asyncio.sleep(0.01)
        trace.append("history done")
        return []

    gateway.get_daily_history = slow_history
    original = gateway.place_order

    async def place(order):
        trace.append("order")
        await original(order)

    gateway.place_order = place
    await run_all(gateway, [
        make_command("get_daily_history", {"instrument": MES, "start": None, "end": None, "session": "NY_RTH"}),
        make_command("place_order", {"order": {"order_id": 1, "qty": 1}})])
    assert trace == ["history started", "history done", "order"]


@pytest.mark.parametrize("message_type", ["tick_update", "bar_update"])
async def test_every_feed_event_carries_event_time_and_recv_ts(message_type):
    if message_type == "tick_update":
        from ibkr_helpers import connected
        gateway, inbound, transport, contracts = await connected()
        await gateway.subscribe_tick(contracts[MES])
        req_id = next(iter(gateway.tick_requests))
        for index in range(3):
            gateway.on_tick(gateway.generation, req_id, 1000.0 + index, 5000.0, 1)
    else:
        gateway, inbound = make_gateway(tape=ListTape(session_seconds(MES, "NY_RTH", REPLAY_DATE, 3)))
        await gateway.connect()
        await gateway.subscribe_tick(await gateway.resolve_contract(MES))
        await gateway.start_feed()
        await gateway.feed_task
    feed = [e for e in inbound.events if e["event_type"] == message_type]
    assert feed and all(e["data"].get("broker_ts") and e["data"].get("recv_ts") for e in feed)
    assert all(e["event_time"] == e["data"]["broker_ts"] for e in feed)


@pytest.mark.parametrize("name", ["ibkr", "simulation"])
def test_gateway_picked_by_registered_name(name):
    from alchemist.gateways import GATEWAYS, IbGateway, SimulationGateway, get_gateway
    assert get_gateway(name) is {"ibkr": IbGateway, "simulation": SimulationGateway}[name] is GATEWAYS[name]


async def test_sessions_declared_again_for_new_instrument():
    from alchemist.event_loop.book import RuntimeBook
    from alchemist.exchanges import exchange_profile
    from alchemist.instruments import FDAX
    from request_helpers import DirectRequests
    from seed_helpers import SeedGateway, SeedStrategy, make_seeder
    seeder, _ = make_seeder(SeedGateway())
    book = RuntimeBook()
    book.attach(seeder.requests, seeder)
    await book.admit(SeedStrategy("cme", MES))
    await book.admit(SeedStrategy("eurex", FDAX, session="FRA_DAY"))
    declared = [data["sessions"] for kind, data in seeder.requests.commands if kind == "set_instrument_sessions"]
    assert [set(d) for d in declared] == [{MES}, {MES, FDAX}]
    session = declared[-1][FDAX]
    assert session == exchange_profile(FDAX).data_session
    assert exchange_profile(FDAX).session(session).tz == "Europe/Berlin"


@pytest.mark.parametrize("request_type,reply_type", [("get_executions", "executions"),
                                                     ("get_open_orders", "open_orders"),
                                                     ("poll_positions", "positions")])
async def test_reply_event_is_named_after_its_request(request_type, reply_type):
    gateway, inbound, _ = stub()
    gateway.get_open_orders = lambda timeout=5.0: asyncio.sleep(0, result=[])
    await run_all(gateway, [make_command(request_type, {"since": None} if request_type == "get_executions" else {})])
    assert [e["event_type"] for e in inbound.events] == [reply_type]
