from datetime import datetime, timezone

import pytest

from alchemist.config import SafetyConfig
from alchemist.instruments import FDAX, MES
from alchemist.safety.freeze import FreezeDetector

from safety_helpers import FRIDAY_CLOSED, MONDAY_OPEN, Rig, execution, order


def heartbeat(rig):
    rig.event("heartbeat")


def heartbeat_and_feed(*instruments):
    def tick(rig):
        rig.event("heartbeat")
        for instrument in instruments:
            rig.feed(instrument)
    return tick


@pytest.mark.parametrize("interval,timeout,check", [(5.0, 3.0, 1.0)])
async def test_scenario_heartbeat_gap(interval, timeout, check):
    rig = Rig(config=SafetyConfig(heartbeat_interval_s=interval, heartbeat_timeout_s=timeout,
                                  watch_check_interval_s=check))
    rig.run(20, on_tick=lambda r: r.now % 5 == 0 and heartbeat(r))
    t0 = rig.now
    requested_at = []

    def note(r):
        if len(r.requests("reconnect")) > len(requested_at):
            requested_at.append(r.now - check - t0)

    rig.run(21, on_tick=note)
    assert rig.requests("reconnect") and requested_at[0] <= interval + timeout + check
    assert requested_at[1] - requested_at[0] >= interval + timeout
    assert len(rig.requests("reconnect")) == 2 and len(rig.alerts()) == 2


async def test_outage_fill_repaired_by_order_id():
    rig = Rig()
    rig.place(42, "A")
    rig.place(43, "B")
    rig.event("order_update", {"order_id": 43, "status": "OPEN"})
    rig.run(1, on_tick=heartbeat)
    rig.event("connection_lost")
    rig.run(1, on_tick=heartbeat)
    assert len(rig.alerts()) == 1
    rig.run(9, on_tick=heartbeat)
    rig.event("connection_restored", {"next_order_id": 100})
    rig.broker_executions = [execution(42, 5000.0), execution(43, 4990.0)]
    rig.auto_reply = True
    rig.run(1, on_tick=heartbeat)
    assert len(rig.requests("get_executions")) == 1 and len(rig.requests("get_open_orders")) == 1
    repaired = {e["data"]["order_id"]: e["data"] for e in rig.repairs()}
    assert repaired[42]["strategy"] == "A" and repaired[42]["avg_fill_price"] == 5000.0
    assert repaired[42]["status"] == "FILLED" and repaired[43]["strategy"] == "B"


@pytest.mark.parametrize("case", ["a", "b", "c"])
async def test_scenario_stale_feed_ladder(case):
    windows = (("13:00", "16:00"),) if case == "c" else ()
    rig = Rig(config=SafetyConfig(maintenance_windows=windows))
    rig.subscribe(MES)
    rig.feed(MES)
    rig.now += 46

    def tick(r):
        r.event("heartbeat")
        if case == "a" and r.requests("resubscribe"):
            r.feed(MES)

    rig.run(120, on_tick=tick)
    kinds = rig.kinds()
    if case == "a":
        assert kinds == ["resubscribe"]
    elif case == "b":
        assert kinds == ["resubscribe"] * 3 + ["reconnect"]
    else:
        assert kinds == ["resubscribe"] * 3 + ["reconnect", "restart_broker_process"]


async def test_scenario_closed_session_quiet():
    rig = Rig(start=FRIDAY_CLOSED)
    rig.subscribe(MES)
    rig.subscribe(FDAX)
    rig.feed(MES)
    rig.feed(FDAX)
    rig.run(600, on_tick=heartbeat)
    assert rig.kinds() == []
    cme_open_eurex_closed = datetime(2026, 4, 20, 23, 0, tzinfo=timezone.utc).timestamp()
    rig = Rig(start=cme_open_eurex_closed)
    rig.subscribe(MES)
    rig.subscribe(FDAX)
    rig.feed(FDAX)
    rig.run(600, on_tick=heartbeat_and_feed(MES))
    assert rig.kinds() == []


async def test_scenario_watch_state_from_buffers():
    rig = Rig()
    rig.subscribe(MES)
    rig.subscribe(FDAX)
    rig.feed(MES)
    rig.feed(FDAX)
    rig.run(1, on_tick=heartbeat)
    rig.place(70, "C")
    rig.unsubscribe(FDAX)
    rig.run(60, on_tick=heartbeat)
    ladders = rig.requests("resubscribe")
    assert ladders and all(c["data"]["instruments"] == [MES] for c in ladders)
    rig.event("connection_restored", {"next_order_id": 1})
    rig.broker_executions = [execution(70, 5001.0)]
    rig.auto_reply = True
    rig.run(1, on_tick=heartbeat)
    assert [(e["data"]["order_id"], e["data"]["strategy"]) for e in rig.repairs()] == [(70, "C")]


@pytest.mark.parametrize("min_age", [120.0])
async def test_scenario_dead_orders(min_age):
    rig = Rig(start=MONDAY_OPEN - 150, config=SafetyConfig(reject_synthesis_min_age_s=min_age,
                                                           check_interval_s=1000.0))
    rig.place(50)
    rig.place(52)
    rig.monitor.drain(rig.now)
    rig.now += 50
    rig.place(51)
    rig.monitor.drain(rig.now)
    rig.now = MONDAY_OPEN
    rig.broker_open_orders = [order(52)]
    rig.event("connection_restored", {"next_order_id": 1})
    rig.auto_reply = True
    rig.step()
    rejected = [e["data"] for e in rig.repairs() if e["data"]["status"] == "REJECTED"]
    assert [u["order_id"] for u in rejected] == [50]


@pytest.mark.parametrize("interval", [60.0])
async def test_scenario_stuck_order_timer(interval):
    rig = Rig(config=SafetyConfig(check_interval_s=interval))
    rig.place(60)
    rig.place(61)
    rig.event("order_update", {"order_id": 61, "status": "OPEN"})
    rig.broker_executions = [execution(60, 5000.0)]
    rig.auto_reply = True
    rig.run(61, on_tick=heartbeat)
    repaired = rig.repairs()
    assert [(e["data"]["order_id"], e["data"]["status"]) for e in repaired] == [(60, "FILLED")]
    assert len(rig.requests("get_executions")) == 1
    quiet = Rig(config=SafetyConfig(check_interval_s=interval))
    quiet.place(62)
    quiet.event("order_update", {"order_id": 62, "status": "OPEN"})
    quiet.run(61, on_tick=heartbeat)
    assert quiet.requests("get_executions") == []


async def test_scenario_frozen_loop():
    dumps = []
    rig = Rig()
    detector = FreezeDetector(SafetyConfig(freeze_threshold=10), dump=lambda: dumps.append(rig.now))
    checks = []

    def second(check_in):
        if check_in:
            detector.check_in()
        detector.tick()
        rig.now += 1
        rig.event("heartbeat")
        rig.step()
        checks.append(rig.monitor.last_check)

    for _ in range(5):
        second(True)
    for _ in range(12):
        second(False)
    first_freeze = len(dumps)
    for _ in range(5):
        second(True)
    healthy = len(dumps)
    for _ in range(10):
        second(False)
    assert (first_freeze, healthy, len(dumps)) == (1, 1, 2)
    assert checks == sorted(set(checks)) and len(checks) == 32


@pytest.mark.parametrize("poll", [None, 30.0])
@pytest.mark.parametrize("reply", ["positions", "error"])
async def test_scenario_positions_poll(poll, reply):
    from alchemist.messages import make_reply
    rig = Rig(config=SafetyConfig(positions_poll_s=poll))

    def answer(r):
        r.event("heartbeat")
        for command in r.requests("poll_positions"):
            if command["correlation_id"] not in r.answered:
                r.answered.add(command["correlation_id"])
                result, error = ([{"instrument": MES, "size": 1}], None) if reply == "positions" else (None, "down")
                r.inbound.push(make_reply(command, r.now, result=result, error=error))

    rig.run(90, on_tick=answer)
    sent = rig.requests("poll_positions")
    assert (len(sent) == 0) if poll is None else (1 <= len(sent) <= 3)
