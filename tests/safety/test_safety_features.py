import asyncio
import inspect
import time
from datetime import datetime, timezone

import pytest

from alchemist.config import SafetyConfig
from alchemist.event_loop.buffer import CommandBuffer, EventBuffer
from alchemist.exchanges import EXCHANGES
from alchemist.instruments import ES, FDAX, MES
from alchemist.messages import make_event
from alchemist.safety.freeze import FreezeDetector
from alchemist.safety.monitor import SafetyMonitor

from safety_helpers import FRIDAY_CLOSED, MONDAY_OPEN, Rig, execution, order


def heartbeat(rig):
    rig.event("heartbeat")


@pytest.mark.parametrize("interval,timeout,check", [(5.0, 3.0, 1.0)])
async def test_heartbeat_gap_requests_reconnect(interval, timeout, check):
    rig = Rig(config=SafetyConfig(heartbeat_interval_s=interval, heartbeat_timeout_s=timeout,
                                  watch_check_interval_s=check))
    rig.event("heartbeat")
    last = rig.now
    while not rig.requests("reconnect"):
        rig.now += check
        rig.step()
    assert rig.now - last <= interval + timeout + check


async def test_connection_lost_alerts():
    rig = Rig()
    rig.event("connection_lost")
    rig.run(1, on_tick=heartbeat)
    assert [a["data"]["reason"] for a in rig.alerts()] == ["connection_lost"]


@pytest.mark.parametrize("threshold", [45.0])
async def test_stale_feed_starts_ladder(threshold):
    rig = Rig(config=SafetyConfig(stale_feed_s=threshold))
    rig.subscribe(MES)
    rig.feed(MES)
    last = rig.now
    while not rig.requests("resubscribe"):
        rig.now += 1
        heartbeat(rig)
        rig.step()
    assert rig.now - last <= threshold + rig.config.watch_check_interval_s


@pytest.mark.parametrize("instruments", [(MES,), (MES, FDAX)], ids=["CME", "CME+Eurex"])
async def test_no_stale_alarm_when_every_exchange_in_break(instruments):
    for start in (FRIDAY_CLOSED, datetime(2026, 4, 20, 21, 30, tzinfo=timezone.utc).timestamp()):
        rig = Rig(start=start)
        for instrument in instruments:
            rig.subscribe(instrument)
            rig.feed(instrument)
        rig.run(300, on_tick=heartbeat)
        assert rig.kinds() == []


async def test_day_only_book_still_watched_overnight():
    overnight = datetime(2026, 4, 21, 2, 0, tzinfo=timezone.utc).timestamp()
    rig = Rig(start=overnight)
    rig.subscribe(ES)
    rig.feed(ES)
    rig.run(60, on_tick=heartbeat)
    assert rig.kinds()[:1] == ["resubscribe"]


@pytest.mark.parametrize("attempts", [4])
async def test_ladder_order(attempts):
    rig = Rig(config=SafetyConfig(resubscribe_max_attempts=attempts))
    rig.subscribe(MES)
    rig.feed(MES)
    rig.run(200, on_tick=heartbeat)
    kinds = rig.kinds()
    assert kinds.index("reconnect") > max(i for i, k in enumerate(kinds) if k == "resubscribe")


@pytest.mark.parametrize("cap", [3])
async def test_resubscribe_without_feed_event_capped(cap):
    rig = Rig(config=SafetyConfig(resubscribe_without_feed_cap=cap))
    rig.subscribe(MES)
    rig.feed(MES)
    rig.run(200, on_tick=heartbeat)
    kinds = rig.kinds()
    assert kinds[:kinds.index("reconnect")].count("resubscribe") == cap


@pytest.mark.parametrize("cap", [3])
async def test_connection_restored_without_feed_capped_then_reconnect(cap):
    rig = Rig(config=SafetyConfig(resubscribe_without_feed_cap=cap))
    rig.subscribe(MES)
    for _ in range(cap + 1):
        rig.event("connection_restored", {"next_order_id": 1})
        rig.run(1, on_tick=heartbeat)
    assert rig.kinds() == ["resubscribe"] * cap + ["reconnect"]


@pytest.mark.parametrize("inside,restarts", [(True, 1), (False, 0)])
async def test_restart_only_in_maintenance_window(inside, restarts):
    windows = (("13:00", "16:00"),) if inside else (("02:00", "03:00"),)
    rig = Rig(config=SafetyConfig(maintenance_windows=windows))
    rig.subscribe(MES)
    rig.feed(MES)
    rig.run(300, on_tick=heartbeat)
    assert rig.kinds().count("restart_broker_process") == restarts


def outage(rig, broker_executions, broker_open_orders=()):
    rig.event("connection_restored", {"next_order_id": 1})
    rig.broker_executions = list(broker_executions)
    rig.broker_open_orders = list(broker_open_orders)
    rig.auto_reply = True
    rig.run(1, on_tick=heartbeat)
    return rig.repairs()


async def test_resting_stop_filled_while_in_position_repaired():
    rig = Rig()
    rig.place(43, "B")
    rig.event("order_update", {"order_id": 43, "status": "OPEN"})
    repaired = outage(rig, [execution(43, 4990.0)])
    assert [(e["data"]["order_id"], e["data"]["status"]) for e in repaired] == [(43, "FILLED")]


async def test_two_strategies_same_instrument_attributed_by_id():
    rig = Rig()
    rig.place(1, "A")
    rig.place(2, "B")
    repaired = outage(rig, [execution(2, 5000.0), execution(1, 5001.0)])
    assert sorted((e["data"]["order_id"], e["data"]["strategy"]) for e in repaired) == [(1, "A"), (2, "B")]


@pytest.mark.parametrize("age", [100.0, 150.0])
@pytest.mark.parametrize("found_in", ["neither", "open_orders"])
async def test_dead_order_rejected_only_after_grace_and_absent_both(age, found_in):
    rig = Rig(start=MONDAY_OPEN - age, config=SafetyConfig(check_interval_s=10_000.0))
    rig.place(50)
    rig.monitor.drain(rig.now)
    rig.now = MONDAY_OPEN
    repaired = outage(rig, [], [order(50)] if found_in == "open_orders" else [])
    rejected = [e for e in repaired if e["data"]["status"] == "REJECTED"]
    assert len(rejected) == (1 if age >= 120 and found_in == "neither" else 0)


@pytest.mark.parametrize("loop_state", ["healthy", "blocked"])
def test_freeze_dump_only_when_check_ins_missed(loop_state):
    dumps = []
    detector = FreezeDetector(SafetyConfig(freeze_threshold=10), dump=lambda: dumps.append(1))
    for _ in range(30):
        if loop_state == "healthy":
            detector.check_in()
        detector.tick()
    assert len(dumps) == (0 if loop_state == "healthy" else 1)


@pytest.mark.parametrize("poll,max_polls", [(None, 0), (60.0, 10)])
async def test_positions_poll_respects_config(poll, max_polls):
    rig = Rig(config=SafetyConfig(positions_poll_s=poll))
    rig.run(600, every=5.0, on_tick=heartbeat)
    sent = len(rig.requests("poll_positions"))
    assert sent <= max_polls and (sent > 0) is (poll is not None)


async def test_order_from_hot_added_strategy_is_repaired():
    rig = Rig()
    rig.run(120, every=5.0, on_tick=heartbeat)
    rig.place(80, "late")
    repaired = outage(rig, [execution(80, 5002.0)])
    assert [(e["data"]["order_id"], e["data"]["strategy"]) for e in repaired] == [(80, "late")]


def test_safety_holds_no_book_or_gateway_reference():
    parameters = set(inspect.signature(SafetyMonitor).parameters)
    assert parameters.isdisjoint({"gateway", "book", "strategies", "clock"})
    rig = Rig()
    assert not any(hasattr(rig.monitor, name) for name in ("gateway", "book", "clock"))


@pytest.mark.parametrize("interval", [60.0])
async def test_stuck_check_runs_every_interval(interval):
    rig = Rig(config=SafetyConfig(check_interval_s=interval))
    checks = []
    original = rig.monitor.check_stuck_orders

    def counting(now):
        before = rig.monitor.last_stuck_check
        original(now)
        if rig.monitor.last_stuck_check != before:
            checks.append(now)

    rig.monitor.check_stuck_orders = counting
    rig.run(600, on_tick=heartbeat)
    assert len(checks) == 10


async def test_stuck_check_does_not_block_bar_dispatch():
    rig = Rig(config=SafetyConfig(check_interval_s=1.0))
    rig.place(90)
    started = time.perf_counter()
    rig.run(5, on_tick=heartbeat)
    assert rig.requests("get_executions") and time.perf_counter() - started < 0.5
    assert rig.repairs() == []


@pytest.mark.parametrize("watcher", ["running", "blocked"])
def test_tap_push_never_waits(watcher):
    def push_time(buffer, count=20_000):
        started = time.perf_counter()
        for index in range(count):
            buffer.push(make_event("tick_update", {"instrument": MES}, float(index)))
        return time.perf_counter() - started

    baseline = min(push_time(EventBuffer()) for _ in range(3))
    tapped = EventBuffer()
    monitor = SafetyMonitor(tapped, CommandBuffer(), EXCHANGES, SafetyConfig(watch_check_interval_s=0.01))
    if watcher == "blocked":
        monitor.check = lambda now: time.sleep(0.2)
    asyncio.run(monitor.start())
    try:
        timed = min(push_time(tapped) for _ in range(3))
    finally:
        asyncio.run(monitor.stop())
    assert timed <= baseline * 1.5 + 0.005


async def test_watcher_runs_while_loop_blocked():
    inbound, outbound = EventBuffer(), CommandBuffer()
    monitor = SafetyMonitor(inbound, outbound, EXCHANGES, SafetyConfig(watch_check_interval_s=0.1))
    checks = []
    original = monitor.check

    def counting(now):
        checks.append(now)
        original(now)

    monitor.check = counting
    await monitor.start()
    time.sleep(1.0)
    await monitor.stop()
    assert len(checks) >= 9
