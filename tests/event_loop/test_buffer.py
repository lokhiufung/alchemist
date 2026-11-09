import asyncio
import logging
import threading
import time

import pytest

from alchemist.clock import ReplayClock
from alchemist.config import EventLoopConfig
from alchemist.event_loop.buffer import EventBuffer

from el_helpers import FakeWallClock, Harness, RecordingStrategy, bar_update, order_update, tick


def drain_all(buffer):
    buffer.begin_pass()
    events = []
    while (event := buffer.pop()) is not None:
        events.append(event)
    return events


@pytest.mark.parametrize("ticks_ahead", [0, 2000])
def test_control_lane_drained_before_data_lane(ticks_ahead):
    buffer = EventBuffer(EventLoopConfig())
    for index in range(ticks_ahead):
        buffer.push(tick(100.0 + index * 0.001, 5000.0, 1))
    buffer.push(order_update(7))
    assert drain_all(buffer)[0]["event_type"] == "order_update"


def test_each_lane_drains_in_arrival_order_and_nothing_is_dropped():
    buffer = EventBuffer(EventLoopConfig())
    for index in range(50):
        buffer.push(tick(100.0 + index, 1.0, 1))
        buffer.push(order_update(index))
    events = drain_all(buffer)
    assert [e["data"]["order_id"] for e in events if e["event_type"] == "order_update"] == list(range(50))
    assert [e["data"]["broker_ts"] for e in events if e["event_type"] == "tick_update"] == [100.0 + i for i in range(50)]
    assert len(events) == 100


def test_end_of_data_waits_for_the_data_lane():
    from el_helpers import end_of_data
    buffer = EventBuffer(EventLoopConfig())
    buffer.push(end_of_data())
    buffer.push(tick(100.0, 1.0, 1))
    assert [e["event_type"] for e in drain_all(buffer)] == ["tick_update", "end_of_data"]


@pytest.mark.parametrize("burst", [1000])
def test_burst_sends_one_wakeup_while_loop_awake(burst):
    buffer = EventBuffer(EventLoopConfig())
    for index in range(burst):
        buffer.push(tick(100.0 + index * 0.001, 1.0, 1))
    assert buffer.wakeups == 1
    drain_all(buffer)
    buffer.push(tick(200.0, 1.0, 1))
    assert buffer.wakeups == 2


async def test_push_racing_end_of_drain_is_not_left_for_timeout():
    config = EventLoopConfig(drain_timeout_s=0.5)
    buffer = EventBuffer(config)
    clock = type("C", (), {})()
    from alchemist.clock import WallClock
    wall = WallClock(buffer.drain_wakeup)
    buffer.begin_pass()
    assert buffer.pop() is None
    buffer.push(order_update(1))
    started = time.perf_counter()
    await buffer.wait(wall)
    assert time.perf_counter() - started < config.drain_timeout_s / 5
    assert buffer.pop()["event_type"] == "order_update"


async def test_drain_starts_on_wake_without_timer():
    from alchemist.clock import WallClock
    config = EventLoopConfig(drain_timeout_s=0.5)
    buffer = EventBuffer(config)
    buffer.bind_loop(asyncio.get_running_loop())
    wall = WallClock(buffer.drain_wakeup)
    asyncio.get_running_loop().call_later(0.01, lambda: buffer.push(order_update(1)))
    started = time.perf_counter()
    await buffer.wait(wall)
    assert time.perf_counter() - started < 0.2


async def test_reader_thread_wakes_loop_through_thread_safe_call():
    from alchemist.clock import WallClock
    buffer = EventBuffer(EventLoopConfig(drain_timeout_s=2.0))
    buffer.bind_loop(asyncio.get_running_loop())
    wall = WallClock(buffer.drain_wakeup)
    threading.Thread(target=lambda: [buffer.push(tick(100.0 + i, 1.0, 1)) for i in range(500)]).start()
    started = time.perf_counter()
    await buffer.wait(wall)
    assert time.perf_counter() - started < 1.0
    assert buffer.wakeups == 1 and buffer.threadsafe_wakeups == 1


async def test_drain_pass_yields_after_batch_budget():
    config = EventLoopConfig(drain_batch_budget_s=0.002)
    strategy = RecordingStrategy()
    harness = Harness([strategy], config=config)
    passes = []
    original = harness.loop.dispatch

    async def slow_dispatch(event):
        time.sleep(0.0007)
        await original(event)

    harness.loop.dispatch = slow_dispatch
    original_pass = harness.loop.drain_pass

    async def measuring_pass():
        started = time.perf_counter()
        await original_pass()
        passes.append(time.perf_counter() - started)

    harness.loop.drain_pass = measuring_pass
    for index in range(60):
        harness.push(bar_update(1_776_259_800.0 + index))
    harness.push(__import__("el_helpers").end_of_data())
    wakeups_before = harness.buffer.wakeups
    await harness.run()
    assert max(passes) <= config.drain_batch_budget_s + 0.004
    assert len(passes) > 1 and harness.buffer.wakeups == wakeups_before


@pytest.mark.parametrize("lane", ["control", "data"])
async def test_backlog_over_threshold_raises_error_drops_nothing(lane, caplog):
    threshold = 50
    config = EventLoopConfig(backlog_alert=threshold)
    strategy = RecordingStrategy()
    harness = Harness([strategy], config=config)
    for index in range(threshold + 1):
        harness.push(order_update(index) if lane == "control" else bar_update(1_776_259_800.0 + index))
    harness.push(__import__("el_helpers").end_of_data())
    with caplog.at_level(logging.ERROR):
        await harness.run()
    errors = [r for r in caplog.records if r.levelno == logging.ERROR and "backlog" in r.getMessage().lower()]
    assert bool(errors) is (lane == "control")
    delivered = [e for e in strategy.events if e[0] == ("order" if lane == "control" else "bar")]
    assert len(delivered) == threshold + 1


def test_no_error_when_control_lane_is_at_threshold(caplog):
    buffer = EventBuffer(EventLoopConfig(backlog_alert=10))
    for index in range(10):
        buffer.push(order_update(index))
    assert buffer.control_backlog_exceeded() is False
    buffer.push(order_update(10))
    assert buffer.control_backlog_exceeded() is True
