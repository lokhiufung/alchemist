import asyncio
import logging
import threading
from datetime import datetime, timezone

import pytest

from alchemist.config import EventLoopConfig
from alchemist.event_loop.loop import HANDLERS
from alchemist.instruments import MES

from el_helpers import (OPEN, FakeSystemTime, Harness, RecordingStrategy, balance_update, bar_update, end_of_data,
                        order_update, tick)


async def test_scenario_ticks_to_bars():
    strategy = RecordingStrategy()
    harness = Harness([strategy], message_type="tick_update")
    harness.push(tick(OPEN + 0.1, 5000.0, 1), tick(OPEN + 0.5, 5001.0, 2), tick(OPEN + 0.9, 4999.0, 1),
                 tick(OPEN + 1.2, 5000.5, 3), tick(OPEN + 0.7, 5002.0, 1), end_of_data())
    await harness.run()
    assert strategy.events == [("bar", OPEN, 5000.0, 5001.0, 4999.0, 4999.0, 4),
                               ("bar", OPEN + 1, 5000.5, 5000.5, 5000.5, 5000.5, 3)]


async def test_scenario_quiet_market_flush():
    strategy = RecordingStrategy()
    harness = Harness([strategy], message_type="tick_update")
    strategy.watch_time = harness.watch_time
    harness.push(tick(OPEN + 0.2, 5000.0, 1))

    async def stop_later():
        while not strategy.bars:
            await asyncio.sleep(0)
        harness.loop.stopping = True

    task = asyncio.create_task(stop_later())
    await harness.run()
    await task
    assert strategy.events == [("bar", OPEN, 5000.0, 5000.0, 5000.0, 5000.0, 1)]
    arrived = strategy.watch_at_bar[0]
    assert OPEN + 2.0 <= arrived <= OPEN + 2.0 + harness.config.drain_timeout_s + 1e-9


async def test_scenario_bar_updates():
    strategy = RecordingStrategy()
    harness = Harness([strategy])
    strategy.watch_time = harness.watch_time
    feed = [*[bar_update(OPEN + i) for i in range(10)], bar_update(OPEN + 5), bar_update(OPEN + 3), end_of_data()]

    async def replay_one_at_a_time():
        for event in feed:
            await harness.buffer.wait_idle()
            harness.push(event)

    producer = asyncio.create_task(replay_one_at_a_time())
    await harness.run()
    await producer
    assert strategy.bars == [OPEN + i for i in range(10)]
    assert strategy.watch_at_bar == [OPEN + i + 1 for i in range(10)]


async def test_scenario_fill_jumps_tick_burst():
    strategy = RecordingStrategy()
    harness = Harness([strategy], message_type="tick_update")
    harness.buffer.bind_loop(asyncio.get_running_loop())
    order = []
    strategy.on_order_update = lambda update: order.append(("order", len(strategy.events)))
    ready = threading.Event()

    def produce():
        for index in range(5000):
            harness.buffer.push(tick(OPEN + index * 0.0001, 5000.0, 1))
        harness.buffer.push(order_update(1))
        harness.buffer.push(end_of_data())
        ready.set()

    producer = threading.Thread(target=produce)
    producer.start()
    ready.wait()
    await harness.run()
    producer.join()
    assert order and order[0][1] <= 1
    assert sum(e[6] for e in strategy.events if e[0] == "bar") == 5000
    assert harness.buffer.wakeups <= 3


async def test_scenario_updates_reach_every_state():
    strategies = [RecordingStrategy("in_session"), RecordingStrategy("flattening"), RecordingStrategy("ended")]
    strategies[1].flat_idle = False
    harness = Harness(strategies)
    harness.push(order_update(1), balance_update(), end_of_data())
    await harness.run()
    for strategy in strategies:
        assert strategy.events == [("order", 1, "FILLED"), ("balance", "USD", 5.0)]


async def test_scenario_broken_strategy_quarantined(caplog):
    first, broken, third = RecordingStrategy("A"), RecordingStrategy("B", raises_on_bar=True), RecordingStrategy("C")
    harness = Harness([first, broken, third])
    harness.push(*[bar_update(OPEN + i) for i in range(5)], end_of_data())
    with caplog.at_level(logging.ERROR):
        await harness.run()
    assert first.bars == third.bars == [OPEN + i for i in range(5)]
    assert broken.quarantined and broken.bars == []
    errors = [r for r in caplog.records if r.levelno == logging.ERROR and getattr(r, "strategy", None) == "B"]
    assert len(errors) == 3


async def test_scenario_replay_equals_live():
    ticks = [tick(OPEN + index * 0.05, 5000.0 + (index % 7), 1 + index % 3) for index in range(60)]

    async def run(system):
        strategy = RecordingStrategy()
        harness = Harness([strategy], message_type="tick_update", system=system)
        harness.push(*ticks[:30], order_update(1), *ticks[30:], order_update(2), end_of_data())
        await harness.run()
        return strategy.events

    replay = await run(FakeSystemTime(0.0))
    live = await run(FakeSystemTime(OPEN + 0.003))
    assert replay == live and any(e[0] == "order" for e in replay)
