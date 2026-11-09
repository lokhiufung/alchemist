import asyncio
from datetime import datetime, timedelta, timezone

import pytest

from alchemist.clock import ReplayClock, WallClock

START = datetime(2026, 4, 15, 13, 30, tzinfo=timezone.utc)


def test_replay_clock_never_goes_back():
    clock = ReplayClock(START)
    clock.advance(START + timedelta(seconds=5))
    with pytest.raises(ValueError):
        clock.advance(START + timedelta(seconds=4))
    assert clock.now() == START + timedelta(seconds=5)


def test_replay_clock_timestamp_is_epoch_of_now():
    assert ReplayClock(START).timestamp() == START.timestamp()


async def test_replay_wait_returns_at_once():
    await asyncio.wait_for(ReplayClock(START).wait(3600.0), 0.01)


async def test_wall_clock_wait_wakes_on_event():
    wakeup = asyncio.Event()
    clock = WallClock(wakeup)
    asyncio.get_running_loop().call_later(0.01, wakeup.set)
    started = asyncio.get_running_loop().time()
    await clock.wait(5.0)
    assert asyncio.get_running_loop().time() - started < 1.0


async def test_wall_clock_wait_times_out_without_event():
    started = asyncio.get_running_loop().time()
    await WallClock(asyncio.Event()).wait(0.02)
    assert asyncio.get_running_loop().time() - started >= 0.015


def test_wall_clock_now_is_utc_aware():
    assert WallClock().now().tzinfo is not None
    assert WallClock().now().utcoffset() == timedelta(0)
