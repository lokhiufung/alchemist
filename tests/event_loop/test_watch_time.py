from alchemist.event_loop.watch_time import WatchTime


def test_watch_time_is_last_event_time_plus_system_time_since_arrival():
    watch = WatchTime()
    watch.observe(event_time=1_000.0, arrived_at=50.0)
    assert watch.now(system_now=52.5) == 1_002.5


def test_new_event_resets_the_watch_to_its_event_time():
    watch = WatchTime()
    watch.observe(event_time=1_000.0, arrived_at=50.0)
    watch.observe(event_time=1_010.0, arrived_at=51.0)
    assert watch.now(system_now=51.0) == 1_010.0


def test_older_event_never_moves_the_watch_back():
    watch = WatchTime()
    watch.observe(event_time=1_010.0, arrived_at=50.0)
    watch.observe(event_time=1_000.0, arrived_at=51.0)
    assert watch.now(system_now=51.0) == 1_011.0


def test_watch_time_is_none_before_the_first_event():
    assert WatchTime().now(system_now=10.0) is None


def test_moment_is_the_watch_time_as_utc_datetime():
    from datetime import datetime, timezone
    watch = WatchTime(system_time=lambda: 52.0)
    watch.observe(event_time=1_000.0, arrived_at=50.0)
    assert watch.moment() == datetime.fromtimestamp(1_002.0, timezone.utc)


async def test_ready_waits_for_the_first_event():
    import asyncio
    watch = WatchTime()
    waiter = asyncio.ensure_future(watch.ready())
    await asyncio.sleep(0)
    assert not waiter.done()
    watch.observe(event_time=1.0, arrived_at=1.0)
    await asyncio.wait_for(waiter, 1.0)
