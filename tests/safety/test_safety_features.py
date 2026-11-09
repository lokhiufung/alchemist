import logging
from datetime import timedelta

import pytest

from alchemist.config import SafetyConfig
from alchemist.instruments import ES, FDAX, MES
from alchemist.safety.monitor import backoff_delay

from safety_helpers import Rig, utc

MONDAY_OPEN = utc(15, 15, 0)


@pytest.mark.parametrize("interval,timeout", [(5.0, 3.0)])
async def test_socket_failure_caught_within_interval(interval, timeout):
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(heartbeat_interval_s=interval, heartbeat_timeout_s=timeout))
    drop = MONDAY_OPEN + timedelta(seconds=7)
    rig.gateway.heartbeat_ok = lambda now: now < drop.timestamp()
    rig.gateway.last_tick = drop.timestamp()
    await rig.steps(MONDAY_OPEN, MONDAY_OPEN + timedelta(seconds=30), interval)
    first_reconnect = next(c[1] for c in rig.gateway.calls if c[0] == "reconnect")
    assert first_reconnect - drop.timestamp() <= interval + timeout


async def test_heartbeat_timeout_is_passed_to_the_gateway():
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(heartbeat_timeout_s=2.5))
    await rig.monitor.step()
    assert rig.gateway.calls[0][2] == 2.5


@pytest.mark.parametrize("threshold", [45.0])
async def test_stale_feed_starts_ladder(threshold):
    rig = Rig(MONDAY_OPEN)
    rig.gateway.last_tick = MONDAY_OPEN.timestamp()
    await rig.steps(MONDAY_OPEN, MONDAY_OPEN + timedelta(seconds=120))
    first = next(c[1] for c in rig.gateway.calls if c[0] == "subscribe_tick")
    assert first - MONDAY_OPEN.timestamp() <= threshold + rig.config.heartbeat_interval_s + 1e-9
    assert first - MONDAY_OPEN.timestamp() > threshold


@pytest.mark.parametrize("instruments", [(MES,), (MES, FDAX)])
async def test_no_stale_alarm_when_every_exchange_in_break(instruments):
    saturday = utc(18, 12, 0)
    rig = Rig(saturday, instruments=instruments)
    rig.gateway.last_tick = utc(17, 21, 0).timestamp()
    await rig.steps(saturday, saturday + timedelta(minutes=10), 60.0)
    assert [n for n in rig.gateway.names() if n in ("subscribe_tick", "reconnect")] == []


async def test_day_only_book_still_watched_overnight():
    overnight = utc(15, 3, 0)
    rig = Rig(overnight, instruments=(MES,))
    rig.gateway.last_tick = overnight.timestamp() - 100
    await rig.steps(overnight, overnight + timedelta(seconds=10))
    assert "subscribe_tick" in rig.gateway.names()


async def test_stale_threshold_can_be_overridden_per_exchange():
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(stale_overrides={"CME": 200.0}))
    rig.gateway.last_tick = MONDAY_OPEN.timestamp() - 100
    await rig.steps(MONDAY_OPEN, MONDAY_OPEN + timedelta(seconds=30))
    assert "subscribe_tick" not in rig.gateway.names()


@pytest.mark.parametrize("attempts", [4])
async def test_ladder_order(attempts):
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(resubscribe_without_tick_cap=attempts))
    rig.gateway.last_tick = MONDAY_OPEN.timestamp() - 100
    await rig.monitor.step()
    names = [n for n in rig.gateway.names() if n in ("subscribe_tick", "reconnect")]
    assert names[:attempts + 1] == ["subscribe_tick"] * attempts + ["reconnect"]


@pytest.mark.parametrize("cap", [3])
async def test_resubscribe_without_tick_capped(cap):
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(resubscribe_without_tick_cap=cap))
    rig.gateway.last_tick = MONDAY_OPEN.timestamp() - 100
    await rig.monitor.step()
    names = [n for n in rig.gateway.names() if n in ("subscribe_tick", "reconnect")]
    assert names[:cap + 1] == ["subscribe_tick"] * cap + ["reconnect"]


async def test_resubscribe_backoff_is_exponential():
    rig = Rig(MONDAY_OPEN)
    rig.gateway.last_tick = MONDAY_OPEN.timestamp() - 100
    await rig.monitor.step()
    base = rig.config.resubscribe_backoff_base_s
    assert rig.sleeps[:3] == [base, base * 2, base * 4]


@pytest.mark.parametrize("window,restarts", [(("14:00", "16:00"), 1), (("02:00", "04:00"), 0)])
async def test_process_restart_only_in_maintenance_window(window, restarts):
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(maintenance_windows=(window,)))
    rig.gateway.last_tick = MONDAY_OPEN.timestamp() - 100
    rig.gateway.reconnect_failures = 2
    await rig.monitor.step()
    assert len(rig.restarts) == restarts


async def test_cheap_rungs_come_before_a_process_restart():
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(maintenance_windows=(("14:00", "16:00"),)))
    rig.gateway.last_tick = MONDAY_OPEN.timestamp() - 100
    rig.gateway.reconnect_failures = 1
    await rig.monitor.step()
    names = rig.gateway.names()
    assert names.count("subscribe_tick") >= 3 and "reconnect" in names
    assert rig.gateway.before_first_reconnect().count("subscribe_tick") == 3
    assert len(rig.restarts) == 1 and rig.restarts[0] >= rig.gateway.calls[names.index("reconnect")][1]


async def test_ticks_resuming_after_the_first_resubscribe_stop_the_ladder():
    rig = Rig(MONDAY_OPEN)
    rig.gateway.last_tick = MONDAY_OPEN.timestamp() - 100
    rig.gateway.on_resubscribe = lambda: setattr(rig.gateway, "last_tick", rig.clock.timestamp())
    await rig.monitor.step()
    assert rig.gateway.names().count("subscribe_tick") == 1 and "reconnect" not in rig.gateway.names()


async def test_connectivity_restored_signal_resubscribes_and_is_capped():
    rig = Rig(MONDAY_OPEN)
    rig.gateway.last_tick = MONDAY_OPEN.timestamp()
    for _ in range(4):
        rig.gateway.resubscribe_request = True
        rig.advance_to(rig.clock.now() + timedelta(seconds=1))
        await rig.monitor.step()
    names = [n for n in rig.gateway.names() if n in ("subscribe_tick", "reconnect")]
    assert names[:4] == ["subscribe_tick"] * 3 + ["reconnect"]
    assert rig.monitor.blind_resubscribes == 0


@pytest.mark.parametrize("failures", [1, 50])
async def test_reconnect_never_gives_up(failures):
    rig = Rig(MONDAY_OPEN)
    rig.gateway.reconnect_failures = failures
    await rig.monitor.reconnect_forever()
    assert rig.gateway.names().count("reconnect") == failures + 1
    assert max(rig.sleeps) <= rig.config.reconnect_cap_s


@pytest.mark.parametrize("base,cap", [(15.0, 120.0)])
def test_backoff_delays(base, cap):
    assert [backoff_delay(n, base, cap) for n in range(1, 9)] == [min(cap, base * 2 ** (n - 1)) for n in range(1, 9)]


async def test_persistent_reconnect_failure_logs_critical(caplog):
    rig = Rig(MONDAY_OPEN)
    rig.gateway.reconnect_failures = 3
    with caplog.at_level(logging.INFO):
        await rig.monitor.reconnect_forever()
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)


async def test_restore_subscribes_each_fed_instrument_once():
    rig = Rig(MONDAY_OPEN, instruments=(MES, ES, FDAX))
    await rig.monitor.restore_subscriptions()
    ticks = [c[2] for c in rig.gateway.calls if c[0] == "subscribe_tick"]
    assert sorted(ticks) == ["ES", "FDAX", "MES"]
    assert {"subscribe_order_update", "subscribe_account_update"} <= set(rig.gateway.names())


async def test_failure_counter_only_goes_up_until_restored():
    rig = Rig(MONDAY_OPEN)
    rig.gateway.heartbeat_ok = lambda now: False
    rig.gateway.reconnect_failures = 0
    await rig.monitor.step()
    await rig.monitor.step()
    assert rig.monitor.failures == 2
    rig.gateway.heartbeat_ok = lambda now: True
    await rig.monitor.step()
    assert rig.monitor.failures == 0


from safety_helpers import PendingStrategy, execution, order


async def test_resting_stop_filled_while_in_position_repaired():
    now = MONDAY_OPEN.timestamp()
    in_position = PendingStrategy("p", MES, [order(70, MES, 30, now, side="SELL")], processing=False)
    rig = Rig(MONDAY_OPEN, strategies=[in_position])
    rig.gateway.executions = [execution(70, MES, 4990.0, now, side="SELL")]
    await rig.monitor.repair(rig.book.strategies)
    assert [(f["order_id"], f["status"]) for f in rig.buffer.of_type("repaired_fill")] == [(70, "FILLED")]


async def test_two_strategies_same_instrument_attributed_by_id():
    now = MONDAY_OPEN.timestamp()
    first = PendingStrategy("first", MES, [order(80, MES, 10, now)])
    second = PendingStrategy("second", MES, [order(81, MES, 10, now)])
    rig = Rig(MONDAY_OPEN, strategies=[first, second])
    rig.gateway.executions = [execution(81, MES, 5001.0, now)]
    await rig.monitor.repair(rig.book.strategies)
    fills = rig.buffer.of_type("repaired_fill")
    assert [f["order_id"] for f in fills] == [81] and fills[0]["avg_fill_price"] == 5001.0


@pytest.mark.parametrize("age", [100, 150])
@pytest.mark.parametrize("found_in", ["neither", "open_orders"])
async def test_dead_order_rejected_only_after_grace_and_absent_both(age, found_in):
    now = MONDAY_OPEN.timestamp()
    rig = Rig(MONDAY_OPEN, strategies=[PendingStrategy("s", MES, [order(90, MES, age, now)])])
    if found_in == "open_orders":
        rig.gateway.open_orders = [{"order_id": 90, "instrument": MES}]
    await rig.monitor.repair(rig.book.strategies)
    expected = 1 if (age >= 120 and found_in == "neither") else 0
    assert len(rig.buffer.of_type("order_update")) == expected


async def test_dead_order_check_also_requires_absence_from_executions():
    now = MONDAY_OPEN.timestamp()
    rig = Rig(MONDAY_OPEN, strategies=[PendingStrategy("s", MES, [order(91, MES, 500, now)])])
    rig.gateway.executions = [execution(91, MES, 5000.0, now)]
    await rig.monitor.repair(rig.book.strategies)
    assert rig.buffer.of_type("order_update") == [] and len(rig.buffer.of_type("repaired_fill")) == 1


async def test_unreadable_broker_state_repairs_nothing():
    now = MONDAY_OPEN.timestamp()
    rig = Rig(MONDAY_OPEN, strategies=[PendingStrategy("s", MES, [order(92, MES, 500, now)])])
    rig.gateway.fail_reads = True
    assert await rig.monitor.repair(rig.book.strategies) == 0
    assert rig.buffer.events == []


@pytest.mark.parametrize("loop_state", ["healthy", "blocked"])
def test_freeze_dump_only_when_check_ins_missed(loop_state):
    from alchemist.safety.freeze import FreezeDetector
    dumps = []
    detector = FreezeDetector(SafetyConfig(freeze_threshold=10), dump=lambda: dumps.append(1))
    for _ in range(30):
        if loop_state == "healthy":
            detector.check_in()
        detector.tick()
    assert len(dumps) == (0 if loop_state == "healthy" else 1)


@pytest.mark.parametrize("poll,max_polls", [(None, 0), (60.0, 10)])
async def test_positions_poll_respects_config(poll, max_polls):
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(positions_poll_s=poll))
    rig.gateway.last_tick = lambda: rig.clock.timestamp()
    rig.gateway.positions = [{"account_id": "ACC", "instrument": MES, "size": 1, "avg_cost": 1.0, "currency": "USD"}]
    await rig.steps(MONDAY_OPEN, MONDAY_OPEN + timedelta(minutes=10), 5.0)
    polls = rig.gateway.names().count("get_positions")
    assert (polls == 0) if poll is None else (1 <= polls <= max_polls)


async def test_strategy_committed_after_start_is_repaired():
    now = MONDAY_OPEN.timestamp()
    rig = Rig(MONDAY_OPEN)
    await rig.monitor.repair(rig.book.strategies)
    late = PendingStrategy("late", MES, [order(95, MES, 10, now)])
    rig.book.strategies.append(late)
    rig.gateway.executions = [execution(95, MES, 5000.0, now)]
    await rig.monitor.repair(rig.book.strategies)
    assert [f["order_id"] for f in rig.buffer.of_type("repaired_fill")] == [95]


async def test_stuck_check_runs_every_interval():
    now = MONDAY_OPEN.timestamp()
    rig = Rig(MONDAY_OPEN, strategies=[PendingStrategy("s", MES, [order(96, MES, 5, now)])])
    rig.gateway.last_tick = lambda: rig.clock.timestamp()
    await rig.steps(MONDAY_OPEN, MONDAY_OPEN + timedelta(minutes=10), 5.0)
    assert rig.gateway.names().count("get_executions") == 10


async def test_stuck_check_does_not_block_bar_dispatch():
    import asyncio
    now = MONDAY_OPEN.timestamp()
    rig = Rig(MONDAY_OPEN, strategies=[PendingStrategy("s", MES, [order(97, MES, 5, now)])])
    rig.gateway.last_tick = lambda: rig.clock.timestamp()
    original = rig.gateway.get_executions

    async def slow(since=None):
        await asyncio.sleep(0.05)
        return await original(since)

    rig.gateway.get_executions = slow
    ticks = []
    checking = asyncio.create_task(rig.monitor.stuck_check())
    while not checking.done():
        ticks.append(1)
        await asyncio.sleep(0)
    assert len(ticks) > 5


async def test_reconnect_repairs_every_strategy_in_every_phase():
    now = MONDAY_OPEN.timestamp()
    idle_with_resting = PendingStrategy("p", MES, [order(98, MES, 10, now, side="SELL")], processing=False)
    rig = Rig(MONDAY_OPEN, strategies=[idle_with_resting])
    rig.gateway.executions = [execution(98, MES, 4990.0, now, side="SELL")]
    await rig.monitor.reconnect_forever()
    assert [f["order_id"] for f in rig.buffer.of_type("repaired_fill")] == [98]
