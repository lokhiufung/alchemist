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
