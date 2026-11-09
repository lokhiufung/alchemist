import logging
from datetime import timedelta

import pytest

from alchemist.config import SafetyConfig
from alchemist.instruments import ES, FDAX, MES

from safety_helpers import Rig, utc

MONDAY_OPEN = utc(15, 15, 0)


async def test_scenario_dead_socket(caplog):
    t0 = MONDAY_OPEN + timedelta(seconds=2)
    rig = Rig(MONDAY_OPEN)
    rig.gateway.heartbeat_ok = lambda now: now < t0.timestamp() or rig.gateway.recovered
    rig.gateway.last_tick = lambda: rig.clock.timestamp()
    rig.gateway.reconnect_failures = 3
    with caplog.at_level(logging.INFO):
        await rig.steps(MONDAY_OPEN, MONDAY_OPEN + timedelta(seconds=10))
    reconnects = [c[1] for c in rig.gateway.calls if c[0] == "reconnect"]
    assert len(reconnects) == 4 and reconnects[0] <= t0.timestamp() + 8
    assert rig.sleeps[:3] == [15.0, 30.0, 60.0]
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)
    assert rig.monitor.failures == 0


@pytest.mark.parametrize("case", ["a_ticks_resume", "b_no_tick", "c_maintenance_window"])
async def test_scenario_stale_feed_ladder(case):
    windows = (("14:00", "16:00"),) if case == "c_maintenance_window" else ()
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(maintenance_windows=windows))
    rig.gateway.last_tick = MONDAY_OPEN.timestamp() - 46
    if case == "a_ticks_resume":
        rig.gateway.on_resubscribe = lambda: setattr(rig.gateway, "last_tick", rig.clock.timestamp())
    if case == "c_maintenance_window":
        rig.gateway.reconnect_failures = 1
    await rig.monitor.step()
    names = rig.gateway.names()
    if case == "a_ticks_resume":
        assert names.count("subscribe_tick") == 1 and "reconnect" not in names and rig.restarts == []
    elif case == "b_no_tick":
        assert rig.gateway.before_first_reconnect().count("subscribe_tick") == 3
        assert names.count("reconnect") == 1 and rig.restarts == []
    else:
        assert "reconnect" in names and len(rig.restarts) == 1


@pytest.mark.parametrize("case", ["saturday", "cme_open_eurex_closed"])
async def test_scenario_closed_session_quiet(case):
    if case == "saturday":
        start, instruments = utc(18, 12, 0), (MES, FDAX)
        last_tick = utc(17, 20, 0).timestamp()
    else:
        start, instruments = utc(15, 15, 0), (MES, FDAX)
        last_tick = start.timestamp()
    rig = Rig(start, instruments=instruments)
    rig.gateway.last_tick = last_tick if case == "saturday" else (lambda: rig.clock.timestamp())
    await rig.steps(start, start + timedelta(minutes=10), 30.0)
    assert [n for n in rig.gateway.names() if n in ("subscribe_tick", "reconnect")] == []
    assert rig.restarts == []


async def test_scenario_streams_restored():
    rig = Rig(MONDAY_OPEN, instruments=(MES, ES, FDAX))
    rig.book.commit(type("S", (), {"name": "a", "signal_instrument": MES, "execution_instrument": MES,
                                   "session": "NY_RTH"})(), [])
    rig.book.commit(type("S", (), {"name": "b", "signal_instrument": MES, "execution_instrument": MES,
                                   "session": "NY_RTH"})(), [])
    rig.gateway.heartbeat_ok = lambda now: False
    rig.gateway.last_tick = MONDAY_OPEN.timestamp()
    await rig.monitor.step()
    ticks = [c[2] for c in rig.gateway.calls if c[0] == "subscribe_tick"]
    assert sorted(ticks) == ["ES", "FDAX", "MES"]
    names = rig.gateway.names()
    assert names.index("subscribe_order_update") > names.index("reconnect")
    assert "subscribe_account_update" in names
