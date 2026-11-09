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
    rig.book.commit(PendingStrategy("a", MES, processing=False), [])
    rig.book.commit(PendingStrategy("b", MES, processing=False), [])
    rig.gateway.heartbeat_ok = lambda now: False
    rig.gateway.last_tick = MONDAY_OPEN.timestamp()
    await rig.monitor.step()
    ticks = [c[2] for c in rig.gateway.calls if c[0] == "subscribe_tick"]
    assert sorted(ticks) == ["ES", "FDAX", "MES"]
    names = rig.gateway.names()
    assert names.index("subscribe_order_update") > names.index("reconnect")
    assert "subscribe_account_update" in names


from safety_helpers import PendingStrategy, execution, order


async def test_scenario_dead_orders():
    now = MONDAY_OPEN.timestamp()
    strategy = PendingStrategy("s", MES, [order(50, MES, 150, now), order(51, MES, 100, now),
                                          order(52, MES, 150, now)])
    rig = Rig(MONDAY_OPEN, strategies=[strategy])
    rig.gateway.open_orders = [{"order_id": 52, "instrument": MES}]
    await rig.monitor.repair(rig.book.strategies)
    rejected = rig.buffer.of_type("order_update")
    assert [(u["order_id"], u["status"]) for u in rejected] == [(50, "REJECTED")]
    assert rig.buffer.of_type("repaired_fill") == []


async def test_scenario_stuck_order_timer():
    now = MONDAY_OPEN.timestamp()
    stuck = PendingStrategy("a", MES, [order(60, MES, 5, now)], processing=True)
    idle = PendingStrategy("b", ES, [order(61, ES, 5, now)], processing=False)
    rig = Rig(MONDAY_OPEN, strategies=[stuck, idle])
    rig.gateway.last_tick = lambda: rig.clock.timestamp()
    rig.gateway.executions = [execution(60, MES, 5000.0, now + 1), execution(61, ES, 5000.0, now + 1)]
    await rig.steps(MONDAY_OPEN, MONDAY_OPEN + timedelta(seconds=61))
    assert [f["order_id"] for f in rig.buffer.of_type("repaired_fill")] == [60]

    quiet = Rig(MONDAY_OPEN, strategies=[PendingStrategy("c", MES, [], processing=False)])
    quiet.gateway.last_tick = lambda: quiet.clock.timestamp()
    await quiet.steps(MONDAY_OPEN, MONDAY_OPEN + timedelta(seconds=120))
    assert "get_executions" not in quiet.gateway.names() and "get_open_orders" not in quiet.gateway.names()


async def test_scenario_frozen_loop():
    from alchemist.safety.freeze import FreezeDetector
    dumps = []
    detector = FreezeDetector(SafetyConfig(freeze_threshold=10), dump=lambda: dumps.append(1))
    for _ in range(5):
        detector.check_in()
        detector.tick()
    assert dumps == []
    for _ in range(12):
        detector.tick()
    assert len(dumps) == 1
    detector.check_in()
    detector.tick()
    for _ in range(10):
        detector.tick()
    assert len(dumps) == 2


@pytest.mark.parametrize("interval", [None, 30.0])
@pytest.mark.parametrize("case", ["answers", "fetch_fails"])
async def test_scenario_positions_poll(interval, case, caplog):
    rig = Rig(MONDAY_OPEN, config=SafetyConfig(positions_poll_s=interval))
    rig.gateway.last_tick = lambda: rig.clock.timestamp()
    rig.gateway.positions = [{"account_id": "ACC", "instrument": MES, "size": 1, "avg_cost": 5000.0,
                              "currency": "USD"}]
    rig.gateway.fail_reads = case == "fetch_fails"
    with caplog.at_level(logging.WARNING):
        await rig.steps(MONDAY_OPEN, MONDAY_OPEN + timedelta(seconds=90))
    fetches = rig.gateway.names().count("get_positions")
    updates = rig.buffer.of_type("position_update")
    if interval is None:
        assert fetches == 0 and updates == []
    elif case == "answers":
        assert 1 <= fetches <= 3 and all(u["size"] == 1 and u["instrument"] == MES for u in updates)
    else:
        assert 1 <= fetches <= 3 and updates == []
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) <= 3
