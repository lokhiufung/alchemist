import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest

from alchemist.config import EventLoopConfig
from alchemist.exchanges import exchange_profile
from alchemist.instruments import ES, FDAX, MES, NQ

from el_helpers import end_of_data, order_update
from sd_helpers import FlattenStrategy, Rig, ShutdownGateway, at

FAST = EventLoopConfig(flatten_timeout_s=0.05)


def cutoff_cases():
    return [("CME", MES, "NY_RTH", 15, 59, 0, 1), ("EUREX", FDAX, "FRA_DAY", 21, 59, 0, 1)]


@pytest.mark.parametrize("exchange,instrument,session,hour,minute,second,_", cutoff_cases())
async def test_cutoff_on_session_clock(exchange, instrument, session, hour, minute, second, _):
    strategy = FlattenStrategy("s", instrument, flat_on_request=None, session=session)
    start = at(instrument, session, hour, minute, 0) - timedelta(seconds=30)
    rig = Rig([strategy], start)
    rig.push_second_bars(instrument, start, 120)
    rig.buffer.push(end_of_data())
    await rig.run()
    first = strategy.request_times[0]
    cutoff = at(instrument, session, hour, minute, 0).timestamp()
    assert cutoff <= first <= cutoff + 1.0
    assert all(t >= cutoff for t in strategy.request_times)


async def test_overnight_session_cutoff_on_its_own_clock():
    strategy = FlattenStrategy("s", MES, flat_on_request=None, session="NY_GLOBEX")
    start = at(MES, "NY_GLOBEX", 16, 58, 30)
    rig = Rig([strategy], start)
    rig.push_second_bars(MES, start, 60)
    rig.buffer.push(end_of_data())
    await rig.run()
    cutoff = at(MES, "NY_GLOBEX", 16, 59, 0).timestamp()
    assert cutoff <= strategy.request_times[0] <= cutoff + 1.0


async def test_early_close_cutoff_from_exchange_profile():
    strategy = FlattenStrategy("s", MES, flat_on_request=None)
    start = at(MES, "NY_RTH", 12, 58, 30, day=27, month=11)
    rig = Rig([strategy], start)
    rig.push_second_bars(MES, start, 120)
    rig.buffer.push(end_of_data())
    await rig.run()
    cutoff = at(MES, "NY_RTH", 12, 59, 0, day=27, month=11).timestamp()
    assert cutoff <= strategy.request_times[0] <= cutoff + 1.0


@pytest.mark.parametrize("instrument,session", [(MES, "NY_RTH"), (FDAX, "FRA_DAY")])
async def test_scenario_session_end_flatten(instrument, session):
    hour, minute = (15, 59) if instrument is MES else (21, 59)
    flat_third = FlattenStrategy("third", instrument, flat_on_request=3, session=session)
    never_flat = FlattenStrategy("never", instrument, flat_on_request=None, session=session)
    start = at(instrument, session, hour, minute, 0) - timedelta(seconds=20)
    rig = Rig([flat_third, never_flat], start)
    rig.push_second_bars(instrument, start, 90)
    rig.buffer.push(end_of_data())
    await rig.run()
    cutoff = at(instrument, session, hour, minute, 0).timestamp()
    assert len(flat_third.request_times) == 3
    first = flat_third.request_times[0]
    assert first >= cutoff
    assert all(not (first <= ts + 1.0 <= first + 3.0) for ts in flat_third.bar_log)
    span = never_flat.request_times[-1] - never_flat.request_times[0]
    assert 9.0 <= span <= 10.0 + 1e-9


async def test_flatten_repeats_until_flat_signal_or_timeout():
    flat = FlattenStrategy("flat", MES, flat_on_request=2)
    start = at(MES, "NY_RTH", 15, 59, 0) - timedelta(seconds=2)
    rig = Rig([flat], start)
    rig.push_second_bars(MES, start, 40)
    rig.buffer.push(end_of_data())
    await rig.run()
    assert len(flat.request_times) == 2

    signalled = FlattenStrategy("signalled", MES, flat_on_request=4)
    rig = Rig([signalled], start)
    rig.push_second_bars(MES, start, 40)
    original = rig.loop.dispatch_bar
    at_signal = []

    async def dispatch_bar(instrument, bar):
        await original(instrument, bar)
        if len(signalled.request_times) == 3 and not at_signal:
            at_signal.append(len(signalled.request_times))
            rig.shutdown.request_shutdown("signal")

    rig.loop.dispatch_bar = dispatch_bar
    rig.buffer.push(end_of_data())
    await rig.run()
    assert at_signal == [3] and len(signalled.request_times) == 4


async def test_day_strategy_added_in_evening_not_due_that_evening():
    strategy = FlattenStrategy("day", MES, flat_on_request=None)
    evening = at(MES, "NY_RTH", 20, 0, 0)
    rig = Rig([strategy], evening)
    rig.push_second_bars(MES, evening, 30)
    next_cutoff = at(MES, "NY_RTH", 15, 59, 0, day=16)
    rig.push_second_bars(MES, next_cutoff - timedelta(seconds=5), 20)
    rig.buffer.push(end_of_data())
    await rig.run()
    assert strategy.request_times and strategy.request_times[0] >= next_cutoff.timestamp()


def working_order(order_id, instrument):
    return {"order_id": order_id, "instrument": instrument, "side": "BUY", "qty": 1, "order_type": "LMT",
            "account_id": "ACC"}


def position(instrument, size):
    return {"account_id": "ACC", "instrument": instrument, "size": size, "avg_cost": 100.0, "currency": "USD"}


@pytest.mark.parametrize("case", ["readable", "unreadable"])
async def test_scenario_shutdown_flat(case, caplog):
    mes_slot, es_slot = FlattenStrategy("mes", MES, flat_on_request=None), FlattenStrategy("es", ES, flat_on_request=None)
    gateway = ShutdownGateway(open_orders=[working_order(11, MES)],
                              positions=[position(ES, 1), position(NQ, 2)], unreadable=case == "unreadable")
    rig = Rig([mes_slot, es_slot], at(MES, "NY_RTH", 11, 0, 0), gateway=gateway, config=FAST)
    rig.shutdown.request_shutdown("signal")
    with caplog.at_level(logging.INFO):
        await rig.run()
    assert len(rig.buffer) == 0 and rig.bar_recorder.finalized and rig.trade_recorder.finalized
    if case == "readable":
        assert gateway.cancelled == [11]
        assert [(o["instrument"], o["side"], o["qty"], o["order_type"]) for o in gateway.placed] == [
            (ES, "SELL", 1, "MKT")]
        assert rig.shutdown.not_flat_at_shutdown is False
        assert [p["size"] for p in gateway.positions] == [2]
    else:
        assert gateway.placed == []
        assert rig.shutdown.not_flat_at_shutdown is True
        assert any(r.levelno >= logging.CRITICAL for r in caplog.records)


async def test_unreadable_open_orders_block_force_flatten():
    gateway = ShutdownGateway(positions=[position(ES, 1)], unreadable=True)
    rig = Rig([FlattenStrategy("es", ES, flat_on_request=None)], at(MES, "NY_RTH", 11, 0, 0), gateway=gateway, config=FAST)
    rig.shutdown.request_shutdown("signal")
    await rig.run()
    assert gateway.placed == []


async def test_leftover_position_with_working_order_is_flagged_not_forced():
    gateway = ShutdownGateway(open_orders=[working_order(5, ES)], positions=[position(ES, 1)])

    async def stuck_cancel(order_id):
        gateway.cancelled.append(order_id)

    gateway.cancel_order = stuck_cancel
    config = EventLoopConfig(flatten_timeout_s=0.05)
    rig = Rig([FlattenStrategy("es", ES, flat_on_request=None)], at(MES, "NY_RTH", 11, 0, 0), gateway=gateway,
              config=config)
    rig.shutdown.request_shutdown("signal")
    await rig.run()
    assert gateway.placed == [] and rig.shutdown.not_flat_at_shutdown is True


async def test_end_of_data_leaves_no_queued_event():
    strategy = FlattenStrategy("s", MES, flat_on_request=None)
    rig = Rig([strategy], at(MES, "NY_RTH", 10, 0, 0))
    rig.push_second_bars(MES, at(MES, "NY_RTH", 10, 0, 0), 5)
    rig.buffer.push(end_of_data())
    await rig.run()
    assert len(rig.buffer) == 0 and rig.bar_recorder.finalized and rig.trade_recorder.finalized


async def test_is_flat_reads_the_gateway_positions_of_configured_instruments():
    gateway = ShutdownGateway(positions=[position(ES, 1), position(NQ, 4)])
    rig = Rig([FlattenStrategy("es", ES, flat_on_request=None)], at(MES, "NY_RTH", 11, 0, 0), gateway=gateway)
    assert await rig.shutdown.is_flat() is False
    gateway.positions = [position(NQ, 4)]
    assert await rig.shutdown.is_flat() is True


async def test_event_loop_never_calls_safety_repair():
    import re
    from pathlib import Path
    offenders = [p for p in Path("alchemist/event_loop").glob("*.py")
                 if re.search(r"repair_stuck|get_executions|reconcile_stuck", p.read_text())]
    assert offenders == []


async def test_watch_time_drives_flatten_and_flush():
    import time
    from datetime import datetime as dt
    from el_helpers import Harness, RecordingStrategy, tick
    strategy = FlattenStrategy("s", MES, flat_on_request=None)
    start = at(MES, "NY_RTH", 15, 58, 50)
    rig = Rig([strategy], start)
    rig.push_second_bars(MES, start, 30)
    rig.buffer.push(end_of_data())
    started = time.time()
    await rig.run()
    cutoff = at(MES, "NY_RTH", 15, 59, 0).timestamp()
    assert strategy.request_times[0] == cutoff and abs(strategy.request_times[0] - started) > 86_400
    from alchemist.messages import make_event
    flush_strategy = RecordingStrategy()
    harness = Harness([flush_strategy], message_type="tick_update")
    emitted = []
    original = harness.loop.emit_bar

    async def spy(instrument, bar, source, origin=None):
        emitted.append(source)
        await original(instrument, bar, source, origin)

    harness.loop.emit_bar = spy
    harness.push(tick(1_776_259_800.2, 1.0, 1), make_event("heartbeat", {}, 1_776_259_805.0))

    async def end_after_flush():
        while not flush_strategy.bars:
            await asyncio.sleep(0)
        harness.push(end_of_data())

    ender = asyncio.create_task(end_after_flush())
    await harness.run()
    await ender
    assert flush_strategy.bars == [1_776_259_800.0] and emitted == ["flush"]


async def test_process_due_does_not_recompute_session_windows_on_every_call(monkeypatch):
    from alchemist.exchanges import ExchangeProfile
    calls = []
    original = ExchangeProfile.session_window_at

    def counting(self, name, at):
        calls.append(1)
        return original(self, name, at)

    monkeypatch.setattr(ExchangeProfile, "session_window_at", counting)
    strategies = [FlattenStrategy(f"s{i}", MES, flat_on_request=None) for i in range(15)]
    start = at(MES, "NY_RTH", 11, 0, 0)
    rig = Rig(strategies, start)
    for second in range(500):
        rig.watch_time.advance(start + timedelta(seconds=second))
        await rig.shutdown.process_due()
    assert len(calls) <= 15 * 3


@pytest.mark.parametrize("start,expected", [
    (datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc), datetime(2026, 4, 16, 21, 0, tzinfo=timezone.utc)),
    (datetime(2026, 4, 15, 14, 0, tzinfo=timezone.utc), datetime(2026, 4, 15, 21, 0, tzinfo=timezone.utc)),
])
def test_run_exits_at_the_first_run_end_after_start(start, expected):
    from alchemist.event_loop.shutdown import run_exit_after
    assert run_exit_after(start, EventLoopConfig()) == expected


async def test_loop_exits_by_itself_at_run_end():
    strategy = FlattenStrategy("s", MES, flat_on_request=None, session="NY_GLOBEX")
    start = at(MES, "NY_GLOBEX", 16, 59, 50)
    rig = Rig([strategy], start, config=FAST)
    rig.push_second_bars(MES, start, 30)
    await rig.run()
    assert rig.shutdown.shutdown_requested
    assert rig.watch_time.now() >= rig.shutdown.run_exit.timestamp()
