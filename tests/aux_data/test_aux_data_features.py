import asyncio
import logging
from datetime import date, timedelta

import pytest

from time_helpers import ManualWatchTime
from alchemist.external_data.sources import ConfirmedBackupAuxSource, FileAuxSource, build_aux_source
from alchemist.instruments import FDAX, MES

from aux_helpers import (tape_bar, TARGET, ScriptedSource, StubStrategy, berlin, make_provider, ny, row,
                         run_minutes, utc)

PREVIOUS = date(2026, 4, 14)


@pytest.mark.parametrize("second,expected", [("confirms", 7.0), ("disagrees", None), ("missing", None)])
async def test_backup_value_used_only_when_confirmed(second, expected):
    clock = ManualWatchTime(ny(7))
    primary, backup, confirm = (ScriptedSource(clock) for _ in range(3))
    backup.publish(ny(0), TARGET, 7.0)
    if second != "missing":
        confirm.publish(ny(0), TARGET, 7.0 if second == "confirms" else 8.0)
    rows = await ConfirmedBackupAuxSource(primary, backup, confirm).fetch("X", PREVIOUS, TARGET)
    assert [r["value"] for r in rows] == ([] if expected is None else [expected])


def test_source_built_from_config_spec_with_nested_sources():
    file = {"source": "file", "root": "aux"}
    source = build_aux_source({"source": "confirmed_backup", "primary": file, "backup": file, "confirm": file})
    assert isinstance(source, ConfirmedBackupAuxSource)
    assert all(isinstance(s, FileAuxSource) for s in (source.primary, source.backup, source.confirm))


def test_unknown_source_name_refused():
    with pytest.raises(KeyError, match="nope"):
        build_aux_source({"source": "nope"})


async def test_primary_value_wins_over_backup():
    clock = ManualWatchTime(ny(7))
    primary, backup, confirm = (ScriptedSource(clock) for _ in range(3))
    primary.publish(ny(0), TARGET, 1.0)
    backup.publish(ny(0), TARGET, 7.0)
    confirm.publish(ny(0), TARGET, 7.0)
    rows = await ConfirmedBackupAuxSource(primary, backup, confirm).fetch("X", PREVIOUS, TARGET)
    assert [r["value"] for r in rows] == [1.0]


@pytest.mark.parametrize("instrument,start,end", [(MES, ny(0), ny(9, 29)), (FDAX, berlin(0), berlin(7, 59))])
async def test_carried_value_never_arms(instrument, start, end):
    clock = ManualWatchTime(start)
    source = ScriptedSource(clock)
    source.publish(start, PREVIOUS, 9.0)
    member = StubStrategy("m", instrument, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    await run_minutes(provider, clock, start, end, interval=300)
    assert member.ready_calls == 0 and not provider.groups[0].armed


async def test_fetch_at_most_once_per_target_before_settle_time():
    clock = ManualWatchTime(ny(10, day=14))
    source = ScriptedSource(clock)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    provider.sync_groups()
    group = provider.groups[0]
    group.begin_target(TARGET, ny(10, day=14))
    for minute in range(5):
        clock.advance(ny(10, minute, day=14))
        await group.step(provider, clock.moment())
    assert source.fetches <= 1


async def test_arm_groups_per_exchange_are_independent():
    clock = ManualWatchTime(utc(15, 4))
    cme_source, eurex_source = ScriptedSource(clock), ScriptedSource(clock)
    cme_source.publish(utc(14, 21), TARGET, 1.0)
    cme = StubStrategy("cme", MES, ["X"])
    eurex = StubStrategy("eurex", FDAX, ["Y"])
    provider = make_provider({"X": cme_source, "Y": eurex_source}, [cme, eurex], clock)
    await run_minutes(provider, clock, utc(15, 4), utc(15, 5))
    healthy = [(g.profile.name, g.armed) for g in provider.groups]
    eurex_source.always_fail = True
    await run_minutes(provider, clock, utc(15, 6), utc(15, 8))
    assert [(g.profile.name, g.armed) for g in provider.groups] == healthy


async def test_groups_stepped_every_interval():

    clock = ManualWatchTime(ny(7))

    async def stepping_sleep(timeout):
        clock.step(timeout)
        if clock.moment() >= ny(7, 10):
            raise asyncio.CancelledError

    source = ScriptedSource(clock)
    provider = make_provider({"X": source}, [StubStrategy("m", MES, ["X"])], clock, sleep=stepping_sleep,
                             step_interval_s=60.0)
    with pytest.raises(asyncio.CancelledError):
        await provider.run()
    assert source.fetches == 10


async def test_not_armed_by_open_sits_session_out():
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(10), TARGET, 1.0)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    await run_minutes(provider, clock, ny(7), ny(15), interval=300)
    after_open = [t for t in source.fetch_times if t >= ny(9, 30)]
    assert after_open == [] and member.ready_calls == 0


async def test_seed_error_leaves_group_unarmed(caplog):
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(0), TARGET, 1.0)
    member = StubStrategy("m", MES, ["X"], raise_on_ready_call=1)
    provider = make_provider({"X": source}, [member], clock)
    with caplog.at_level(logging.WARNING):
        await run_minutes(provider, clock, ny(7), ny(7, 5))
    assert not provider.groups[0].armed
    assert [c for c in member.calls if c[0]] == []
    assert len([r for r in caplog.records if r.levelno == logging.CRITICAL]) == 1


async def test_start_after_open_still_arms():
    clock = ManualWatchTime(ny(11))
    source = ScriptedSource(clock)
    source.publish(ny(0), TARGET, 1.0)
    provider = make_provider({"X": source}, [StubStrategy("m", MES, ["X"])], clock)
    await run_minutes(provider, clock, ny(11), ny(11, 1))
    assert provider.groups[0].armed


async def test_target_roll_disarms_group():
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(0), TARGET, 1.0)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    await run_minutes(provider, clock, ny(7), ny(7, 1))
    assert provider.groups[0].armed
    await run_minutes(provider, clock, ny(16, 1), ny(16, 2))
    assert not provider.groups[0].armed and member.calls[-1][0] is False
    bar = tape_bar(MES, ny(9, 31, day=16).timestamp(), 1.0)
    provider.annotate(MES, bar)
    assert "aux_X" not in bar


async def test_changed_value_after_arm_is_adopted_and_reseeded():
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(7), TARGET, 1.0)
    source.publish(ny(8, 30), TARGET, 3.0)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock, recheck_interval_s=3600.0)
    await run_minutes(provider, clock, ny(7), ny(9, 0))
    assert member.ready_calls == 2 and member.calls[-1][1]["X"][-1]["value"] == 3.0
    fetches_after_arm = source.fetches
    await run_minutes(provider, clock, ny(9, 1), ny(9, 20))
    assert source.fetches == fetches_after_arm


@pytest.mark.parametrize("outcome,armed", [("fetch_fails", True), ("target_gone", True), ("reseed_raises", False)])
async def test_recheck_outcomes(outcome, armed):
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(7), TARGET, 1.0)
    member = StubStrategy("m", MES, ["X"], raise_on_ready_call=2 if outcome == "reseed_raises" else None)
    provider = make_provider({"X": source}, [member], clock)
    await run_minutes(provider, clock, ny(7), ny(7, 1))
    if outcome == "fetch_fails":
        source.fail_from = ny(8, 1)
    elif outcome == "target_gone":
        source.publications.clear()
    else:
        source.publish(ny(8), TARGET, 2.0)
    await run_minutes(provider, clock, ny(8), ny(8, 5))
    assert provider.groups[0].armed is armed


async def test_annotate_adds_bound_values_before_dispatch():
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(0), TARGET, 1.25)
    provider = make_provider({"X": source}, [StubStrategy("m", MES, ["X"])], clock)
    await run_minutes(provider, clock, ny(7), ny(7, 1))
    bound, unbound = tape_bar(MES, ny(9, 31).timestamp(), 1.0), tape_bar(FDAX, ny(9, 31).timestamp(), 1.0)
    unbound_before = dict(unbound)
    provider.annotate(MES, bound)
    provider.annotate(FDAX, unbound)
    assert bound["aux_X"] == 1.25 and unbound == unbound_before


async def test_annotate_leaves_other_trading_dates_alone():
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(0), TARGET, 1.25)
    provider = make_provider({"X": source}, [StubStrategy("m", MES, ["X"])], clock)
    await run_minutes(provider, clock, ny(7), ny(7, 1))
    other_day = tape_bar(MES, ny(9, 31, day=17).timestamp(), 1.0)
    provider.annotate(MES, other_day)
    assert "aux_X" not in other_day


async def test_rebuild_while_armed_gets_current_frames():
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(0), TARGET, 1.0)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    await run_minutes(provider, clock, ny(7), ny(7, 1))
    assert provider.frames_for(MES) == member.calls[-1][1] != {}


@pytest.mark.parametrize("joiner_reads,same_group", [(["X"], True), (["Y"], False)])
async def test_late_joiner_gets_own_group_unless_covered(joiner_reads, same_group):
    clock = ManualWatchTime(ny(7))
    sources = {"X": ScriptedSource(clock), "Y": ScriptedSource(clock)}
    sources["X"].publish(ny(0), TARGET, 1.0)
    provider = make_provider(sources, [StubStrategy("a", MES, ["X"])], clock)
    await run_minutes(provider, clock, ny(7), ny(7, 1))
    provider.member_joined(StubStrategy("b", MES, joiner_reads))
    await run_minutes(provider, clock, ny(7, 2), ny(7, 3))
    assert (len(provider.groups) == 1) is same_group


async def test_departed_member_is_dropped():
    clock = ManualWatchTime(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(0), TARGET, 1.0)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    await run_minutes(provider, clock, ny(7), ny(7, 1))
    provider.member_left("m")
    await run_minutes(provider, clock, ny(7, 2), ny(7, 2))
    assert provider.groups[0].member_names == set()


async def test_resident_task_crash_logs_critical(caplog):
    clock = ManualWatchTime(ny(7))
    provider = make_provider({"X": ScriptedSource(clock)}, [StubStrategy("m", MES, ["X"])], clock)

    async def explode():
        raise RuntimeError("boom")

    provider.step_all = explode
    with caplog.at_level(logging.CRITICAL):
        await provider.start_resident_task()
        await asyncio.sleep(0.01)
        await provider.stop()
    assert any(r.levelno == logging.CRITICAL for r in caplog.records)




def test_aux_holds_no_runtime_book_and_no_clock():
    import inspect
    from alchemist.external_data.provider import AuxProvider
    assert set(inspect.signature(AuxProvider).parameters).isdisjoint({"book", "clock"})
