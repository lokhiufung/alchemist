import logging
from datetime import date, timedelta

import pytest

from alchemist.clock import ReplayClock
from alchemist.external_data.provider import AuxDataError
from alchemist.external_data.sources import ConfirmedBackupAuxSource, FileAuxSource
from alchemist.instruments import FDAX, MES

from aux_helpers import (tape_bar, TARGET, ScriptedSource, StubStrategy, berlin, declare_names, make_provider, ny, row,
                         run_minutes, utc)

PREVIOUS = date(2026, 4, 14)
NEXT = date(2026, 4, 16)


def bar_at(instrument, moment):
    return tape_bar(instrument, moment.timestamp(), 5000.0)


def ready_times(strategy, log):
    def record(moment):
        if strategy.ready_calls and not log:
            log.append(moment)
    return record


async def test_scenario_arm_before_open():
    declare_names("X")
    clock = ReplayClock(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(8), TARGET, 1.5)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    armed_at: list = []
    await run_minutes(provider, clock, ny(7), ny(9, 31), on_step=ready_times(member, armed_at))
    assert armed_at and ny(8) <= armed_at[0] <= ny(8, 1)
    assert member.ready_calls == 1
    ready, frames = member.calls[0]
    assert ready is True and frames["X"][-1] == row(TARGET, 1.5)
    bar = bar_at(MES, ny(9, 31))
    provider.annotate(MES, bar)
    assert bar["aux_X"] == 1.5


async def test_scenario_unsettled_value_waits(caplog):
    declare_names("X")
    clock = ReplayClock(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(0), PREVIOUS, 9.0)
    source.publish(ny(8, 30), TARGET, 2.0)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    armed_at: list = []
    with caplog.at_level(logging.WARNING):
        await run_minutes(provider, clock, ny(7), ny(8, 29), on_step=ready_times(member, armed_at))
        assert member.ready_calls == 0 and not any(g.armed for g in provider.groups)
        warnings_before_arm = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert len(warnings_before_arm) == 1
        await run_minutes(provider, clock, ny(8, 30), ny(8, 31), on_step=ready_times(member, armed_at))
    assert armed_at and armed_at[0] <= ny(8, 31)
    bar = bar_at(MES, ny(9))
    provider.annotate(MES, bar)
    assert bar["aux_X"] == 2.0


@pytest.mark.parametrize("case", ["row_after_open", "process_started_after_open"])
async def test_scenario_missed_open(case):
    declare_names("X")
    start = ny(7) if case == "row_after_open" else ny(10)
    clock = ReplayClock(start)
    source = ScriptedSource(clock)
    source.publish(ny(10) if case == "row_after_open" else ny(8), TARGET, 1.0)
    source.publish(ny(16, 30), NEXT, 3.0)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    if case == "process_started_after_open":
        await run_minutes(provider, clock, ny(10), ny(10, 2))
        assert member.ready_calls == 1
        return
    await run_minutes(provider, clock, ny(7), ny(16))
    assert member.ready_calls == 0
    assert not [t for t in source.fetch_times if ny(9, 30) <= t < ny(16)]
    armed_before_next_open: list = []
    await run_minutes(provider, clock, ny(16, 1), ny(23, 59), on_step=ready_times(member, armed_before_next_open))
    assert member.ready_calls == 1 and armed_before_next_open[0] < ny(9, 30, day=16)


@pytest.mark.parametrize("case", ["change", "fetch_fails", "reseed_raises"])
async def test_scenario_recheck_adopts_change(case, caplog):
    declare_names("X")
    clock = ReplayClock(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(7), TARGET, 1.0)
    member = StubStrategy("m", MES, ["X"], raise_on_ready_call=2 if case == "reseed_raises" else None)
    provider = make_provider({"X": source}, [member], clock)
    await run_minutes(provider, clock, ny(8), ny(8, 59))
    group = provider.groups[0]
    assert group.armed and member.ready_calls == 1
    if case == "fetch_fails":
        source.fail_from = ny(9)
    else:
        source.publish(ny(9), TARGET, 2.0)
    with caplog.at_level(logging.WARNING):
        await run_minutes(provider, clock, ny(9), ny(9, 30))
    bar = bar_at(MES, ny(9, 31))
    provider.annotate(MES, bar)
    if case == "change":
        assert member.ready_calls == 2 and member.calls[-1][1]["X"][-1]["value"] == 2.0
        assert bar["aux_X"] == 2.0 and group.armed
        assert len([r for r in caplog.records if r.levelno == logging.WARNING]) == 1
    elif case == "fetch_fails":
        assert group.armed and bar["aux_X"] == 1.0 and member.ready_calls == 1
    else:
        assert not group.armed and "aux_X" not in bar
        assert member.calls[-1][0] is False


async def test_scenario_independent_groups():
    declare_names("X", "Y")
    clock = ReplayClock(utc(15, 4))
    cme_source, eurex_source = ScriptedSource(clock), ScriptedSource(clock)
    cme_source.publish(utc(14, 21), TARGET, 1.0)
    eurex_source.always_fail = True
    cme, eurex = StubStrategy("cme", MES, ["X"]), StubStrategy("eurex", FDAX, ["Y"])
    provider = make_provider({"X": cme_source, "Y": eurex_source}, [cme, eurex], clock)
    armed_at: list = []
    await run_minutes(provider, clock, utc(15, 4), utc(15, 14), on_step=ready_times(cme, armed_at))
    assert cme.ready_calls == 1 and eurex.ready_calls == 0
    assert armed_at[0] < ny(9, 30)
    states = {g.profile.name: g.armed for g in provider.groups}
    assert states == {"CME": True, "EUREX": False}


@pytest.mark.parametrize("case", ["confirms", "differs", "absent"])
async def test_scenario_backup_needs_confirmation(case):
    declare_names("X")
    clock = ReplayClock(ny(7))
    primary, backup, confirm = (ScriptedSource(clock) for _ in range(3))
    backup.publish(ny(0), TARGET, 7.0)
    if case == "confirms":
        confirm.publish(ny(0), TARGET, 7.0)
    elif case == "differs":
        confirm.publish(ny(0), TARGET, 8.0)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": ConfirmedBackupAuxSource(primary, backup, confirm)}, [member], clock)
    await run_minutes(provider, clock, ny(7), ny(9, 29))
    assert (member.ready_calls == 1) is (case == "confirms")
    if case == "confirms":
        assert member.calls[0][1]["X"][-1]["value"] == 7.0


def write_series(folder, name, rows):
    (folder / f"{name}.csv").write_text("date,value\n" + "".join(f"{d.isoformat()},{v}\n" for d, v in rows))


@pytest.mark.parametrize("case", ["present", "missing"])
async def test_scenario_replay_resolves_at_start(case, tmp_path):
    declare_names("X")
    rows = [(PREVIOUS, 4.0)] + ([(TARGET, 5.0)] if case == "present" else [])
    write_series(tmp_path, "X", rows)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": FileAuxSource(str(tmp_path))}, [member], ReplayClock(ny(0)), resolve_date=TARGET)
    if case == "missing":
        with pytest.raises(AuxDataError, match="X"):
            await provider.start()
        return
    await provider.start()
    assert member.ready_calls == 1 and member.calls[0][1]["X"][-1] == row(TARGET, 5.0)
    bar = bar_at(MES, ny(9, 30))
    provider.annotate(MES, bar)
    assert bar["aux_X"] == 5.0


async def test_scenario_late_member():
    declare_names("X", "Y")
    clock = ReplayClock(ny(7))
    x_source, y_source = ScriptedSource(clock), ScriptedSource(clock)
    x_source.publish(ny(0), TARGET, 1.0)
    first = StubStrategy("first", MES, ["X"])
    provider = make_provider({"X": x_source, "Y": y_source}, [first], clock)
    await run_minutes(provider, clock, ny(7), ny(7, 2))
    assert first.ready_calls == 1
    covered, new_name = StubStrategy("covered", MES, ["X"]), StubStrategy("new", MES, ["Y"])
    provider.book.strategies += [covered, new_name]
    await run_minutes(provider, clock, ny(7, 3), ny(7, 4))
    assert covered.ready_calls == 1 and new_name.ready_calls == 0
    assert len(provider.groups) == 2
    y_source.publish(ny(7, 5), TARGET, 9.0)
    await run_minutes(provider, clock, ny(7, 5), ny(7, 7))
    assert new_name.ready_calls == 1
    assert {n for g in provider.groups for n in g.member_names} == {"first", "covered", "new"}


async def test_scenario_frames_for_rebuild():
    declare_names("X")
    clock = ReplayClock(ny(7))
    source = ScriptedSource(clock)
    source.publish(ny(0), PREVIOUS, 4.0)
    source.publish(ny(0), TARGET, 5.0)
    member = StubStrategy("m", MES, ["X"])
    provider = make_provider({"X": source}, [member], clock)
    await run_minutes(provider, clock, ny(7), ny(7, 2))
    assert provider.frames_for(MES) == member.calls[-1][1]
    assert provider.frames_for(FDAX) == {}
