from __future__ import annotations

import asyncio
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

from alchemist.config import AuxConfig
from alchemist.constants.aux_data import AUX_POLL_S
from alchemist.exchanges import ExchangeProfile, exchange_profile
from alchemist.aux_data.sources import AuxSource
from alchemist.service import Service
from alchemist.instruments import Instrument

logger = logging.getLogger("aux_data")

Column = tuple[Instrument, str]


def next_trading_day(profile: ExchangeProfile, day: date) -> date:
    while not profile.is_trading_day(day):
        day += timedelta(days=1)
    return day


def target_date(profile: ExchangeProfile, session: str, now: datetime) -> date:
    today = now.astimezone(ZoneInfo(profile.tz)).date()
    candidate = next_trading_day(profile, today)
    if candidate == today and now >= profile.session_window(session, candidate)[1]:
        candidate = next_trading_day(profile, today + timedelta(days=1))
    return candidate


class AuxArmGroup:
    def __init__(self, profile: ExchangeProfile, arm_session: str, columns: set[Column], members: set[str]) -> None:
        self.profile = profile
        self.arm_session = arm_session
        self.columns: set[Column] = set(columns)
        self.member_names: set[str] = set(members)
        self.notified: set[str] = set()
        self.armed = False
        self.values: dict[Column, float] = {}
        self.frames: dict[Column, list[dict]] = {}
        self.target: date | None = None
        self.begin_target_state()

    def begin_target_state(self) -> None:
        self.started_before_deadline: bool | None = None
        self.sat_out = False
        self.attempted_after_open = False
        self.failed_to_seed = False
        self.fetched_for_target = False
        self.frame_loaded = False
        self.last_fetch_at: datetime | None = None
        self.served_fingerprint: tuple | None = None

    def covers(self, columns: set[Column]) -> bool:
        return columns <= self.columns

    @property
    def deadline(self) -> datetime:
        return self.profile.session_window(self.arm_session, self.target)[0]

    @property
    def settle_at(self) -> datetime:
        previous = self.target - timedelta(days=1)
        while not self.profile.is_trading_day(previous):
            previous -= timedelta(days=1)
        return self.profile.session_window(self.arm_session, previous)[1]

    def begin_target(self, target: date, now: datetime, aux: "AuxProvider | None" = None) -> None:
        if self.armed and aux is not None:
            self.disarm(aux)
        self.armed = False
        self.values, self.frames = {}, {}
        self.target = target
        self.begin_target_state()

    def members_of(self, aux: "AuxProvider") -> list:
        return [aux.members[n] for n in sorted(self.member_names) if n in aux.members]

    def disarm(self, aux: "AuxProvider") -> None:
        self.armed = False
        for member in self.members_of(aux):
            if member.name in self.notified:
                member.on_aux_ready(False, {})
        self.notified.clear()

    def member_frames(self, member) -> dict[str, list[dict]]:
        return {name: self.frames.get((member.signal_instrument, name), []) for name in member.aux_names()}

    async def fetch_columns(self, aux: "AuxProvider") -> dict[Column, list[dict]]:
        start = self.target - timedelta(days=aux.config.frame_days)
        fetched: dict[Column, list[dict]] = {}
        for instrument, name in sorted(self.columns, key=lambda c: (c[0].symbol, c[1])):
            fetched[(instrument, name)] = list(await aux.sources[name].fetch(name, start, self.target))
        return fetched

    def settled_values(self, fetched: dict[Column, list[dict]]) -> dict[Column, float] | None:
        values: dict[Column, float] = {}
        for column, rows in fetched.items():
            match = next((r for r in rows if r["date"] == self.target), None)
            if match is None:
                return None
            values[column] = match["value"]
        return values

    def warn_on_change(self, fetched: dict[Column, list[dict]]) -> None:
        fingerprint = tuple((c[1], max((r["date"] for r in rows), default=None)) for c, rows in sorted(
            fetched.items(), key=lambda item: (item[0][0].symbol, item[0][1])))
        if fingerprint != self.served_fingerprint:
            self.served_fingerprint = fingerprint
            logger.warning("aux source serves another date than the target", extra={
                "event": "aux_target_mismatch", "group": self.profile.name, "target": self.target,
                "served": fingerprint})

    def arm(self, aux: "AuxProvider", values: dict[Column, float], frames: dict[Column, list[dict]], now: datetime) -> bool:
        self.values, self.frames = values, frames
        if self.seed_members(aux):
            self.armed = True
            self.last_fetch_at = now
            return True
        self.values, self.frames = {}, {}
        self.failed_to_seed = True
        return False

    def seed_members(self, aux: "AuxProvider") -> bool:
        try:
            for member in self.members_of(aux):
                member.on_aux_ready(True, self.member_frames(member))
                self.notified.add(member.name)
        except Exception:
            logger.critical("aux seeding failed for the group", exc_info=True, extra={
                "event": "aux_seed_failed", "group": self.profile.name, "target": self.target})
            for member in self.members_of(aux):
                if member.name in self.notified:
                    member.on_aux_ready(False, {})
            self.notified.clear()
            return False
        return True

    async def step(self, aux: "AuxProvider", now: datetime) -> None:
        target = target_date(self.profile, self.arm_session, now)
        if target != self.target:
            self.begin_target(target, now, aux)
        before_open = now < self.deadline
        if self.armed:
            self.admit_newcomers(aux)
            if before_open and now - self.last_fetch_at >= timedelta(seconds=aux.config.recheck_interval_s):
                await self.recheck(aux, now)
            return
        if self.started_before_deadline is None:
            self.started_before_deadline = before_open
        if self.sat_out or self.failed_to_seed:
            return
        if not before_open:
            if self.started_before_deadline or self.attempted_after_open:
                self.sat_out = True
                return
            self.attempted_after_open = True
        elif now < self.settle_at and self.fetched_for_target and not self.frame_loaded:
            return
        await self.try_arm(aux, now)

    async def try_arm(self, aux: "AuxProvider", now: datetime) -> None:
        self.fetched_for_target = True
        try:
            fetched = await self.fetch_columns(aux)
        except Exception as exc:
            logger.warning("aux fetch failed", extra={"event": "aux_fetch_failed", "group": self.profile.name,
                                                      "reason": str(exc)})
            return
        self.frame_loaded = any(fetched.values())
        values = self.settled_values(fetched) if now >= self.settle_at else None
        if values is None:
            self.warn_on_change(fetched)
            return
        self.arm(aux, values, fetched, now)

    def admit_newcomers(self, aux: "AuxProvider") -> None:
        for member in self.members_of(aux):
            if member.name in self.notified:
                continue
            try:
                member.on_aux_ready(True, self.member_frames(member))
                self.notified.add(member.name)
            except Exception:
                logger.critical("aux seeding failed for a member", exc_info=True, extra={
                    "event": "aux_seed_failed", "group": self.profile.name, "strategy": member.name})
                self.notified.add(member.name)

    async def recheck(self, aux: "AuxProvider", now: datetime) -> None:
        self.last_fetch_at = now
        try:
            fetched = await self.fetch_columns(aux)
        except Exception as exc:
            logger.warning("aux recheck failed", extra={"event": "aux_recheck_failed", "group": self.profile.name,
                                                        "reason": str(exc)})
            return
        values = self.settled_values(fetched)
        if values is None or values == self.values:
            return
        logger.warning("aux value changed", extra={
            "event": "aux_value_changed", "group": self.profile.name, "target": self.target,
            "old_values": self.values, "new_values": values})
        self.values, self.frames = values, fetched
        if not self.seed_members(aux):
            self.disarm(aux)
            self.failed_to_seed = True


class AuxProvider(Service):
    def __init__(self, sources: dict[str, AuxSource], watch_time, config: AuxConfig,
                 sleep=asyncio.sleep) -> None:
        self.sources = sources
        self.watch_time = watch_time
        self.config = config
        self.sleep = sleep
        self.members: dict[str, object] = {}
        self.groups: list[AuxArmGroup] = []
        self.task: asyncio.Task | None = None
        self.annotated: dict[Instrument, list[AuxArmGroup]] = {}

    def member_joined(self, strategy) -> None:
        self.members[strategy.name] = strategy

    def member_left(self, name: str) -> None:
        self.members.pop(name, None)

    async def start(self) -> None:
        await self.start_resident_task()

    async def members_changed(self) -> None:
        if self.watch_time.now() is not None:
            await self.step_all()

    async def start_resident_task(self) -> None:
        self.task = asyncio.create_task(self.guarded_run())

    async def guarded_run(self) -> None:
        try:
            await self.run()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.critical("aux data resident task crashed", exc_info=True, extra={"event": "aux_task_crashed"})

    async def stop(self) -> None:
        if self.task is not None:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None

    async def run(self) -> None:
        await self.watch_time.ready()
        while True:
            stepped_at = self.watch_time.now()
            await self.step_all()
            while self.watch_time.now() - stepped_at < self.config.step_interval_s:
                await self.sleep(min(self.config.step_interval_s, AUX_POLL_S))

    def sync_groups(self) -> None:
        members = {name: s for name, s in self.members.items() if s.aux_names()}
        for group in self.groups:
            group.member_names &= members.keys()
        assigned = {n for g in self.groups for n in g.member_names}
        first_sync = not self.groups
        for name, member in members.items():
            if name in assigned:
                continue
            profile = exchange_profile(member.signal_instrument)
            session = profile.session("regular").name
            columns = {(member.signal_instrument, n) for n in member.aux_names()}
            home = next((g for g in self.groups if g.profile is profile and g.arm_session == session
                         and (g.covers(columns) or first_sync)), None)
            if home is None:
                home = AuxArmGroup(profile, session, set(), set())
                self.groups.append(home)
            home.columns |= columns
            home.member_names.add(name)
        self.annotated = {}
        for group in self.groups:
            for instrument, _ in group.columns:
                self.annotated.setdefault(instrument, []).append(group)

    async def step_all(self) -> None:
        self.sync_groups()
        now = self.watch_time.moment()
        results = await asyncio.gather(*(g.step(self, now) for g in self.groups), return_exceptions=True)
        for group, result in zip(self.groups, results):
            if isinstance(result, Exception):
                logger.error("aux group failed", extra={"event": "aux_group_failed", "group": group.profile.name,
                                                        "reason": repr(result)})

    def annotate(self, instrument: Instrument, bar: dict) -> None:
        for group in self.annotated.get(instrument, ()):
            if not group.armed:
                continue
            day = datetime.fromtimestamp(bar["start_ts"], ZoneInfo(group.profile.tz)).date()
            if day != group.target:
                continue
            for (bound, name), value in group.values.items():
                if bound == instrument:
                    bar[f"aux_{name}"] = value

    def frames_for(self, instrument: Instrument) -> dict:
        frames: dict[str, list[dict]] = {}
        for group in self.groups:
            if group.armed:
                for (bound, name), rows in group.frames.items():
                    if bound == instrument:
                        frames[name] = rows
        return frames
