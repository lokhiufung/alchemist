from __future__ import annotations

import asyncio
import dataclasses
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol, runtime_checkable

import alchemist.calculations  # noqa: F401  registers the built-in calculations
from alchemist.clock import Clock, ReplayClock, WallClock
from alchemist.config import Configuration
from alchemist.constants.mode import Mode
from alchemist.deploy_socket import DeploySocketServer
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.deploy import DeployService
from alchemist.event_loop.buffer import EventBuffer
from alchemist.event_loop.loop import EventLoop
from alchemist.event_loop.recorders import BarRecorder, NullRecorder, TradeRecorder, load_recorded_bars
from alchemist.event_loop.seeder import Seeder, no_recorded_bars
from alchemist.event_loop.shutdown import ShutdownCoordinator
from alchemist.exchanges import EXCHANGES, exchange_profile
from alchemist.external_data.provider import AuxProvider
from alchemist.gateways import get_gateway
from alchemist.loader import STRATEGY_FILES, StrategyLoadError, load_strategies
from alchemist.registry import get_aux_source, get_strategy
from alchemist.safety.monitor import SafetyMonitor
from alchemist.safety.null import NullSafety
from alchemist.stores import CalcStateStore, InMemoryCalcStateStore, InMemoryRoundTripStore, RoundTripStore
from alchemist.strategy_builder.strategy import Stores

logger = logging.getLogger(__name__)

NOT_FLAT_EXIT_CODE = 2
SOCKET_FILE = "deploy.sock"


@runtime_checkable
class Service(Protocol):
    async def start(self) -> None: ...
    async def stop(self) -> None: ...


def log_deploy_fingerprint(config: Configuration) -> None:
    logger.info("config_sha256=%s bot_id=%s strategies=%s", config.sha256, config.system.bot_id,
                sorted(config.strategies))


class GatewayService:
    def __init__(self, gateway) -> None:
        self.gateway = gateway

    async def start(self) -> None:
        await self.gateway.connect()
        await self.gateway.subscribe_order_update()
        await self.gateway.subscribe_account_update()

    async def stop(self) -> None:
        await self.gateway.disconnect()


class EventLoopService:
    def __init__(self, event_buffer: EventBuffer) -> None:
        self.event_buffer = event_buffer

    async def start(self) -> None:
        self.event_buffer.bind_loop(asyncio.get_running_loop())

    async def stop(self) -> None:
        return None


def keep_run_op(run_op) -> None:
    return None


class SlotsService:
    def __init__(self, book: RuntimeBook, slots: list, aux: AuxProvider, after_cold_start=keep_run_op) -> None:
        self.book, self.slots, self.aux = book, slots, aux
        self.after_cold_start = after_cold_start

    async def start(self) -> None:
        for slot in self.slots:
            await self.book.admit(slot)
        await self.aux.members_changed()
        self.after_cold_start(self.book)

    async def stop(self) -> None:
        for task in [t for tasks in self.book.retry_tasks.values() for t in tasks]:
            task.cancel()


def check_strategy_folders(root: Path) -> None:
    for folder in sorted(p for p in Path(root).iterdir() if p.is_dir() and not p.name.startswith(("_", "."))):
        if not (folder / "strategy.py").exists():
            raise StrategyLoadError(f"{folder.name}/strategy.py: file is missing")


def replay_clock_start(config: Configuration) -> datetime:
    replay = config.replay
    for strategy in config.strategies.values():
        profile = exchange_profile(strategy.signal_instrument)
        if replay.session in profile.sessions or replay.session in profile.aliases:
            return profile.session_window(replay.session, replay.date)[0].astimezone(timezone.utc)
    return datetime.combine(replay.date, datetime.min.time(), timezone.utc)


class Runtime:
    def __init__(self, config: Configuration | None, mode: Mode, services: list[tuple[str, Service]],
                 event_loop: EventLoop | None, shutdown, gateway, clock: Clock | None = None, safety=None,
                 slots: list | None = None) -> None:
        self.config = config
        self.mode = mode
        self.services = services
        self.event_loop = event_loop
        self.shutdown = shutdown
        self.gateway = gateway
        self.clock = clock
        self.safety = safety
        self.slots = slots or []
        self.started: list[tuple[str, Service]] = []

    @classmethod
    def build(cls, config: Configuration, mode: Mode) -> Runtime:
        is_replay = mode is Mode.REPLAY
        loaded_folders = set(load_strategies(config.system.strategies_root))
        check_strategy_folders(config.system.strategies_root)
        buffer = EventBuffer(config.event_loop)
        clock: Clock = ReplayClock(replay_clock_start(config)) if is_replay else WallClock(buffer.drain_wakeup)
        gateway = get_gateway(config.gateway.name)(buffer, config.gateway, clock)
        book = RuntimeBook()
        root = config.system.log_path
        recordings, state = root / "recordings", root / "state"
        if is_replay:
            recorded_bars = no_recorded_bars
            stores = Stores(InMemoryCalcStateStore(), InMemoryRoundTripStore())
            bar_recorder, trade_recorder = NullRecorder(), NullRecorder()
            safety = NullSafety()
            aux_config = dataclasses.replace(config.aux, resolve_date=config.replay.date)
        else:
            def recorded_bars(instrument, session):
                return load_recorded_bars(recordings, instrument, session, clock)
            stores = Stores(CalcStateStore(state, config.system.bot_id), RoundTripStore(state, config.system.bot_id))
            bar_recorder, trade_recorder = BarRecorder(recordings, book), TradeRecorder(recordings, gateway, book)
            safety = SafetyMonitor(gateway, book, buffer, EXCHANGES, clock, config.safety)
            aux_config = config.aux
        seeder = Seeder(gateway, clock, config.event_loop, recorded_bars=recorded_bars)
        book.attach(gateway, seeder)
        sources = {name: get_aux_source(options["source"])(**{k: v for k, v in options.items() if k != "source"})
                   for name, options in config.aux_sources.items()}
        aux = AuxProvider(sources, book, clock, aux_config)
        def build_slot(strategy_config):
            try:
                strategy_class = get_strategy(strategy_config.strat)
            except KeyError as exc:
                raise StrategyLoadError(f"{strategy_config.strat}: no such strategy is registered") from exc
            return strategy_class(strategy_config, gateway, clock, stores, config.execution,
                                  config.event_loop.rebuild_yield_budget_s)

        slots = [build_slot(cfg) for cfg in config.strategies.values()]
        shutdown = ShutdownCoordinator(book, gateway, clock, config.event_loop)
        loop = EventLoop(buffer, book, gateway, aux, safety, bar_recorder, trade_recorder, shutdown, clock,
                         config.event_loop, clock_advance=clock.advance if is_replay else None)
        after_cold_start = keep_run_op if is_replay else (lambda b: setattr(b, "run_op", loop.enqueue_book_op))
        services = [("gateway", GatewayService(gateway)), ("event_loop", EventLoopService(buffer)),
                    ("aux_data", aux), ("slots", SlotsService(book, slots, aux, after_cold_start)),
                    ("safety", safety)]
        if not is_replay:
            def build_from_config(strategy_config):
                loaded_folders.update(load_strategies(config.system.strategies_root, exclude=loaded_folders))
                return build_slot(strategy_config)

            service = DeployService(config.path, config, book, gateway, clock, build_from_config, lambda: None,
                                    lambda: loop.book_closed or shutdown.shutdown_requested)
            services.append(("deploy_socket", DeploySocketServer(root / SOCKET_FILE, service)))
        return cls(config, mode, services, loop, shutdown, gateway, clock, safety, slots)

    async def __aenter__(self) -> Runtime:
        try:
            for name, service in self.services:
                await service.start()
                self.started.append((name, service))
        except BaseException:
            await self.stop_started()
            raise
        return self

    async def __aexit__(self, *exc) -> None:
        await self.stop_started()

    async def stop_started(self) -> None:
        while self.started:
            name, service = self.started.pop()
            try:
                await service.stop()
            except Exception:
                logger.exception("service %s failed to stop", name)

    async def run(self) -> None:
        if self.mode is not Mode.REPLAY:
            self.shutdown.install_signal_handlers(asyncio.get_running_loop())
        await self.gateway.start_feed()
        await self.event_loop.run()
        if self.shutdown.not_flat_at_shutdown:
            raise SystemExit(NOT_FLAT_EXIT_CODE)
