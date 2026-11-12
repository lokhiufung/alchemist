from __future__ import annotations

import asyncio
import logging
import signal
import time
from dataclasses import dataclass
from functools import partial
from pathlib import Path

import alchemist.calculations  # noqa: F401  registers the built-in calculations
from alchemist.config import Configuration
from alchemist.constants.mode import Mode
from alchemist.constants.runtime import NOT_FLAT_EXIT_CODE, SERVICE_LOGGERS, SOCKET_FILE
from alchemist.deploy_socket import DeploySocketServer
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.buffer import CommandBuffer, EventBuffer
from alchemist.event_loop.deploy import DeployCoordinator
from alchemist.event_loop.loop import EventLoop
from alchemist.event_loop.recorders import BarRecorder, TradeRecorder, load_recorded_bars
from alchemist.event_loop.seeder import Seeder
from alchemist.event_loop.shutdown import ShutdownCoordinator
from alchemist.event_loop.watch_time import WatchTime
from alchemist.exchanges import EXCHANGES
from alchemist.aux_data.provider import AuxProvider
from alchemist.aux_data.sources import build_aux_source
from alchemist.gateways import get_gateway
from alchemist.loader import StrategyLoadError, load_strategies
from alchemist.logger import attach_watch_time
from alchemist.messages import Command, make_event
from alchemist.registry import get_strategy
from alchemist.requests import Requests
from alchemist.safety.monitor import SafetyMonitor
from alchemist.service import Service
from alchemist.stores import FileDailyStore
from alchemist.strategy_builder.strategy import Stores

startup_logger = logging.getLogger("startup")
shutdown_logger = logging.getLogger("shutdown")
alert_logger = logging.getLogger("safety")


def service_logger(name: str, runner_logger: logging.Logger) -> logging.Logger:
    return logging.getLogger(SERVICE_LOGGERS[name]) if name in SERVICE_LOGGERS else runner_logger


def log_deploy_fingerprint(config: Configuration, mode: Mode) -> None:
    strategies = {key: {"strat": s.strat, "signal_instrument": s.signal_instrument.symbol,
                        "execution_instrument": s.execution_instrument.symbol}
                  for key, s in sorted(config.strategies.items())}
    startup_logger.info("deploy fingerprint", extra={
        "event": "deploy_fingerprint", "mode": mode.value, "bot_id": config.system.bot_id,
        "config_sha256": config.sha256, "strategies": strategies})


@dataclass(frozen=True)
class Adapters:
    deploy_socket: bool
    signal_handlers: bool


ADAPTERS = {Mode.REPLAY: Adapters(deploy_socket=False, signal_handlers=False),
            Mode.PAPER: Adapters(deploy_socket=True, signal_handlers=True),
            Mode.LIVE: Adapters(deploy_socket=True, signal_handlers=True)}


def log_alert(command: Command) -> None:
    alert_logger.critical(command["data"]["message"], extra={"event": "alert", **command["data"]})


def install_signal_handlers(loop: asyncio.AbstractEventLoop, inbound: EventBuffer) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda s=sig: inbound.push(make_event("os_signal", {"signal": s.name},
                                                                           time.time())))


class EventLoopService(Service):
    def __init__(self, event_loop: EventLoop, requests: Requests) -> None:
        self.event_loop = event_loop
        self.requests = requests
        self.task: asyncio.Task | None = None

    async def start(self) -> None:
        self.event_loop.event_buffer.bind_loop(asyncio.get_running_loop())
        self.task = asyncio.get_running_loop().create_task(self.event_loop.run())
        self.requests.raise_order_id_floor(await self.requests.request("order_id_floor"))

    async def stop(self) -> None:
        if self.task is not None and not self.task.done():
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)


class SlotsService(Service):
    def __init__(self, book: RuntimeBook, slots: list, aux: AuxProvider, watch_time: WatchTime) -> None:
        self.book, self.slots, self.aux, self.watch_time = book, slots, aux, watch_time

    async def start(self) -> None:
        await self.watch_time.ready()
        for slot in self.slots:
            await self.book.admit(slot)
        await self.aux.members_changed()

    async def stop(self) -> None:
        for task in [t for tasks in self.book.retry_tasks.values() for t in tasks]:
            task.cancel()


def check_strategy_folders(root: Path) -> None:
    for folder in sorted(p for p in Path(root).iterdir() if p.is_dir() and not p.name.startswith(("_", "."))):
        if not (folder / "strategy.py").exists():
            raise StrategyLoadError(f"{folder.name}/strategy.py: file is missing")


class Runtime:
    def __init__(self, config: Configuration | None, adapters: Adapters, services: list[tuple[str, Service]],
                 event_loop_service: EventLoopService | None, shutdown, gateway, inbound: EventBuffer | None = None,
                 safety=None, slots: list | None = None) -> None:
        self.config = config
        self.adapters = adapters
        self.services = services
        self.event_loop_service = event_loop_service
        self.shutdown = shutdown
        self.gateway = gateway
        self.inbound = inbound
        self.safety = safety
        self.slots = slots or []
        self.started: list[tuple[str, Service]] = []

    @classmethod
    def build(cls, config: Configuration, mode: Mode) -> Runtime:
        adapters = ADAPTERS[mode]
        loaded_folders = set(load_strategies(config.system.strategies_root))
        check_strategy_folders(config.system.strategies_root)
        inbound, outbound = EventBuffer(config.event_loop), CommandBuffer()
        watch_time = WatchTime()
        attach_watch_time(watch_time)
        requests = Requests(outbound)
        gateway = get_gateway(config.gateway.name)(inbound, outbound, config.gateway)
        gateway.sinks["alert"] = log_alert
        book = RuntimeBook()
        root = config.system.log_path
        recordings, state = root / "recordings", root / "state"
        stores = Stores(FileDailyStore(state, config.system.bot_id, "calc_state"),
                        FileDailyStore(state, config.system.bot_id, "round_trips"))
        seeder = Seeder(requests, watch_time, config.event_loop, recorded_bars=partial(load_recorded_bars, recordings))
        book.attach(requests, seeder)
        sources = {name: build_aux_source(spec) for name, spec in config.aux_sources.items()}
        aux = AuxProvider(sources, watch_time, config.aux)
        book.member_listeners.append(aux)
        safety = SafetyMonitor(inbound, outbound, EXCHANGES, config.safety)

        def build_slot(strategy_config):
            try:
                strategy_class = get_strategy(strategy_config.strat)
            except KeyError as exc:
                raise StrategyLoadError(f"{strategy_config.strat}: no such strategy is registered") from exc
            return strategy_class(strategy_config, requests, watch_time, stores, config.execution,
                                  config.event_loop.rebuild_yield_budget_s)

        def build_from_config(strategy_config):
            loaded_folders.update(load_strategies(config.system.strategies_root, exclude=loaded_folders))
            return build_slot(strategy_config)

        slots = [build_slot(cfg) for cfg in config.strategies.values()]
        shutdown = ShutdownCoordinator(book, requests, watch_time, config.event_loop)
        deploy = DeployCoordinator(config.path, config, book, requests, watch_time, build_from_config,
                                   lambda: shutdown.run_exit, lambda: loop.book_closed or shutdown.shutdown_requested)
        loop = EventLoop(inbound, requests, book, aux, [BarRecorder(recordings), TradeRecorder(recordings)], shutdown,
                         watch_time, deploy, safety.check_in, config.event_loop)
        event_loop_service = EventLoopService(loop, requests)
        services = [("gateway", gateway), ("event_loop", event_loop_service), ("aux_data", aux),
                    ("slots", SlotsService(book, slots, aux, watch_time)), ("safety", safety)]
        if adapters.deploy_socket:
            socket = DeploySocketServer(root / SOCKET_FILE, inbound)
            gateway.sinks["deploy_result"] = socket.write_result
            services.append(("deploy_socket", socket))
        return cls(config, adapters, services, event_loop_service, shutdown, gateway, inbound, safety, slots)

    async def __aenter__(self) -> Runtime:
        try:
            for name, service in self.services:
                await service.start()
                self.started.append((name, service))
                service_logger(name, startup_logger).info("service started", extra={"event": "service_started", "service": name})
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
                service_logger(name, shutdown_logger).info("service stopped", extra={"event": "service_stopped", "service": name})
            except Exception:
                service_logger(name, shutdown_logger).exception("service failed to stop", extra={
                    "event": "service_stop_failed", "service": name})

    async def run(self) -> None:
        if self.adapters.signal_handlers:
            install_signal_handlers(asyncio.get_running_loop(), self.inbound)
        await self.gateway.start_feed()
        await self.event_loop_service.task
        if self.shutdown.not_flat_at_shutdown:
            raise SystemExit(NOT_FLAT_EXIT_CODE)
