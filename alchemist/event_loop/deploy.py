from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import sys
from datetime import datetime, timedelta
from pathlib import Path
from typing import AsyncIterator, Callable
from zoneinfo import ZoneInfo

from alchemist.clock import Clock
from alchemist.config import Configuration, ConfigError, StrategyConfig, load_config
from alchemist.event_loop.book import BookError, RuntimeBook
from alchemist.exchanges import exchange_profile
from alchemist.loader import StrategyLoadError
from alchemist.strategy_builder.signal_dag import SignalBuildError

logger = logging.getLogger("deploy")

TERMINAL_SUCCESS = ("registered", "noop")
NEXT_SESSION_LOOKAHEAD_DAYS = 10


class DeployRefused(Exception):
    def __init__(self, status: str, reason: str | None = None) -> None:
        super().__init__(f"{status}: {reason}")
        self.status, self.reason = status, reason


def without_slot(config: Configuration, key: str) -> tuple:
    return (config.system, config.gateway, config.safety, config.aux_sources, config.aux, config.event_loop,
            config.execution, config.replay, {k: v for k, v in config.strategies.items() if k != key})


class DeployService:
    def __init__(self, config_path: Path, running_config: Configuration, book: RuntimeBook, gateway, clock: Clock,
                 build_slot: Callable[[StrategyConfig], object], process_exit_at: Callable[[], datetime | None],
                 is_closed: Callable[[], bool]) -> None:
        self.config_path = Path(config_path)
        self.running_config = running_config
        self.book = book
        self.gateway = gateway
        self.clock = clock
        self.build_slot = build_slot
        self.process_exit_at = process_exit_at
        self.is_closed = is_closed
        self.revision = running_config.sha256
        self.busy = False

    def file_sha(self) -> str:
        return hashlib.sha256(self.config_path.read_bytes()).hexdigest()

    def line(self, status: str, reason: str | None = None) -> dict:
        return {"status": status, "reason": reason, "revision": self.file_sha()}

    async def deploy(self, key: str, config_sha256: str) -> AsyncIterator[dict]:
        if self.is_closed():
            yield self.line("shutting_down")
            return
        if self.busy:
            yield self.line("busy")
            return
        self.busy = True
        try:
            async for line in self.run_request(key, config_sha256):
                logger.info("deploy status", extra={
                    "event": "deploy_status", "strategy": key, "status": line["status"], "reason": line["reason"],
                    "revision": line["revision"]})
                yield line
        finally:
            self.busy = False

    async def run_request(self, key: str, config_sha256: str) -> AsyncIterator[dict]:
        try:
            candidate, new_config = await self.preflight(key, config_sha256)
        except DeployRefused as refusal:
            yield self.line(refusal.status, refusal.reason)
            return
        yield self.line("preparing")
        try:
            await self.book.admit(candidate, guard=lambda book: self.commit_guard(candidate))
        except DeployRefused as refusal:
            yield self.line(refusal.status, refusal.reason)
            return
        except Exception as exc:
            logger.exception("deploy failed", extra={"event": "deploy_failed", "strategy": key})
            yield self.line("failed", str(exc))
            return
        self.running_config, self.revision = new_config, self.file_sha()
        yield self.line("registered")

    async def preflight(self, key: str, config_sha256: str):
        if config_sha256 != self.file_sha():
            raise DeployRefused("rejected", "stale_config")
        running = {s.name for s in self.book.strategies}
        if key in running and config_sha256 == self.revision:
            raise DeployRefused("noop")
        try:
            new_config = load_config(self.config_path)
        except ConfigError as exc:
            raise DeployRefused("rejected", "stale_config") from exc
        if key not in new_config.strategies:
            raise DeployRefused("rejected", "unknown_key")
        if key in running:
            raise DeployRefused("rejected", "conflict")
        if without_slot(new_config, key) != without_slot(self.running_config, key):
            raise DeployRefused("rejected", "config_drift")
        strategy_config = new_config.strategies[key]
        try:
            candidate = self.build_slot(strategy_config)
        except StrategyLoadError as exc:
            raise DeployRefused("rejected", "import_error") from exc
        except (SignalBuildError, ValueError, TypeError) as exc:
            raise DeployRefused("rejected", "params_invalid") from exc
        self.check_session(candidate)
        self.check_exit(candidate)
        if not any(s.config.account_id == strategy_config.account_id for s in self.book.strategies):
            raise DeployRefused("rejected", "unknown_account_route")
        await self.check_exposure(candidate, strategy_config)
        return candidate, new_config

    def check_session(self, candidate) -> None:
        profile = exchange_profile(candidate.signal_instrument)
        if profile.is_open(candidate.session, self.clock.now()):
            raise DeployRefused("rejected", "session_open")

    def check_exit(self, candidate) -> None:
        exit_at = self.process_exit_at()
        if exit_at is None:
            return
        profile = exchange_profile(candidate.signal_instrument)
        now = self.clock.now()
        for offset in range(NEXT_SESSION_LOOKAHEAD_DAYS):
            day = now.astimezone(ZoneInfo(profile.tz)).date() + timedelta(days=offset)
            if not profile.is_trading_day(day):
                continue
            start, end = profile.session_window(candidate.session, day)
            if start > now:
                if end > exit_at:
                    raise DeployRefused("rejected", "after_process_exit")
                return

    async def check_exposure(self, candidate, strategy_config: StrategyConfig) -> None:
        account, instrument = strategy_config.account_id, strategy_config.execution_instrument
        try:
            positions = await self.gateway.get_positions()
            orders = await self.gateway.get_open_orders()
        except Exception as exc:
            logger.warning("exposure unreadable", extra={"event": "exposure_unreadable", "reason": repr(exc)})
            raise DeployRefused("rejected", "unmanaged_exposure") from exc
        exposed = any(p["account_id"] == account and p["instrument"] == instrument and p["size"] for p in positions)
        exposed = exposed or any(o.get("account_id", account) == account and o["instrument"] == instrument
                                 for o in orders)
        managed = any(s.config.account_id == account and s.execution_instrument == instrument
                      for s in self.book.strategies)
        if exposed and not managed:
            raise DeployRefused("rejected", "unmanaged_exposure")

    def commit_guard(self, candidate) -> None:
        if self.is_closed():
            raise DeployRefused("shutting_down")
        self.check_session(candidate)


async def send_deploy_request(socket_path: Path, strategy_key: str, config_sha256: str) -> int:
    try:
        reader, writer = await asyncio.open_unix_connection(str(socket_path))
    except OSError as exc:
        print(f"cannot reach the deploy socket {socket_path}: {exc}", file=sys.stderr)
        return 1
    writer.write((json.dumps({"key": strategy_key, "config_sha256": config_sha256}) + "\n").encode())
    await writer.drain()
    last = None
    while raw := await reader.readline():
        text = raw.decode().rstrip()
        print(text, flush=True)
        last = json.loads(text)["status"]
    writer.close()
    return 0 if last in TERMINAL_SUCCESS else 1
