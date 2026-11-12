from __future__ import annotations

import asyncio
import dataclasses
import hashlib
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import AsyncIterator, Callable
from zoneinfo import ZoneInfo

from alchemist.config import ConfigError, Configuration, StrategyConfig, load_config
from alchemist.constants.deploy import DeployStatus, RefusalReason
from alchemist.event_loop.book import BookError, RuntimeBook
from alchemist.exchanges import exchange_profile
from alchemist.loader import StrategyLoadError
from alchemist.strategy_builder.signal_dag import SignalBuildError

logger = logging.getLogger("deploy")


class DeployRefused(Exception):
    def __init__(self, status: DeployStatus, reason: RefusalReason | None = None) -> None:
        super().__init__(f"{status}: {reason}")
        self.status, self.reason = status, reason


def without_slot(config: Configuration, key: str) -> tuple:
    return (config.system, config.gateway, config.safety, config.aux_sources, config.aux, config.event_loop,
            config.execution, config.replay, {k: v for k, v in config.strategies.items() if k != key})


class DeployCoordinator:
    def __init__(self, config_path: Path, running_config: Configuration, book: RuntimeBook, requests, watch_time,
                 build_slot: Callable[[StrategyConfig], object], process_exit_at: Callable[[], datetime | None],
                 is_closed: Callable[[], bool]) -> None:
        self.config_path = Path(config_path)
        self.running_config = running_config
        self.book = book
        self.requests = requests
        self.watch_time = watch_time
        self.tasks: set[asyncio.Task] = set()
        self.build_slot = build_slot
        self.process_exit_at = process_exit_at
        self.is_closed = is_closed
        self.revision = running_config.sha256
        self.busy = False

    def file_sha(self) -> str:
        return hashlib.sha256(self.config_path.read_bytes()).hexdigest()

    def line(self, status: DeployStatus, reason: RefusalReason | str | None = None) -> dict:
        return {"status": status, "reason": reason, "revision": self.file_sha()}

    def handle(self, request: dict) -> None:
        task = asyncio.get_running_loop().create_task(self.answer(request))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)

    async def answer(self, request: dict) -> None:
        async for line in self.deploy(request["key"], request["config_sha256"]):
            self.requests.command("deploy_result", request_id=request["request_id"], line=line,
                                  final=line["status"] != DeployStatus.PREPARING)

    async def deploy(self, key: str, config_sha256: str) -> AsyncIterator[dict]:
        if self.is_closed():
            yield self.line(DeployStatus.SHUTTING_DOWN)
            return
        if self.busy:
            yield self.line(DeployStatus.BUSY)
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
        yield self.line(DeployStatus.PREPARING)
        try:
            await self.book.admit(candidate, guard=lambda book: self.commit_guard(candidate))
        except DeployRefused as refusal:
            yield self.line(refusal.status, refusal.reason)
            return
        except Exception as exc:
            logger.exception("deploy failed", extra={"event": "deploy_failed", "strategy": key})
            yield self.line(DeployStatus.FAILED, str(exc))
            return
        self.running_config, self.revision = new_config, self.file_sha()
        yield self.line(DeployStatus.REGISTERED)

    async def preflight(self, key: str, config_sha256: str):
        if config_sha256 != self.file_sha():
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.STALE_CONFIG)
        running = {s.name for s in self.book.strategies}
        if key in running and config_sha256 == self.revision:
            raise DeployRefused(DeployStatus.NOOP)
        try:
            new_config = load_config(self.config_path)
        except ConfigError as exc:
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.STALE_CONFIG) from exc
        if key not in new_config.strategies:
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.UNKNOWN_KEY)
        if key in running:
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.CONFLICT)
        if without_slot(new_config, key) != without_slot(self.running_config, key):
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.CONFIG_DRIFT)
        strategy_config = new_config.strategies[key]
        try:
            candidate = self.build_slot(strategy_config)
        except StrategyLoadError as exc:
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.IMPORT_ERROR) from exc
        except (SignalBuildError, ValueError, TypeError) as exc:
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.PARAMS_INVALID) from exc
        self.check_session(candidate)
        self.check_exit(candidate)
        if not any(s.config.account_id == strategy_config.account_id for s in self.book.strategies):
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.UNKNOWN_ACCOUNT_ROUTE)
        await self.check_exposure(candidate, strategy_config)
        return candidate, new_config

    def check_session(self, candidate) -> None:
        profile = exchange_profile(candidate.signal_instrument)
        if profile.is_open(candidate.session, self.watch_time.moment()):
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.SESSION_OPEN)

    def check_exit(self, candidate) -> None:
        exit_at = self.process_exit_at()
        if exit_at is None:
            return
        profile = exchange_profile(candidate.signal_instrument)
        run_date = exit_at.astimezone(ZoneInfo(profile.session(candidate.session).tz)).date()
        if profile.is_trading_day(run_date):
            start, end = profile.session_window(candidate.session, run_date)
            if self.watch_time.moment() < start and end <= exit_at:
                return
        raise DeployRefused(DeployStatus.REJECTED, RefusalReason.AFTER_PROCESS_EXIT)

    async def check_exposure(self, candidate, strategy_config: StrategyConfig) -> None:
        account, instrument = strategy_config.account_id, strategy_config.execution_instrument
        try:
            positions = await self.requests.request("get_positions")
            orders = await self.requests.request("get_open_orders")
        except Exception as exc:
            logger.warning("exposure unreadable", extra={"event": "exposure_unreadable", "reason": repr(exc)})
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.UNMANAGED_EXPOSURE) from exc
        exposed = any(p["account_id"] == account and p["instrument"] == instrument and p["size"] for p in positions)
        exposed = exposed or any(o.get("account_id", account) == account and o["instrument"] == instrument
                                 for o in orders)
        managed = any(s.config.account_id == account and s.execution_instrument == instrument
                      for s in self.book.strategies)
        if exposed and not managed:
            raise DeployRefused(DeployStatus.REJECTED, RefusalReason.UNMANAGED_EXPOSURE)

    def commit_guard(self, candidate) -> None:
        if self.is_closed():
            raise DeployRefused(DeployStatus.SHUTTING_DOWN)
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
    return 0 if last is not None and DeployStatus(last).is_success else 1
