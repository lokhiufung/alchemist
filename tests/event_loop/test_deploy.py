import asyncio
import hashlib
import json
import os
import stat
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from alchemist.clock import ReplayClock
from alchemist.config import EventLoopConfig, load_config
from alchemist.deploy_socket import DeploySocketServer
from alchemist.event_loop.book import RuntimeBook
from alchemist.event_loop.deploy import DeployService, send_deploy_request
from alchemist.event_loop.seeder import Seeder
from alchemist.instruments import ES, MES
from alchemist.loader import StrategyLoadError
from alchemist.strategy_builder.signal_dag import SignalBuildError

from seed_helpers import SeedGateway, SeedStrategy

CLOSED = datetime(2026, 4, 15, 22, 0, tzinfo=timezone.utc)
OPEN_SESSION = datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc)

CONFIG = """\
system:
  bot_id: bot
  log_path: {log}
  strategies_root: strategies
gateway:
  name: simulation
  {gateway_extra}
strategies:
  a:
    strat: stub
    account_id: ACC
    signal_instrument: MES
    execution_instrument: MES
  b:
    strat: stub
    account_id: {b_account}
    signal_instrument: ES
    execution_instrument: ES
"""


class Rig:
    def __init__(self, tmp_path, now=CLOSED, b_account="ACC", gateway_extra="") -> None:
        self.tmp = tmp_path
        self.path = tmp_path / "config.yaml"
        self.text = CONFIG.format(log=tmp_path, b_account=b_account, gateway_extra=gateway_extra)
        self.path.write_text(self.text)
        self.clock = ReplayClock(now)
        self.gateway = SeedGateway()
        self.seeder = Seeder(self.gateway, self.clock, EventLoopConfig())
        self.book = RuntimeBook()
        self.book.attach(self.gateway, self.seeder)
        self.running_config = load_config(self.path)
        self.started_with = self.running_config.sha256
        self.factory_error = None
        self.exit_at = None
        self.built = []
        self.closed = False
        self.service = DeployService(self.path, self.running_config, self.book, self.gateway, self.clock,
                                     self.build, lambda: self.exit_at, lambda: self.closed)
        self.book.run_op = self.run_op

    async def run_op(self, fn):
        return fn(self.book)

    def build(self, strategy_config):
        if self.factory_error is not None:
            raise self.factory_error
        candidate = SeedStrategy(strategy_config.name, strategy_config.signal_instrument,
                                 account=strategy_config.account_id)
        self.built.append(candidate)
        return candidate

    async def start_a(self):
        await self.book.admit(SeedStrategy("a", MES, account="ACC"))

    async def deploy(self, key="b", sha=None):
        lines = []
        async for line in self.service.deploy(key, sha or self.service.file_sha()):
            lines.append(line)
        return lines

    def snapshot(self):
        return (sorted(s.name for s in self.book.strategies), sorted(self.book.contracts, key=str),
                list(self.gateway.subscribed), list(self.gateway.unsubscribed))


def statuses(lines):
    return [(line["status"], line["reason"]) for line in lines]


async def test_scenario_deploy_strategy_registered_then_noop(tmp_path):
    rig = Rig(tmp_path)
    await rig.start_a()
    lines = await rig.deploy("b")
    assert statuses(lines) == [("preparing", None), ("registered", None)]
    assert all(line["revision"] == rig.service.file_sha() for line in lines)
    assert [s.name for s in rig.book.strategies] == ["a", "b"]
    assert rig.gateway.subscribed.count(MES) == 1 and rig.gateway.subscribed.count(ES) == 1
    again = await rig.deploy("b")
    assert statuses(again) == [("noop", None)] and again[0]["revision"] == rig.service.file_sha()


@pytest.mark.parametrize("case,reason", [("session_open", "session_open"), ("conflict", "conflict"),
                                         ("unknown_key", "unknown_key"), ("stale_config", "stale_config")])
async def test_scenario_deploy_strategy_refusals(tmp_path, case, reason):
    rig = Rig(tmp_path, now=OPEN_SESSION if case == "session_open" else CLOSED)
    await rig.start_a()
    before = rig.snapshot()
    key, sha = "b", None
    if case == "conflict":
        rig.path.write_text(rig.text + "\n# edited\n")
        key = "a"
    elif case == "unknown_key":
        key = "ghost"
    elif case == "stale_config":
        sha = "0" * 64
    lines = await rig.deploy(key, sha)
    assert statuses(lines) == [("rejected", reason)]
    assert rig.snapshot() == before


async def test_scenario_deploy_strategy_busy(tmp_path):
    rig = Rig(tmp_path)
    await rig.start_a()
    gate = asyncio.Event()
    original = rig.seeder.attempt

    async def slow(strategy):
        await gate.wait()
        return await original(strategy)

    rig.seeder.attempt = slow
    first = asyncio.create_task(rig.deploy("b"))
    await asyncio.sleep(0.01)
    second = await rig.deploy("b")
    gate.set()
    first_lines = await first
    assert statuses(second) == [("busy", None)] and statuses(first_lines)[-1] == ("registered", None)


async def test_busy_noop_shutting_down_replies(tmp_path):
    rig = Rig(tmp_path)
    await rig.start_a()
    await rig.deploy("b")
    noop = await rig.deploy("b")
    assert noop[0]["status"] == "noop" and noop[0]["revision"] == rig.service.file_sha()
    rig.closed = True
    assert statuses(await rig.deploy("b")) == [("shutting_down", None)]


def reason_cases():
    return ["stale_config", "config_drift", "unknown_key", "conflict", "import_error", "params_invalid",
            "session_open", "after_process_exit", "unknown_account_route", "unmanaged_exposure"]


@pytest.mark.parametrize("reason", reason_cases())
async def test_each_refusal_reason_touches_nothing(tmp_path, reason):
    rig = Rig(tmp_path, now=OPEN_SESSION if reason == "session_open" else CLOSED,
              b_account="OTHER" if reason == "unknown_account_route" else "ACC")
    await rig.start_a()
    before = rig.snapshot()
    key, sha = "b", None
    if reason == "stale_config":
        sha = "f" * 64
    elif reason == "config_drift":
        rig.path.write_text(rig.text.replace("name: simulation", "name: simulation\n  option: changed"))
    elif reason == "unknown_key":
        key = "ghost"
    elif reason == "conflict":
        rig.path.write_text(rig.text + "# note\n")
        key = "a"
    elif reason == "import_error":
        rig.factory_error = StrategyLoadError("b/strategy.py: boom")
    elif reason == "params_invalid":
        rig.factory_error = SignalBuildError("param 'x': bad")
    elif reason == "after_process_exit":
        rig.exit_at = CLOSED + timedelta(hours=1)
    elif reason == "unmanaged_exposure":
        rig.gateway.positions = [{"account_id": "ACC", "instrument": ES, "size": 1, "avg_cost": 1.0, "currency": "USD"}]
    lines = await rig.deploy(key, sha)
    assert statuses(lines) == [("rejected", reason)]
    assert rig.snapshot() == before and len(rig.book.strategies) == 1


async def test_unreadable_broker_refuses_with_unmanaged_exposure(tmp_path):
    rig = Rig(tmp_path)
    await rig.start_a()
    rig.gateway.exposure_unreadable = True
    assert statuses(await rig.deploy("b")) == [("rejected", "unmanaged_exposure")]


async def test_exposure_managed_by_a_running_strategy_on_the_same_account_is_allowed(tmp_path):
    rig = Rig(tmp_path)
    await rig.start_a()
    rig.gateway.positions = [{"account_id": "ACC", "instrument": MES, "size": 1, "avg_cost": 1.0, "currency": "USD"}]
    assert statuses(await rig.deploy("b"))[-1] == ("registered", None)


async def test_failed_candidate_releases_only_its_own_instruments(tmp_path):
    rig = Rig(tmp_path)
    await rig.start_a()

    async def explode(strategy):
        raise RuntimeError("seed exploded")

    rig.seeder.attempt = explode
    lines = await rig.deploy("b")
    assert statuses(lines) == [("preparing", None), ("failed", "seed exploded")]
    assert ES not in rig.book.contracts and ES in rig.gateway.unsubscribed
    assert MES in rig.book.contracts and [s.name for s in rig.book.strategies] == ["a"]
    assert MES not in rig.gateway.unsubscribed


async def test_commit_refused_when_shutdown_begins_during_preparation(tmp_path):
    rig = Rig(tmp_path)
    await rig.start_a()
    original = rig.seeder.attempt

    async def closing(strategy):
        rig.closed = True
        return await original(strategy)

    rig.seeder.attempt = closing
    lines = await rig.deploy("b")
    assert lines[-1]["status"] == "shutting_down" and [s.name for s in rig.book.strategies] == ["a"]


async def test_a_fetch_miss_while_preparing_is_not_a_failure(tmp_path):
    rig = Rig(tmp_path)
    await rig.start_a()
    rig.gateway.daily_failures = 1
    lines = await rig.deploy("b")
    assert lines[-1]["status"] == "registered"
    rig.seeder.cancel_retries()


def short_dir():
    return Path(tempfile.mkdtemp(prefix="dp", dir="/tmp"))


async def test_socket_permissions():
    folder = short_dir() / "sock"
    server = DeploySocketServer(folder / "deploy.sock", None)
    await server.start()
    try:
        assert stat.S_IMODE(os.stat(folder).st_mode) == 0o700
        assert stat.S_IMODE(os.stat(folder / "deploy.sock").st_mode) == 0o600
    finally:
        await server.stop()
    assert not (folder / "deploy.sock").exists()


async def test_socket_round_trip_streams_status_lines(tmp_path, capsys):
    class FakeService:
        async def deploy(self, key, sha):
            yield {"status": "preparing", "reason": None, "revision": sha}
            yield {"status": "registered", "reason": None, "revision": sha}

    folder = short_dir()
    server = DeploySocketServer(folder / "deploy.sock", FakeService())
    await server.start()
    try:
        code = await send_deploy_request(folder / "deploy.sock", "b", "abc")
    finally:
        await server.stop()
    out = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert code == 0 and [o["status"] for o in out] == ["preparing", "registered"]


@pytest.mark.parametrize("terminal,code", [("noop", 0), ("rejected", 1), ("failed", 1), ("busy", 1)])
async def test_client_exit_code_follows_the_terminal_status(terminal, code):
    class FakeService:
        async def deploy(self, key, sha):
            yield {"status": terminal, "reason": "x" if terminal != "noop" else None, "revision": sha}

    folder = short_dir()
    server = DeploySocketServer(folder / "deploy.sock", FakeService())
    await server.start()
    try:
        assert await send_deploy_request(folder / "deploy.sock", "b", "abc") == code
    finally:
        await server.stop()


async def test_client_without_a_server_exits_with_failure(tmp_path):
    assert await send_deploy_request(tmp_path / "missing.sock", "b", "abc") == 1
