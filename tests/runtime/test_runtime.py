import asyncio
import logging
import os
import subprocess
import sys
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

import pytest

import alchemist.gateways  # noqa: F401
from alchemist.clock import ReplayClock
from alchemist.config import load_config
from alchemist.constants.mode import Mode
from alchemist.event_loop.recorders import NullRecorder
from alchemist.runtime import Runtime, log_deploy_fingerprint
from alchemist.safety.null import NullSafety
from alchemist.stores import InMemoryCalcStateStore, InMemoryRoundTripStore

ROOT = Path(__file__).resolve().parents[2]

CONFIG_TEMPLATE = """\
system:
  bot_id: test_bot
  log_path: {out}
  strategies_root: {strategies}

gateway:
  name: simulation

replay:
  tape: synthetic:golden_session
  date: 2026-04-15
  session: NY_RTH
  journal: {out}/broker_journal.jsonl

strategies:
  macd_rsi_rth_long:
    strat: macd_rsi_rth_long
    account_id: SIM
    signal_instrument: MES
    execution_instrument: MES
"""


def write_config(tmp_path, strategies_root):
    path = tmp_path / "config.yaml"
    path.write_text(CONFIG_TEMPLATE.format(out=tmp_path / "out", strategies=strategies_root))
    return path


def good_strategies(tmp_path):
    root = tmp_path / "strategies"
    import shutil
    shutil.copytree(ROOT / "local" / "strategies" / "macd_rsi_rth_long", root / "macd_rsi_rth_long")
    return root


def test_build_order_follows_the_system_map(tmp_path):
    config = load_config(write_config(tmp_path, ROOT / "local" / "strategies"))
    runtime = Runtime.build(config, Mode.REPLAY)
    assert [name for name, _ in runtime.services] == ["gateway", "event_loop", "aux_data", "slots", "safety"]


def test_replay_build_passes_the_do_nothing_stand_ins(tmp_path):
    config = load_config(write_config(tmp_path, ROOT / "local" / "strategies"))
    runtime = Runtime.build(config, Mode.REPLAY)
    assert isinstance(runtime.clock, ReplayClock) and isinstance(runtime.safety, NullSafety)
    assert isinstance(runtime.event_loop.bar_recorder, NullRecorder)
    assert isinstance(runtime.event_loop.trade_recorder, NullRecorder)
    slot = runtime.slots[0]
    assert isinstance(slot.stores.calc_state, InMemoryCalcStateStore)
    assert isinstance(slot.stores.round_trips, InMemoryRoundTripStore)


@pytest.mark.parametrize("mode", [Mode.PAPER, Mode.LIVE])
def test_paper_and_live_build_wall_clock_safety_and_file_stand_ins(mode, tmp_path):
    from alchemist.clock import WallClock
    from alchemist.event_loop.recorders import BarRecorder, TradeRecorder
    from alchemist.safety.monitor import SafetyMonitor
    from alchemist.stores import CalcStateStore, RoundTripStore
    config = load_config(write_config(tmp_path, ROOT / "local" / "strategies"))
    runtime = Runtime.build(config, mode)
    assert isinstance(runtime.clock, WallClock) and isinstance(runtime.safety, SafetyMonitor)
    assert isinstance(runtime.event_loop.bar_recorder, BarRecorder)
    assert isinstance(runtime.event_loop.trade_recorder, TradeRecorder)
    assert isinstance(runtime.slots[0].stores.calc_state, CalcStateStore)
    assert isinstance(runtime.slots[0].stores.round_trips, RoundTripStore)
    names = [name for name, _ in runtime.services]
    assert names[-2:] == ["safety", "deploy_socket"]
    assert runtime.services[-2][1] is runtime.safety


class RecordingService:
    def __init__(self, name, log, fail_on_start=False):
        self.name, self.log, self.fail_on_start = name, log, fail_on_start

    async def start(self):
        self.log.append(("start", self.name))
        if self.fail_on_start:
            raise RuntimeError(f"{self.name} would not start")

    async def stop(self):
        self.log.append(("stop", self.name))


def bare_runtime(services):
    return Runtime(None, Mode.REPLAY, services, None, None, None)


async def test_services_start_in_order_and_stop_in_reverse():
    log = []
    names = ["gateway", "event_loop", "aux_data", "slots", "safety"]
    async with bare_runtime([(n, RecordingService(n, log)) for n in names]):
        pass
    assert log == [("start", n) for n in names] + [("stop", n) for n in reversed(names)]


async def test_failed_start_stops_started_services():
    log = []
    services = [("a", RecordingService("a", log)), ("b", RecordingService("b", log)),
                ("c", RecordingService("c", log, fail_on_start=True)), ("d", RecordingService("d", log))]
    with pytest.raises(RuntimeError, match="c would not start"):
        async with bare_runtime(services):
            pass
    assert log == [("start", "a"), ("start", "b"), ("start", "c"), ("stop", "b"), ("stop", "a")]


@pytest.mark.parametrize("fault", ["syntax_error", "missing_strategy_file"])
def test_broken_strategy_folder_exits_nonzero(fault, tmp_path):
    root = tmp_path / "strategies"
    folder = root / "broken_one"
    folder.mkdir(parents=True)
    if fault == "syntax_error":
        (folder / "strategy.py").write_text("def (\n")
    else:
        (folder / "calculations.py").write_text("x = 1\n")
    config = write_config(tmp_path, root)
    result = subprocess.run([sys.executable, "run_strategies.py", "--config", str(config), "--mode", "replay"],
                            cwd=ROOT, capture_output=True, text=True, timeout=120)
    assert result.returncode != 0
    assert "broken_one" in result.stderr and "strategy.py" in result.stderr


def test_unset_environment_variable_exits_nonzero_naming_it(tmp_path):
    path = tmp_path / "config.yaml"
    path.write_text(CONFIG_TEMPLATE.format(out="${DEFINITELY_NOT_SET}", strategies="x"))
    env = {k: v for k, v in os.environ.items() if k != "DEFINITELY_NOT_SET"}
    result = subprocess.run([sys.executable, "run_strategies.py", "--config", str(path), "--mode", "replay"],
                            cwd=ROOT, capture_output=True, text=True, timeout=120, env=env)
    assert result.returncode != 0 and "DEFINITELY_NOT_SET" in result.stderr


def test_log_deploy_fingerprint_names_hash_bot_and_strategies(tmp_path, caplog):
    config = load_config(write_config(tmp_path, ROOT / "local" / "strategies"))
    with caplog.at_level(logging.INFO):
        log_deploy_fingerprint(config, Mode.REPLAY)
    record = caplog.records[-1]
    assert (record.name, record.event, record.mode) == ("startup", "deploy_fingerprint", "replay")
    assert (record.config_sha256, record.bot_id) == (config.sha256, "test_bot")
    assert list(record.strategies) == ["macd_rsi_rth_long"]


async def test_sigterm_flattens_then_exits():
    from el_helpers import StubAux, StubRecorder, StubSafety
    from alchemist.config import EventLoopConfig
    from alchemist.event_loop.book import RuntimeBook
    from alchemist.event_loop.buffer import EventBuffer
    from alchemist.event_loop.loop import EventLoop
    from alchemist.event_loop.shutdown import ShutdownCoordinator
    from alchemist.instruments import ES
    from sd_helpers import FlattenStrategy, ShutdownGateway

    log = []
    config = EventLoopConfig(flatten_timeout_s=0.05)
    clock = ReplayClock(datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc))
    book, gateway = RuntimeBook(), ShutdownGateway(positions=[
        {"account_id": "ACC", "instrument": ES, "size": 1, "avg_cost": 1.0, "currency": "USD"}])
    book.strategies.append(FlattenStrategy("es", ES, flat_on_request=None))
    shutdown = ShutdownCoordinator(book, gateway, clock, config)
    loop = EventLoop(EventBuffer(config), book, gateway, StubAux(), StubSafety(), StubRecorder(), StubRecorder(),
                     shutdown, clock, config)
    services = [(n, RecordingService(n, log)) for n in ("gateway", "event_loop", "safety")]
    runtime = Runtime(None, Mode.PAPER, services, loop, shutdown, gateway)
    async with runtime:
        shutdown.request_shutdown("signal")
        await runtime.run()
    assert [(o["side"], o["qty"], o["order_type"]) for o in gateway.placed] == [("SELL", 1, "MKT")]
    assert [e for e in log if e[0] == "stop"] == [("stop", "safety"), ("stop", "event_loop"), ("stop", "gateway")]


async def test_run_exits_with_code_2_when_not_flat_at_shutdown():
    class Loop:
        async def run(self):
            return None

    class Shutdown:
        not_flat_at_shutdown = True

    class Gateway:
        async def start_feed(self):
            return None

    runtime = Runtime(None, Mode.REPLAY, [], Loop(), Shutdown(), Gateway())
    with pytest.raises(SystemExit) as raised:
        await runtime.run()
    assert raised.value.code == 2


def test_no_component_reads_the_run_mode():
    allowed = {"runtime.py", "mode.py"}
    offenders = [p.name for p in Path("alchemist").rglob("*.py")
                 if p.name not in allowed and "constants.mode" in p.read_text()]
    assert offenders == []


async def test_cold_start_admits_every_slot_then_hands_the_book_to_the_loop():
    from alchemist.runtime import SlotsService

    admitted, switched = [], []

    class Book:
        async def admit(self, slot):
            admitted.append(slot)

    class Aux:
        async def members_changed(self):
            switched.append("aux")

    service = SlotsService(Book(), ["a", "b"], Aux(), after_cold_start=lambda book: switched.append("loop"))
    await service.start()
    assert admitted == ["a", "b"] and switched == ["aux", "loop"]


def test_deploy_client_without_a_running_process_exits_nonzero(tmp_path):
    config = write_config(tmp_path, ROOT / "local" / "strategies")
    result = subprocess.run([sys.executable, "deploy_strategy.py", "--config", str(config), "--key",
                             "macd_rsi_rth_long"], cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert result.returncode == 1 and "deploy socket" in result.stderr
