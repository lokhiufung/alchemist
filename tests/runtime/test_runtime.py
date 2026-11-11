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
from alchemist.config import load_config
from alchemist.constants.mode import Mode
from alchemist.event_loop.recorders import BarRecorder, TradeRecorder
from alchemist.runtime import ADAPTERS, Runtime, log_deploy_fingerprint
from alchemist.safety.monitor import SafetyMonitor
from alchemist.stores import CalcStateStore, RoundTripStore

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


@pytest.mark.parametrize("mode", [Mode.REPLAY, Mode.PAPER, Mode.LIVE])
def test_every_mode_builds_the_same_core(mode, tmp_path):
    config = load_config(write_config(tmp_path, ROOT / "local" / "strategies"))
    runtime = Runtime.build(config, mode)
    loop = runtime.event_loop_service.event_loop
    assert isinstance(runtime.safety, SafetyMonitor) and not hasattr(runtime, "clock")
    assert isinstance(loop.bar_recorder, BarRecorder) and isinstance(loop.trade_recorder, TradeRecorder)
    assert isinstance(runtime.slots[0].stores.calc_state, CalcStateStore)
    assert isinstance(runtime.slots[0].stores.round_trips, RoundTripStore)
    names = [name for name, _ in runtime.services]
    core = ["gateway", "event_loop", "aux_data", "slots", "safety"]
    assert names == (core if mode is Mode.REPLAY else core + ["deploy_socket"])


@pytest.mark.parametrize("mode,socket,signals", [(Mode.REPLAY, False, False), (Mode.PAPER, True, True),
                                                 (Mode.LIVE, True, True)])
def test_mode_selects_only_the_edge_adapters(mode, socket, signals):
    assert (ADAPTERS[mode].deploy_socket, ADAPTERS[mode].signal_handlers) == (socket, signals)


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
    return Runtime(None, ADAPTERS[Mode.REPLAY], services, None, None, None)


async def test_services_start_in_order_and_stop_in_reverse():
    log = []
    names = ["gateway", "event_loop", "aux_data", "slots", "safety"]
    async with bare_runtime([(n, RecordingService(n, log)) for n in names]):
        pass
    assert log == [("start", n) for n in names] + [("stop", n) for n in reversed(names)]


async def test_component_services_log_to_their_own_logger(caplog):
    names = ["gateway", "event_loop", "aux_data", "slots", "safety", "deploy_socket"]
    with caplog.at_level(logging.INFO):
        async with bare_runtime([(n, RecordingService(n, [])) for n in names]):
            pass
    loggers = {(r.service, r.event): r.name for r in caplog.records if hasattr(r, "service")}
    assert loggers == {
        **{(n, "service_started"): n for n in ["gateway", "event_loop", "aux_data", "safety"]},
        **{(n, "service_stopped"): n for n in ["gateway", "event_loop", "aux_data", "safety"]},
        ("slots", "service_started"): "startup", ("slots", "service_stopped"): "shutdown",
        ("deploy_socket", "service_started"): "deploy", ("deploy_socket", "service_stopped"): "deploy"}


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


class LoopService:
    def __init__(self, loop):
        self.event_loop = loop
        self.task = None

    async def start(self):
        self.task = asyncio.get_running_loop().create_task(self.event_loop.run())

    async def stop(self):
        return None


async def test_os_signal_event_flattens_then_exits():
    from el_helpers import StubAux, StubDeploy, StubRecorder, StubSafety
    from alchemist.config import EventLoopConfig
    from alchemist.event_loop.book import RuntimeBook
    from alchemist.event_loop.buffer import EventBuffer
    from alchemist.event_loop.loop import EventLoop
    from alchemist.event_loop.shutdown import ShutdownCoordinator
    from alchemist.instruments import ES
    from alchemist.messages import make_event
    from request_helpers import DirectRequests
    from sd_helpers import FlattenStrategy, ShutdownGateway
    from time_helpers import ManualWatchTime

    log = []
    config = EventLoopConfig(flatten_timeout_s=0.05)
    watch_time = ManualWatchTime(datetime(2026, 4, 15, 15, 0, tzinfo=timezone.utc))
    book, gateway = RuntimeBook(), ShutdownGateway(positions=[
        {"account_id": "ACC", "instrument": ES, "size": 1, "avg_cost": 1.0, "currency": "USD"}])
    requests = DirectRequests(gateway)
    book.strategies.append(FlattenStrategy("es", ES, flat_on_request=None))
    book.sessions_by_strategy["es"] = "NY_RTH"
    shutdown = ShutdownCoordinator(book, requests, watch_time, config)
    inbound = EventBuffer(config)
    loop = EventLoop(inbound, requests, book, StubAux(), StubRecorder(), StubRecorder(), shutdown, watch_time,
                     StubDeploy(), StubSafety().check_in, config)
    loop_service = LoopService(loop)
    services = [("gateway", RecordingService("gateway", log)), ("event_loop", loop_service),
                ("safety", RecordingService("safety", log))]
    runtime = Runtime(None, ADAPTERS[Mode.REPLAY], services, loop_service, shutdown, gateway, inbound)
    async with runtime:
        inbound.push(make_event("os_signal", {"signal": "SIGTERM"}, 0.0))
        await runtime.run()
    assert shutdown.shutdown_requested
    assert [(o["side"], o["qty"], o["order_type"]) for o in gateway.placed] == [("SELL", 1, "MKT")]
    assert [e for e in log if e[0] == "stop"] == [("stop", "safety"), ("stop", "gateway")]


async def test_run_exits_with_code_2_when_not_flat_at_shutdown():
    class Shutdown:
        not_flat_at_shutdown = True

    class Gateway:
        async def start_feed(self):
            return None

    async def finished():
        return None

    loop_service = LoopService(None)
    loop_service.task = asyncio.ensure_future(finished())
    runtime = Runtime(None, ADAPTERS[Mode.REPLAY], [], loop_service, Shutdown(), Gateway())
    with pytest.raises(SystemExit) as raised:
        await runtime.run()
    assert raised.value.code == 2


def test_no_component_reads_the_run_mode():
    allowed = {"runtime.py", "mode.py"}
    offenders = [p.name for p in Path("alchemist").rglob("*.py")
                 if p.name not in allowed and "constants.mode" in p.read_text()]
    assert offenders == []


async def test_cold_start_waits_for_the_first_event_then_admits_every_slot():
    from alchemist.runtime import SlotsService

    admitted, switched = [], []

    class Book:
        async def admit(self, slot):
            admitted.append(slot)

    class Aux:
        async def members_changed(self):
            switched.append("aux")

    from time_helpers import ManualWatchTime
    watch_time = ManualWatchTime()
    service = SlotsService(Book(), ["a", "b"], Aux(), watch_time)
    starting = asyncio.ensure_future(service.start())
    await asyncio.sleep(0)
    assert admitted == []
    watch_time.set(1_776_259_800.0)
    await starting
    assert admitted == ["a", "b"] and switched == ["aux"]


def test_deploy_client_without_a_running_process_exits_nonzero(tmp_path):
    config = write_config(tmp_path, ROOT / "local" / "strategies")
    result = subprocess.run([sys.executable, "deploy_strategy.py", "--config", str(config), "--key",
                             "macd_rsi_rth_long"], cwd=ROOT, capture_output=True, text=True, timeout=60)
    assert result.returncode == 1 and "deploy socket" in result.stderr
