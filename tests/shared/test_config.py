import hashlib
from datetime import date

import pytest

from alchemist.config import ConfigError, load_config
from alchemist.instruments import FDAX, MES, MNQ

GOLDEN_YAML = """\
system:
  bot_id: golden
  log_path: ${GOLDEN_OUT}
  strategies_root: tests/golden/strategies

gateway:
  name: simulation

replay:
  tape: synthetic:golden_session
  date: 2026-04-15
  session: NY_RTH
  pad_before_s: 0
  pad_after_s: 0
  fill_reference: close
  slippage_ticks: 0
  journal: ${GOLDEN_OUT}/broker_journal.jsonl

strategies:
  macd_rsi_rth_long:
    strat: macd_rsi_rth_long
    account_id: SIM
    signal_instrument: MES
    execution_instrument: MES
"""


def write(tmp_path, text):
    path = tmp_path / "config.yaml"
    path.write_text(text)
    return path


def test_unset_env_var_named(tmp_path, monkeypatch):
    monkeypatch.delenv("GOLDEN_OUT", raising=False)
    with pytest.raises(ConfigError, match="GOLDEN_OUT"):
        load_config(write(tmp_path, GOLDEN_YAML))


def test_config_sha256_is_file_bytes(tmp_path, monkeypatch):
    monkeypatch.setenv("GOLDEN_OUT", str(tmp_path))
    path = write(tmp_path, GOLDEN_YAML)
    assert load_config(path).sha256 == hashlib.sha256(path.read_bytes()).hexdigest()


def test_golden_config_parses(tmp_path, monkeypatch):
    monkeypatch.setenv("GOLDEN_OUT", str(tmp_path))
    config = load_config(write(tmp_path, GOLDEN_YAML))
    assert config.gateway.name == "simulation"
    assert config.replay.date == date(2026, 4, 15)
    assert config.replay.journal == tmp_path / "broker_journal.jsonl"
    assert config.system.log_path == tmp_path
    assert config.strategies["macd_rsi_rth_long"].execution_instrument == MES
    assert config.strategies["macd_rsi_rth_long"].name == "macd_rsi_rth_long"


def test_list_form_instruments_keep_first_as_single_name(tmp_path, monkeypatch):
    monkeypatch.setenv("GOLDEN_OUT", str(tmp_path))
    text = GOLDEN_YAML.replace("signal_instrument: MES", "signal_instrument: [MES, MNQ]").replace(
        "execution_instrument: MES", "execution_instrument: [FDAX]")
    strategy = load_config(write(tmp_path, text)).strategies["macd_rsi_rth_long"]
    assert strategy.signal_instruments == (MES, MNQ)
    assert strategy.signal_instrument == MES
    assert strategy.execution_instruments == (FDAX,)
    assert strategy.execution_instrument == FDAX


def test_config_without_replay_section_has_none(tmp_path, monkeypatch):
    monkeypatch.setenv("GOLDEN_OUT", str(tmp_path))
    text = GOLDEN_YAML.split("replay:")[0] + "strategies: {}\n"
    assert load_config(write(tmp_path, text)).replay is None


def test_gateway_options_are_everything_but_name(tmp_path, monkeypatch):
    monkeypatch.setenv("GOLDEN_OUT", str(tmp_path))
    text = GOLDEN_YAML.replace("  name: simulation", "  name: ibkr\n  host: 127.0.0.1\n  port: 7497")
    gateway = load_config(write(tmp_path, text)).gateway
    assert (gateway.name, gateway.options) == ("ibkr", {"host": "127.0.0.1", "port": 7497})


def test_safety_defaults_follow_the_map(tmp_path, monkeypatch):
    monkeypatch.setenv("GOLDEN_OUT", str(tmp_path))
    config = load_config(write(tmp_path, GOLDEN_YAML))
    safety, gateway = config.safety, config.gateway
    assert (gateway.reconnect_base_s, gateway.reconnect_cap_s) == (15.0, 120.0)
    assert (gateway.heartbeat_interval_s, gateway.heartbeat_timeout_s) == (5.0, 3.0)
    assert (safety.heartbeat_interval_s, safety.heartbeat_timeout_s) == (5.0, 3.0)
    assert (safety.stale_feed_s, safety.watch_check_interval_s) == (45.0, 1.0)


def test_gateway_watch_numbers_are_configurable_per_process(tmp_path, monkeypatch):
    monkeypatch.setenv("GOLDEN_OUT", str(tmp_path))
    text = GOLDEN_YAML.replace("  name: simulation", "  name: simulation\n  heartbeat_interval_s: 2.0\n  reconnect_cap_s: 60.0")
    gateway = load_config(write(tmp_path, text)).gateway
    assert (gateway.heartbeat_interval_s, gateway.reconnect_cap_s, gateway.options) == (2.0, 60.0, {})


AUX_YAML = GOLDEN_YAML + """
aux_sources:
  vix_close:
    source: file
    root: data/aux
  eurex_series:
    source: file
    root: data/aux
    supplied_sessions: [FRA_DAY]
"""


def test_aux_names_come_from_aux_sources_keys(tmp_path, monkeypatch):
    monkeypatch.setenv("GOLDEN_OUT", str(tmp_path))
    config = load_config(write(tmp_path, AUX_YAML))
    assert config.aux_sources["eurex_series"] == {"source": "file", "root": "data/aux"}
    assert config.strategies["macd_rsi_rth_long"].aux_names == {"vix_close": None, "eurex_series": ("FRA_DAY",)}
