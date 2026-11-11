from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from typing import Any

import yaml

from alchemist.constants.gateway import RECONNECT_BASE_S, RECONNECT_CAP_S
from alchemist.constants.safety import (
    CHECK_IN_INTERVAL_S,
    FREEZE_THRESHOLD,
    HEARTBEAT_INTERVAL_S,
    HEARTBEAT_TIMEOUT_S,
    REJECT_SYNTHESIS_MIN_AGE_S,
    RESUBSCRIBE_BACKOFF_BASE_S,
    RESUBSCRIBE_MAX_ATTEMPTS,
    RESUBSCRIBE_WITHOUT_FEED_CAP,
    STALE_FEED_S,
    STUCK_CHECK_INTERVAL_S,
    WATCH_CHECK_INTERVAL_S,
)
from alchemist.instruments import Instrument, instrument

ENV_REFERENCE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


class ConfigError(ValueError):
    pass


@dataclass(frozen=True)
class SystemConfig:
    bot_id: str
    log_path: Path
    strategies_root: Path = Path("local/strategies")


@dataclass(frozen=True)
class GatewayConfig:
    name: str
    options: dict = field(default_factory=dict)
    replay: "ReplayConfig | None" = None
    heartbeat_interval_s: float = HEARTBEAT_INTERVAL_S
    heartbeat_timeout_s: float = HEARTBEAT_TIMEOUT_S
    reconnect_base_s: float = RECONNECT_BASE_S
    reconnect_cap_s: float = RECONNECT_CAP_S


@dataclass(frozen=True)
class ReplayConfig:
    tape: str
    date: date
    session: str
    pad_before_s: int = 0
    pad_after_s: int = 0
    fill_reference: str = "close"
    slippage_ticks: int = 0
    journal: Path | None = None


@dataclass(frozen=True)
class SafetyConfig:
    maintenance_windows: tuple[tuple[str, str], ...] = ()
    positions_poll_s: float | None = None
    heartbeat_interval_s: float = HEARTBEAT_INTERVAL_S
    heartbeat_timeout_s: float = HEARTBEAT_TIMEOUT_S
    stale_feed_s: float = STALE_FEED_S
    stale_overrides: dict = field(default_factory=dict)
    watch_check_interval_s: float = WATCH_CHECK_INTERVAL_S
    resubscribe_max_attempts: int = RESUBSCRIBE_MAX_ATTEMPTS
    resubscribe_without_feed_cap: int = RESUBSCRIBE_WITHOUT_FEED_CAP
    resubscribe_backoff_base_s: float = RESUBSCRIBE_BACKOFF_BASE_S
    check_interval_s: float = STUCK_CHECK_INTERVAL_S
    reject_synthesis_min_age_s: float = REJECT_SYNTHESIS_MIN_AGE_S
    freeze_threshold: int = FREEZE_THRESHOLD
    check_in_interval_s: float = CHECK_IN_INTERVAL_S


@dataclass(frozen=True)
class EventLoopConfig:
    bar_flush_grace_s: float = 1.0
    drain_timeout_s: float = 0.020
    drain_batch_budget_s: float = 0.002
    backlog_alert: int = 5000
    on_bar_error_quarantine: int = 3
    gil_switch_interval_s: float = 0.0005
    cutoff_offset_s: float = 60.0
    run_end: str = "17:00"
    run_tz: str = "America/New_York"
    flatten_timeout_s: float = 10.0
    scale_trigger_s: float = 0.003
    rebuild_yield_budget_s: float = 0.001
    seed_backoff_base_s: float = 5.0
    seed_backoff_max_s: float = 300.0
    seed_backoff_jitter: float = 0.2
    warmup_min_trading_days: int = 1
    warmup_max_calendar_days: int = 400
    seed_fetch_ttl_s: float = 300.0


@dataclass(frozen=True)
class AuxConfig:
    step_interval_s: float = 60.0
    recheck_interval_s: float = 3600.0
    confirmations: int = 1
    frame_days: int = 730


@dataclass(frozen=True)
class ExecutionConfig:
    reminder_interval_s: float = 300.0


@dataclass(frozen=True)
class StrategyConfig:
    name: str
    strat: str
    account_id: str
    signal_instruments: tuple[Instrument, ...]
    execution_instruments: tuple[Instrument, ...]
    execution: dict | None = None
    snapshot_interval: int = 60

    @property
    def signal_instrument(self) -> Instrument:
        return self.signal_instruments[0]

    @property
    def execution_instrument(self) -> Instrument:
        return self.execution_instruments[0]


@dataclass(frozen=True)
class Configuration:
    path: Path
    sha256: str
    system: SystemConfig
    gateway: GatewayConfig
    safety: SafetyConfig
    aux_sources: dict
    aux: AuxConfig
    execution: ExecutionConfig
    event_loop: EventLoopConfig
    strategies: dict[str, StrategyConfig] = field(default_factory=dict)
    replay: ReplayConfig | None = None


def resolve_environment(value: Any) -> Any:
    if isinstance(value, str):
        def substitute(match: re.Match) -> str:
            name = match.group(1)
            if name not in os.environ:
                raise ConfigError(f"environment variable {name} is not set")
            return os.environ[name]
        return ENV_REFERENCE.sub(substitute, value)
    if isinstance(value, dict):
        return {key: resolve_environment(item) for key, item in value.items()}
    if isinstance(value, list):
        return [resolve_environment(item) for item in value]
    return value


def instruments_from(value: Any) -> tuple[Instrument, ...]:
    names = value if isinstance(value, list) else [value]
    return tuple(instrument(name) for name in names)


def build_strategy(name: str, raw: dict) -> StrategyConfig:
    return StrategyConfig(
        name=name,
        strat=raw["strat"],
        account_id=raw["account_id"],
        signal_instruments=instruments_from(raw["signal_instrument"]),
        execution_instruments=instruments_from(raw["execution_instrument"]),
        execution=raw.get("execution"),
        snapshot_interval=raw.get("snapshot_interval", 60),
    )


def build_replay(raw: dict | None) -> ReplayConfig | None:
    if raw is None:
        return None
    journal = raw.get("journal")
    return ReplayConfig(**{**raw, "journal": Path(journal) if journal else None})


GATEWAY_FIELDS = ("heartbeat_interval_s", "heartbeat_timeout_s", "reconnect_base_s", "reconnect_cap_s")


def build_gateway(raw: dict, replay: ReplayConfig | None) -> GatewayConfig:
    typed = {key: raw.pop(key) for key in GATEWAY_FIELDS if key in raw}
    return GatewayConfig(name=raw.pop("name"), options=raw, replay=replay, **typed)


def build_safety(raw: dict | None) -> SafetyConfig:
    raw = dict(raw or {})
    if "maintenance_windows" in raw:
        raw["maintenance_windows"] = tuple(tuple(pair) for pair in raw["maintenance_windows"])
    return SafetyConfig(**raw)


def load_config(path: Path) -> Configuration:
    raw_bytes = Path(path).read_bytes()
    raw = resolve_environment(yaml.safe_load(raw_bytes) or {})
    system = raw["system"]
    gateway = dict(raw["gateway"])
    replay = build_replay(raw.get("replay"))
    return Configuration(
        path=Path(path),
        sha256=hashlib.sha256(raw_bytes).hexdigest(),
        system=SystemConfig(
            bot_id=system["bot_id"],
            log_path=Path(system["log_path"]),
            strategies_root=Path(system.get("strategies_root", "local/strategies")),
        ),
        gateway=build_gateway(gateway, replay),
        safety=build_safety(raw.get("safety")),
        aux_sources=raw.get("aux_sources") or {},
        aux=AuxConfig(**(raw.get("aux") or {})),
        execution=ExecutionConfig(**(raw.get("execution") or {})),
        event_loop=EventLoopConfig(**(raw.get("event_loop") or {})),
        strategies={name: build_strategy(name, body) for name, body in (raw.get("strategies") or {}).items()},
        replay=replay,
    )
