from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from alchemist.gateways.simulation_gateway import ReplayConfig
from alchemist.instruments import Instrument
from alchemist.safety.monitor import SafetyConfig


@dataclass(frozen=True)
class SystemConfig:
    bot_id: str
    log_path: Path
    strategies_root: Path = Path("local/strategies")
    bar_flush_grace_s: float = 1.0
    order_check_interval_s: float = 60.0


@dataclass(frozen=True)
class StrategyConfig:
    """One slot's entry; name is its strategy key, strat the registered strategy."""
    name: str
    strat: str
    account_id: str
    signal_instrument: Instrument
    execution_instrument: Instrument
    execution: dict | None = None
    snapshot_interval: int = 0


@dataclass(frozen=True)
class Configuration:
    """Deployment config, one typed section per component (System map decisions 5, 47)."""
    path: Path
    sha256: str
    system: SystemConfig
    gateway: dict
    safety: SafetyConfig
    aux_sources: dict
    strategies: dict[str, StrategyConfig] = field(default_factory=dict)
    replay: ReplayConfig | None = None


def load_config(path: Path) -> Configuration:
    """Read YAML, resolve ${VAR} from the environment, build the typed sections."""
    ...
