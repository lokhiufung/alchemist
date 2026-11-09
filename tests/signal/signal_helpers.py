from collections import deque
from dataclasses import make_dataclass
from datetime import datetime, timezone

from alchemist.config import StrategyConfig
from alchemist.instruments import MES, Instrument
from alchemist.strategy_builder.signal_dag import SignalDag, SignalParams

SESSION_OPEN_TS = datetime(2026, 4, 15, 13, 30, tzinfo=timezone.utc).timestamp()


def strategy_config(instrument: Instrument = MES) -> StrategyConfig:
    return StrategyConfig("slot", "strat", "ACC", (instrument,), (instrument,))


def make_bar(index: int, close: float, high: float | None = None, low: float | None = None, **extra) -> dict:
    start = SESSION_OPEN_TS + index
    return {"instrument": MES, "start_ts": start, "end_ts": start + 1.0, "open": close,
            "high": close if high is None else high, "low": close if low is None else low,
            "close": close, "volume": 1.0, "resolution_s": 1.0, "broker_ts": start + 1.0, **extra}


def make_signal(build, extra_fields=(), **defaults) -> type[SignalDag]:
    fields = {"session": ("session", str, "regular"), "direction": ("direction", str, "long"),
              "rearm_required": ("rearm_required", bool, False)}
    for name, value in defaults.items():
        fields[name] = (name, type(value), value)
    for field in extra_fields:
        fields[field[0]] = field
    params = make_dataclass("Params", list(fields.values()), bases=(SignalParams,), frozen=True)
    return type("TestSignal", (SignalDag,), {"__module__": __name__, "Params": params, "build_graph": lambda self, g, p: build(g, p)})


class Stepper:
    def __init__(self, signal, first_index: int = 1) -> None:
        self.signal = signal
        self.bars: deque = deque()
        self.index = first_index

    def step(self, close: float, strategy_data: dict | None = None, **bar_extra) -> None:
        self.bars.append(make_bar(self.index, close, **bar_extra))
        self.index += 1
        self.signal.step(self.bars, strategy_data or {})
