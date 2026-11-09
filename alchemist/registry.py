from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

BUILTIN_SCOPE = "builtin"
_PACKAGE_DIR = Path(__file__).resolve().parent

_calcs: dict[str, dict[str, tuple[type, str]]] = {}
_signal_dags: dict[str, tuple[type, str]] = {}
_strategies: dict[str, tuple[type, str]] = {}
_gateways: dict[str, tuple[type, str]] = {}
_tapes: dict[str, tuple[type, str]] = {}
_aux_sources: dict[str, tuple[type, str]] = {}
_aux_names: dict[str, "AuxName"] = {}


def _caller_file(depth: int) -> Path:
    return Path(sys._getframe(depth).f_globals.get("__file__", "<unknown>")).resolve()


def scope_of(file: Path) -> str:
    return BUILTIN_SCOPE if _PACKAGE_DIR in file.parents else str(file.parent)


def _bind(table: dict[str, tuple[type, str]], name: str, cls: type, file: Path) -> None:
    if name in table:
        raise ValueError(f"{name!r} bound twice: {table[name][1]} and {file}")
    table[name] = (cls, str(file))


def _decorator(table_for: Callable[[Path], dict], name: str) -> Callable[[type], type]:
    file = _caller_file(3)

    def bind(cls: type) -> type:
        _bind(table_for(file), name, cls, file)
        return cls
    return bind


def register_calc(name: str) -> Callable[[type], type]:
    """StrategySignal behaviour 1: binds name in the calling folder's scope."""
    return _decorator(lambda f: _calcs.setdefault(scope_of(f), {}), name)


def get_calc(name: str, scope: str) -> type:
    for table in (_calcs.get(scope, {}), _calcs.get(BUILTIN_SCOPE, {})):
        if name in table:
            return table[name][0]
    raise KeyError(f"no calculation {name!r} in scope {scope} or the built-ins")


def register_signal_dag(name: str) -> Callable[[type], type]:
    return _decorator(lambda f: _signal_dags, name)


def lookup(table: dict[str, tuple[type, str]], kind: str, name: str) -> type:
    if name not in table:
        raise KeyError(f"no {kind} named {name!r}; known: {sorted(table)}")
    return table[name][0]


def get_signal_dag(name: str) -> type:
    return lookup(_signal_dags, "signal dag", name)


def register_strategy(name: str) -> Callable[[type], type]:
    return _decorator(lambda f: _strategies, name)


def get_strategy(name: str) -> type:
    return lookup(_strategies, "strategy", name)


def register_gateway(name: str) -> Callable[[type], type]:
    return _decorator(lambda f: _gateways, name)


def get_gateway(name: str) -> type:
    return lookup(_gateways, "gateway", name)


def register_tape(name: str) -> Callable[[type], type]:
    return _decorator(lambda f: _tapes, name)


def get_tape(name: str) -> type:
    return lookup(_tapes, "tape", name)


def register_aux_source(name: str) -> Callable[[type], type]:
    return _decorator(lambda f: _aux_sources, name)


def get_aux_source(name: str) -> type:
    return lookup(_aux_sources, "aux source", name)


@dataclass(frozen=True)
class AuxName:
    name: str
    supplied_sessions: tuple[str, ...] | None = None


def register_aux_name(name: str, supplied_sessions: tuple[str, ...] | None = None) -> AuxName:
    if name in _aux_names:
        raise ValueError(f"auxiliary name {name!r} registered twice")
    _aux_names[name] = AuxName(name, supplied_sessions)
    return _aux_names[name]


def find_aux_name(name: str) -> AuxName | None:
    return _aux_names.get(name)


def scope_of_class(cls: Any) -> str:
    return scope_of(Path(sys.modules[cls.__module__].__file__ or "<unknown>").resolve())
