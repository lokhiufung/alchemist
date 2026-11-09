from __future__ import annotations

import inspect
import logging
import operator
import types
import typing
from collections import deque
from dataclasses import dataclass, fields
from datetime import datetime, timezone
from typing import Any, Callable
from zoneinfo import ZoneInfo

from alchemist.calculations.calculation import Calculation, SessionFixedCalculation
from alchemist.exchanges import ExchangeProfile, exchange_profile
from alchemist.instruments import Instrument
from alchemist.registry import find_aux_source, get_calc, scope_of_class
from alchemist.strategy_builder.strategy_signal import StrategySignal, WarmupRequirements

logger = logging.getLogger(__name__)

BAR_FIELDS = ("open", "high", "low", "close", "volume", "start_ts", "end_ts")
HISTORY_KINDS = ("daily", "intraday", "aux")
ARITHMETIC = {"+": operator.add, "-": operator.sub, "*": operator.mul, "/": operator.truediv}
COMPARISON = {"<": operator.lt, "<=": operator.le, ">": operator.gt, ">=": operator.ge, "==": operator.eq}
LOGICAL = ("AND", "OR")
OPERATORS = (*ARITHMETIC, *COMPARISON, *LOGICAL)
LEVEL_KINDS = ("bar", "strategy_data", "calc", "expr")


class SignalBuildError(ValueError):
    pass


@dataclass(frozen=True)
class SignalParams:
    session: str
    direction: str
    max_trades_per_day: int = 1
    warm_up_months: int = 0
    rearm_required: bool = False


@dataclass(frozen=True)
class Node:
    name: str
    kind: str


@dataclass
class NodeSpec:
    node: Node
    inputs: tuple[Node, ...] = ()
    field: str | None = None
    op: str | None = None
    constant: Any = None
    has_constant: bool = False
    fn: Callable[..., Any] | None = None
    calc: Calculation | None = None
    history: str | None = None
    exit_side: bool = False

    @property
    def name(self) -> str:
        return self.node.name

    @property
    def kind(self) -> str:
        return self.node.kind


def kleene_and(values: list[bool | None]) -> bool | None:
    if any(v is False for v in values):
        return False
    return None if any(v is None for v in values) else True


def kleene_or(values: list[bool | None]) -> bool | None:
    if any(v is True for v in values):
        return True
    return None if any(v is None for v in values) else False


def apply_operator(op: str, left: Any, right: Any) -> Any:
    if op == "AND":
        return kleene_and([left, right])
    if op == "OR":
        return kleene_or([left, right])
    if left is None or right is None:
        return None
    return {**ARITHMETIC, **COMPARISON}[op](left, right)


def conforms(value: Any, hint: Any) -> bool:
    if hint is bool:
        return isinstance(value, bool)
    if hint is int:
        return isinstance(value, int) and not isinstance(value, bool)
    if hint is float:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    origin = typing.get_origin(hint)
    if origin in (typing.Union, types.UnionType):
        return any(conforms(value, arg) for arg in typing.get_args(hint) if arg is not type(None))
    target = origin or hint
    return not isinstance(target, type) or isinstance(value, target)


class Graph:
    def __init__(self, scope: str, profile: ExchangeProfile, session_name: str) -> None:
        self.scope = scope
        self.profile = profile
        self.session_name = session_name
        self.specs: dict[str, NodeSpec] = {}
        self.entry_nodes: list[Node] = []
        self.exit_nodes: list[Node] = []

    def add(self, spec: NodeSpec) -> Node:
        if spec.name in self.specs:
            raise SignalBuildError(f"duplicate node name {spec.name!r}")
        self.specs[spec.name] = spec
        return spec.node

    def require_nodes(self, owner: str, nodes: tuple[Any, ...]) -> None:
        for item in nodes:
            if not isinstance(item, Node) or item.name not in self.specs:
                raise SignalBuildError(f"node {owner!r}: input {item!r} is not a node of this graph")

    def leaf(self, kind: str, name: str, field: str) -> Node:
        existing = self.specs.get(name)
        if existing is not None and existing.kind == kind and existing.field == field:
            return existing.node
        return self.add(NodeSpec(Node(name, kind), field=field))

    def bar(self, field: str) -> Node:
        if field in BAR_FIELDS:
            return self.leaf("bar", field, field)
        source = find_aux_source(field)
        if source is None:
            raise SignalBuildError(f"market data {field!r} is not a bar field or a registered auxiliary name")
        supplied = getattr(source, "supplied_sessions", None)
        if supplied is not None and self.session_name not in supplied:
            raise SignalBuildError(
                f"market data {field!r} is not supplied for session {self.session_name!r} (supplied: {list(supplied)})")
        return self.leaf("bar", f"aux_{field}", f"aux_{field}")

    def strategy_data(self, field: str) -> Node:
        return self.leaf("strategy_data", field, field)

    def resolve_session(self, owner: str, label: str) -> str:
        try:
            return self.profile.session(label).name
        except KeyError:
            raise SignalBuildError(f"node {owner!r}: session {label!r} is not a session of {self.profile.name}") from None

    def calc(self, name: str, calc_key: str, *inputs: Node, **params) -> Node:
        try:
            instance = get_calc(calc_key, self.scope)()
        except KeyError:
            raise SignalBuildError(f"node {name!r}: calculation {calc_key!r} is not registered") from None
        self.require_nodes(name, inputs)
        wired = tuple(self.bar(field) for field in instance.bar_fields) + inputs
        accepted = inspect.signature(instance.setup).parameters
        if "session" in accepted:
            params["session"] = self.resolve_session(name, params.get("session", self.session_name))
        if "profile" in accepted:
            params["profile"] = self.profile
        try:
            instance.setup(**params)
        except Exception as exc:
            raise SignalBuildError(f"node {name!r}: {exc}") from exc
        return self.add(NodeSpec(Node(name, "calc"), inputs=wired, calc=instance))

    def expr(self, name: str, fn: Callable[..., Any], *inputs: Node) -> Node:
        self.require_nodes(name, inputs)
        return self.add(NodeSpec(Node(name, "expr"), inputs=inputs, fn=fn))

    def warmup(self, node: Node, history: str) -> Node:
        spec = self.specs.get(node.name)
        if spec is None or spec.kind != "calc":
            raise SignalBuildError(f"node {node.name!r}: only a calculation can be a warmup node")
        if history not in HISTORY_KINDS:
            raise SignalBuildError(f"node {node.name!r}: history {history!r} is not one of {HISTORY_KINDS}")
        if history != "aux" and spec.inputs and any(self.specs[i.name].kind not in ("bar", "expr") for i in spec.inputs):
            raise SignalBuildError(f"node {node.name!r}: a {history} warmup node may read only market data")
        spec.history = history
        if history == "aux":
            names = frozenset(i.field[4:] for i in map(self.specs.get, self.aux_leaf_names(spec)) if i)
            spec.calc.bind_aux(names)
        return node

    def aux_leaf_names(self, spec: NodeSpec) -> list[str]:
        found = []
        for node in spec.inputs:
            child = self.specs[node.name]
            if child.kind == "bar" and child.field.startswith("aux_"):
                found.append(child.name)
            found.extend(self.aux_leaf_names(child))
        return found

    def freeze_at_entry(self, node: Node) -> Node:
        spec = self.specs.get(node.name)
        if spec is None or spec.kind not in ("calc", "expr"):
            raise SignalBuildError(f"node {node.name!r}: only a calculation or expression can be frozen at entry")
        spec.exit_side = True
        return node

    def condition(self, name: str, left: Node, op: str, right: Node | float) -> Node:
        if op not in OPERATORS:
            raise SignalBuildError(f"node {name!r}: operator {op!r} is not one of {OPERATORS}")
        self.require_nodes(name, (left,))
        if isinstance(right, Node):
            self.require_nodes(name, (right,))
            return self.add(NodeSpec(Node(name, "condition"), inputs=(left, right), op=op))
        return self.add(NodeSpec(Node(name, "condition"), inputs=(left,), op=op, constant=right, has_constant=True))

    def any_of(self, name: str, *conditions: Node) -> Node:
        self.require_nodes(name, conditions)
        return self.add(NodeSpec(Node(name, "any_of"), inputs=conditions, op="OR"))

    def entry(self, *conditions: Node) -> None:
        self.require_nodes("entry", conditions)
        self.entry_nodes.extend(conditions)

    def exit(self, name: str, left: Node, op: str, right: Node | float) -> None:
        self.exit_nodes.append(self.condition(name, left, op, right))


class SignalDag(StrategySignal):
    Params: type[SignalParams] = SignalParams

    def build_graph(self, g: Graph, p: Any) -> None:
        raise NotImplementedError

    @classmethod
    def from_config(cls, strategy_config) -> SignalDag:
        return cls(strategy_config.signal_instrument)

    def __init__(self, instrument: Instrument) -> None:
        self.instrument = instrument
        self.profile = exchange_profile(instrument)
        self.params = self.checked_params()
        graph = Graph(scope_of_class(type(self)), self.profile, self.resolve_session(graph_owner="params"))
        self.session = graph.session_name
        self.build_graph(graph, self.params)
        self.finish_build(graph)
        self.reset_state()

    def resolve_session(self, graph_owner: str) -> str:
        try:
            return self.profile.session(self.params.session).name
        except KeyError:
            raise SignalBuildError(
                f"param 'session': {self.params.session!r} is not a session of {self.profile.name}") from None

    def checked_params(self) -> SignalParams:
        try:
            params = self.Params()
        except TypeError as exc:
            raise SignalBuildError(f"params: {exc}") from exc
        hints = typing.get_type_hints(self.Params)
        for item in fields(params):
            value = getattr(params, item.name)
            if not conforms(value, hints[item.name]):
                raise SignalBuildError(
                    f"param {item.name!r}: {value!r} does not match type {hints[item.name]}")
        if params.direction not in ("long", "short"):
            raise SignalBuildError(f"param 'direction': {params.direction!r} must be 'long' or 'short'")
        if params.warm_up_months < 0:
            raise SignalBuildError(f"param 'warm_up_months': {params.warm_up_months} must be 0 or more")
        return params

    def finish_build(self, graph: Graph) -> None:
        if not graph.entry_nodes:
            raise SignalBuildError("signal has no entry condition")
        if not graph.exit_nodes:
            raise SignalBuildError("signal has no exit")
        if self.params.rearm_required and "entry" in graph.specs:
            raise SignalBuildError("node 'entry' clashes with the rearm gate; rename the node")
        for spec in graph.specs.values():
            if spec.kind in ("calc", "expr") and any(i.kind == "strategy_data" for i in spec.inputs):
                spec.exit_side = True
        self.specs = list(graph.specs.values())
        self.by_name = graph.specs
        self.entry_nodes = list(graph.entry_nodes)
        self.exit_nodes = list(graph.exit_nodes)

    def reset_state(self) -> None:
        self.node_values: dict[str, Any] = {}
        self.stale: set[str] = set()
        self.entry_answer = False
        self.warned_day = None
        self.gate_day = None
        self.previous_raw: bool | None = None
        self.frozen: dict[str, dict[str, Any]] | None = None

    @property
    def rearm_required(self) -> bool:
        return self.params.rearm_required

    def warmup_specs(self) -> list[NodeSpec]:
        return [s for s in self.specs if s.history is not None]

    def warmup_requirements(self) -> WarmupRequirements:
        by_kind = {kind: tuple(s.name for s in self.warmup_specs() if s.history == kind) for kind in HISTORY_KINDS}
        daily_months = max([self.params.warm_up_months]
                           + [s.calc.daily_months() for s in self.warmup_specs() if s.history == "daily"])
        fixed = tuple(s.name for s in self.specs if isinstance(s.calc, SessionFixedCalculation))
        return WarmupRequirements(daily_months=daily_months, intraday=by_kind["intraday"],
                                  daily=by_kind["daily"], aux=by_kind["aux"], session_fixed=fixed)

    def seed(self, history: dict) -> None:
        for spec in self.warmup_specs():
            frame = history.get(spec.history, {} if spec.history == "aux" else [])
            if spec.history != "aux" and spec.inputs:
                frame = [tuple(self.seed_value(node, row) for node in spec.inputs) for row in frame]
            spec.calc.warmup(frame)

    def seed_value(self, node: Node, row: dict) -> Any:
        spec = self.by_name[node.name]
        if spec.kind == "bar":
            return row.get(spec.field)
        values = [self.seed_value(i, row) for i in spec.inputs]
        return None if any(v is None for v in values) else spec.fn(*values)

    def is_warm(self) -> bool:
        return all(s.calc.value() is not None for s in self.warmup_specs())

    def step(self, bars: deque, strategy_data: dict) -> None:
        bar = bars[-1]
        values: dict[str, Any] = {}
        stale: set[str] = set()
        for spec in self.specs:
            values[spec.name] = self.compute(spec, values, stale, bar, strategy_data)
        self.node_values, self.stale = values, stale
        raw = kleene_and([values[n.name] for n in self.entry_nodes])
        self.update_entry(bar, raw)

    def compute(self, spec: NodeSpec, values: dict, stale: set, bar: dict, strategy_data: dict) -> Any:
        if spec.kind == "bar":
            return bar.get(spec.field)
        if spec.kind == "strategy_data":
            return strategy_data.get(spec.field)
        frozen = self.frozen is not None and spec.exit_side
        if frozen and spec.kind == "calc":
            return self.frozen["held"][spec.name]
        inputs = [self.input_value(i, values, strategy_data, frozen) for i in spec.inputs]
        if spec.kind in ("condition", "any_of"):
            if spec.kind == "any_of":
                return kleene_or(inputs)
            right = spec.constant if spec.has_constant else inputs[1]
            return apply_operator(spec.op, inputs[0], right)
        if any(i.name in stale for i in spec.inputs):
            stale.add(spec.name)
            return None
        if any(v is None for v in inputs):
            return None
        try:
            return spec.calc.step(*inputs) if spec.kind == "calc" else spec.fn(*inputs)
        except Exception:
            logger.warning("node %s failed on bar ending %s", spec.name, bar.get("end_ts"), exc_info=True)
            stale.add(spec.name)
            return None

    def trading_day(self, bar: dict):
        return datetime.fromtimestamp(bar["end_ts"], ZoneInfo(self.profile.tz)).date()

    def input_value(self, node: Node, values: dict, strategy_data: dict, frozen: bool) -> Any:
        if not frozen:
            return values[node.name]
        if node.kind == "strategy_data":
            return strategy_data.get(self.by_name[node.name].field)
        return self.frozen["inputs"][node.name]

    def update_entry(self, bar: dict, raw: bool | None) -> None:
        day = self.trading_day(bar)
        if day != self.gate_day:
            self.gate_day, self.previous_raw = day, None
        rising = raw is True and self.previous_raw is not True
        self.previous_raw = raw
        self.entry_answer = (rising and self.is_warm()) if self.rearm_required else raw is True
        if raw is None and self.rearm_required:
            self.warn_null_entry(day)

    def warn_null_entry(self, day) -> None:
        if day == self.warned_day:
            return
        self.warned_day = day
        logger.warning("entry is null on %s; missing refs: %s", day, sorted(self.missing_refs()))

    def missing_refs(self) -> set[str]:
        missing: set[str] = set()
        pending = list(self.entry_nodes)
        while pending:
            node = pending.pop()
            spec = self.by_name[node.name]
            if self.node_values.get(node.name) is not None:
                continue
            blockers = [i for i in spec.inputs if self.node_values.get(i.name) is None]
            if blockers:
                pending.extend(blockers)
            else:
                missing.add(node.name)
        return missing

    def evaluate_entry(self) -> bool:
        return self.entry_answer

    def evaluate_exit_conditions(self) -> dict[str, bool]:
        return {n.name: self.node_values.get(n.name) is True for n in self.exit_nodes}

    def exit_condition_level(self, name: str) -> float | None:
        spec = self.by_name[name]
        if spec not in [self.by_name[n.name] for n in self.exit_nodes]:
            raise KeyError(f"{name!r} is not an exit condition")
        left = spec.inputs[0]
        if left.name != "close" or left.kind != "bar" or spec.has_constant or spec.op not in COMPARISON:
            return None
        return self.level(spec.inputs[1].name)

    def level(self, name: str) -> float | None:
        value = self.node_values.get(name)
        if name in self.stale or isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return float(value)

    def has_level(self, name: str) -> bool:
        spec = self.by_name.get(name)
        return spec is not None and spec.kind in LEVEL_KINDS

    def entry_conditions(self) -> dict[str, bool | None]:
        return {n.name: self.node_values.get(n.name) for n in self.entry_nodes}

    def exit_condition_names(self) -> list[str]:
        return [n.name for n in self.exit_nodes]

    def condition_names(self) -> set[str]:
        return {s.name for s in self.specs if s.kind in ("condition", "any_of")}

    def values(self) -> dict[str, Any]:
        return dict(self.node_values)

    def freeze_exit_inputs(self, bar: dict) -> None:
        inputs: dict[str, Any] = {}
        held: dict[str, Any] = {}
        for spec in self.specs:
            if not spec.exit_side:
                continue
            for node in spec.inputs:
                if node.kind != "strategy_data":
                    source = self.by_name[node.name]
                    inputs[node.name] = bar.get(source.field) if node.kind == "bar" else self.node_values.get(node.name)
            if spec.kind == "calc":
                held[spec.name] = self.node_values.get(spec.name)
        self.frozen = {"inputs": inputs, "held": held}

    def release_exit_inputs(self) -> None:
        self.frozen = None

    def snapshot_session_state(self) -> dict:
        state = {}
        for spec in self.specs:
            if isinstance(spec.calc, SessionFixedCalculation):
                snapshot = spec.calc.snapshot_state()
                if snapshot is not None:
                    state[spec.name] = snapshot
        return state

    def restore_session_state(self, state: dict) -> None:
        for spec in self.specs:
            if isinstance(spec.calc, SessionFixedCalculation) and spec.name in state:
                spec.calc.restore_state(state[spec.name])
                self.node_values[spec.name] = spec.calc.value()
