import logging

import pytest

from alchemist.calculations import Calculation
from alchemist.registry import register_aux_source, register_calc
from alchemist.strategy_builder.signal_dag import SignalBuildError

from signal_helpers import Stepper, make_bar, make_signal, strategy_config


def simple_entry_exit(g, p):
    close = g.bar("close")
    g.entry(g.condition("above", close, ">", 100.0))
    g.exit("stop", close, "<", 90.0)


@register_calc("boom_on_setup")
class BoomOnSetup(Calculation):
    def setup(self, **params) -> None:
        raise ValueError("cannot be well defined")


def build_with(**changes):
    def build(g, p):
        close = g.bar("close")
        g.entry(g.condition("above", close, ">", 100.0))
        g.exit("stop", close, "<", 90.0)
    return build


BAD_SIGNALS = {
    "unregistered calculation": (
        lambda g, p: (g.calc("fancy", "no_such_calc", g.bar("close")), simple_entry_exit(g, p)), {}, "no_such_calc"),
    "duplicate node name": (
        lambda g, p: (g.calc("same", "sma", g.bar("close"), period=3), g.calc("same", "sma", g.bar("close"), period=4),
                      simple_entry_exit(g, p)), {}, "same"),
    "no entry": (lambda g, p: g.exit("stop", g.bar("close"), "<", 1.0), {}, "entry"),
    "no exit": (lambda g, p: g.entry(g.condition("above", g.bar("close"), ">", 1.0)), {}, "exit"),
    "unknown session": (simple_entry_exit, {"session": "MOON"}, "MOON"),
    "bool where an int is expected": (simple_entry_exit, {"extra_fields": [("period", int, True)]}, "period"),
    "None param": (simple_entry_exit, {"extra_fields": [("period", int, None)]}, "period"),
    "direction not long or short": (simple_entry_exit, {"direction": "sideways"}, "direction"),
    "calculation setup raises": (
        lambda g, p: (g.calc("fragile", "boom_on_setup", g.bar("close")), simple_entry_exit(g, p)), {}, "fragile"),
    "param type mismatch": (simple_entry_exit, {"extra_fields": [("period", int, "ten")]}, "period"),
    "unregistered market-data name": (
        lambda g, p: (g.bar("nonsense"), simple_entry_exit(g, p)), {}, "nonsense"),
    "non-bool rearm": (simple_entry_exit, {"extra_fields": [("rearm_required", bool, "yes")]}, "rearm_required"),
    "negative warm_up_months": (simple_entry_exit, {"extra_fields": [("warm_up_months", int, -1)]}, "warm_up_months"),
    "unsupported operator": (
        lambda g, p: (g.entry(g.condition("odd", g.bar("close"), "!=", 1.0)), g.exit("stop", g.bar("close"), "<", 1.0)),
        {}, "!="),
    "node named entry with rearm": (
        lambda g, p: (g.calc("entry", "sma", g.bar("close"), period=3), simple_entry_exit(g, p)),
        {"rearm_required": True}, "entry"),
}


@pytest.mark.parametrize("case", BAD_SIGNALS)
def test_scenario_bad_signal_refused(case):
    build, overrides, named = BAD_SIGNALS[case]
    with pytest.raises(SignalBuildError, match=named):
        make_signal(build, **overrides).from_config(strategy_config())


def test_scenario_warmup():
    def build(g, p):
        close = g.bar("close")
        sma5 = g.warmup(g.calc("sma5", "sma", close, period=5), history="intraday")
        g.entry(g.condition("above", close, ">", sma5))
        g.exit("stop", close, "<", 1.0)

    signal_class = make_signal(build)
    bars = [make_bar(i, 100.0 + i) for i in range(5)]
    short, enough = signal_class.from_config(strategy_config()), signal_class.from_config(strategy_config())
    assert short.warmup_requirements().intraday == ("sma5",)
    short.seed({"intraday": bars[:4]})
    enough.seed({"intraday": bars})
    assert (short.is_warm(), enough.is_warm()) == (False, True)


def test_scenario_three_valued_logic(caplog):
    def build_or(g, p):
        a = g.expr("a", lambda entry_price: entry_price, g.strategy_data("entry_price"))
        b = g.condition("b_true", g.bar("close"), ">", 0.0)
        g.entry(g.any_of("a_or_b", g.condition("a_cond", a, ">", 0.0), b))
        g.exit("stop", g.bar("close"), "<", -1.0)

    def build_and(g, p):
        a = g.expr("a", lambda entry_price: entry_price, g.strategy_data("entry_price"))
        g.entry(g.condition("a_cond", a, ">", 0.0), g.condition("c_false", g.bar("close"), "<", 0.0))
        g.exit("stop", g.bar("close"), "<", -1.0)

    def build_null_alone(g, p):
        a = g.expr("a", lambda entry_price: entry_price, g.strategy_data("entry_price"))
        g.entry(g.condition("a_cond", a, ">", 0.0))
        g.exit("stop", g.bar("close"), "<", -1.0)

    results = {}
    with caplog.at_level(logging.WARNING):
        for name, build in {"or": build_or, "and": build_and, "alone": build_null_alone}.items():
            stepper = Stepper(make_signal(build, rearm_required=True).from_config(strategy_config()))
            stepper.step(5.0)
            results[name] = stepper.signal.evaluate_entry()
        stepper.step(5.0)
    assert results == {"or": True, "and": False, "alone": False}
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert [(r.event, "entry_price" in r.missing_refs) for r in warnings] == [("entry_null", True)]


def test_scenario_failed_step():
    calls = []

    @register_calc("flaky")
    class Flaky(Calculation):
        def step(self, close):
            calls.append(close)
            if len(calls) == 3:
                raise RuntimeError("boom")
            return close * 2.0

    def build(g, p):
        close = g.bar("close")
        flaky = g.calc("flaky", "flaky", close)
        g.entry(g.condition("above", flaky, ">", 1.0))
        g.exit("stop", close, "<", 1.0)

    stepper = Stepper(make_signal(build).from_config(strategy_config()))
    seen = []
    for close in (1.0, 2.0, 3.0, 4.0):
        stepper.step(close)
        seen.append((stepper.signal.values()["flaky"], stepper.signal.level("flaky")))
    assert seen == [(2.0, 2.0), (4.0, 4.0), (None, None), (8.0, 8.0)]


def level_signal(rearm: bool, level: float = 100.0):
    def build(g, p):
        close = g.bar("close")
        g.entry(g.condition("above", close, ">", level))
        g.exit("stop", close, "<", 1.0)

    return make_signal(build, rearm_required=rearm).from_config(strategy_config())


@pytest.mark.parametrize("rearm,expected", [(False, [False, True, True, False, True]),
                                            (True, [False, True, False, False, True])])
def test_scenario_rearm_edge(rearm, expected):
    stepper = Stepper(level_signal(rearm))
    answers = []
    for close in (99.0, 101.0, 102.0, 99.0, 101.0):
        stepper.step(close)
        answers.append(stepper.signal.evaluate_entry())
    assert answers == expected


def test_scenario_exits_and_levels():
    def build(g, p):
        close = g.bar("close")
        stop_px = g.expr("stop_px", lambda avg: avg - 2.0, g.calc("avg", "sma", close, period=2))
        g.entry(g.condition("above", close, ">", 1000.0))
        g.exit("stop", close, "<", stop_px)
        g.exit("target", close, ">=", 110.0)

    stepper = Stepper(make_signal(build).from_config(strategy_config()))
    signal = stepper.signal
    seen = []
    for close in (100.0, 100.0, 90.0, 300.0, 111.0):
        stepper.step(close)
        seen.append((signal.evaluate_exit_conditions(), signal.evaluate_exit()))
    assert [answers for answers, _ in seen] == [
        {"stop": False, "target": False}, {"stop": False, "target": False},
        {"stop": True, "target": False}, {"stop": False, "target": True}, {"stop": True, "target": True}]
    assert [overall for _, overall in seen] == [False, False, True, True, True]
    assert list(seen[0][0]) == ["stop", "target"] == signal.exit_condition_names()
    assert signal.exit_condition_level("stop") == signal.level("stop_px") == pytest.approx(203.5)
    assert signal.exit_condition_level("target") is None


def freeze_signal():
    def build(g, p):
        close = g.bar("close")
        sma = g.calc("sma3", "sma", close, period=3)
        stop_px = g.freeze_at_entry(g.expr("stop_px", lambda avg: avg - 2.0, sma))
        g.entry(g.condition("above", close, ">", 1000.0))
        g.exit("stop", close, "<", stop_px)

    return Stepper(make_signal(build).from_config(strategy_config()))


def test_scenario_freeze_release():
    stepper = freeze_signal()
    signal = stepper.signal
    for close in (10.0, 20.0, 30.0):
        stepper.step(close)
    entry_stop = signal.values()["stop_px"]
    signal.freeze_exit_inputs(stepper.bars[-1])
    frozen = []
    for close in (40.0, 50.0, 60.0):
        stepper.step(close)
        frozen.append((signal.values()["stop_px"], signal.values()["close"]))
    signal.release_exit_inputs()
    released = []
    for close in (70.0, 80.0):
        stepper.step(close)
        released.append(signal.values()["stop_px"])
    assert frozen == [(entry_stop, 40.0), (entry_stop, 50.0), (entry_stop, 60.0)]
    assert released == [pytest.approx((50 + 60 + 70) / 3 - 2.0), pytest.approx((60 + 70 + 80) / 3 - 2.0)]


def test_scenario_cold_rebuild():
    def run(signal_class):
        stepper = Stepper(signal_class.from_config(strategy_config()))
        stepper.signal.seed({"intraday": [make_bar(-i, 98.0) for i in range(5)]})
        for number, close in enumerate((99.0, 101.0, 102.0, 103.0), start=1):
            stepper.step(close)
            if number == 2:
                stepper.signal.freeze_exit_inputs(stepper.bars[-1])
        return stepper

    def build(g, p):
        close = g.bar("close")
        sma = g.warmup(g.calc("sma5", "sma", close, period=5), history="intraday")
        stop_px = g.freeze_at_entry(g.expr("stop_px", lambda avg: avg - 0.5, sma))
        g.entry(g.condition("above", close, ">", 100.0))
        g.exit("stop", close, "<", stop_px)

    signal_class = make_signal(build, rearm_required=True)
    first = run(signal_class)
    first_values = first.signal.values()
    assert first.signal.evaluate_entry() is False

    rebuilt = signal_class.from_config(strategy_config())
    assert rebuilt.values() == {} and not rebuilt.is_warm()
    second = run(signal_class)
    assert second.signal.values() == first_values
    fresh = Stepper(signal_class.from_config(strategy_config()))
    fresh.signal.seed({"intraday": [make_bar(-i, 98.0) for i in range(5)]})
    fresh.step(99.0)
    fresh.step(101.0)
    assert fresh.signal.evaluate_entry() is True
