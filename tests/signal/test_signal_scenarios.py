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
            stepper.step(5.0)
            results[name] = stepper.signal.evaluate_entry()
    assert results == {"or": True, "and": False, "alone": False}
    warnings = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1 and "entry_price" in warnings[0]


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
