import pytest

from alchemist.calculations import Calculation, SessionFixedCalculation
from alchemist.registry import register_aux_source, register_calc
from alchemist.strategy_builder.signal_dag import SignalBuildError
from alchemist.instruments import FDAX

from signal_helpers import Stepper, make_bar, make_signal, strategy_config


def entry_exit(g, close):
    g.entry(g.condition("above", close, ">", 100.0))
    g.exit("stop", close, "<", 90.0)


def test_sma_setup_error_names_node_and_reason():
    def build(g, p):
        close = g.bar("close")
        g.calc("slow_avg", "sma", close, period=0)
        entry_exit(g, close)

    with pytest.raises(SignalBuildError, match="slow_avg.*period"):
        make_signal(build).from_config(strategy_config())


def test_session_calculation_given_unknown_session_names_node():
    def build(g, p):
        close = g.bar("close")
        g.calc("left", "session_bars_left", session="MOON")
        entry_exit(g, close)

    with pytest.raises(SignalBuildError, match="left.*MOON"):
        make_signal(build).from_config(strategy_config())


def test_params_that_fit_build():
    def build(g, p):
        close = g.bar("close")
        g.calc("left", "session_bars_left", session=p.session)
        entry_exit(g, close)

    assert make_signal(build).from_config(strategy_config()) is not None


def test_refused_signal_leaves_no_state_for_next_build():
    from alchemist import registry
    before = {scope: dict(table) for scope, table in registry._calcs.items()}

    def bad(g, p):
        g.calc("x", "sma", g.bar("close"), period=0)

    def good(g, p):
        entry_exit(g, g.bar("close"))

    with pytest.raises(SignalBuildError):
        make_signal(bad).from_config(strategy_config())
    signal = make_signal(good).from_config(strategy_config())
    assert signal.values() == {}
    assert {scope: dict(table) for scope, table in registry._calcs.items()} == before


def test_market_data_the_session_cannot_supply_is_refused():
    @register_aux_source("eurex_only_series")
    class EurexOnly:
        supplied_sessions = ("FRA_DAY",)

    def build(g, p):
        close = g.bar("close")
        g.bar("eurex_only_series")
        entry_exit(g, close)

    with pytest.raises(SignalBuildError, match="eurex_only_series"):
        make_signal(build).from_config(strategy_config())
    assert make_signal(build).from_config(strategy_config(FDAX)) is not None


def test_registered_aux_name_reads_the_annotated_bar_key():
    @register_aux_source("vix_close")
    class Vix:
        supplied_sessions = None

    def build(g, p):
        close = g.bar("close")
        vix = g.bar("vix_close")
        g.entry(g.condition("calm", vix, "<", 20.0))
        g.exit("stop", close, "<", 90.0)

    stepper = Stepper(make_signal(build).from_config(strategy_config()))
    stepper.step(100.0, aux_vix_close=15.0)
    assert stepper.signal.entry_conditions() == {"calm": True}


def test_rebinding_a_registry_name_to_another_class_raises():
    @register_calc("twice")
    class First(Calculation): ...

    with pytest.raises(ValueError, match="twice"):
        register_calc("twice")(type("Second", (Calculation,), {}))


def test_folder_calc_shadows_builtin_and_folders_do_not_collide(tmp_path):
    @register_calc("sma")
    class ShadowSma(Calculation):
        def step(self, close):
            return -1.0

        def setup(self, period):
            pass

    def build(g, p):
        close = g.bar("close")
        g.calc("avg", "sma", close, period=3)
        entry_exit(g, close)

    stepper = Stepper(make_signal(build).from_config(strategy_config()))
    stepper.step(100.0)
    assert stepper.signal.values()["avg"] == -1.0


def test_session_label_resolved_from_exchange_profile():
    def build(g, p):
        entry_exit(g, g.bar("close"))

    assert make_signal(build).from_config(strategy_config()).session == "NY_RTH"
    assert make_signal(build).from_config(strategy_config(FDAX)).session == "FRA_DAY"


def test_unavailable_value_not_carried_from_previous_bar():
    def build(g, p):
        close = g.bar("close")
        g.expr("target", lambda entry: entry + 1.0, g.strategy_data("entry_price"))
        entry_exit(g, close)

    stepper = Stepper(make_signal(build).from_config(strategy_config()))
    stepper.step(100.0, {"entry_price": 100.0})
    assert stepper.signal.values()["target"] == 101.0
    stepper.step(100.0, {})
    assert stepper.signal.values()["target"] is None


def test_failed_step_is_null_and_level_is_none():
    def build(g, p):
        close = g.bar("close")
        g.expr("ratio", lambda x: 1.0 / x, close)
        entry_exit(g, close)

    stepper = Stepper(make_signal(build).from_config(strategy_config()))
    stepper.step(4.0)
    assert stepper.signal.level("ratio") == 0.25
    stepper.step(0.0)
    assert stepper.signal.values()["ratio"] is None and stepper.signal.level("ratio") is None


def test_dependants_of_a_failed_step_are_unavailable():
    def build(g, p):
        close = g.bar("close")
        ratio = g.expr("ratio", lambda x: 1.0 / x, close)
        g.expr("doubled", lambda r: r * 2, ratio)
        entry_exit(g, close)

    stepper = Stepper(make_signal(build).from_config(strategy_config()))
    stepper.step(0.0)
    assert stepper.signal.values()["doubled"] is None


@pytest.mark.parametrize("history,warm", [(0, False), (4, False), (5, True)])
def test_warm_only_with_enough_history(history, warm):
    def build(g, p):
        close = g.bar("close")
        g.warmup(g.calc("sma5", "sma", close, period=5), history="intraday")
        entry_exit(g, close)

    signal = make_signal(build).from_config(strategy_config())
    signal.seed({"intraday": [make_bar(i, 100.0) for i in range(history)]})
    assert signal.is_warm() is warm


def test_signal_without_warmup_nodes_is_warm():
    def build(g, p):
        entry_exit(g, g.bar("close"))

    assert make_signal(build).from_config(strategy_config()).is_warm() is True


def test_daily_warmup_reports_months_and_receives_daily_bars():
    @register_calc("daily_range")
    class DailyRange(SessionFixedCalculation):
        def setup(self, months):
            self.months, self.range = months, None

        def daily_months(self):
            return self.months

        def warmup(self, daily_bars):
            self.range = daily_bars[-1]["high"] - daily_bars[-1]["low"] if daily_bars else None

        def value(self):
            return self.range

        def snapshot_state(self):
            return None if self.range is None else {"range": self.range}

        def restore_state(self, state):
            self.range = state["range"]

    def build(g, p):
        close = g.bar("close")
        g.warmup(g.calc("rng", "daily_range", months=3), history="daily")
        entry_exit(g, close)

    signal = make_signal(build, warm_up_months=2).from_config(strategy_config())
    needs = signal.warmup_requirements()
    assert (needs.daily_months, needs.daily, needs.session_fixed) == (3, ("rng",), ("rng",))
    signal.seed({"daily": [make_bar(0, 100.0, high=110.0, low=90.0)]})
    assert signal.values() == {} and signal.is_warm()


def test_restore_session_state_puts_value_back():
    @register_calc("held")
    class Held(SessionFixedCalculation):
        def setup(self):
            self.stored = None

        def value(self):
            return self.stored

        def snapshot_state(self):
            return None if self.stored is None else {"stored": self.stored}

        def restore_state(self, state):
            self.stored = state["stored"]

        def warmup(self, daily_bars):
            self.stored = 20.0

    def build(g, p):
        close = g.bar("close")
        g.warmup(g.calc("held", "held"), history="daily")
        entry_exit(g, close)

    first = make_signal(build).from_config(strategy_config())
    first.seed({"daily": [make_bar(0, 1.0)]})
    snapshot = first.snapshot_session_state()
    second = make_signal(build).from_config(strategy_config())
    assert not second.is_warm()
    second.restore_session_state(snapshot)
    assert second.is_warm() and second.values()["held"] == snapshot["held"]["stored"] == 20.0
