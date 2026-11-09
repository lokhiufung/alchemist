import pytest

from alchemist.calculations import Calculation, SessionFixedCalculation
from alchemist.registry import register_aux_name, register_calc
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
    register_aux_name("eurex_only_series", supplied_sessions=("FRA_DAY",))

    def build(g, p):
        close = g.bar("close")
        g.bar("eurex_only_series")
        entry_exit(g, close)

    with pytest.raises(SignalBuildError, match="eurex_only_series"):
        make_signal(build).from_config(strategy_config())
    assert make_signal(build).from_config(strategy_config(FDAX)) is not None


def test_registered_aux_name_reads_the_annotated_bar_key():
    register_aux_name("vix_close")

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


def run_closes(signal, closes, strategy_data=None):
    stepper = Stepper(signal)
    answers = []
    for close in closes:
        stepper.step(close, strategy_data)
        answers.append(signal.evaluate_entry())
    return answers


@pytest.mark.parametrize("rearm,entries", [(True, 1), (False, 5)])
def test_sustained_entry_run_entries(rearm, entries):
    def build(g, p):
        entry_exit(g, g.bar("close"))

    signal = make_signal(build, rearm_required=rearm).from_config(strategy_config())
    assert sum(run_closes(signal, [101.0] * 5)) == entries


def test_rearm_edge_resets_on_a_new_trading_date():
    def build(g, p):
        entry_exit(g, g.bar("close"))

    stepper = Stepper(make_signal(build, rearm_required=True).from_config(strategy_config()))
    stepper.step(101.0)
    first_day = stepper.signal.evaluate_entry()
    stepper.step(101.0)
    same_day = stepper.signal.evaluate_entry()
    stepper.index = 86_400
    stepper.step(101.0)
    assert (first_day, same_day, stepper.signal.evaluate_entry()) == (True, False, True)


def test_rearm_gate_closed_until_warm():
    def build(g, p):
        close = g.bar("close")
        g.warmup(g.calc("sma5", "sma", close, period=5), history="intraday")
        entry_exit(g, close)

    signal = make_signal(build, rearm_required=True).from_config(strategy_config())
    stepper = Stepper(signal)
    stepper.step(101.0)
    cold = signal.evaluate_entry()
    signal.seed({"intraday": [make_bar(i, 100.0) for i in range(5)]})
    stepper.step(101.0)
    assert (cold, signal.evaluate_entry()) == (False, False)


@pytest.mark.parametrize("case,expected", [("or", True), ("and", False), ("alone", False)])
def test_null_leg_follows_kleene_rules(case, expected):
    def build(g, p):
        close = g.bar("close")
        unknown = g.condition("unknown", g.expr("a", lambda e: e, g.strategy_data("entry_price")), ">", 0.0)
        true_leg = g.condition("true_leg", close, ">", 0.0)
        false_leg = g.condition("false_leg", close, "<", 0.0)
        if case == "or":
            g.entry(g.any_of("either", unknown, true_leg))
        elif case == "and":
            g.entry(unknown, false_leg)
        else:
            g.entry(unknown)
        g.exit("stop", close, "<", -1.0)

    assert run_closes(make_signal(build).from_config(strategy_config()), [5.0]) == [expected]


def test_evaluate_twice_same_answers():
    def build(g, p):
        entry_exit(g, g.bar("close"))

    signal = make_signal(build, rearm_required=True).from_config(strategy_config())
    Stepper(signal).step(101.0)
    first = (signal.evaluate_entry(), signal.evaluate_exit_conditions(), signal.entry_conditions())
    second = (signal.evaluate_entry(), signal.evaluate_exit_conditions(), signal.entry_conditions())
    assert first == second and first[0] is True


def test_exit_is_or_of_named_conditions():
    def build(g, p):
        close = g.bar("close")
        g.entry(g.condition("above", close, ">", 1000.0))
        g.exit("low", close, "<", 10.0)
        g.exit("high", close, ">", 90.0)
        g.exit("middle", close, "==", 50.0)

    signal = make_signal(build).from_config(strategy_config())
    stepper = Stepper(signal)
    for close, expected in ((50.0, True), (95.0, True), (60.0, False), (5.0, True)):
        stepper.step(close)
        answers = signal.evaluate_exit_conditions()
        assert list(answers) == ["low", "high", "middle"]
        assert signal.evaluate_exit() is expected is any(answers.values())


def test_condition_names_listed_before_first_bar():
    def build(g, p):
        close = g.bar("close")
        first = g.condition("first", close, ">", 1.0)
        second = g.condition("second", close, ">", 2.0)
        g.entry(g.any_of("either", first, second))
        g.exit("stop", close, "<", 0.0)

    signal = make_signal(build).from_config(strategy_config())
    assert signal.condition_names() >= {"first", "second", "either"}
    assert signal.exit_condition_names() == ["stop"]
    assert signal.entry_conditions() == {"either": None}


def test_values_returns_a_copy():
    def build(g, p):
        entry_exit(g, g.bar("close"))

    signal = make_signal(build).from_config(strategy_config())
    Stepper(signal).step(101.0)
    copy = signal.values()
    copy["close"] = -1.0
    assert signal.values()["close"] == 101.0


def freeze_fixture(late_series_at_entry):
    register_aux_name("late_series")

    def build(g, p):
        close = g.bar("close")
        late = g.bar("late_series")
        g.freeze_at_entry(g.expr("stop_px", lambda value: value - 1.0, late))
        g.entry(g.condition("above", close, ">", 1000.0))
        g.exit("stop", close, "<", 1.0)

    return Stepper(make_signal(build).from_config(strategy_config()))


def test_input_missing_at_entry_stays_unavailable():
    stepper = freeze_fixture(None)
    stepper.step(100.0)
    stepper.signal.freeze_exit_inputs(stepper.bars[-1])
    stepper.step(100.0, aux_late_series=50.0)
    held = stepper.signal.values()["stop_px"]
    stepper.signal.release_exit_inputs()
    stepper.step(100.0, aux_late_series=50.0)
    assert (held, stepper.signal.values()["stop_px"]) == (None, 49.0)


def test_release_returns_exit_inputs_to_live():
    stepper = freeze_fixture(40.0)
    stepper.step(100.0, aux_late_series=40.0)
    stepper.signal.freeze_exit_inputs(stepper.bars[-1])
    stepper.step(100.0, aux_late_series=60.0)
    frozen = stepper.signal.values()["stop_px"]
    stepper.signal.release_exit_inputs()
    stepper.step(100.0, aux_late_series=60.0)
    assert (frozen, stepper.signal.values()["stop_px"]) == (39.0, 59.0)


def test_signal_without_exit_side_nodes_ignores_freeze():
    def build(g, p):
        entry_exit(g, g.bar("close"))

    stepper = Stepper(make_signal(build).from_config(strategy_config()))
    stepper.step(101.0)
    stepper.signal.freeze_exit_inputs(stepper.bars[-1])
    stepper.step(95.0)
    assert stepper.signal.values()["close"] == 95.0
    stepper.signal.release_exit_inputs()


def test_strategy_data_stays_live_while_frozen():
    def build(g, p):
        close = g.bar("close")
        g.expr("target_px", lambda entry, atr: entry + atr, g.strategy_data("entry_price"),
               g.expr("atr_like", lambda c: c * 0.1, close))
        g.entry(g.condition("above", close, ">", 1000.0))
        g.exit("stop", close, "<", 1.0)

    stepper = Stepper(make_signal(build).from_config(strategy_config()))
    stepper.step(100.0, {"entry_price": 100.0})
    stepper.signal.freeze_exit_inputs(stepper.bars[-1])
    stepper.step(200.0, {"entry_price": 150.0})
    assert stepper.signal.values()["target_px"] == 160.0


def test_each_freeze_replaces_the_previous_one():
    stepper = freeze_fixture(1.0)
    stepper.step(100.0, aux_late_series=10.0)
    stepper.signal.freeze_exit_inputs(stepper.bars[-1])
    stepper.step(100.0, aux_late_series=20.0)
    stepper.signal.freeze_exit_inputs(stepper.bars[-1])
    stepper.step(100.0, aux_late_series=30.0)
    assert stepper.signal.values()["stop_px"] == 19.0


def test_rebuild_and_replay_equals_fresh_process():
    def build(g, p):
        close = g.bar("close")
        sma = g.calc("sma3", "sma", close, period=3)
        g.entry(g.condition("above", close, ">", sma))
        g.exit("stop", close, "<", 1.0)

    signal_class = make_signal(build, rearm_required=True)
    closes = [10.0, 11.0, 12.0, 11.0, 13.0]
    first, second = Stepper(signal_class.from_config(strategy_config())), None
    for close in closes:
        first.step(close)
    second = Stepper(signal_class.from_config(strategy_config()))
    for close in closes:
        second.step(close)
    assert first.signal.values() == second.signal.values()


def test_aux_names_lists_the_series_the_signal_reads():
    register_aux_name("vix_close")

    def with_aux(g, p):
        close = g.bar("close")
        g.entry(g.condition("calm", g.bar("vix_close"), "<", 20.0))
        g.exit("stop", close, "<", 90.0)

    def without_aux(g, p):
        entry_exit(g, g.bar("close"))

    assert make_signal(with_aux).from_config(strategy_config()).aux_names() == frozenset({"vix_close"})
    assert make_signal(without_aux).from_config(strategy_config()).aux_names() == frozenset()


from test_signal_scenarios import BAD_SIGNALS


@pytest.mark.parametrize("reason", sorted(BAD_SIGNALS))
def test_bad_signal_refused_and_leaves_no_state(reason):
    from alchemist import registry
    before = {scope: dict(table) for scope, table in registry._calcs.items()}
    build, overrides, named = BAD_SIGNALS[reason]
    with pytest.raises(SignalBuildError, match=named):
        make_signal(build, **overrides).from_config(strategy_config())
    assert {scope: dict(table) for scope, table in registry._calcs.items()} == before


@pytest.mark.parametrize("case", ["setup_raises_on_a_param", "session_calculation_unknown_session", "params_fit"])
def test_calculation_setup_error_refuses_build_and_names_node(case):
    def build(g, p):
        close = g.bar("close")
        if case == "setup_raises_on_a_param":
            g.calc("slow_avg", "sma", close, period=0)
        elif case == "session_calculation_unknown_session":
            g.calc("left", "session_bars_left", session="MOON")
        else:
            g.calc("left", "session_bars_left", session=p.session)
        entry_exit(g, close)

    if case == "params_fit":
        assert make_signal(build).from_config(strategy_config()) is not None
        return
    node = "slow_avg" if case == "setup_raises_on_a_param" else "left"
    with pytest.raises(SignalBuildError, match=node):
        make_signal(build).from_config(strategy_config())
