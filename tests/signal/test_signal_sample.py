import itertools
from pathlib import Path

import pytest

import alchemist.calculations.library  # noqa: F401
from alchemist.loader import import_file
from alchemist.registry import get_signal_dag

from signal_helpers import Stepper, make_bar, strategy_config

SAMPLE = Path(__file__).parent / "strategies" / "level_cross" / "strategy_signal.py"
SEED_CLOSES = [98.0] * 5
CLOSES = [99.0, 99.5, 101.0, 102.0, 103.0, 104.0, 103.0, 98.0, 99.0, 99.0]
ENTRY_BAR = 3
STOP_BAR = 8


def level_cross():
    import_file("level_cross_sample", SAMPLE, "level_cross/strategy_signal.py")
    return get_signal_dag("level_cross").from_config(strategy_config())


@pytest.mark.parametrize("rearm", [True])
def test_level_cross_then_stop_on_frozen_entry_level(rearm):
    signal = level_cross()
    assert signal.rearm_required is rearm
    assert not signal.is_warm()
    signal.seed({"intraday": [make_bar(-5 + i, close) for i, close in enumerate(SEED_CLOSES)]})
    assert signal.is_warm()

    stepper = Stepper(signal)
    entries, exits, targets, stop_px, stop_level = [], [], [], {}, {}
    for number, close in enumerate(CLOSES, start=1):
        stepper.step(close)
        entries.append(signal.evaluate_entry())
        answers = signal.evaluate_exit_conditions()
        exits.append(signal.evaluate_exit())
        targets.append(answers["target"])
        stop_px[number] = signal.values()["stop_px"]
        stop_level[number] = signal.exit_condition_level("stop")
        if number == ENTRY_BAR:
            signal.freeze_exit_inputs(stepper.bars[-1])

    assert [n for n, fired in enumerate(entries, start=1) if fired] == [ENTRY_BAR]
    assert [n for n, fired in enumerate(exits, start=1) if fired] == [STOP_BAR]
    assert not any(targets)
    frozen_value = stop_px[ENTRY_BAR]
    assert frozen_value == pytest.approx(98.6)
    for number in range(ENTRY_BAR + 1, STOP_BAR + 1):
        assert stop_px[number] == pytest.approx(frozen_value)
        assert stop_level[number] == pytest.approx(frozen_value)
