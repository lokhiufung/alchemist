from pathlib import Path

import pytest

from alchemist import registry
from alchemist.registry import get_calc, register_calc


def run_in_folder(folder: Path, source: str) -> None:
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "strategy_signal.py"
    exec(compile(source, str(path), "exec"), {"__file__": str(path), "__name__": f"m_{folder.name}"})


REGISTER_DAG_X = "from alchemist.registry import register_signal_dag\n@register_signal_dag('x')\nclass X: pass\n"
REGISTER_CALC_RSI = "from alchemist.registry import register_calc\n@register_calc('rsi')\nclass Rsi: pass\n"


def test_double_bind_names_both_files(tmp_path):
    run_in_folder(tmp_path / "one", REGISTER_DAG_X)
    with pytest.raises(ValueError) as error:
        run_in_folder(tmp_path / "two", REGISTER_DAG_X)
    assert str(tmp_path / "one") in str(error.value) and str(tmp_path / "two") in str(error.value)


def test_same_calc_name_in_two_folders_ok(tmp_path):
    run_in_folder(tmp_path / "one", REGISTER_CALC_RSI)
    run_in_folder(tmp_path / "two", REGISTER_CALC_RSI)
    assert get_calc("rsi", str(tmp_path / "one")) is not get_calc("rsi", str(tmp_path / "two"))


def test_get_calc_falls_back_to_builtin(tmp_path):
    builtin_file = Path(registry.__file__).parent / "calculations" / "library.py"
    exec(compile(REGISTER_CALC_RSI.replace("Rsi", "BuiltinRsi"), str(builtin_file), "exec"),
         {"__file__": str(builtin_file)})
    assert get_calc("rsi", str(tmp_path / "other")).__name__ == "BuiltinRsi"


def test_unknown_calc_names_scope(tmp_path):
    with pytest.raises(KeyError, match="nope"):
        get_calc("nope", str(tmp_path))

