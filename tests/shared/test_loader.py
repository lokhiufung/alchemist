import pytest

from alchemist.loader import StrategyLoadError, load_strategies
from alchemist.registry import get_strategy


def make_folder(root, name, strategy_source):
    folder = root / name
    folder.mkdir(parents=True)
    (folder / "strategy.py").write_text(strategy_source)
    return folder


def test_broken_folder_raises_strategy_load_error(tmp_path):
    make_folder(tmp_path, "broken_one", "def (\n")
    with pytest.raises(StrategyLoadError) as error:
        load_strategies(tmp_path)
    assert "broken_one" in str(error.value) and "strategy.py" in str(error.value)


def test_good_folder_registers_and_is_listed(tmp_path):
    make_folder(tmp_path, "good_one",
                "from alchemist.registry import register_strategy\n@register_strategy('good_one')\nclass G: pass\n")
    assert load_strategies(tmp_path) == ["good_one"]
    assert get_strategy("good_one").__name__ == "G"


def test_import_error_inside_folder_is_wrapped(tmp_path):
    make_folder(tmp_path, "needs_missing", "import module_that_does_not_exist\n")
    with pytest.raises(StrategyLoadError, match="needs_missing/strategy.py"):
        load_strategies(tmp_path)
