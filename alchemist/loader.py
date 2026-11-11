from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

from alchemist.constants.runtime import STRATEGY_FILES


class StrategyLoadError(Exception):
    pass


def load_strategies(root: Path = Path("local/strategies"), exclude: frozenset[str] | set[str] = frozenset()) -> list[str]:
    loaded = []
    for folder in sorted(p for p in Path(root).iterdir() if p.is_dir() and not p.name.startswith(("_", ".")) and p.name not in exclude):
        for file_name in STRATEGY_FILES:
            path = folder / file_name
            if path.exists():
                import_file(f"alchemist_local.{folder.name}.{path.stem}", path, f"{folder.name}/{file_name}")
        loaded.append(folder.name)
    return loaded


def import_file(module_name: str, path: Path, label: str) -> None:
    try:
        spec = importlib.util.spec_from_file_location(module_name, path)
        if spec is None or spec.loader is None:
            raise ImportError(f"cannot import {path}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        spec.loader.exec_module(module)
    except Exception as exc:
        sys.modules.pop(module_name, None)
        raise StrategyLoadError(f"{label}: {exc}") from exc
