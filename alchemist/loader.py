from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

STRATEGY_FILES = ("calculations.py", "strategy_signal.py", "strategy.py")


def load_strategies(root: Path = Path("local/strategies")) -> list[str]:
    """System map decision 43: import every strategy folder by file path."""
    loaded = []
    for folder in sorted(p for p in root.iterdir() if p.is_dir() and not p.name.startswith(("_", "."))):
        for file_name in STRATEGY_FILES:
            path = folder / file_name
            if path.exists():
                _import_file(f"alchemist_local.{folder.name}.{path.stem}", path)
        loaded.append(folder.name)
    return loaded


def _import_file(module_name: str, path: Path) -> None:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot import {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
