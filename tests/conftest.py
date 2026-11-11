import sys
from pathlib import Path

import pytest

from alchemist import registry

sys.path.extend(str(p) for p in Path(__file__).parent.iterdir() if p.is_dir() and str(p) not in sys.path)

TABLES = ("_calcs", "_signal_dags", "_strategies", "_tapes")


@pytest.fixture(autouse=True)
def isolated_registries():
    saved = {name: {k: dict(v) if name == "_calcs" else v for k, v in getattr(registry, name).items()}
             for name in TABLES}
    yield
    for name, table in saved.items():
        live = getattr(registry, name)
        live.clear()
        live.update(table)
