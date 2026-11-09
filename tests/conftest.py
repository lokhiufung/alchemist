import pytest

from alchemist import registry

TABLES = ("_calcs", "_signal_dags", "_strategies", "_gateways", "_tapes", "_aux_sources", "_aux_names")


@pytest.fixture(autouse=True)
def isolated_registries():
    saved = {name: {k: dict(v) if name == "_calcs" else v for k, v in getattr(registry, name).items()}
             for name in TABLES}
    yield
    for name, table in saved.items():
        live = getattr(registry, name)
        live.clear()
        live.update(table)
