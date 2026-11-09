import pytest

from strategy_helpers import StubSignal, install_stub_signal


@pytest.fixture(autouse=True)
def stub_signal():
    StubSignal.builds = 0
    StubSignal.warm_after_seed = True
    StubSignal.session_state = {}
    install_stub_signal()
    yield
