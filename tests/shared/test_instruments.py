import pytest

from alchemist.instruments import ES, FDAX, FDXM, MES, MNQ, NQ, AssetClass, Instrument, instrument, register_instrument


def test_lookup_by_symbol():
    assert instrument("MES") is MES
    assert (FDAX.multiplier, FDXM.multiplier, FDAX.tick_size) == (25.0, 5.0, 1.0)
    assert FDAX.currency == "EUR" and all(i.currency == "USD" for i in (ES, NQ, MES, MNQ))


def test_unknown_symbol_lists_known():
    with pytest.raises(ValueError, match="MES"):
        instrument("NOPE")


def test_config_can_add_instrument():
    aapl = Instrument(AssetClass.STOCK, "AAPL", "NASDAQ", "USD", 1.0, 0.01)
    register_instrument(aapl)
    assert instrument("AAPL") is aapl
