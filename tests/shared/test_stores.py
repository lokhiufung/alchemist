from alchemist.stores import CalcStateStore, RoundTripStore


def test_round_trip_store_survives_reload(tmp_path):
    RoundTripStore(tmp_path, "bot").record("s1", "2026-04-15")
    RoundTripStore(tmp_path, "bot").record("s1", "2026-04-15")
    assert RoundTripStore(tmp_path, "bot").count("s1", "2026-04-15") == 2


def test_round_trip_count_resets_on_a_new_date(tmp_path):
    store = RoundTripStore(tmp_path, "bot")
    store.record("s1", "2026-04-15")
    assert store.count("s1", "2026-04-16") == 0


def test_calc_state_round_trip(tmp_path):
    CalcStateStore(tmp_path, "bot").save("s1", "2026-04-15", {"atr": 20.0})
    assert CalcStateStore(tmp_path, "bot").load("s1", "2026-04-15") == {"atr": 20.0}


def test_calc_state_missing_is_none(tmp_path):
    assert CalcStateStore(tmp_path, "bot").load("s1", "2026-04-15") is None


def test_calc_state_stale_date_is_none(tmp_path):
    store = CalcStateStore(tmp_path, "bot")
    store.save("s1", "2026-04-14", {"atr": 20.0})
    assert store.load("s1", "2026-04-15") is None


def test_calc_state_corrupt_file_is_none(tmp_path):
    store = CalcStateStore(tmp_path, "bot")
    store.save("s1", "2026-04-15", {"atr": 20.0})
    next((tmp_path / "bot").glob("*")).write_text("{not json")
    assert store.load("s1", "2026-04-15") is None
