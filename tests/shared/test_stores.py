import pytest

from alchemist.stores import FileDailyStore, InMemoryDailyStore, round_trip_count


@pytest.fixture(params=["file", "memory"])
def store(request, tmp_path):
    return FileDailyStore(tmp_path, "bot", "calc_state") if request.param == "file" else InMemoryDailyStore()


def test_saved_state_loads_on_the_same_date(store):
    store.save("s1", "2026-04-15", {"atr": 20.0})
    assert store.load("s1", "2026-04-15") == {"atr": 20.0}


def test_missing_state_is_none(store):
    assert store.load("s1", "2026-04-15") is None


def test_stale_date_is_none(store):
    store.save("s1", "2026-04-14", {"atr": 20.0})
    assert store.load("s1", "2026-04-15") is None


def test_round_trip_count_defaults_to_zero_and_resets_on_a_new_date(store):
    store.save("s1", "2026-04-15", {"count": 2})
    assert (round_trip_count(store, "s1", "2026-04-15"), round_trip_count(store, "s1", "2026-04-16")) == (2, 0)


def test_file_store_survives_reload(tmp_path):
    FileDailyStore(tmp_path, "bot", "round_trips").save("s1", "2026-04-15", {"count": 2})
    assert round_trip_count(FileDailyStore(tmp_path, "bot", "round_trips"), "s1", "2026-04-15") == 2


def test_file_stores_with_different_names_do_not_collide(tmp_path):
    FileDailyStore(tmp_path, "bot", "calc_state").save("s1", "2026-04-15", {"atr": 1.0})
    assert FileDailyStore(tmp_path, "bot", "round_trips").load("s1", "2026-04-15") is None


def test_corrupt_file_is_none(tmp_path):
    store = FileDailyStore(tmp_path, "bot", "calc_state")
    store.save("s1", "2026-04-15", {"atr": 20.0})
    store.path("s1").write_text("{not json")
    assert store.load("s1", "2026-04-15") is None
