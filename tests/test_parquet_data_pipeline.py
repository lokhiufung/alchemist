import polars as pl
import pytest
import pytz

from datetime import datetime
from unittest.mock import MagicMock

from alchemist.data_pipelines.parquet_data_pipeline import ParquetDataPipeline
from alchemist.products.base_product import BaseProduct


def test_select_rows_by_date_range_filters_inclusive_bounds():
    """
    Ensure start/end date strings trim the dataframe to the expected window.
    """
    df = pl.DataFrame(
        {
            "ts": [
                datetime(2024, 1, 1),
                datetime(2024, 1, 2),
                datetime(2024, 1, 3),
            ],
            "open": [1.0, 2.0, 3.0],
            "high": [1.5, 2.5, 3.5],
            "low": [0.5, 1.5, 2.5],
            "close": [1.1, 2.1, 3.1],
            "volume": [10, 20, 30],
        }
    )
    pipeline = ParquetDataPipeline(file_path="dummy")

    filtered = pipeline.select_rows_by_date_range(df, start="2024-01-02", end="2024-01-02")

    assert filtered.height == 1
    assert filtered["ts"][0] == datetime(2024, 1, 2)


def test_adjust_timezone_converts_to_epoch_seconds():
    """
    Naive timestamps should be assumed US/Eastern and converted to epoch seconds.
    """
    naive_ts = datetime(2024, 1, 1, 12, 0, 0)
    df = pl.DataFrame(
        {
            "ts": [naive_ts],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
        }
    )
    pipeline = ParquetDataPipeline(file_path="dummy")

    adjusted = pipeline.adjust_timezone(df)

    expected_ts = int(pytz.timezone("US/Eastern").localize(naive_ts).timestamp())
    assert adjusted["ts"].dtype == pl.Int64
    assert adjusted["ts"][0] == expected_ts


def test_sanity_check_required_columns_raises_for_missing_column():
    pipeline = ParquetDataPipeline(file_path="dummy")
    df_missing_close = pl.DataFrame(
        {
            "ts": [datetime(2024, 1, 1)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "volume": [1000],
        }
    )

    with pytest.raises(ValueError, match="Missing required column: close"):
        pipeline.sanity_check_required_columns(df_missing_close)


def test_load_and_process_data_returns_empty_after_date_filter(tmp_path, monkeypatch):
    file_path = tmp_path / "bars.parquet"
    df = pl.DataFrame(
        {
            "ts": [datetime(2024, 1, 1, 0, 0)],
            "open": [100.0],
            "high": [101.0],
            "low": [99.0],
            "close": [100.5],
            "volume": [1000],
        }
    )
    df.write_parquet(file_path)
    pipeline = ParquetDataPipeline(file_path=str(file_path))
    monkeypatch.setattr(pipeline, "sanity_check_missing_values", lambda df: None)

    result = pipeline.load_and_process_data(start_date="2025-01-01", end_date="2025-01-02")

    assert result.is_empty()


def test_historical_bars_returns_standardized_bar_dicts(monkeypatch):
    pipeline = ParquetDataPipeline(file_path="unused")
    product = BaseProduct(name="ES", base_currency="USD", exch="CME")
    sample_df = pl.DataFrame(
        {
            "ts": [datetime.fromtimestamp(1700000000), datetime.fromtimestamp(1700000060)],
            "open": [100.0, 101.0],
            "high": [101.0, 102.0],
            "low": [99.0, 100.0],
            "close": [100.5, 101.5],
            "volume": [10, 20],
        }
    )
    pipeline.load_and_process_data = MagicMock(return_value=sample_df)

    updates = pipeline.historical_bars(
        product=product,
        freq="1m",
        start_date="2024-01-01",
        end_date="2024-01-02",
    )

    assert len(updates) == 2
    assert updates[0]["ts"] == 1700000000
    assert updates[0]["resolution"] == "1m"
    assert updates[0]["data"]["open"] == 100.0
    assert updates[1]["data"]["close"] == 101.5
    pipeline.load_and_process_data.assert_called_once_with(
        start_date="2024-01-01",
        end_date="2024-01-02",
        auto_resample_freqs=None,
    )


def test_create_resampled_cols_adds_frequency_columns():
    """
    Verify resampled OHLCV columns are appended with correct aggregated values.
    """
    df = pl.DataFrame(
        {
            "ts": [
                datetime(2024, 1, 1, 0, 0),
                datetime(2024, 1, 1, 0, 1),
                datetime(2024, 1, 1, 0, 2),
                datetime(2024, 1, 1, 0, 3),
            ],
            "open": [1.0, 2.0, 3.0, 4.0],
            "high": [2.0, 3.0, 4.0, 5.0],
            "low": [0.0, 1.0, 2.0, 3.0],
            "close": [1.5, 2.5, 3.5, 4.5],
            "volume": [10, 20, 30, 40],
        }
    )
    pipeline = ParquetDataPipeline(file_path="dummy")

    resampled = pipeline.create_resampled_cols(df, freqs=["2m"])

    expected_by_ts = {
        datetime(2024, 1, 1, 0, 0): {"open_2m": 1.0, "high_2m": 3.0, "low_2m": 0.0, "close_2m": 2.5, "volume_2m": 30},
        datetime(2024, 1, 1, 0, 1): {"open_2m": 1.0, "high_2m": 3.0, "low_2m": 0.0, "close_2m": 2.5, "volume_2m": 30},
        datetime(2024, 1, 1, 0, 2): {"open_2m": 3.0, "high_2m": 5.0, "low_2m": 2.0, "close_2m": 4.5, "volume_2m": 70},
        datetime(2024, 1, 1, 0, 3): {"open_2m": 3.0, "high_2m": 5.0, "low_2m": 2.0, "close_2m": 4.5, "volume_2m": 70},
    }

    for row in resampled.iter_rows(named=True):
        agg = expected_by_ts[row["ts"]]
        assert row["open_2m"] == agg["open_2m"]
        assert row["high_2m"] == agg["high_2m"]
        assert row["low_2m"] == agg["low_2m"]
        assert row["close_2m"] == agg["close_2m"]
        assert row["volume_2m"] == agg["volume_2m"]
