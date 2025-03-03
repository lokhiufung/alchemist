import pytest
from datetime import datetime, timedelta
from alchemist.datas.bar_data import BarData
from alchemist.datas.common import Bar


@pytest.fixture
def bar_data():
    """
    Fixture to initialize a BarData instance with max_len=100 and frequency='1m'.
    Returns:
        BarData: An instance of the BarData class for use in tests.
    """
    return BarData(max_len=100, freq="1m")


def test_on_bar_update(bar_data, mocker):
    """
    Test the on_bar_update method to ensure it correctly creates a new bar, appends
    it to the data list, and triggers necessary steps (e.g., push and _step).

    Steps:
        1. Mock the _step and push methods to isolate their behavior.
        2. Call on_bar_update with sample parameters.
        3. Validate that a new bar is created and appended to the data list.
        4. Confirm that _step and push methods are called exactly once.
    """
    ts = datetime(2024, 1, 1, 12, 0)
    mock_step = mocker.patch.object(bar_data, "_step")  # Mock the `_step` method
    mock_push = mocker.patch.object(bar_data, "push")  # Mock the `push` method

    bar_data.on_bar_update(ts, 100.0, 105.0, 95.0, 102.0, 1000)

    # Validate the bar is created and appended
    assert len(bar_data.data) == 1
    created_bar = bar_data.data[0]
    assert isinstance(created_bar, Bar)
    assert created_bar.ts == ts
    assert created_bar.open == 100.0
    assert created_bar.high == 105.0
    assert created_bar.low == 95.0
    assert created_bar.close == 102.0
    assert created_bar.volume == 1000

    # Validate that _step and push methods are called
    mock_step.assert_called_once()
    mock_push.assert_called_once()


def test_multiple_on_bar_updates(bar_data, mocker):
    """
    Test the on_bar_update method with multiple updates to ensure that
    multiple bars are created and appended correctly.

    Steps:
        1. Mock the _step and push methods.
        2. Call on_bar_update twice with different timestamps and bar values.
        3. Validate that two bars are created and added to the data list.
        4. Confirm the correctness of values for each bar.
    """
    mocker.patch.object(bar_data, "_step")
    mocker.patch.object(bar_data, "push")

    ts1 = datetime(2024, 1, 1, 12, 0)
    ts2 = datetime(2024, 1, 1, 12, 1)
    
    bar_data.on_bar_update(ts1, 100.0, 105.0, 95.0, 102.0, 1000)
    bar_data.on_bar_update(ts2, 102.0, 110.0, 100.0, 108.0, 1500)

    assert len(bar_data.data) == 2

    # Validate the first bar
    bar1 = bar_data.data[0]
    assert bar1.ts == ts1
    assert bar1.open == 100.0
    assert bar1.high == 105.0
    assert bar1.low == 95.0
    assert bar1.close == 102.0
    assert bar1.volume == 1000

    # Validate the second bar
    bar2 = bar_data.data[1]
    assert bar2.ts == ts2
    assert bar2.open == 102.0
    assert bar2.high == 110.0
    assert bar2.low == 100.0
    assert bar2.close == 108.0
    assert bar2.volume == 1500


def test_max_len_enforcement(bar_data, mocker):
    """
    Test that the BarData class enforces the max_len attribute correctly,
    removing the oldest bars when the length exceeds the limit.

    Steps:
        1. Mock the _step and push methods.
        2. Generate 101 bars to exceed the max_len of 100.
        3. Validate that only 100 bars are retained in the data list.
        4. Confirm that the oldest bar is correctly removed.
    """
    mocker.patch.object(bar_data, "_step")
    mocker.patch.object(bar_data, "push")

    start_ts = datetime(2024, 1, 1, 12, 0)  # Start at 12:00
    for i in range(101):  # Create 101 bars to exceed max_len of 100
        ts = start_ts + timedelta(minutes=i)  # Increment time by i minutes
        bar_data.on_bar_update(ts, 100.0, 105.0, 95.0, 102.0, 1000)

    assert len(bar_data.data) == 100  # Ensure only 100 bars are kept
    assert bar_data.data[0].ts == start_ts + timedelta(minutes=1)  # The oldest bar should have been removed


def test_push_called(bar_data, mocker):
    """
    Test that the push method is called whenever on_bar_update is invoked.

    Steps:
        1. Mock the push method.
        2. Call on_bar_update with sample parameters.
        3. Validate that push is called exactly once during the method execution.
    """
    mock_push = mocker.patch.object(bar_data, "push")

    ts = datetime(2024, 1, 1, 12, 0)
    bar_data.on_bar_update(ts, 100.0, 105.0, 95.0, 102.0, 1000)

    # Validate that push is called
    mock_push.assert_called_once()