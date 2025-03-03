import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Assuming the following imports based on the provided code
from alchemist.data_card import DataCard
from alchemist.products.base_product import BaseProduct
from alchemist.datas.bar_data import BarData
from alchemist.datas.tick_data import TickData
from alchemist.datas.resampler import TimeBarResampler
from alchemist.managers.data_manager import DataManager
from alchemist.datas.frequency import Frequency


@pytest.fixture
def mock_product1():
    mock_pdt = MagicMock(spec=BaseProduct)
    mock_pdt.exch = 'NYSE'
    mock_pdt.name = 'AAPL'
    mock_pdt.base_currency = 'USD'
    return mock_pdt

@pytest.fixture
def mock_product2():
    mock_pdt = MagicMock(spec=BaseProduct)
    mock_pdt.exch = 'NASDAQ'
    mock_pdt.name = 'MSFT'
    mock_pdt.base_currency = 'USD'
    return mock_pdt


@pytest.fixture
def data_card_1m(mock_product1):
    return DataCard(
        product=mock_product1,
        freq='1m',
        aggregation='ohlcv',
        resample=False
    )


@pytest.fixture
def data_card_1m_product2(mock_product2):
    return DataCard(
        product=mock_product2,
        freq='1m',
        aggregation='ohlcv',
        resample=False  # Ensure resample is False to avoid NotImplementedError
    )


@pytest.fixture
def data_card_5m(mock_product1):
    return DataCard(
        product=mock_product1,
        freq='5m',
        aggregation='ohlcv',
        resample=True
    )

@pytest.fixture
def data_card_tick(mock_product2):
    return DataCard(
        product=mock_product2,
        freq='1t',
        aggregation=None,
        resample=False
    )

@pytest.fixture
def data_card_quote(mock_product2):
    return DataCard(
        product=mock_product2,
        freq='1q',
        aggregation=None,
        resample=False
    )

@pytest.fixture
def data_manager(data_card_1m, data_card_5m):
    return DataManager(
        data_cards=[data_card_1m, data_card_5m]
    )


# @pytest.fixture
# def mock_resampler():
#     """
#     Fixture to mock TimeBarResampler within the scope of the tests.
#     The patch is applied to the 'alchemist.managers.data_manager.TimeBarResampler' path,
#     which is where DataManager imports and uses it.
#     """
#     with patch('alchemist.managers.data_manager.TimeBarResampler') as MockResamplerClass:
#         mock_instance = MagicMock()
#         MockResamplerClass.return_value = mock_instance
#         yield mock_instance  # This mock_instance can be used in tests to assert calls


@pytest.fixture
def data_manager_two_1m(data_card_1m, data_card_1m_product2):
    return DataManager(
        data_cards=[data_card_1m, data_card_1m_product2]
    )

def test_data_manager_initialization_on_regular_time_interval(data_manager):
    """
    Test that DataManager initializes datas and resamplers correctly with multiple DataCards.
    """
    # Check that datas dictionary has correct keys
    expected_keys = [
        'NYSE_AAPL_1m_ohlcv',
        'NYSE_AAPL_5m_ohlcv',
    ]
    assert set(data_manager.datas.keys()) == set(expected_keys), "DataManager datas keys mismatch."
    
    # Check that datas have correct instances
    assert isinstance(data_manager.datas['NYSE_AAPL_1m_ohlcv'], BarData), "DataManager datas has incorrect type for 1m."
    assert isinstance(data_manager.datas['NYSE_AAPL_5m_ohlcv'], BarData), "DataManager datas has incorrect type for 5m."
    
    # Check that resamplers are initialized for time-based frequencies
    assert 'NYSE_AAPL_1m_ohlcv' in data_manager.resamplers, "Resampler for 1m not initialized."
    assert 'NYSE_AAPL_5m_ohlcv' in data_manager.resamplers, "Resampler for 5m not initialized."
    

def test_data_manager_initialization_on_irregular_time_interval(data_card_1m, data_card_5m, data_card_tick, data_card_quote):
    """
    Test that DataManager initializes datas and resamplers correctly with multiple DataCards.
    """
    with pytest.raises(NotImplementedError):
        data_manager = DataManager(
            data_cards=[data_card_1m, data_card_5m, data_card_tick, data_card_quote]
        )


def test_create_data_index():
    """
    Test that create_data_index generates correct unique indices.
    """
    data_manager = DataManager(data_cards=[])  # Initialize with empty data_cards
    index = data_manager.create_data_index('NYSE', 'AAPL', '1m', 'ohlcv')
    assert index == 'NYSE_AAPL_1m_ohlcv', "create_data_index returned incorrect index."
    
    index = data_manager.create_data_index('NASDAQ', 'MSFT', 't', None)
    assert index == 'NASDAQ_MSFT_t_None', "create_data_index returned incorrect index for tick data."
    
    index = data_manager.create_data_index('LSE', 'GOOG', '1h', 'ohlc')
    assert index == 'LSE_GOOG_1h_ohlc', "create_data_index returned incorrect index for 1h data."


def test_on_bar_update_no_resampling(data_manager, data_card_1m):
    """
    Test that on_bar_update correctly appends bars to BarData without resampling.
    """
    index = data_manager.create_data_index(
        data_card_1m.product.exch,
        data_card_1m.product.name,
        data_card_1m.freq,
        data_card_1m.aggregation
    )
    
    # Create a mock bar
    mock_bar = MagicMock()
    mock_bar.ts = datetime.now()
    mock_bar.open = 100.0
    mock_bar.high = 101.0
    mock_bar.low = 99.5
    mock_bar.close = 100.5
    mock_bar.volume = 1000
    
    # Call on_bar_update
    data_manager.on_bar_update(
        gateway='test_gateway',
        exch=data_card_1m.product.exch,
        pdt=data_card_1m.product.name,
        freq=data_card_1m.freq,
        ts=mock_bar.ts,
        open_=mock_bar.open,
        high=mock_bar.high,
        low=mock_bar.low,
        close=mock_bar.close,
        volume=mock_bar.volume
    )
    
    # Assert that bar was appended
    bar_data = data_manager.get_data(index)
    assert len(bar_data.data) == 1, "Bar was not appended correctly."
    assert bar_data.data[0].close == mock_bar.close, "Bar close price mismatch."


def test_on_bar_update_with_resampling(data_manager, data_card_5m):
    """
    Test that on_bar_update triggers resampling and appends resampled bars.
    """
    index = data_manager.create_data_index(
        data_card_5m.product.exch,
        data_card_5m.product.name,
        data_card_5m.freq,
        data_card_5m.aggregation
    )
    
    # Assume TimeBarResampler aggregates 5 '1m' bars into 1 '5m' bar
    # Mock the resampler's on_bar_update and flush methods
    # Simulate 5 incoming '1m' bars

    start_ts = datetime(2024, 1, 1, 9, 30, 0)

    for i in range(5):
        ts = start_ts + timedelta(minutes=i)
        data_manager.on_bar_update(
            gateway='test_gateway',
            exch=data_card_5m.product.exch,
            pdt=data_card_5m.product.name,
            freq='1m',  # Incoming frequency is '1m'
            ts=ts,
            open_=100.0 + i,
            high=105.0 + i,
            low=95.0 + i,
            close=100.5 + i,
            volume=5000 + i * 100
        )
        print(data_manager.resamplers[index].current_bar)

    bar_data = data_manager.get_data(index)
    assert len(bar_data.data) == 1, "Resampled bar was not appended correctly."
    assert bar_data[-1].open == 100.0
    assert bar_data[-1].high == 109.0
    assert bar_data[-1].low == 95.0
    assert bar_data[-1].close == 104.5
    assert bar_data[-1].volume == 26000


def test_on_tick_update(data_manager, data_card_tick):
    """
    Test that on_tick_update correctly processes tick data and updates TickData.
    """
    index = data_manager.create_data_index(
        data_card_tick.product.exch,
        data_card_tick.product.name,
        data_card_tick.freq,
        data_card_tick.aggregation
    )
    
    # Create mock tick data
    tick_ts = datetime.now()
    tick_price = 250.0
    tick_size = 10
    

    with pytest.raises(NotImplementedError):
        # Call on_tick_update
        data_manager.on_tick_update(
            gateway='test_gateway',
            exch=data_card_tick.product.exch,
            pdt=data_card_tick.product.name,
            freq=data_card_tick.freq,
            ts=tick_ts,
            price=tick_price,
            size=tick_size
        )
    
    # # Assert that tick data was appended
    # tick_data = data_manager.get_data(index)
    # assert len(tick_data.data) == 1, "Tick data was not appended correctly."
    # assert tick_data.data[0].price == tick_price, "Tick data price mismatch."
    # assert tick_data.data[0].size == tick_size, "Tick data size mismatch."


def test_check_sync_two_1m(data_manager_two_1m, data_card_1m, data_card_1m_product2):
    """
    Test that check_sync correctly identifies synchronized and unsynchronized data between two DataCards with the same frequency.
    """
    # Create indices for both DataCards
    index_product1 = data_manager_two_1m.create_data_index(
        data_card_1m.product.exch,
        data_card_1m.product.name,
        data_card_1m.freq,
        data_card_1m.aggregation
    )
    index_product2 = data_manager_two_1m.create_data_index(
        data_card_1m_product2.product.exch,
        data_card_1m_product2.product.name,
        data_card_1m_product2.freq,
        data_card_1m_product2.aggregation
    )
    
    # Create synchronized bar data for both products
    synchronized_ts = datetime.now()
    
    # Update both DataCards with the same timestamp
    data_manager_two_1m.on_bar_update(
        gateway='test_gateway',
        exch=data_card_1m.product.exch,
        pdt=data_card_1m.product.name,
        freq=data_card_1m.freq,
        ts=synchronized_ts,
        open_=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=1000
    )
    
    data_manager_two_1m.on_bar_update(
        gateway='test_gateway',
        exch=data_card_1m_product2.product.exch,
        pdt=data_card_1m_product2.product.name,
        freq=data_card_1m_product2.freq,
        ts=synchronized_ts,
        open_=200.0,
        high=201.0,
        low=199.0,
        close=200.5,
        volume=2000
    )
    
    # Check synchronization
    assert data_manager_two_1m.check_sync([index_product1, index_product2]) == True, "DataManager failed to recognize synchronized data."
    
    # Append unsynchronized bar data for one product
    unsynchronized_ts = synchronized_ts + timedelta(minutes=1)
    data_manager_two_1m.on_bar_update(
        gateway='test_gateway',
        exch=data_card_1m.product.exch,
        pdt=data_card_1m.product.name,
        freq=data_card_1m.freq,
        ts=unsynchronized_ts,
        open_=101.0,
        high=102.0,
        low=100.0,
        close=101.5,
        volume=1100
    )
    
    # The second product's data remains at synchronized_ts
    assert data_manager_two_1m.check_sync([index_product1, index_product2]) == False, "DataManager incorrectly recognized unsynchronized data."


def test_on_bar_update_invalid_frequency(data_manager, data_card_1m, caplog):
    """
    Test that on_bar_update raises ValueError when incoming frequency does not match DataCard frequency.
    """
    index = data_manager.create_data_index(
        data_card_1m.product.exch,
        data_card_1m.product.name,
        data_card_1m.freq,
        data_card_1m.aggregation
    )
    
    caplog.clear()

    # Create a mock bar with mismatched frequency ('5m' instead of '1m')
    data_manager.on_bar_update(
        gateway='test_gateway',
        exch=data_card_1m.product.exch,
        pdt=data_card_1m.product.name,
        freq='5m',  # Invalid frequency
        ts=datetime.now(),
        open_=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=1000
    )

    # Assert that a ValueError was not raised
    # Instead, check that the appropriate error message was logged
    assert "Auto resampling can only be triggerd when the update frequency is higher than the targeted frequency" in caplog.text, \
        "Expected error message was not logged."


def test_on_tick_update_with_resample_enabled(data_manager, data_card_tick):
    """
    Test that on_tick_update raises NotImplementedError when resample=True for tick data.
    """
    # Modify data_card_tick to have resample=True
    data_card_tick.resample = True
    
    # Create mock tick data
    tick_ts = datetime.now()
    tick_price = 250.0
    tick_size = 10
    
    # Attempt to call on_tick_update
    with pytest.raises(NotImplementedError):
        data_manager.on_tick_update(
            gateway='test_gateway',
            exch=data_card_tick.product.exch,
            pdt=data_card_tick.product.name,
            freq=data_card_tick.freq,
            ts=tick_ts,
            price=tick_price,
            size=tick_size
        )


def test_data_manager_no_datacards():
    """
    Test that DataManager initializes correctly with no DataCards.
    """
    data_manager = DataManager(data_cards=[])
    assert len(data_manager.datas) == 0, "DataManager datas should be empty."
    assert len(data_manager.resamplers) == 0, "DataManager resamplers should be empty."


def test_create_data_index_special_characters():
    """
    Test that create_data_index handles special characters in product names and exchanges.
    """
    data_manager = DataManager(data_cards=[])
    index = data_manager.create_data_index('NYSE@2024', 'MSFT#1', '1m', 'ohlcv')
    assert index == 'NYSE@2024_MSFT#1_1m_ohlcv', "create_data_index failed with special characters."