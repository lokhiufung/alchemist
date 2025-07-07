"""
data_manager.py
===============

This module implements the DataManager class, which is responsible for managing and resampling
market data within the automated trading system. The DataManager handles multiple data cards,
each representing a unique combination of product, exchange, frequency, and aggregation method.
It creates and maintains data buffers and associated resamplers to ensure that incoming data
(from tick or bar updates) is processed, synchronized, and stored appropriately for use by the
trading strategies.

Key functionalities include:
    - Initializing data buffers and resamplers for various market data frequencies.
    - Creating unique data indices to efficiently update and retrieve data.
    - Handling bar updates by resampling high-frequency data into lower-frequency data streams.
    - (Future) Handling tick updates (currently not implemented).
    - Checking synchronization across multiple data buffers.

The DataManager leverages internal classes such as TickData, BarData, and TimeBarResampler to manage
the specific details of data storage and resampling.
"""

import re
import typing

from datetime import datetime

from alchemist.data_card import DataCard
from alchemist.datas.base_data import BaseData
from alchemist.datas.tick_data import TickData
from alchemist.datas.bar_data import BarData
from alchemist.datas.resampler import TimeBarResampler
from alchemist.indicators.base_indicator import BaseIndicator
from alchemist.datas.frequency import Frequency
from alchemist.logger import get_logger


class DataManager:
    """
    DataManager

    Manages market data streams for the automated trading system by creating data buffers and 
    associated resamplers based on a list of data cards. Each data card specifies a product, 
    its trading frequency, and the aggregation method to be used for resampling. The DataManager 
    is responsible for initializing the data structures, handling incoming bar (and eventually tick) 
    updates, and ensuring data synchronization across multiple channels.

    Attributes:
        MAX_LEN (int): The maximum length of the data buffer for each data stream.
        max_len (int): The instance-level maximum length (defaulted to MAX_LEN).
        data_cards (List[DataCard]): List of DataCard instances describing the data streams.
        datas (dict): Dictionary mapping a unique index to a data buffer (TickData or BarData).
        resamplers (dict): Dictionary mapping a unique index to a TimeBarResampler instance.
        logger: Logger instance for debugging and monitoring.
    """

    MAX_LEN: int = 100000

    def __init__(self, data_cards: typing.List[DataCard]):
        """
        Initializes the DataManager with a list of data cards.

        Args:
            data_cards (List[DataCard]): A list of DataCard instances, each specifying details
                such as the product, frequency, and aggregation method for a data stream.

        The constructor sets up internal data structures (datas and resamplers) and initializes them
        by calling the respective helper methods.
        """
        self.max_len = self.MAX_LEN
        self.data_cards = data_cards

        self.datas = {}
        self.resamplers = {}
        self.logger = get_logger('dm', console_logger_lv='info', file_logger_lv='debug')
        
        self.initialize_datas()
        self.initialize_resamplers()

    @staticmethod
    def create_data_index(exch, pdt: str, freq: str, aggregation: str) -> str:
        """
        Creates a unique index for a data card based on exchange, product, frequency, and aggregation.

        This index is used to quickly identify and update the corresponding data buffer when new data
        (e.g., tick, bar) arrives.

        Args:
            exch (str): The exchange identifier.
            pdt (str): The product or ticker symbol.
            freq (str): The frequency of the data (e.g., '1s', '1m').
            aggregation (str): The aggregation method used (e.g., 'OHLC').

        Returns:
            str: A unique index string in the format '{exch}_{pdt}_{freq}_{aggregation}'.
        """
        # create a unique index for each data card
        # so that when update from on_tick, on_quote, ...
        # we can quickly know which data to update
        # hash by pdt, exch, freq, aggregation
        return f'{exch}_{pdt}_{freq}_{aggregation}'
    
    @staticmethod
    def factor_data_index(index: str) -> typing.Tuple[str, str, str, str]:
        return index.split('_')

    def initialize_resamplers(self):
        """
        Initializes resamplers for each data card.

        For each data card, a unique index is created, and a TimeBarResampler is instantiated if the
        frequency resolution is one of the supported types ('s', 'm', 'h', or 'd'). If the frequency
        resolution is not supported, a NotImplementedError is raised.
        """
        # use resampler whatever
        for data_card in self.data_cards:
            product = data_card.product
            exch, pdt = product.exch, product.name
            index = self.create_data_index(exch, pdt, data_card.freq, data_card.aggregation)
            unit, resolution = data_card.frequency.unit, data_card.frequency.resolution
            if resolution == 's' or resolution == 'm' or resolution == 'h' or resolution == 'd':
                self.resamplers[index] = TimeBarResampler(frequency=data_card.frequency)
            else:
                raise NotImplementedError("Resample only supports 's', 'm', 'h', or 'd' resolutions.")

    def initialize_datas(self):
        """
        Initializes data buffers for each data card.

        For each data card, a unique index is created to store the data. Depending on the resolution 
        specified in the data card's frequency, a different data type is used:
            - TickData for tick-level data (resolution 't').
            - BarData for bar-level data (resolutions 's', 'm', 'h', or 'd').

        Raises:
            ValueError: If the provided resolution is invalid or not supported.
        """
        # create a unique index for each data card
        # so that when update from on_tick, on_quote, ...
        # we can quickly know which data to update
        # hash by pdt, exch, freq, aggregation
        for data_card in self.data_cards:
            product = data_card.product
            exch, pdt = product.exch, product.name
            index = self.create_data_index(exch, pdt, data_card.freq, data_card.aggregation)
            # e.g `1s`, `1m`, `5m`; must be intraday
            unit, resolution = data_card.frequency.unit, data_card.frequency.resolution
            if resolution == 't':
                # tick data
                assert unit == 1, 'tick data must be 1 unit'
                self.datas[index] = TickData(self.max_len, freq=data_card.freq, index=index)
            elif resolution == 'q':
                # quote data
                assert unit == 1, 'quote data must be 1 unit'
                pass
            elif resolution == 's' or resolution == 'm' or resolution == 'h' or resolution == 'd':
                # bar data
                self.datas[index] = BarData(self.max_len, freq=data_card.freq, index=index)
            else:
                raise ValueError(f'Invalid resolution: {resolution}')

    def get_data(self, index: str) -> BaseData:
        """
        Retrieves the data buffer associated with the given index.

        Args:
            index (str): The unique index corresponding to a data stream.

        Returns:
            BaseData: The data buffer (either TickData or BarData) associated with the index.
        """
        return self.datas[index]

    def on_bar_update(self, gateway: str, exch: str, pdt: str, freq: str, ts: datetime, open_: float, high: float, low: float, close: float, volume: float):
        """
        Handles bar updates from an external gateway.

        The method finds all data buffers matching the provided exchange and product, and then uses
        the corresponding resampler to process the incoming bar data. If the resampler's frequency
        is higher than or equal to the targeted frequency, the bar data is resampled and updated
        in the data buffer.

        Args:
            gateway: The source gateway of the bar update.
            exch (str): The exchange identifier.
            pdt (str): The product or ticker symbol.
            freq (str): The frequency of the incoming data update.
            ts (float): The timestamp of the bar update.
            open_ (float): The opening price.
            high (float): The high price.
            low (float): The low price.
            close (float): The closing price.
            volume (float): The traded volume during the bar.
        """
        frequency = Frequency(freq=freq)
        key = f'{exch}_{pdt}_[1-9][smhd]'  # TODO: hard coded the regex
        indexes = [k for k in self.datas.keys() if re.match(key, k)]

        for index in indexes:
            resampler = self.resamplers[index]
            data = self.datas[index]
            if resampler.frequency >= frequency:
                resampler.on_bar_update(freq, ts, open_, high, low, close, volume)

                resampled_bar = resampler.flush()
                if resampled_bar is not None:
                    data.on_bar_update(
                        ts=resampled_bar.ts,
                        open_=resampled_bar.open,
                        high=resampled_bar.high,
                        low=resampled_bar.low,
                        close=resampled_bar.close,
                        volume=resampled_bar.volume,
                    )
            else:
                self.logger.error(f'Auto resampling can only be triggerd when the update frequency is higher than the targeted frequency: {data=} {resampler.frequency=} {frequency=}')
    
    def on_tick_update(self, gateway, exch, pdt, freq, ts, price, size):
        """
        Handles tick updates from an external gateway.

        Currently, tick updates are not supported. This method serves as a placeholder for future
        implementation where tick data will be processed and potentially resampled.

        Args:
            gateway: The source gateway of the tick update.
            exch (str): The exchange identifier.
            pdt (str): The product or ticker symbol.
            freq (str): The frequency of the incoming tick data.
            ts (float): The timestamp of the tick update.
            price (float): The price associated with the tick.
            size (float): The size or volume associated with the tick.

        Raises:
            NotImplementedError: Always raised since tick updates are not currently supported.
        """
        raise NotImplementedError('Do not support tick update currently.')
        # frequency = Frequency(freq=freq)
        # key = f'{exch}_{pdt}'
        # indexes = [k for k in self.datas.keys() if re.match(key, k)]
        # for index in indexes:
        #     resampler = self.resamplers[index]
        #     data = self.datas[index]
        #     if resampler.frequency != frequency:
        #         raise NotImplementedError(f'Auto-resampling is not allowed on tick data currently: {data=} {frequency=} {resampler.frequency=}')
        #     else:
        #         data.on_bar_update(
        #             ts=ts,
        #             price=price,
        #             size=size,
        #         )

    def check_highest_resolution(self, ts, indexes) -> bool:
        ### TODO: implement this method, only allow calling next when the all highest resolution data are ready
        """
        1. get the normalized value of the ts (in seconds)
        2. get the normalized value of the highest frequency (in seconds)
        3. check the (1) mod (2) == 0
        4. if not, return `False`, otherwise return `True`

        """
        highest_freq: Frequency = self.datas[indexes[0]].frequency
        normalized_ts = ts.timestamp()  # convert to seconds
        normalized_highest_freq = highest_freq.normalized_value
        return normalized_ts % normalized_highest_freq == 0

    def check_sync(self, indexes) -> bool:
        """
        Checks whether the data buffers corresponding to the provided indexes are synchronized.

        The method verifies that the latest timestamp (ts) in each data buffer is identical. If any
        buffer is empty or timestamps do not match, the function returns False, indicating a lack of
        synchronization.

        Args:
            indexes (List[str]): A list of unique data indexes to be checked.

        Returns:
            bool: True if all data buffers are synchronized; False otherwise.
        """
        current_tss = []
        for index in indexes:
            data = self.get_data(index=index)
            if len(data) > 0:
                current_ts = data[-1].ts
                current_tss.append(current_ts)
            else:
                return False
        first_current_ts = current_tss[0]
        for current_ts in current_tss:
            if current_ts != first_current_ts:
                return False
        return True