import typing

import ray

from alchemist.strategies.base_strategy import BaseStrategy
from alchemist.datas.bar_data import BarData
from alchemist.products.base_product import BaseProduct
from alchemist.indicators.sma_indicator import SmaIndicator


@ray.remote
class MovingAverageCrossoverStrategy(BaseStrategy):
    """
    A benchmark strategy that uses a simple moving average crossover to generate buy/sell signals.
    """
    STRAT = 'strat-baseline-2'
    PARAMS = {
        'fast_period': 5,
        'slow_period': 10,
    }

    def __init__(self, name, zmq_send_port, zmq_recv_ports, products: typing.List[BaseProduct], data_cards, data_pipeline=None, data_pipelin_kwargs=None, max_len=1000, params=None, monitor_actor=None, is_backtesting=False, trading_periods=None):
        super().__init__(name, zmq_send_port, zmq_recv_ports, products, data_cards, data_pipeline, data_pipelin_kwargs, max_len, params, monitor_actor, is_backtesting, trading_periods)
        self.product = self.products[0]
        
        data_card = self.data_cards[0]
        data_card_index = self.create_data_index(data_card.product.exch, data_card.product.name, data_card.freq, aggregation=data_card.aggregation)
        self.data = self.datas[data_card_index]
        self.fast_sma_indicator = SmaIndicator(
            close_line=self.data.close,
            ts_line=self.data.ts,
            min_period=self.params['fast_period']
        )
        self.slow_sma_indicator = SmaIndicator(
            close_line=self.data.close,
            ts_line=self.data.ts,
            min_period=self.params['slow_period']
        )

    def compute_signals(self):
        return {
            'sma_fast_1': self.fast_sma_indicator.sma[-1],
            'sma_slow_1': self.slow_sma_indicator.sma[-1],
            'sma_fast_2': self.fast_sma_indicator.sma[-2],
            'sma_slow_2': self.slow_sma_indicator.sma[-2],
        }
    
    def compute_size(self, signals, side):
        return 1.0

    def should_enter(self, signals):
        # fast crossover slow from below
        if signals['sma_fast_1'] > signals['sma_slow_1'] and signals['sma_fast_2'] < signals['sma_slow_2']:
            return True
        return False

    def should_exit(self, signals, current_side):
        bearish_crossover = signals['sma_fast_1'] < signals['sma_slow_1'] and signals['sma_fast_2'] > signals['sma_slow_2']
        if bearish_crossover and current_side > 0:
            return True
        return False
    
