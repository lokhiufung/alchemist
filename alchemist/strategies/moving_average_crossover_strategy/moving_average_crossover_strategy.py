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

    def __init__(self, name, zmq_send_port, zmq_recv_ports, products: typing.List[BaseProduct], data_cards, data_pipeline=None, data_pipelin_kwargs=None, max_len=1000, params=None, monitor_actor=None):
        super().__init__(name, zmq_send_port, zmq_recv_ports, products, data_cards, data_pipeline, data_pipelin_kwargs, max_len, params, monitor_actor)
        self.product = self.products[0]
        
        data_card = self.data_cards[0]
        data_card_index = self.create_data_index(data_card.product.exch, data_card.product.name, data_card.freq, aggregation=data_card.aggregation)
        self.data = self.datas[data_card_index]
        self.fast_sma_indicator = SmaIndicator(
            data=self.data,
            min_period=self.params['fast_period']
        )
        self.slow_sma_indicator = SmaIndicator(
            data=self.data,
            min_period=self.params['slow_period']
        )

    def next(self):
        """
        Override the on_bar method to handle bar updates and generate signals.
        """
        # TODO: it is better to update the indicator api so that we dont have to check the len of different sma indicators)
        if len(self.fast_sma_indicator.sma) > 1 and len(self.slow_sma_indicator.sma) > 1:
            # print(self.fast_sma_indicator.sma[-1], self.slow_sma_indicator.sma[-1], self.fast_sma_indicator.sma[-2], self.slow_sma_indicator.sma[-2])
            if self.fast_sma_indicator.sma[-1] > self.slow_sma_indicator.sma[-1] and self.fast_sma_indicator.sma[-2] < self.slow_sma_indicator.sma[-2]:
                self.buy(
                    gateway='mock_gateway',
                    product=self.products[0],
                    price=self.data[-1].close,
                    size=1,
                    order_type='MARKET',
                    time_in_force='GTC',
                )
            elif self.fast_sma_indicator.sma[-1] < self.slow_sma_indicator.sma[-1] and self.fast_sma_indicator.sma[-2] > self.slow_sma_indicator.sma[-2]:
                self.sell(
                    gateway='mock_gateway',
                    product=self.products[0],
                    price=self.data[-1].close,
                    size=1,
                    order_type='MARKET',
                    time_in_force='GTC',
                )
    
    def get_signals(self):
        return {
            'fast - slow sma': self.fast_sma_indicator.sma[-1] - self.slow_sma_indicator.sma[-1]
        }
