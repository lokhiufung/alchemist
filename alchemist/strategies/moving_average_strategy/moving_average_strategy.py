import typing

import ray

from alchemist.strategies.base_strategy import BaseStrategy
from alchemist.datas.bar_data import BarData
from alchemist.products.base_product import BaseProduct
from alchemist.indicators.sma_indicator import SmaIndicator


@ray.remote
class MovingAverageStrategy(BaseStrategy):
    """
    A benchmark strategy that uses a simple moving average to generate buy/sell signals.
    """
    STRAT = 'strat-baseline-1'
    PARAMS = {
        'period': 10
    }

    def __init__(self, name, zmq_send_port, zmq_recv_ports, products: typing.List[BaseProduct], data_cards, data_pipeline=None, data_pipelin_kwargs=None, max_len=1000, params=None, monitor_actor=None, is_backtesting=False):
        super().__init__(name, zmq_send_port, zmq_recv_ports, products, data_cards, data_pipeline, data_pipelin_kwargs, max_len, params, monitor_actor, is_backtesting)
        self.product = self.products[0]
        
        data_card = self.data_cards[0]
        data_card_index = self.create_data_index(data_card.product.exch, data_card.product.name, data_card.freq, aggregation=data_card.aggregation)
        self.data = self.datas[data_card_index]
        self.sma_indicator = SmaIndicator(
            close_line=self.data.close,
            ts_line=self.data.ts,
            min_period=self.params['period']
        )

    def next(self):
        """
        Override the on_bar method to handle bar updates and generate signals.
        """
        # if len(self.sma_indicator.sma) > 1:
        #     print('close[-1]={} sma[-1]={} close[-2]={} sma[-2]={}'.format(self.data[-1].close, self.sma_indicator.sma[-1], self.data[-2].close, self.sma_indicator.sma[-2]))

        # else:
        #     print('len sma={}'.format(len(self.sma_indicator.sma)))
        if self.data[-1].close > self.sma_indicator.sma[-1] and self.data[-2].close <= self.sma_indicator.sma[-2]:
            self.buy(
                gateway='mock_gateway',
                product=self.products[0],
                price=self.data[-1].close,
                size=1,
                order_type='MARKET',
                time_in_force='GTC',
            )
        elif self.data[-1].close < self.sma_indicator.sma[-1] and self.data[-2].close >= self.sma_indicator.sma[-2]:
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
            'sma - close': self.sma_indicator.sma[-1] - self.data[-1].close,
        }
