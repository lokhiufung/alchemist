from alchemist.indicators.base_indicator import BaseIndicator


class SmaIndicator(BaseIndicator):

    DATA_LINES = (
        'sma',
    )

    def __init__(self, data, min_period=1):
        super().__init__(data, min_period=min_period)
        self.data = self.data_0
        self.min_period = min_period

    def next(self):
        close_prices = [bar.close for bar in self.data[-self.min_period:]]
        new_value = sum(close_prices) / self.min_period
        self.sma.append(new_value)
            

