from alchemist.indicators.base_indicator import BaseIndicator


class EmaIndicator(BaseIndicator):

    DATA_LINES = (
        'ema',
    )

    def __init__(self, data, min_period=1):
        super().__init__(data, min_period=min_period)
        self.data = self.data_0
        self.min_period = min_period
        self.multiplier = 2 / (min_period + 1)
        self.initialized = False  # flag to handle first EMA calculation

    def next(self):
        close = self.data[-1].close

        if not self.initialized:
            # Simple average for the first EMA value
            init_prices = [bar.close for bar in self.data[-self.min_period:]]
            ema = sum(init_prices) / self.min_period
            self.initialized = True
        else:
            ema = (close - self.ema[-1]) * self.multiplier + self.ema[-1]

        self.ema.append(ema)