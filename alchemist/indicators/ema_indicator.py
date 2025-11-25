from alchemist.indicators.base_indicator import BaseIndicator


class EmaIndicator(BaseIndicator):

    DATA_LINES = (
        'ema',
    )

    def __init__(self, close_line, min_period=1, ts_line=None):
        super().__init__(close_line, min_period=min_period, ts_line=ts_line)
        self.close_line = self.data_0
        self.multiplier = 2 / (min_period + 1)
        self.initialized = False  # flag to handle first EMA calculation

    def next(self):
        close = self.close_line[-1]

        if not self.initialized:
            # Simple average for the first EMA value
            init_prices = self.close_line[-self.min_period:]
            ema = sum(init_prices) / self.min_period
            self.initialized = True
        else:
            ema = (close - self.ema[-1]) * self.multiplier + self.ema[-1]

        self.ema.append(ema)
