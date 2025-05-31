import math

from alchemist.indicators.base_indicator import BaseIndicator


class BollingerbandIndicator(BaseIndicator):
    DATA_LINES = (
        'top',
        'bot'
    )

    def __init__(self, data, min_period=20, num_std_dev=2):
        super().__init__(data, min_period=min_period)
        self.data = self.data_0
        self.min_period = min_period
        self.num_std_dev = num_std_dev

    def next(self):
        window = self.data[-self.min_period:]
        close_prices = [bar.close for bar in window]

        mean = sum(close_prices) / self.min_period
        variance = sum((x - mean) ** 2 for x in close_prices) / self.min_period
        std_dev = math.sqrt(variance)

        upper_band = mean + self.num_std_dev * std_dev
        lower_band = mean - self.num_std_dev * std_dev

        self.top.append(upper_band)
        self.bot.append(lower_band)
