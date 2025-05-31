from alchemist.indicators.base_indicator import BaseIndicator


class ATRIndicator(BaseIndicator):
    DATA_LINES = (
        'atr',
    )

    def __init__(self, data, min_period=14):
        super().__init__(data, min_period=min_period)
        self.data = self.data_0
        self.min_period = min_period

    def next(self):
        trs = []
        for i in range(-self.min_period + 1, 0):
            current = self.data[i]
            previous = self.data[i - 1]

            high = current.high
            low = current.low
            prev_close = previous.close

            tr = max(
                high - low,
                abs(high - prev_close),
                abs(low - prev_close)
            )
            trs.append(tr)

        atr = sum(trs) / self.min_period
        self.atr.append(atr)
