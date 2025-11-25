from alchemist.indicators.base_indicator import BaseIndicator


class RsiIndicator(BaseIndicator):
    DATA_LINES = (
        'rsi',
    )

    def __init__(self, close_line, min_period=14, ts_line=None):
        super().__init__(close_line, min_period=min_period, ts_line=ts_line)
        self.close_line = self.data_0

    def next(self):
        if len(self.close_line) <= self.min_period:
            return

        gains = []
        losses = []
        
        for previous, current in zip(self.close_line[-(self.min_period + 1):-1], self.close_line[-(self.min_period + 1) + 1:]):
            delta = current - previous
            if delta > 0:
                gains.append(delta)
                losses.append(0.0)
            elif delta < 0:
                gains.append(0.0)
                losses.append(-delta)
            else:
                gains.append(0.0)
                losses.append(0.0)

        avg_gain = sum(gains) / self.min_period
        avg_loss = sum(losses) / self.min_period

        if avg_loss == 0.0 and avg_gain == 0.0:
            rsi = 50.0
        elif avg_loss == 0.0:
            rsi = 100.0
        elif avg_gain == 0.0:
            rsi = 0.0
        else:
            rs = avg_gain / avg_loss
            rsi = 100 - (100 / (1 + rs))

        self.rsi.append(rsi)
