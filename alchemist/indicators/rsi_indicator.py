from alchemist.indicators.base_indicator import BaseIndicator


class RsiIndicator(BaseIndicator):
    DATA_LINES = (
        'rsi',
    )

    def next(self):
        # TODO
        if len(self.data_0) <= self.min_period:
            return

        recent_bars = list(self.data_0)[-(self.min_period + 1):]
        gains = []
        losses = []

        for previous_bar, current_bar in zip(recent_bars[:-1], recent_bars[1:]):
            delta = current_bar.close - previous_bar.close
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
