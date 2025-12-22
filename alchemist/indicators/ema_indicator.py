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


class IntradayEmaIndicator(BaseIndicator):
    DATA_LINES = (
        'ema',
    )

    def __init__(self, close_line, period, ts_line):
        if ts_line is None:
            raise ValueError('IntradayEmaIndicator requires a ts_line for daily rollovers')

        super().__init__(
            close_line,
            min_period=1,
            ts_line=ts_line,
            update_mode='time',
            rollover_frequency='1d',
        )
        self.close_line = self.data_0
        self.multiplier = 2 / (period + 1)
        self.reset_state()

    def reset_state(self):
        self._prev_close = None
        self._prev_ema = None

    def next(self):
        prev_close = self._prev_close
        current_close = self.close_line[-1]

        # first bar of the day should align with the shifted series -> NaN
        if prev_close is None:
            self._prev_close = current_close
            self.ema.append(float('nan'))
            return

        if self._prev_ema is None:
            ema = prev_close
        else:
            ema = (prev_close - self._prev_ema) * self.multiplier + self._prev_ema

        self._prev_close = current_close
        self._prev_ema = ema
        self.ema.append(ema)
