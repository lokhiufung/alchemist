from alchemist.indicators.base_indicator import BaseIndicator


class OpenPriceIndicator(BaseIndicator):
    DATA_LINES = (
        'open_price',
    )

    def __init__(self, open_line, ts_line):
        if ts_line is None:
            raise ValueError('OpenPriceIndicator requires a ts_line for daily rollovers')

        super().__init__(
            open_line,
            min_period=1,
            ts_line=ts_line,
            update_mode='time',
            rollover_frequency='1d',
        )
        self.open_line = self.data_0
        self.reset_state()

    def reset_state(self):
        self._current_open = None

    def next(self):
        if self._current_open is None:
            self._current_open = self.open_line[-1]

        self.open_price.append(self._current_open)
