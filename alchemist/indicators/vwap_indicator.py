from alchemist.indicators.base_indicator import BaseIndicator


class DailyVwapIndicator(BaseIndicator):
    # params = (
    #     ('start_hour', 6),  # Start accumulating at 6:00 am
    # )
    DATA_LINES = (
        'vwap',
    )

    def __init__(self, high_line, low_line, close_line, volume_line, ts_line, start_hour=9, start_minute=30):
        super().__init__(
            high_line,
            low_line,
            close_line,
            volume_line,
            min_period=1,
            ts_line=ts_line,
            update_mode='time',
            rollover_frequency='1d',
        )
        self.high_line = self.data_0
        self.low_line = self.data_1
        self.close_line = self.data_2
        self.volume_line = self.data_3
        self.start_hour = start_hour
        self.start_minute = start_minute
        self.reset_state()

    def reset_state(self):
        self.cum_volume = 0.0
        self.cum_vol_price = 0.0
        self.start_vwap_calculation = False

    def next(self):
        dt = self.ts_line[-1]

        # start calculating at configured hour
        if dt.hour == self.start_hour and dt.minute == self.start_minute:
            self.cum_volume = 0.0
            self.cum_vol_price = 0.0
            self.start_vwap_calculation = True
        
        if self.start_vwap_calculation:
            typical_price = (self.high_line[-1] + self.low_line[-1] + self.close_line[-1]) / 3
            vol = self.volume_line[-1]
            self.cum_volume += vol
            self.cum_vol_price += typical_price * vol
            
            if self.cum_volume != 0:
                self.vwap.append(self.cum_vol_price / self.cum_volume)
            else:
                self.vwap.append(float('nan'))
        else:
            self.vwap.append(float('nan'))
