from datetime import datetime

from alchemist.indicators.base_indicator import BaseIndicator


class DailyVwapIndicator(BaseIndicator):
    # params = (
    #     ('start_hour', 6),  # Start accumulating at 6:00 am
    # )
    DATA_LINES = (
        'vwap',
    )

    def __init__(self, data, params):
        super().__init__(data, min_period=1, params=params)
        self.cum_volume = 0
        self.current_date = None
        self.cum_vol_price = 0
        self.start_vwap_calculation = False

        self.data = self.datas[0]

    def next(self):
        dt = self.data[0].ts
        # convert dt to datetime
        # dt = datetime.fromtimestamp(dt)
        
        # Check if we need to reset the cumulative sums at 18:00
        if dt.hour == self.params['start_hour'] and dt.minute == 0:
            self.cum_volume = 0
            self.cum_vol_price = 0
            self.current_date = dt.date()
            self.start_vwap_calculation = True
        
        if self.start_vwap_calculation:
            # Calculate the typical price
            typical_price = (self.data[-1].high + self.data[-1].low + self.data[-1].close) / 3
            
            # Accumulate the volume and volume-weighted price
            self.cum_volume += self.data[-1].volume
            self.cum_vol_price += typical_price * self.data[-1].volume
            
            # Calculate VWAP
            if self.cum_volume != 0:
                self.vwap.append(self.cum_vol_price / self.cum_volume)
            else:
                self.vwap.append(float('nan'))  # Avoid division by zero
        else:
            self.vwap.append(float('nan'))  # Not calculating VWAP yet

