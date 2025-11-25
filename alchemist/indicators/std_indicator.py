import numpy as np

from alchemist.indicators.base_indicator import BaseIndicator



class StdIndicator(BaseIndicator):
    
    DATA_LINES = (
        'std',
    )

    def __init__(self, close_line, min_period=1, ts_line=None):
        super().__init__(close_line, min_period=min_period, ts_line=ts_line)
        self.close_line = self.data_0

    def next(self):
        sigma = np.std(self.close_line[-self.min_period:])
        self.std.append(sigma)
