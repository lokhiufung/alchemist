import numpy as np

from alchemist.indicators.base_indicator import BaseIndicator
from alchemist.data_line import DataLine


class PearsonRIndicator(BaseIndicator):
    DATA_LINES = (
        'pearson_r',
    )

    def __init__(self, x_line, y_line, min_period=20, ts_line=None):
        super().__init__(x_line, y_line, min_period=min_period, ts_line=ts_line)
        self.x_line = self.data_0
        self.y_line = self.data_1

    def next(self):
        x = np.array(self.x_line[-self.min_period:])
        y = np.array(self.y_line[-self.min_period:])
        # Calculate Pearson's r
        pearson_r = np.corrcoef(x, y)[0, 1]
        self.pearson_r.append(float(pearson_r))
        # self.last_calculated_index = current_index
