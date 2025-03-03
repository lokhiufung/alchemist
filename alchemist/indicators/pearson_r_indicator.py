import numpy as np

from alchemist.indicators.base_indicator import BaseIndicator
from alchemist.data_line import DataLine


class PearsonRIndicator(BaseIndicator):
    DATA_LINES = (
        'pearson_r',
    )
    def next(self):
        x = np.array([bar.close for bar in self.data_0[-self.min_period:]])
        y = np.array([bar.close for bar in self.data_1[-self.min_period:]])
        # Calculate Pearson's r
        pearson_r = np.corrcoef(x, y)[0, 1]
        self.pearson_r.append(float(pearson_r))
        # self.last_calculated_index = current_index