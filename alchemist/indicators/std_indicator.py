import numpy as np

from alchemist.indicators.base_indicator import BaseIndicator



class StdIndicator(BaseIndicator):
    
    DATA_LINES = (
        'std',
    )

    def next(self):
        sigma = np.std([self.datas[0][-i].close for i in range(1, self.min_period + 1)])
        self.std.append(sigma)

