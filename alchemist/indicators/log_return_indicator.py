from alchemist.indicators.base_indicator import BaseIndicator


class LogReturnIndicator(BaseIndicator):

    DATA_LINES = (
        'log_return',
    )

    def __init__(self, close_line, ts_line=None):
        super().__init__(close_line, min_period=2, ts_line=ts_line)
        self.close_line = self.data_0
    
    def next(self):
        import math
        if len(self.close_line) < 2:
            self.log_return.append(float('nan'))
            return
        prev = self.close_line[-2]
        curr = self.close_line[-1]
        self.log_return.append(math.log(curr / prev) if prev != 0 else float('nan'))
