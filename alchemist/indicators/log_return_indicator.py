from alchemist.indicators.base_indicator import BaseIndicator


class LogReturnIndicator(BaseIndicator):

    DATA_LINES = (
        'log_return',
    )
    
    def next(self):
        log_return = self.datas[0][-1].close - self.datas[0][-1].close
        self.log_return.append(log_return)