

class BaseDataPipeline:
    """"""

    def historical_ticks(self, product, start, end=None):
        pass

    def historical_bars(self, product, freq, start, end=None):
        pass

    def start(self):
        pass

    def stop(self):
        pass