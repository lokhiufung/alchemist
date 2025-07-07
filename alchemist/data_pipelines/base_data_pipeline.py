

class BaseDataPipeline:
    """"""

    def historical_ticks(self, product, start, end=None):
        """
        Load historical ticks as standardized bars
        """
        pass

    def historical_bars(self, product, freq, start, end=None):
        """
        Load historical bars as standardized bars
        """
        pass

    def start(self):
        pass

    def stop(self):
        pass