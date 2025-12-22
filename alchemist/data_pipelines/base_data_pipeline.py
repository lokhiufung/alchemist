from alchemist.enums import OHLCVEnum


class BaseDataPipeline:
    # ohlcv column names
    TS_COL = OHLCVEnum.TS.value
    OPEN_COL = OHLCVEnum.OPEN.value
    HIGH_COL = OHLCVEnum.HIGH.value
    LOW_COL = OHLCVEnum.LOW.value
    CLOSE_COL = OHLCVEnum.CLOSE.value
    VOLUME_COL = OHLCVEnum.VOLUME.value
    

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