import typing

from alchemist.products.base_product import BaseProduct
from alchemist.datas.frequency import Frequency


class DataCard:
    def __init__(self, product: BaseProduct, freq: str, aggregation: typing.Optional[str]=None, resample: typing.Optional[bool]=False):
        """
        :param product: the product of the data card
        :param freq: the frequency of the data card
        :param aggregation: the aggregation of the data card, e.g ohlcv, ohlc, etc.
        :param resample: defualt `False`, if resample is set to be `True`, the resampler will try to build a bar by aggregating a higher frequency udpate, e.g from '1m' to '5m' or '5m' to '1h', otherwise the data will only be updated when the freq matches the update freq
        """
        self.product = product
        self.frequency = Frequency(freq)
        self.freq = self.frequency.freq
        self.aggregation = aggregation
        self.resample = resample

    def __str__(self):
        return f"DataCard(product={self.product}, freq={self.freq}, aggregation={self.aggregation}, resample={self.resample})"
    
    