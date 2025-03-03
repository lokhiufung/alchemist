"""This class is a wrapper of IB's EClient
It should be a part of ib_api.py, but for the sake of clarity,
it is separated to manage functions in IB's EClient.
It should never be used alone.
"""

import time


from typing import Literal

# NOTE: do NOT write `from external.ibapi.cilent import *`
# it will lead to a different __name__ for the logger = logging.getLogger(__name__) in external/ibapi/client.py
from ibapi.client import EClient
from ibapi.account_summary_tags import *


class IBClient(EClient):
    def __init__(self):
        # pass in IBApi() object (child of EWrapper) as EClient needs EWrapper
        super().__init__(self)
        self._request_id = 1
        self._req_id_to_product = {}
        self._pdts_requested_market_data = []

    # def connect(self):
    #     account = self.account

    #     super().connect(host=account.host, port=account.port, clientId=account.client_id)
    #     self._ib_thread = Thread(name=f'{self.NAME}_api', target=self.run, daemon=True)
    #     self._ib_thread.start()
    #     self._logger.debug(f'{self.NAME} thread started')

    #     if self._wait(self.is_connected, reason='connection'):
    #         # need to wait for the EReader to get ready; otherwise,
    #         # if subscribe too early and the subscription failed,
    #         # it will somehow lead to disconnection from IB
    #         time.sleep(1)
    #         self._subscribe()
    #         # wait for subscription
    #             # self._background_thread = Thread(target=self._run_background_tasks, daemon=True)
    #             # self._background_thread.start()

    def disconnect(self):
        super().disconnect()
        self._unsubscribe()

    def _increment_request_id(self):
        self._request_id += 1

    def _update_request_id_and_corresponding_product(self, product):
        self._req_id_to_product[self._request_id] = product
        self._increment_request_id()


    """
    public channels    
    ---------------------------------------------------
    """
    def _request_market_data(self, **kwargs):
        """Aggregated level 1 orderbook + trade data (slower than tick by tick data)"""
        product = kwargs['product']
        # market data subscription is for both bid/ask and last price/qty
        # if e.g. the 'orderbook' channel has already requested market data, 
        # do not request again for the 'tradebook' channel
        # if product.pdt in self._pdts_requested_market_data:
        #     self._logger.debug(f'{self.NAME} has already requested {product.pdt} market data, do not request again')
        #     return
        self.reqMktData(
            self._request_id,
            product,
            # generic_tick_list, snapshot, regulatory_snapshot are params in IB's reqMktData(...)
            kwargs.get('genericTickList', ''),
            kwargs.get('snapshot', False), 
            kwargs.get('regulatorySnapshot', False),
            []
        )
        self._logger.debug(f'{self.NAME} requested (req_id={self._request_id}) {product.pdt} market data')
        self._pdts_requested_market_data.append(product.pdt)
        self._update_request_id_and_corresponding_product(product)

    # def _request_tick_by_tick_data(self, tick_type: Literal['Last', 'AllLast', 'BidAsk', 'MidPoint'], product, number_of_ticks=0, ignore_size=False):
    #     """Level 1 orderbook/Trade data/Mid-point data"""
    #     self.reqTickByTickData(
    #         self._request_id,
    #         product,
    #         tick_type,
    #         # IB will continue sending ticks to you if set to 0
    #         number_of_ticks,
    #         ignore_size,
    #     )
    #     self._logger.debug(f'{self.NAME} requested (req_id={self._request_id}) {product.symbol} tick by tick data ({tick_type=})')
    #     self._update_request_id_and_corresponding_product(product)

    def _request_market_depth(self, **kwargs):
        """Level 2 orderbook"""
        product = kwargs['product']
        orderbook_depth = self._orderbook_depth[product.pdt]
        self.reqMktDepth(
            self._request_id,
            product,
            orderbook_depth,
            kwargs.get('isSmartDepth', False), 
            []  # for IB internal use only                       
        )
        self._logger.debug(f'{self.NAME} requested (req_id={self._request_id}) {product.pdt} market depth ({orderbook_depth=})')
        self._update_request_id_and_corresponding_product(product)

    def _request_real_time_bar(self, **kwargs):
        """5 Seconds Real Time Bars"""
        product = kwargs['product']
        self.reqRealTimeBars(
            self._request_id,
            product,
            kwargs['period'],
            kwargs.get('whatToShow', 'TRADES'),
            kwargs.get('useRTH', False),
            []  # for IB internal use only
        )
        self._logger.debug(f'{self.NAME} requested (req_id={self._request_id}) {product.pdt} real time bar ({bar_size=})')
        self._update_request_id_and_corresponding_product(product)


    """
    private channels    
    ---------------------------------------------------
    """
    def _request_account_updates(self, acc: str):
        self.reqAccountUpdates(True, acc)

    # TODO
    def _request_account_updates_multi(self):
        self.reqAccountUpdatesMulti()
        
    def _request_account_summary(self, **kwargs):
        self.reqAccountSummary(
            self._request_id,
            kwargs.get('groupName', 'All'),
            kwargs.get('tags', AccountSummaryTags.AllTags)
        )
        self._increment_request_id()