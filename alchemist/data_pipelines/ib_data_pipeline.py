import threading
import time
from datetime import datetime, timedelta
from pytz import timezone, utc

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
from ibapi.common import BarData

from alchemist.data_pipelines.base_data_pipeline import BaseDataPipeline
from alchemist.products.base_product import BaseProduct
from alchemist.account import Account
from alchemist import standardized_messages


class IBApp(EWrapper, EClient):
    """
    A client wrapper for Interactive Brokers API to handle historical data requests.

    Attributes:
        data (list): A list to store retrieved historical bars.
        req_id (int): The request ID counter for API calls.
        clientId (int): The client ID for the connection.
        data_event (threading.Event): Event to signal data collection completion.
    """
    def __init__(self):
        """
        Initializes the IBApp instance with necessary attributes.
        """
        EClient.__init__(self, self)
        self.data = []
        self.req_id = 0
        self.clientId = 0  # You can set this to any integer
        self.data_event = threading.Event()

    def on_start_recording(self):
        """
        Clears the data event and resets the data buffer to start recording.
        """
        self.data_event.clear()
        self.data = []  # clean the data buffer too

    def on_stop_recording(self):
        """
        Sets the data event to indicate that recording has stopped.
        """
        self.data_event.set()

    def next_req_id(self):
        """
        Generates and returns the next request ID.

        Returns:
            int: The next request ID.
        """
        self.req_id += 1
        return self.req_id

    def request_historical_bars(
        self,
        reqId: int,
        contract: Contract,
        endDateTime: str,
        durationStr: str,
        barSizeSetting: str,
        whatToShow: str,
        useRTH: int,
        formatDate: int,
        keepUpToDate: bool,
        chartOptions: list,
    ) -> None:
        """
        Requests historical bar data from Interactive Brokers.

        Args:
            reqId (int): The request ID.
            contract (Contract): The IB contract for the product.
            endDateTime (str): The end date/time for the data.
            durationStr (str): The duration of data to retrieve.
            barSizeSetting (str): The size of the bars.
            whatToShow (str): The type of data to show (e.g., 'TRADES').
            useRTH (int): Whether to use regular trading hours (1 for yes, 0 for no).
            formatDate (int): Date format for the data.
            keepUpToDate (bool): Whether to keep data updated.
            chartOptions (list): Additional chart options.
        """
        self.on_start_recording()
        self.reqHistoricalData(
            reqId=reqId,
            contract=contract,
            endDateTime=endDateTime,
            durationStr=durationStr,
            barSizeSetting=barSizeSetting,
            whatToShow=whatToShow,
            useRTH=useRTH,
            formatDate=formatDate,
            keepUpToDate=keepUpToDate,
            chartOptions=chartOptions
        )

    def error(self, reqId: int, errorCode: int, errorString: str, advancedOrderRejectJson: str = "") -> None:
        """
        Handles errors from the Interactive Brokers API.

        Args:
            reqId (int): The request ID associated with the error.
            errorCode (int): The error code.
            errorString (str): The error message.
            advancedOrderRejectJson (str, optional): JSON details of the rejection.
        """
        print(f"Error {reqId}, {errorCode}: {errorString}")

    def historicalData(self, reqId: int, bar: BarData) -> None:
        """
        Collects historical bar data.

        Args:
            reqId (int): The request ID.
            bar (BarData): The bar data object.
        """
        self.data.append(bar)

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:
        """
        Indicates the end of historical data retrieval.

        Args:
            reqId (int): The request ID.
            start (str): The start date of the data range.
            end (str): The end date of the data range.
        """
        self.on_stop_recording()


class IBDataPipeline(BaseDataPipeline):
    """
    A pipeline for managing Interactive Brokers data retrieval.

    Attributes:
        account (Account): The account information.
        app (IBApp): The IB API application instance.
    """
    def __init__(self, account):
        """
        Initializes the pipeline with account details.

        Args:
            account (Account): The account details for the Interactive Brokers connection.
        """
        self.account = account
        self.app = IBApp()

    def start(self) -> None:
        """
        Starts the Interactive Brokers connection.
        """
        self.app.connect(self.account.host, self.account.port, clientId=self.account.client_id)
        # Start the socket in a thread
        self.app_thread = threading.Thread(target=self.app.run, daemon=True)
        self.app_thread.start()
        time.sleep(2)
    
    def stop(self) -> None:
        """
        Stops the Interactive Brokers connection.
        """
        self.disconnect()

    def convert_to_ib_contract(self, product: BaseProduct) -> Contract:
        """
        Converts a product into an IB contract.

        Args:
            product (BaseProduct): The product to convert.

        Returns:
            Contract: The Interactive Brokers contract.
        """
        contract = Contract()
        contract.symbol = product.name
        contract.exchange = product.exch
        contract.currency = product.base_currency
        if product.product_type == 'FUTURE':
            contract.secType = 'FUT'
            contract.lastTradeDateOrContractMonth = datetime.strptime(product.contract_month, "%Y-%m").strftime("%Y%m")
        elif product.product_type == 'STOCK':
            contract.secType = 'STK'
        return contract

    def historical_bars(self, product: BaseProduct, freq: str, start: str, end: str = None):
        """
        Retrieves historical bars for a product.

        Args:
            product (BaseProduct): The product to retrieve data for.
            freq (str): The frequency of bars.
            start (str): The start date in "%Y-%m-%d" format.
            end (str, optional): The end date. Defaults to None.

        Returns:
            list: A list of historical bars.
        """
        start_date = datetime.strptime(start, "%Y-%m-%d")
        updates = []
        updates = self.fetch_historical_bars(product, start_date, freq, end)
        return updates

    def fetch_historical_bars(self, product, start_date, freq, end_date=None):
        """
        Fetches historical bar data for a specific product.

        Args:
            product (BaseProduct): The product for which to fetch historical bars.
            start_date (datetime): The start date for the data retrieval.
            freq (str): The frequency of the bars (e.g., '1m', '5m').
            end_date (datetime, optional): The end date for the data retrieval. Defaults to None.

        Returns:
            list: A list of standardized bar messages containing the historical data.
        """
        contract = self.convert_to_ib_contract(product)
        barSize = self.map_freq_to_barSize(freq)
        if end_date is None:
            end_date = datetime.now()
        duration_str = self.calculate_duration(start_date, end_date)
        end_date_str = end_date.strftime("%Y%m%d %H:%M:%S")
        self.app.data = []
        self.app.data_event.clear()
        reqId = self.app.next_req_id()
        self.app.request_historical_bars(reqId, contract, end_date_str, duration_str, barSize, 'TRADES', 0, 1, False, [])
        # Wait until data is collected or timeout occurs
        # self.app.data_event.wait(timeout=30)  # Wait for up to 30 seconds
        self.app.data_event.wait()
        # Process the data collected
        updates = []
        for bar in self.app.data:
            # Parse bar.date to datetime
            ts = datetime.strptime(bar.date.split(' ')[0] + ' ' + bar.date.split(' ')[1], "%Y%m%d %H:%M:%S")
            msg = standardized_messages.create_bar_message(
                ts=ts,
                gateway='ib_gateway',
                exch=product.exch,
                pdt=product.name,
                resolution=freq,
                open_=bar.open,
                high=bar.high,
                low=bar.low,
                close=bar.close,
                volume=float(bar.volume)
            )
            updates.append(msg[-1][-1])
        return updates

    def map_freq_to_barSize(self, freq):
        """
        Maps the provided frequency to Interactive Brokers' bar size settings.

        Args:
            freq (str): The desired bar frequency (e.g., '1m', '5m', '1d').

        Returns:
            str: The corresponding bar size setting for IB API.
        """
        # Map our freq to ibapi barSize settings
        mapping = {
            '1s': '1 secs',
            '5s': '5 secs',
            '15s': '15 secs',
            '30s': '30 secs',
            '1m': '1 min',
            '2m': '2 mins',
            '3m': '3 mins',
            '5m': '5 mins',
            '15m': '15 mins',
            '30m': '30 mins',
            '1h': '1 hour',
            '1d': '1 day',
        }
        return mapping.get(freq, '1 min')

    def calculate_duration(self, start_date, end_date):
        """
        Calculates the duration string for historical data requests based on start and end dates.

        Args:
            start_date (datetime): The start date of the data range.
            end_date (datetime): The end date of the data range.

        Returns:
            str: A duration string compatible with IB API (e.g., '1 D', '2 W').
        """
        delta = end_date - start_date
        days = delta.days
        # seconds = delta.seconds
        total_seconds = delta.total_seconds()
        if days >= 365:
            years = days // 365
            duration = f"{years} Y"
        elif days >= 30:
            months = days // 30
            duration = f"{months} M"
        elif days >= 7:
            weeks = days // 7
            duration = f"{weeks} W"
        elif days >= 1:
            duration = f"{days} D"
        elif total_seconds >= 3600:
            hours = total_seconds // 3600
            duration = f"{int(hours)} H"
        elif total_seconds >= 60:
            minutes = total_seconds // 60
            duration = f"{int(minutes)} M"
        else:
            duration = f"{int(total_seconds)} S"
        return duration

    def disconnect(self):
        """
        Disconnects the Interactive Brokers connection and terminates the app thread.
        """
        self.app.disconnect()
        self.app_thread.join()