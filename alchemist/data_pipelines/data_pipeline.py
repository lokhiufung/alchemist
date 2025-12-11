import pytz
import typing

import pandas as pd

from trading_data.datalake_client import DatalakeClient
from trading_data.common.date_ranges import get_dates

from alchemist.products.base_product import BaseProduct
from alchemist.data_pipelines.base_data_pipeline import BaseDataPipeline
from alchemist import standardized_messages


class DataPipeline(BaseDataPipeline):
    def __init__(self, data_source):
        self.data_source = data_source
        self.dl_client = DatalakeClient()

    def historical_bars(self, product: BaseProduct, freq, start: str, end: str) -> list:
        # 1. load data as a single pandas dataframe
        # TODO: hard code
        if product.product_type == 'FUTURE':
            pdt_type = 'future'
        else:
            pdt_type = product.product_type.lower()

        df = self.load_data(
            pdt=product.name,
            pdt_type=pdt_type,
            start_date=start,
            end_date=end,
        )
        # 2. normalize the data into stardardized updates
        updates = []
        for row in df.itertuples():
            # Set ts to be a Unix timestamp in seconds
            eastern = pytz.timezone("US/Eastern")
            ts = int(eastern.localize(row.Index.to_pydatetime()).timestamp())
            msg = standardized_messages.create_bar_message(
                ts=ts,
                gateway='xx_gateway',  # TODO
                exch=product.exch,
                pdt=product.name,
                resolution=freq,
                open_=row.open,
                high=row.high,
                low=row.low,
                close=row.close,
                volume=row.volume
            )
            updates.append(msg[-1][-1])
        return updates

    def start(self):
        pass

    def end(self):
        pass

    def load_data(self, pdt: str, pdt_type: str, start_date: str, end_date: str) -> pd.DataFrame:
        dates = get_dates(start_date, end_date)
        dfs = []
        for date in dates:
            try:
                df = self.dl_client.get_table(self.data_source, pdt, ver_name='min_bar', date=date, asset_type=pdt_type)
                dfs.append(df)
            except:
                continue  # TODO can log the message for INFO
        df = pd.concat(dfs)
        return df

