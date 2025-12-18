from datetime import datetime

import polars as pl

from alchemist.products.base_product import BaseProduct
from alchemist.data_pipelines.base_data_pipeline import BaseDataPipeline
from alchemist import standardized_messages


class ParquetDataPipeline(BaseDataPipeline):

    def __init__(self, file_path: str):
        self.file_path = file_path

    def select_rows_by_date_range(self, df: pl.DataFrame, start: str=None, end: str=None) -> pl.DataFrame:
        # Filter by date
        # Assuming start/end are strings 'YYYY-MM-DD'
        predicates = []
        if start:
            try:
                s_dt = datetime.strptime(start, "%Y-%m-%d")
                predicates.append(pl.col(self.TS_COL) >= s_dt)
            except ValueError:
                print(f"Invalid start date format: {start}. Expected YYYY-MM-DD.")
        
        if end:
            try:
                # Inclusive end of day
                e_dt = datetime.strptime(end, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
                predicates.append(pl.col(self.TS_COL) <= e_dt)
            except ValueError:
                print(f"Invalid end date format: {end}. Expected YYYY-MM-DD.")

        if predicates:
            combined_filter = predicates[0]
            for p in predicates[1:]:
                combined_filter = combined_filter & p
            df = df.filter(combined_filter)
        
        return df

    def adjust_timezone(self, df: pl.DataFrame) -> pl.DataFrame:
        # Timezone handling and epoch calculation
        dtype = df.schema[self.TS_COL]

        # Prepare timestamp expression
        ts_expr = pl.col(self.TS_COL)

        if dtype.time_zone is None:
            # Naive: assume US/Eastern
            # replace_time_zone sets the timezone for naive datetimes
            ts_expr = ts_expr.dt.replace_time_zone("US/Eastern")
        
        # Convert to epoch seconds
        ts_expr = ts_expr.dt.epoch(time_unit="s")

        return df.with_columns(ts_expr.alias(self.TS_COL)) 

    def sanity_check_required_columns(self, df: pl.DataFrame):
        required_cols = [
            self.OPEN_COL,
            self.HIGH_COL,
            self.LOW_COL,
            self.CLOSE_COL,
            self.VOLUME_COL
        ]
        
        available_cols = df.columns

        for col in required_cols:
            if col not in available_cols:
                raise ValueError(f"Missing required column: {col}")
    
    def sanity_check_missing_values(self, df: pl.DataFrame):
        required_cols = [
            self.OPEN_COL,
            self.HIGH_COL,
            self.LOW_COL,
            self.CLOSE_COL,
            self.VOLUME_COL
        ]

        for col_name in required_cols:
            if col_name in df.columns:
                # Total number of missing values
                total_missing = df[col_name].is_null().sum()

                # Highest number of consecutive missing values
                consecutive_missing = df.with_columns(
                    (pl.col(col_name).is_null()).alias("is_null")
                ).with_columns(
                    pl.when(pl.col("is_null"))
                    .then(pl.col("is_null").cum_sum().over(pl.col("is_null").rle_id()))
                    .otherwise(0)
                    .alias("consecutive_nulls")
                )["consecutive_nulls"].max()
                
                if total_missing > 0:
                    print(f"Column '{col_name}': {total_missing} missing values.")
                if consecutive_missing > 0:
                    print(f"Column '{col_name}': Highest consecutive missing values is {consecutive_missing}.")

    def create_resampled_cols(self, df: pl.DataFrame, freqs: list[str]) -> pl.DataFrame:
        # use native polars to add resampled columns for each freq
        df = df.sort(self.TS_COL)

        for freq in freqs:
            resampled_df = df.group_by_dynamic(self.TS_COL, every=freq).agg([
                pl.col(self.OPEN_COL).first().alias(f"open_{freq}"),
                pl.col(self.HIGH_COL).max().alias(f"high_{freq}"),
                pl.col(self.LOW_COL).min().alias(f"low_{freq}"),
                pl.col(self.CLOSE_COL).last().alias(f"close_{freq}"),
                pl.col(self.VOLUME_COL).sum().alias(f"volume_{freq}"),
                # capture the timestamp of the last bar in the window so we can align on it
                pl.col(self.TS_COL).last().alias("bin_last_ts")
            ])

            df = df.join(
                    resampled_df,
                    left_on=self.TS_COL,
                    right_on="bin_last_ts",
                    how="left"
                )

        return df

    def load_and_process_data(
        self,
        start_date: str,
        end_date: str,
        auto_resample_freqs: list[str] = None
    ) -> pl.DataFrame:
        # Load data
        try:
            df = pl.read_parquet(self.file_path)
        except Exception as e:
            print(f"Error loading parquet file {self.file_path}: {e}")
            return []

        # Normalize column names to lowercase
        df = df.rename({c: c.lower() for c in df.columns})

        # Ensure sorted
        if not df[self.TS_COL].is_sorted():
            df = df.sort(self.TS_COL)


        df = self.select_rows_by_date_range(df, start_date, end_date)

        # df = self.adjust_timezone(df)

        # Perform sanity checks
        # 1. check if the required columns are present
        self.sanity_check_required_columns(df)
        # 2. check for missing values in required columns
        self.sanity_check_missing_values(df)


        # Select columns and execute, if missing values exist, forward fill them
        df = df.select([
            pl.col(self.TS_COL),
            pl.col(self.OPEN_COL).forward_fill(),
            pl.col(self.HIGH_COL).forward_fill(),
            pl.col(self.LOW_COL).forward_fill(),
            pl.col(self.CLOSE_COL).forward_fill(),
            pl.col(self.VOLUME_COL).forward_fill()
        ])
        
        #  3. check for missing values after forward fill
        self.sanity_check_missing_values(df)
        
        if auto_resample_freqs:
            df = self.create_resampled_cols(df, freqs=auto_resample_freqs)

        return df
    
    def create_bar_message(
        self,
        ts: int,
        gateway: str,
        exch: str,
        pdt: str,
        resolution: str,
        open_: float,
        high: float,
        low: float,
        close: float,
        volume: float
    ):
        msg = standardized_messages.create_bar_message(
            # conver datetime back to epoch int
            ts=ts,
            gateway=gateway,
            exch=exch,
            pdt=pdt,
            resolution=resolution,
            open_=open_,
            high=high,
            low=low,
            close=close,
            volume=volume
        )
        return msg[-1][-1]

    def historical_bars(
        self,
        product: BaseProduct,
        freq: str,
        start_date: str,
        end_date: str,
        auto_resample_freqs: list[str] = None
    ) -> list:
        
        df = self.load_and_process_data(
            start_date=start_date,
            end_date=end_date,
            auto_resample_freqs=auto_resample_freqs
        )

        updates = []
        gateway = 'parquet'  # TEMP: maybe problematic
        exch = product.exch
        pdt = product.name
        resolution = freq
        
        # Iterate and construct messages
        rows = df.iter_rows(named=True)
        for row in rows:
            if row['ts'] is None:
                continue

            # update the base frequency
            updates.append(self.create_bar_message(
                    # conver datetime back to epoch int
                    ts=int(row['ts'].timestamp()),
                    gateway=gateway,
                    exch=exch,
                    pdt=pdt,
                    resolution=resolution,
                    open_=row['open'],
                    high=row['high'],
                    low=row['low'],
                    close=row['close'],
                    volume=row['volume']
                ))
            
            # update the auto resampled frequencies
            if auto_resample_freqs:
                for auto_freq in auto_resample_freqs:
                    if row[f'open_{auto_freq}']:
                        updates.append(self.create_bar_message(
                            # conver datetime back to epoch int
                            ts=int(row['ts'].timestamp()),
                            gateway=gateway,
                            exch=exch,
                            pdt=pdt,
                            resolution=auto_freq,
                            open_=row[f'open_{auto_freq}'],
                            high=row[f'high_{auto_freq}'],
                            low=row[f'low_{auto_freq}'],
                            close=row[f'close_{auto_freq}'],
                            volume=row[f'volume_{auto_freq}']
                        ))

        return updates

    def start(self):
        pass

    def stop(self):
        pass
