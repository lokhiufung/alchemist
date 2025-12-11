import polars as pl
from alchemist.products.base_product import BaseProduct
from alchemist.data_pipelines.base_data_pipeline import BaseDataPipeline
from alchemist import standardized_messages
from datetime import datetime
import pytz

class ParquetDataPipeline(BaseDataPipeline):
    def __init__(self, file_path: str):
        self.file_path = file_path

    def historical_bars(self, product: BaseProduct, freq, start: str, end: str) -> list:
        # Load data
        try:
            df = pl.read_parquet(self.file_path)
        except Exception as e:
            print(f"Error loading parquet file {self.file_path}: {e}")
            return []

        # Normalize column names to lowercase
        df = df.rename({c: c.lower() for c in df.columns})

        # Identify datetime column
        ts_col = None
        for col in ['date', 'datetime', 'timestamp', 'ts']:
            if col in df.columns:
                ts_col = col
                break
        
        if not ts_col:
            # Check for any datetime type column if name match fails?
            print("No datetime index found in parquet file (checked: date, datetime, timestamp, ts).")
            return []

        # Ensure sorted
        if not df[ts_col].is_sorted():
             df = df.sort(ts_col)

        # Filter by date
        # Assuming start/end are strings 'YYYY-MM-DD'
        predicates = []
        if start:
            try:
                s_dt = datetime.strptime(start, "%Y-%m-%d")
                predicates.append(pl.col(ts_col) >= s_dt)
            except ValueError:
                print(f"Invalid start date format: {start}. Expected YYYY-MM-DD.")
        
        if end:
            try:
                # Inclusive end of day
                e_dt = datetime.strptime(end, "%Y-%m-%d").replace(hour=23, minute=59, second=59, microsecond=999999)
                predicates.append(pl.col(ts_col) <= e_dt)
            except ValueError:
                print(f"Invalid end date format: {end}. Expected YYYY-MM-DD.")

        if predicates:
            combined_filter = predicates[0]
            for p in predicates[1:]:
                combined_filter = combined_filter & p
            df = df.filter(combined_filter)

        if df.height == 0:
            return []

        # Timezone handling and epoch calculation
        dtype = df.schema[ts_col]
        
        # Cast to Datetime if it's Date
        if isinstance(dtype, pl.Date):
            df = df.with_columns(pl.col(ts_col).cast(pl.Datetime).alias(ts_col))
            dtype = df.schema[ts_col]

        # Prepare timestamp expression
        ts_expr = pl.col(ts_col)
        
        if isinstance(dtype, pl.Datetime):
             if dtype.time_zone is None:
                 # Naive: assume US/Eastern
                 # replace_time_zone sets the timezone for naive datetimes
                 ts_expr = ts_expr.dt.replace_time_zone("US/Eastern")
             
             # Convert to epoch seconds
             ts_expr = ts_expr.dt.epoch(time_unit="s")
        else:
             # Try to handle string or other types if implementation allows, otherwise error/warn
             # For now, let's assume it's castable or error out
             try:
                 # Attempt string to datetime
                ts_expr = ts_expr.str.to_datetime(strict=False).dt.replace_time_zone("US/Eastern").dt.epoch(time_unit="s")
             except:
                 print(f"Could not convert {ts_col} to datetime.")
                 return []

        # Ensure required columns exist
        required_cols = ['open', 'high', 'low', 'close', 'volume']
        available_cols = df.columns
        for col in required_cols:
            if col not in available_cols:
                df = df.with_columns(pl.lit(0.0).alias(col))

        # Select columns and execute
        df_processed = df.select([
            ts_expr.alias("ts"),
            pl.col("open").fill_null(0.0),
            pl.col("high").fill_null(0.0),
            pl.col("low").fill_null(0.0),
            pl.col("close").fill_null(0.0),
            pl.col("volume").fill_null(0.0)
        ])
        
        updates = []
        gateway = 'parquet'
        exch = product.exch
        pdt = product.name
        resolution = freq
        
        # Iterate and construct messages
        rows = df_processed.iter_rows(named=True)
        for row in rows:
            if row['ts'] is None:
                continue
                
            msg = standardized_messages.create_bar_message(
                ts=int(row['ts']), 
                gateway=gateway,
                exch=exch,
                pdt=pdt,
                resolution=resolution,
                open_=row['open'],
                high=row['high'],
                low=row['low'],
                close=row['close'],
                volume=row['volume']
            )
            # stored data structure in the message
            updates.append(msg[-1][-1])

        return updates

    def start(self):
        pass

    def stop(self):
        pass
