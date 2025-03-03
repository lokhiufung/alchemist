import threading
import time
from datetime import datetime
import ray
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from alchemist.monitors.base_monitor import BaseMonitor
from alchemist.timescaledb.models import Signal, Trade, Order, Position, Portfolio
from alchemist.enums import *
from alchemist.buffer import Buffer
from alchemist.logger import get_logger


@ray.remote
class TimescaleDBMonitor(BaseMonitor):
    def __init__(self, db_url: str, flush_interval: float = 60.0, is_test=False):
        """
        Initialize the TimescaleDB Monitor.

        :param db_url: Database URL for TimescaleDB connection.
        :param flush_interval: Time interval (in seconds) to flush buffers periodically.
        """
        self.is_test = is_test  # a work around for testing, since methods of a ray actor cannot be patched
        self.db_url = db_url
        # self.engine = create_engine(db_url)
        # self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

        self.buffer = Buffer()  # Shared buffer for all data types
        self.flush_interval = flush_interval

        # Initialize logger
        self._logger = get_logger("timescaledb_monitor", console_logger_lv="info", file_logger_lv="debug")

        self.is_flush_thread_started = False
        if not self.is_test:
            # Threading for periodic flushing
            self._stop_event = threading.Event()
            self._flush_thread = threading.Thread(target=self._flush_buffers_periodically, daemon=True)
            self._flush_thread.start()
            self.is_flush_thread_started = True
            self._logger.info('Flush thread started.')

    def _get_session(self) -> Session:
        engine = create_engine(self.db_url)
        SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
        return SessionLocal()

    def log_signal(
            self, 
            strategy_name: str, 
            ts: datetime, 
            signal_name: str, 
            value: float
        ):
        self.buffer.add("signals", {
            "strategy_name": strategy_name,
            "ts": ts,
            "signal_name": signal_name,
            "value": value,
        })

    def log_trade(
            self,
            strategy_name: str,
            ts: datetime,
            oid: str,
            pdt: str,
            filled_price: float,
            filled_size: float,
            side: str,
        ):
        self.buffer.add("trades", {
            "strategy_name": strategy_name,
            "ts": ts,
            'order_id': oid,
            "product": pdt,
            "filled_price": filled_price,
            "filled_size": filled_size,
            "side": side,
        })

    def log_order(
        self,
        strategy_name: str,
        ts: datetime,
        oid: str,
        order_type: str,
        time_in_force: str,
        pdt: str,
        status: str,
        size: float,
        price: float,
        side: str,
    ):
        self.buffer.add("orders", {
            "strategy_name": strategy_name,
            "ts": ts,
            "order_id": oid,
            "order_type": order_type,
            "time_in_force": time_in_force,
            "product": pdt,
            "status": status,
            "size": size,
            "price": price,
            "side": side,
        })

    def log_position(
        self,
        strategy_name: str,
        ts: datetime,
        pdt: str,
        size: float,
        side: str,
        avg_price: float,
        realized_pnl: float = None,
        unrealized_pnl: float = None,
    ):
        self.buffer.add("positions", {
            "strategy_name": strategy_name,
            "ts": ts,
            "product": pdt,
            "size": size,
            "side": side,
            "avg_price": avg_price,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
        })

    def log_portfolio(
        self,
        strategy_name: str,
        ts: datetime,
        balance: float,
        margin_used: float,
        total_value: float,
    ):
        self.buffer.add("portfolios", {
            "strategy_name": strategy_name,
            "ts": ts,
            "balance": balance,
            "margin_used": margin_used,
            "total_value": total_value,
        })

    def _pop(self, buffer_name: str):
        return self.buffer.pop(buffer_name)
    
    def _flush_buffer(self, buffer_name: str, model):
        """Flush a specific buffer to the database."""
        if not self.buffer.has_data(buffer_name):
            return

        session = self._get_session()
        data_to_flush = self._pop(buffer_name)
        try:
            session.bulk_save_objects([model(**data) for data in data_to_flush])
            session.commit()
            self._logger.info(f"Successfully flushed {len(data_to_flush)} entries from buffer '{buffer_name}' to the database.")
        except Exception as e:
            self._logger.error(f"Failed to flush buffer '{buffer_name}': {e}")
            session.rollback()
            self.buffer._buffers[buffer_name].extend(data_to_flush)  # Restore on failure
        finally:
            session.close()

    def _flush_all_buffers(self):
        """Flush all buffers to the database."""
        for buffer_name, model in [
            ("signals", Signal),
            ("trades", Trade),
            ("orders", Order),
            ("positions", Position),
            ("portfolios", Portfolio),
        ]:
            self._flush_buffer(buffer_name, model)

    def _flush_buffers_periodically(self):
        """Periodically flush all buffers using threading."""
        while not self._stop_event.is_set():
            time.sleep(self.flush_interval)
            self._flush_all_buffers()

    def stop(self):
        """Stop the monitor and the flushing thread."""
        if self.is_flush_thread_started:
            self._stop_event.set()
            self._flush_thread.join()
            self._logger.info("Flush Thread stopped.")