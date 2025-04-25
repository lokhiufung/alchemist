import threading
import time
from datetime import datetime

import ray
from telegram import Bot
from telegram.error import TelegramError

import re
_ESCAPE_RE_V1 = re.compile(r'([_*`\[\]\(\)])')

from alchemist.monitors.base_monitor import BaseMonitor


@ray.remote
class TelegramMonitor(BaseMonitor):
    """
    A monitor that sends “important” signals, trades, orders, positions or
    portfolio updates to a Telegram chat via a bot, once every `flush_interval`.
    """

    def __init__(
        self,
        bot_token: str,
        chat_id: str,
        flush_interval: float = 60.0,
        is_test: bool = False,
    ):
        """
        :param bot_token:   Telegram Bot API token (from BotFather)
        :param chat_id:     Target chat or user ID to send messages to
        :param flush_interval: Seconds between message‑batches
        :param is_test:     If True, won’t spin up the background thread
        """
        self.bot_token = bot_token
        self.chat_id = chat_id
        self.flush_interval = flush_interval
        self._buffers = {
            "signals": [],
            "trades": [],
            "orders": [],
            "positions": [],
            "portfolios": [],
        }
        self._lock = threading.Lock()
        self._stop_event = threading.Event()

        # Initialize Telegram Bot client
        self.bot = Bot(token=self.bot_token)

        if not is_test:
            self._thread = threading.Thread(
                target=self._flush_periodically, daemon=True
            )
            self._thread.start()

    def _add(self, category: str, payload: dict):
        with self._lock:
            self._buffers[category].append(payload)

    def log_signal(self, strategy_name, ts, signal_name, value):
        # Example criterion: only send RSI crosses above 70
        self._add("signals", {
            "ts": ts.isoformat(),
            "strategy": strategy_name,
            "signal": signal_name,
            "value": value,
        })

    def log_trade(self, strategy_name, ts, oid, pdt, filled_price, filled_size, side):
        # Example criterion: trades > $100k notional
        self._add("trades", {
            "ts": ts.isoformat(),
            "strategy": strategy_name,
            "order_id": oid,
            "product": pdt,
            "price": filled_price,
            "size": filled_size,
            "side": side,
        })

    def log_order(self, strategy_name, ts, oid, order_type, time_in_force,
                  pdt, status, size, price, side):
        self._add("orders", {
            "ts": ts.isoformat(),
            "strategy": strategy_name,
            "order_id": oid,
            "type": order_type,
            "tif": time_in_force,
            "product": pdt,
            "status": status,
            "size": size,
            "price": price,
            "side": side,
        })

    def log_position(self, strategy_name, ts, pdt, size, side,
                     avg_price, realized_pnl=None, unrealized_pnl=None):
        self._add("positions", {
            "ts": ts.isoformat(),
            "strategy": strategy_name,
            "product": pdt,
            "size": size,
            "side": side,
            "avg_price": avg_price,
            "realized_pnl": realized_pnl,
            "unrealized_pnl": unrealized_pnl,
        })

    def log_portfolio(self, strategy_name, ts, balance, margin_used, total_value):
        self._add("portfolios", {
            "ts": ts.isoformat(),
            "strategy": strategy_name,
            "balance": balance,
            "margin_used": margin_used,
            "total_value": total_value,
        })

    def _flush_periodically(self):
        while not self._stop_event.is_set():
            time.sleep(self.flush_interval)
            self._flush_all()

    def _flush_all(self):
        with self._lock:
            buffers, self._buffers = self._buffers, {
                "signals": [], "trades": [], "orders": [], "positions": [], "portfolios": []
            }

        for category, entries in buffers.items():
            if not entries:
                continue
            text = self._format_message(category, entries)
            self._send_telegram(text)

    def _format_message(self, category: str, entries: list) -> str:
        lines = [f"*{category.upper()}* ({len(entries)})"]
        for e in entries:
            kv = " | ".join(f"{k}={v}" for k, v in e.items())
            lines.append(f"- {kv}")
        return "\n".join(lines)

    def _escape_markdown(self, text: str) -> str:
        """
        Escape Telegram Markdown v1 special characters in the given text.
        """
        return _ESCAPE_RE_V1.sub(r'\\\1', text)

    def _send_telegram(self, text: str):
        try:
            self.bot.send_message(
                chat_id=self.chat_id,
                text=self._escape_markdown(text),
                parse_mode="Markdown"
            )
        except TelegramError as e:
            # fallback logging
            print(f"[TelegramMonitor] Telegram send failed: {e}")

    def stop(self):
        """Call this to cleanly stop the background thread."""
        self._stop_event.set()
        if hasattr(self, "_thread"):
            self._thread.join()