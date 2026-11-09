from dataclasses import dataclass

from alchemist.registry import register_signal_dag
from alchemist.strategy_builder.signal_dag import Graph, SignalDag, SignalParams


@register_signal_dag("macd_signal_dag")
class MacdSignalDag(SignalDag):

    @dataclass(frozen=True)
    class Params(SignalParams):
        session: str = "regular"
        direction: str = "long"
        max_trades_per_day: int = 1
        warm_up_months: int = 2
        rearm_required: bool = True
        macd_fast: int = 12
        macd_slow: int = 26
        macd_signal_period: int = 9
        rsi_period: int = 14
        rsi_max: float = 70.0
        atr_period: int = 14
        target_atr: float = 0.5
        swing_lookback_bars: int = 120
        skip_open_bars: int = 300
        last_entry_bars: int = 1800
        flat_before_close_bars: int = 60

    def build_graph(self, g: Graph, p: Params) -> None:
        close = g.bar("close")
        low = g.bar("low")
        entry_price = g.strategy_data("entry_price")

        ema_fast = g.calc("ema_fast", "ema", close, period=p.macd_fast)
        ema_slow = g.calc("ema_slow", "ema", close, period=p.macd_slow)
        macd_line = g.expr("macd_line", lambda fast, slow: fast - slow, ema_fast, ema_slow)
        macd_signal = g.calc("macd_signal", "ema", macd_line, period=p.macd_signal_period)
        rsi = g.calc("rsi", "rsi", close, period=p.rsi_period)
        atr = g.warmup(g.calc("atr", "daily_atr", period=p.atr_period), history="daily")
        bars_into_session = g.calc("bars_into_session", "session_bars_elapsed", session=p.session)
        bars_to_close = g.calc("bars_to_close", "session_bars_left", session=p.session)

        target_px = g.expr(
            "target_px", lambda entry, atr: entry + atr * p.target_atr, entry_price, atr)
        stop_px = g.freeze_at_entry(
            g.calc("stop_px", "lowest", low, period=p.swing_lookback_bars))

        g.entry(
            g.condition("macd_above_signal", macd_line, ">", macd_signal),
            g.condition("rsi_below_max", rsi, "<", p.rsi_max),
            g.condition("after_open_skip", bars_into_session, ">", p.skip_open_bars),
            g.condition("before_last_entry", bars_to_close, ">", p.last_entry_bars),
        )
        g.exit("stop", close, "<=", stop_px)
        g.exit("target", close, ">=", target_px)
        g.exit("macd_below_signal", macd_line, "<", macd_signal)
        g.exit("end_of_day", bars_to_close, "<=", p.flat_before_close_bars)
