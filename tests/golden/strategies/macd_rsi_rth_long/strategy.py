from alchemist.registry import register_strategy
from alchemist.strategy_builder.strategy import Strategy


@register_strategy("macd_rsi_rth_long")
class MacdRsiRthLongStrategy(Strategy):
    signal_dag = "macd_signal_dag"

    def signal_snapshot(self) -> dict:
        values = self.signal.values()
        return {name: values.get(name) for name in self.snapshot_names}
