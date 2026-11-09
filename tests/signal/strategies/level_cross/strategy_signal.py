from dataclasses import dataclass

from alchemist.registry import register_signal_dag
from alchemist.strategy_builder.signal_dag import Graph, SignalDag, SignalParams


@register_signal_dag("level_cross")
class LevelCross(SignalDag):

    @dataclass(frozen=True)
    class Params(SignalParams):
        session: str = "regular"
        direction: str = "long"
        rearm_required: bool = True
        level: float = 100.0
        target: float = 110.0
        sma_period: int = 5
        stop_offset: float = 0.5

    def build_graph(self, g: Graph, p: Params) -> None:
        close = g.bar("close")
        sma5 = g.warmup(g.calc("sma5", "sma", close, period=p.sma_period), history="intraday")
        stop_px = g.freeze_at_entry(
            g.expr("stop_px", lambda sma: sma - p.stop_offset, sma5))

        g.entry(g.condition("above_level", close, ">", p.level))
        g.exit("stop", close, "<", stop_px)
        g.exit("target", close, ">=", p.target)
