from alchemist.strategies.moving_average_strategy.moving_average_strategy import MovingAverageStrategy
from alchemist.data_pipelines.data_pipeline import DataPipeline


strategy_actor = MovingAverageStrategy.remote()
data_pipeline = DataPipeline()


strategy_actor.start_backtesting(
    data_pipeline=data_pipeline,
)