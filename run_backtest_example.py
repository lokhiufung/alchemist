import ray

from alchemist.strategies.moving_average_strategy.moving_average_strategy import MovingAverageStrategy
from alchemist.data_pipelines.data_pipeline import DataPipeline
from alchemist.data_card import DataCard
from alchemist.products.future_product import FutureProduct


product = FutureProduct('MES', 'USD', 'CME', '2025-06')
data_cards= [
    DataCard(product, '1m', 'ohlcv', False)    
]

strategy_actor = MovingAverageStrategy.remote(
    name='sma_strategy',
    zmq_send_port=12345,
    zmq_recv_ports=[12346],
    products=[product],
    data_cards=data_cards,
)

data_pipeline = DataPipeline(
    data_source='ib'
)


# data_pipeline.start()

# updates = data_pipeline.historical_bars(
#     product=product,
#     freq='1m',
#     start='2025-02-01',
#     end='2025-05-01'
# )

ray.get(strategy_actor.start_backtesting.remote(
    data_pipeline=data_pipeline,
    start_date='2025-02-01',
    end_date='2025-05-01',
    initial_cash=10000.0,
))

