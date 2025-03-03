import ray
from ray import serve


@serve.deployment
class TradeManagementService:
    def __init__(self, gateway_handlers, strategy_handlers):
        self.gateway_handlers = gateway_handlers
        self.strategy_handlers = strategy_handlers
        
        # Connectivity status
        self.gateway_connection_statuses = {}
        self.strategy_connection_statuses = {}

        # Initialize gateways and recorders
        self.gateway_futures = [gateway.start.remote() for _, gateway in self.gateway_handlers.items()]
        self.strategy_futures = [strategy.start.remote() for _, strategy in self.strategy_handlers.items()]

    async def get_connection_status(self, request):
        return {
            "gateways": self.gateway_connection_statuses,
            "strategies": self.strategy_connection_statuses,
        }

    async def shutdown(self):
        for gateway_name, gateway in self.gateway_handlers.items():
            await gateway.shutdown.remote()
        for strategy_name, strategy in self.strategy_handlers.items():
            await strategy.shutdown.remote()
