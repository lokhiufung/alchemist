from alchemist.gateways.gateway import Gateway
from alchemist.gateways.ib_gateway import IbGateway
from alchemist.gateways.simulation_gateway import OrderFillingEngine, SimulationGateway
from alchemist.gateways.utils import OrderIdSequence, check_order, round_price

GATEWAYS: dict[str, type[Gateway]] = {"simulation": SimulationGateway, "ibkr": IbGateway}


def get_gateway(name: str) -> type[Gateway]:
    if name not in GATEWAYS:
        raise KeyError(f"no gateway named {name!r}; known: {sorted(GATEWAYS)}")
    return GATEWAYS[name]


__all__ = ["GATEWAYS", "Gateway", "IbGateway", "OrderFillingEngine", "OrderIdSequence", "SimulationGateway",
           "check_order", "get_gateway", "round_price"]
