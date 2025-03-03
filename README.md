# Alchemist - A Ray-based High-Performance Automated Trading System

## Introduction

Alchemist is a high-performance automated trading system built on [Ray](https://www.ray.io/) that supports distributed processing for strategies and gateways. It is designed for advanced algorithmic trading, offering seamless integration with data sources and real-time order management.

---

## Quick Start

### 1. Setup

Install the dependencies using [Poetry](https://python-poetry.org/):

```bash
poetry install
```

### 2. Create Your Strategy Configuration File
```yaml
products:
  - type: alchemist.products.StockProduct
    name: test_pdt
    base_currency: USD
    exch: test_exch
  # - type: FutureProduct
  #   name: 'YM'
  #   base_currency: 'USD'
  #   exch: 'CBOT'
  #   contract_month: '2024-09'

# TODO: this actually hard-coded for IB. Need to make it more generic. Will make updates to the Account class.
accounts:
  - name: 'ib_account'
    client_id: 999
    host: "127.0.0.1"
    port: 4002
    acc: DU1474528

data_cards:
  - product: test_pdt  # this should match the product name to one of the products defined above
    freq: "1m"
    aggregation: ohlcv

order_ports: [5555, 5556]

# this is optional. monitor is default to be None
# monitor:
#   type: TimescaleDBMonitor
#   db_url: TIMESCALEDB_URI
  # flush_interval: 15.0

gateways:
  test_gateway:
    type: alchemist.gateways.MockGateway
    subscriptions: ["bar"]
    zmq_send_port: 5557
    zmq_recv_ports: [5555, 5556]
  # Uncomment and configure as needed
  # ib_gateway:
  #   type: IbGateway
  #   subscriptions: ["tick"]
  #   zmq_send_port: 5558
  #   zmq_recv_ports: [5555]
  #   accounts: [/* list account names or details if applicable */]

strategies:
  sma_strategy:
      type: alchemist.strategies.MovingAverageStrategy
      name: sma_strategy
      zmq_send_port: 5555
      zmq_recv_ports: [5557]
      # zmq_recv_ports: [5557, 5558]
      n_replicas: 1
```

## 3. Run the Automated Trading System using Ray Serve
```bash
serve run run_strategies:management_service
```

---

## Developer

### Running tests

```bash
pytest tests
# Run tests with coverage report and generate report in html
poetry run pytest --cov=alchemist --cov-report=html
# Run tests with coverage report and generate report in console
poetry run pytest --cov=alchemist --cov-report=term
```
