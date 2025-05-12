# Alchemist - A Ray-based High-Performance Automated Trading System

## **What is Alchemist?**

**Alchemist** is a high-performance, distributed **automated trading system** designed to seamlessly transition **quantitative research into fully automated algorithmic trading strategies**.

Built on **Ray**, Alchemist provides a scalable infrastructure for strategy execution, real-time order management, and seamless integration with market data and broker connections. Whether you’re a **quantitative researcher, algorithmic trader, or hedge fund**, Alchemist helps you **deploy, manage, and scale trading strategies efficiently**.

## **Why Alchemist?**

🚀 **Fast and Scalable** – Built with **Ray** to handle large-scale trading strategies across multiple processes and nodes.

🔌 **Seamless Integration** – Connects easily to market data providers, brokers, and exchanges.

📈 **Backtesting & Live Trading** – Develop strategies, backtest them, and switch to live trading **without rewriting code**.

⚡ **Real-Time Order Management** – Handles execution, risk management, and portfolio exposure dynamically.

🔧 **Customizable Strategy Framework** – A modular system that allows users to develop and optimize their own strategies.

Alchemist is **part of a broader quantitative trading ecosystem**, providing a structured way to move from **strategy research** to **live trading** with minimal friction.

## **Why Do You Need Alchemist?**

### **Problem: Gap from Research to Execution**

Quantitative traders often struggle with the transition from **research to live deployment** due to:

- **Scalability Issues** – Traditional execution frameworks cannot handle large-scale strategies efficiently.
- **Infrastructure Complexity** – Research environments are often disconnected from live market execution.
- **Computational Bottlenecks** – High-performance backtesting and execution require **parallelized, distributed computing**.
- **Integration Challenges** – Many systems lack easy connectivity with brokers, exchanges, and real-time market data.

### **Solution**

Alchemist **eliminates these roadblocks** by providing a fully integrated, high-performance framework that **connects research, backtesting, and execution in one system**.

Whether you’re working on **statistical arbitrage, market-making, or machine learning-based trading**, Alchemist ensures your strategies can transition **from development to live execution seamlessly**

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

### 3. Run the Automated Trading System using Ray Serve
```bash
serve run run_strategies:management_service
```

## Version of TWS
```text
latest 10.35.1e 20250324
```
---

## Developer Guide

Please visit the [full documentation](https://boulder-submarine-0ae.notion.site/Alchemist-A-Ray-based-High-Performance-Automated-Trading-System-1ace87b87fa4803cb9ade11517148d65?pvs=4) for detailed information on setup, configuration, and advanced usage.


### Running tests

```bash
pytest tests
# Run tests with coverage report and generate report in html
pytest --cov=alchemist --cov-report=html
# Run tests with coverage report and generate report in console
pytest --cov=alchemist --cov-report=term
```

## Interactive Brokers

### Volume is scaled by 100 for U.S. stocks
For U.S. equities, IB reports the exchange volume divided by 100. So if 3 410 shares traded in that 5s window, IB sends 34.10. [source](https://ibridgepy.com/ib-api-knowledge-base/#step1-2-1)

### `averagePrice` field includes the commission fee in `updatePortfolio`
The TWS “Avg Price” field (which maps to the API’s averageCost) is defined as:
“Average price is calculated by dividing your cost (execution price + commission) by the quantity of your position.”

### `realtimeBars` is only available in `5s`
Trader Workstation provides only 5 secs bars for real time. Therefore, the LOWEST resolution in `IBGateway` is `5s`.
That means if you want to trade exactly `1m` bars, you need to configure **2 data cards** - 1 for `5s` bars and the other for `1m` bars. Otherwise, `next` will be called for every `5s` bar. (`next` will be called only at syncronization of `5s` and `1m` bars. Hence every `1m`.)


## Code Attribution and Origin

Alchemist is an independent, high-performance automated trading system built using Ray. While some architectural concepts were inspired by previous open-source projects I was involved in, this system has been built from the ground up with a redesigned architecture.

This project is not affiliated with or derived from any specific open-source repository, and any similarities reflect common design patterns in algorithmic trading systems.


## Third-Party Code Attribution

This project includes code adapted from an open-source project licensed under **Apache License 2.0**:

- The file **`zeromq.py`** is adapted from [PFund](https://github.com/PFund-Software-Ltd/pfund), an open-source trading system.
- Modifications have been made to fit the architecture of **Alchemist**.
- The original **Apache 2.0 license notice is retained in `zeromq.py`** in accordance with open-source licensing requirements.

Alchemist is **not affiliated with or endorsed by the authors of Pfund**. Any concerns regarding code attribution are welcome, and I am committed to maintaining transparency and compliance with open-source licensing.