# Changelog

## [1.2.0] - 2025-11-20

### Features
- Introduced `DashboardMonitor` for real-time visualization of system latencies and order slippage using Dash.
- Added dynamic graph generation in `DashboardMonitor` to automatically display all tracked latency metrics.
- Propagated order creation timestamps across `BaseGateway`, `OrderManager`, and `BaseStrategy` to enable precise latency measurement.
- Added unit tests for betting cycle logic to ensure correct state transitions and order handling in `BaseStrategy`.

### Refactor
- Refactored `DashboardMonitor` layout to support flexible and dynamic latency metric visualization.

## [1.1.0] - 2025-10-22

### Features
- Enhance order and position management with risk status checks; implement trading period logic in BaseStrategy
- Add trading_periods parameter to BaseStrategy; implement trading period logic for time-dependent trading
- Add .vscode to .gitignore; create test suite for BacktestManager with comprehensive unit tests
- Update check_highest_resolution method to include frequency parameter; enhance unit tests for resolution checks
- Implement RSI calculation in RsiIndicator; add unit tests for RSI functionality
- Add FutureContractCommission class with margin and multiplier attributes
- Add Commission class and integrate commission handling in BacktestManager; update BaseStrategy to pass commission during backtesting

### Fixes
- Refactor RSI calculation to use recent bars directly for improved clarity
- Correct commission attribute typo and enhance margin handling in BacktestManager; update example to include commission details

### Refactor
- Remove debug print statements from DataPipeline; adjust realized PnL calculations in BacktestManager; update exchange assignment in BaseStrategy

### Docs
- Add backtesting section to README
