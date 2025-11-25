# Changelog

## [2.0.0] - 2025-11-24

### Breaking
- Indicator API now requires `DataLine` inputs (with optional `ts_line`) instead of `BarData` objects; all indicators updated to the new signature. Strategies/tests must pass specific lines (e.g., `bar_data.close`, `bar_data.ts`).
- `BaseIndicator` centralizes readiness/rollover guards for time- and bar-driven updates; subclasses now implement only core math.

### Features
- Added per-indicator unit tests for EMA, Bollinger Bands (including width), ATR, STD, Log Returns, and Daily VWAP.
- Introduced `Frequency.to_start_index` for consistent time-boundary handling in time-driven indicators (e.g., VWAP rollovers).

### Refactor
- Refined rolling computations in indicators (SMA, Bollinger) to use incremental sums for latency-sensitive updates.
- Base plumbing now attaches listeners to `DataLine`s to drive push-based updates and maintains last-seen guards to avoid duplicate emissions.

## [1.2.1] - 2025-11-24

### Features
- Added a `DataLine` abstraction so indicators can subscribe to specific data series with push-based updates while tracking per-line lengths.
- Refactored `BarData` and `TickData` to store each field in dedicated `DataLine`s and return typed `Bar`/`Tick` objects from slices, keeping total counts in sync.
- Updated indicator plumbing to listen on data timestamps and auto-append output values per tick; aligned `RsiIndicator` with the new data-line model.
- Hardened order submission by blocking pending/over-betting scenarios and broadened test coverage for data handling and order lifecycles.

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
