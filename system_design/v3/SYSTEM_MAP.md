# SYSTEM_MAP — Alchemist v3 execution engine

Frozen top-level boundary of Alchemist v3. Agents read this file to decide which component a change belongs to. Agents must not edit it: it changes only by re-running `/system-design` with the human, compiled from the HTML design pages in this folder (`system_map.html` is the human-readable source).

The top-level boundary of the Alchemist v3 execution engine: what it is for, the components it is split into, what each one owns, and the one frozen example every component is tested against.

**Last agreed: 2026-11-09 — v3 final.** v3 is an eight-component execution engine (decision 44). Its first requirement is multi-asset and multi-currency trading (R1, decisions 37–41), and a signal is a Python `SignalDag` class that holds its own params, behind an abstract interface (decision 42). All decisions 1–59 are agreed. Decisions 52–58 (and 44) were decided on 2026-11-09, when the human delegated the open questions. The human accepted every remaining proposed decision and finalized v3 on 2026-11-09. Change history: see `system_map.html`.

## Goal

**Execute a strategy book at the broker across asset classes (futures and stocks in v3; options in a later version, decision 54), instruments, exchanges and currencies, placing the same orders whether it runs on a simulated broker, a paper account or a live account.**

**Non-goals (not this system's job):**

- Strategy research: finding signals, fitting params, or any tooling that produces them.
- Comparing the engine's trades with research results, and monitoring strategies for decay.
- Keeping a registry of strategies and their params versions.
- Version control of strategies and their params: the strategy code under `local/strategies/` belongs to its author.
- Portfolio reporting in a base currency (see decision 39).

## Requirements

What the design must meet. Each links to the decisions that answer it.

| \# | Requirement | What it means | Answered by |
|----|----|----|----|
| R1 | **Multi-asset, multi-currency.** One process trades futures and stocks on several exchanges in their own currencies; options follow in a later version. | The first book is futures: NQ and ES on CME in USD, and FDAX on Eurex in EUR. v3 also trades stocks: the first stock exchange profile is NASDAQ, in USD. Options move to a later version (decision 54). Adding an instrument is configuration; adding an exchange is one new exchange profile; no component branches on an asset class except where the asset class changes the fact (how a contract is resolved). | 31, 37–41, 54 |
| R2 | **Extensible.** A new exchange, strategy, gateway or outside data source is added in one place, without editing unrelated code. | No file branches on an exchange name; gateways and data sources plug in through one interface each. | 30, 31, 32, 43 |
| R3 | **Scales to 10–15 strategies in v3.** Strategies do not hold each other up, and a bar becomes an order within 5 ms at p99. | v3 target: 3 tick-by-tick streams and 10–15 active strategies in one process, proved by a replay load test that must pass. 100 strategies, through process shards, moves to a later version. Two latency measures (decision 55). **Feed latency**: from the broker's timestamp on a message to the message's arrival in the engine (`recv_ts`, stamped on the Gateway's reader thread); measured and logged, not budgeted, because the engine does not control it. **Processing latency**: from a message's arrival to each strategy's final decision on it (an order handed to the Gateway, a cancel, or no action); the 5 ms p99 budget applies to processing latency. Slot cost and dispatch latency are logged as its internal parts. A fixed polling timer must not add queue wait to every event. | 25–29, 33 |
| R4 | **One code path.** Simulation, paper and live run the same components; only what sits behind the Gateway changes. | No separate simulator path that could drift from live. The simulated broker is one gateway that gives `bar_update` messages, like a broker that serves klines (decision 49). | 6, 9, 49 |
| R5 | **Add a strategy without a restart.** One strategy is added to a running paper or live process while the strategies already trading keep their connection, positions, orders, indicators and partly built bars. | The motivating case: a day-session strategy added in the evening while overnight strategies trade. | 34–36 |

## Introduction

Alchemist v3 is an execution engine for strategies on futures and stocks (options in a later version). Its inputs are the broker's market data (or a replay source standing in for the broker), daily auxiliary series from outside data sources, plus deployment config supplied from outside and the author's strategy classes, which hold each signal's params. Its first book trades CME equity-index futures in USD and Eurex DAX futures in EUR. Its only trading consumer is the broker, which receives its orders. A second outside actor, the **operator**, sends deploy requests and receives status lines and alerts. Secondary outputs are logs, bar and trade recordings, and, in replay, the broker journal (decision 2).

1.  Market data enters through the **Gateway**.
2.  The **Event Loop** turns it into bars and dispatches them. Before dispatch, **Aux Data** adds daily auxiliary values from outside data sources to each bar, and tells each strategy that reads them whether it is ready.
3.  Each **Strategy** steps its **StrategySignal** and passes the bar to its **StrategyExecution** state machine, which asks the signal whether to enter or exit.
4.  The resulting orders are tracked by the **Order Manager** and leave again through the Gateway.
5.  Alongside, **Safety** watches the broker connection, reconnects it, and repairs any strategy left waiting on an order during an outage.

Every instrument names its exchange profile and currency, so every bar, order, fill and position knows where it trades and what money it is counted in.

## Diagram

```
  ( broker: CME · Eurex · NASDAQ, or the simulated broker )
        │
        │ tick_update / bar_update, order updates, history, executions
        ▼
  ┌─────────────────────────┐
  │ Gateway                 │
  │ owns: all broker talk   │
  └─────────────────────────┘
        │
        ├── health, last tick, executions ──▶ ┌─────────────────────────┐
        │                                     │ Safety                  │
        │                                     │ owns: reconnect, repair │
        │   ◀── heartbeat, reconnect, queries └─────────────────────────┘
        │
        │ market data, order/position/balance updates, warmup history
        ▼
  ┌─────────────────────────┐
  │ Event Loop              │
  │ owns: bars + dispatch   │
  └─────────────────────────┘
        │
        ├── completed bar, book members ──▶ ┌─────────────────────────┐
        │                                   │ Aux Data                │   ◀── ( outside data sources ): daily series
        │                                   │ owns: aux + arming      │
        │   ◀── bar + aux values            └─────────────────────────┘
        │
        │ bars, order updates, warmup history, flatten
        ▼
  ┌─────────────────────────┐
  │ Strategy                │
  │ owns: position, driving │
  └─────────────────────────┘
        │
        ├── bars + position data, rebuild ──▶ ┌─────────────────────────┐
        │                                     │ StrategySignal          │
        │                                     │ owns: calcs + logic     │
        │   ◀── warm / not warm               └─────────────────────────┘
        │
        │ bars, order updates
        ▼
  ┌─────────────────────────┐
  │ StrategyExecution       │
  │ owns: execution FSM     │
  └─────────────────────────┘
        │
        │ entry/exit orders, cancels, order queries
        ▼
  ┌─────────────────────────┐
  │ Order Manager           │
  │ owns: order lifecycle   │
  └─────────────────────────┘
        │
        │ orders, cancels (sent through the Gateway)
        ▼
  ( broker )
```

Orders leave through the Gateway; StrategyExecution asks StrategySignal directly for entry/exit answers. Aux Data adds values to each bar before the Event Loop dispatches it.

Not drawn: StrategyExecution times the signal's exit-input freeze and release. Aux Data reads daily series straight from outside data sources (or from files in replay), not through the Gateway. Per decision 35: in paper and live, an operator's deploy request reaches the Event Loop over a local socket. Every component reads exchange facts from the shared exchange profiles (decision 31).

Flows not drawn on the spine (each is in the boundary section below):

- Safety → Event Loop: repaired fill and Rejected events and position updates, through the event buffer. Event Loop → Safety: check-ins and the runtime book's members. Strategy → Safety: each slot's submitted, open and cancelling orders, and "processing an order?".
- Strategy → Order Manager: order updates and ownership checks. Order Manager → Strategy: ownership answers. Order Manager → StrategyExecution: query answers.
- StrategyExecution ↔ StrategySignal: entry/exit questions and exit-input freeze/release; entry/exit signals back.
- Aux Data → Strategy: ready / not ready, with the history frames. Strategy → Aux Data: frame requests on a cold rebuild.
- Event Loop → Gateway: history requests; contract, subscribe and unsubscribe requests; session declarations; each completed bar (the simulated broker fills against it). Gateway → Order Manager: the next order id.
- ( operator ) → runner → Event Loop: in paper and live, deploy requests over a local socket (decisions 35, 51); status lines and alerts back to the operator. Safety → ( operator ): CRITICAL logs and stack dumps. Event Loop → ( disk ): bar and trade recordings (paper and live).
- Every component reads exchange facts from the shared exchange profiles (decision 31).

## Component maps

Each component has its own frozen contract. Once you know which component a change belongs to, read its map before opening source files.

| Component | Go here for |
|----|----|
| [Gateway](GATEWAY_MAP.md) | Broker connection, market data in, orders/cancels out, fills back, replay source. |
| [Event Loop](EVENT_LOOP_MAP.md) | Ticks → bars, delivering bars/order updates to strategies, broker warmup history seeding. |
| [Aux Data](AUX_DATA_MAP.md) | Auxiliary series from outside sources, arming per session, values on bars. |
| [Strategy](STRATEGY_MAP.md) | The per-slot object: position booking, driving signal/execution/order manager, rebuild gating. |
| [StrategySignal](STRATEGY_SIGNAL_MAP.md) | Indicators, entry/exit rules, warm state, cold rebuild. |
| [StrategyExecution](STRATEGY_EXECUTION_MAP.md) | Execution phase state machine, when to enter/exit, what order events mean. |
| [Order Manager](ORDER_MANAGER_MAP.md) | Building, tracking and cancelling orders; order ownership. |
| [Safety](SAFETY_MAP.md) | Lost connection / frozen process detection, reconnect, repairing fills missed in an outage. |

## System boundary

For each component: what it owns, what it leaves to others, what it receives, and what it produces.

### Gateway

- **Owns** — All communication with the broker: market data in, orders and cancels out, fills and executions back, for every instrument the process trades. A replay source can stand in for the broker behind it.
- **Does not own** — Building bars or deciding who hears an event (Event Loop); deciding what to trade (Strategy, StrategyExecution); tracking an order's life (Order Manager); noticing a dead connection or repairing strategies after one (Safety).
- **Receives** — From the broker: market data and fills. From Order Manager: orders and cancels, each with its order id. From Safety: heartbeat checks, reconnect and resubscribe requests, requests for executions, open orders and positions. From the Event Loop: history requests for warmup; contract, subscribe and unsubscribe requests; session declarations; each completed bar (the simulated broker fills against it).
- **Produces** — To the Event Loop: `tick_update` and `bar_update` messages (each gateway declares which one it gives per instrument, decision 49), order, position and balance updates, and warmup history. To Order Manager: the next order id. To the broker: orders and cancels. To Safety: connection health, the last tick time and the broker's executions, open orders and positions.
- **Why a component** — component test 1 (own outside source that can fail: the broker) and 3 (extension point: one subclass per broker, picked by name from the engine's gateway table, decision 30).

### Event Loop

- **Owns** — Turning `tick_update` messages into completed bars, taking `bar_update` messages as completed bars, and delivering bars and order updates to the right strategies, in time order, exactly once; and seeding each strategy with the warmup history it needs (daily and prior-session), fetched through Gateway and retried until it lands.
- **Does not own** — The broker connection, and building daily bars from intraday history (Gateway); entry/exit decisions (Strategy, StrategyExecution); recovering from an outage (Safety).
- **Receives** — From Gateway: `tick_update` and `bar_update` messages, order, position and balance updates, and warmup history. From Aux Data: values on each bar. From Safety: repaired fill and Rejected events, position updates, and a check-in request.
- **Produces** — To Strategy: completed bars, order, position and balance updates, warmup history, flatten requests and quarantine calls. To Gateway: history requests and each completed bar. To Safety: check-ins and read access to the runtime book's members. To disk: bar and trade recordings (paper and live).
- **Runtime book** — Also owns the runtime book, the one registry of the strategies the process runs and the per-instrument resources serving them, and adding one strategy to it while the process runs (decisions 34–36). Receives, from the runner's deploy socket in paper/live (decision 51): deploy requests as book operations. Produces, to the runner: one status per step until a terminal status, which the runner streams to the operator. Produces, to Gateway: contract, subscribe and unsubscribe requests for a new strategy's instruments.
- **Why a component** — no component-test question passes; a component by the human's decision (decision 59).

### Aux Data *(decision 44)*

- **Owns** — Reading auxiliary daily series from outside data sources, each through one data-source interface; deciding when a value is settled for its target session; arming and disarming each group of strategies that read them; adding the values to bars; and serving the history frames a cold rebuild needs.
- **Does not own** — Broker history (Gateway, requested by the Event Loop); delivering bars (Event Loop); when a signal may be rebuilt and telling StrategyExecution it is ready (Strategy); how a calculation is seeded (StrategySignal).
- **Receives** — From outside data sources (files in replay): auxiliary daily series. From the Event Loop: each completed bar before dispatch, and the runtime book's members. From Strategy: frame requests on a cold rebuild.
- **Produces** — To the Event Loop: bars with auxiliary values added. To Strategy: ready / not ready, with the history frames.
- **Why a component** — component test 1 (own outside input and own failure modes: source down, value not settled, provisional value changed, backup not confirmed; decision 44).

### Strategy

- **Owns** — Being the one per-slot object the Event Loop talks to: booking its own position from fills, in the instrument's currency, and driving its StrategySignal, StrategyExecution and Order Manager with what the Event Loop delivers — stepping the signal on each bar, passing the bar on to StrategyExecution, and deciding when the signal may be rebuilt (only while flat with no open or submitted orders).
- **Does not own** — Its params (StrategySignal holds them); the calculations and entry/exit logic (StrategySignal); which execution phase the slot is in or what an order event means in it (StrategyExecution); an order's life from submission to closed status (Order Manager); when bars are complete (Event Loop).
- **Receives** — From the Event Loop: completed bars, order updates and warmup history. From Aux Data: ready / not ready and history frames. From outside: deployment config. From StrategySignal: warm/not-warm. From StrategyExecution: whether it is processing an order. From Order Manager: ownership answers.
- **Produces** — To StrategySignal: bars with position data (entry price, seconds since entry) and rebuild requests. To StrategyExecution: bars and order updates. To Order Manager: order updates and ownership checks.
- **Why a component** — component test 3 (extension point: strategy authors write and register strategy classes, decision 43). Decision 19: the only component the Event Loop talks to.

### StrategySignal

- **Owns** — The slot's signal: its calculation DAG and params, stepping the calculations on each bar in dependency order, evaluating the entry and exit logic, reporting whether it is warm, and how to rebuild itself cold by replaying seed bars. *Written as a Python `SignalDag` subclass that holds its own params (decision 42).*
- **Does not own** — When it is stepped or rebuilt (Strategy); the position or fills beyond what Strategy passes in (Strategy); acting on its signals, or when its exit inputs are frozen and released (StrategyExecution); orders (Order Manager); talking to the Event Loop (Strategy).
- **Receives** — From Strategy: bars with position data, and rebuild requests. From StrategyExecution: entry/exit questions and exit-input freeze/release.
- **Produces** — To StrategyExecution: entry/exit signals. To Strategy: warm/not-warm.
- **Why a component** — component test 3 (extension point: strategy authors write `SignalDag` subclasses against the abstract signal interface, decision 42).

### StrategyExecution

- **Owns** — The slot's execution state machine: which phase it is in, when to enter and exit by asking the slot's signal, and what each order event means in the current phase.
- **Does not own** — The signal's logic or when it is stepped (StrategySignal, Strategy); the position (Strategy); an order's life at the broker (Order Manager); talking to the Event Loop (Strategy).
- **Receives** — From Strategy: bars and order updates. From StrategySignal: entry/exit signals. From Order Manager: answers about its orders (status and filled quantity, submitted and open orders, group finality).
- **Produces** — To Order Manager: entry and exit orders, cancels, and order queries. To StrategySignal: entry/exit questions and exit-input freeze/release. To Strategy: its phase, through query answers.
- **Why a component** — no component-test question passes; a component by the human's decision (decision 59).

### Order Manager

- **Owns** — The life of every order the slot sends: building it, tracking it from submission to a closed status, knowing which orders the slot owns, and sending cancels.
- **Does not own** — Deciding to enter or exit (StrategyExecution); the position (Strategy); broker communication (Gateway).
- **Receives** — From StrategyExecution: entry and exit orders, cancels, and order queries. From Strategy: order updates and ownership checks.
- **Produces** — To Gateway: orders and cancels. To Strategy: ownership answers. To StrategyExecution: query answers (its submitted and open orders, group finality, filled quantities).
- **Why a component** — no component-test question passes; a component by the human's decision (decision 59).

### Safety

- **Owns** — Detecting a lost broker connection or a frozen process, reconnecting, un-sticking any strategy left waiting on an order whose fill happened during the outage, and the periodic stuck-order check (decision 50).
- **Does not own** — Talking to the broker itself (Gateway); deciding what to trade (Strategy, StrategyExecution); normal event delivery (Event Loop).
- **Receives** — From Gateway: connection health, the last tick time, and the broker's executions, open orders and positions. From the Event Loop: check-ins and the runtime book's members. From Strategy: each slot's submitted, open and cancelling orders, and "processing an order?".
- **Produces** — To Gateway: heartbeat checks, reconnect and resubscribe requests, and queries. To the Event Loop: repaired fill and Rejected events and position updates, through the event buffer. To the operator: CRITICAL logs and stack dumps.
- **Why a component** — component test 2 (fails and recovers independently of trading, decision 4).

### Shared, owned by no component

- **Exchange profiles** — one per exchange: sessions, calendar, venue, currency, contract identity and, for futures, roll (decision 31).
- **Instruments** — asset class, symbol, exchange profile, currency, multiplier, tick size (decision 37, 41).
- **Configuration loading** and **structured logging**.
- *Per decision 46:* **Clock** — `now()` and `wait(timeout)`, as `WallClock` or `ReplayClock`; and **RunMode** — the run mode, built once by the runner. The Event Loop moves the `ReplayClock` forward to each bar's end time in `emit_bar`; in replay `wait(timeout)` returns at once.
- *Per decision 51:* **Runner** — `run_strategies.py`, `Runtime.build` and the deploy socket server. The runner builds the components in this order: Clock and RunMode, Gateway, Event Loop and runtime book, Aux Data, the slots (Strategy with its StrategySignal, StrategyExecution and Order Manager), Safety. The runner stops them in reverse. The runner owns the local deploy socket and hands each deploy request to the Event Loop as a book operation.

### Open questions on the design pages

None open. On 2026-11-09 every open question on this page and on the component pages was decided: decisions 52–58 here, and each component page lists its own answers under *Open questions*.

## Golden sample

**Agreed 2026-11-09.** This sample replaces the earlier L-cross sample (a fixed rule on a few minutes of hand-made prices) (decisions 11, 12, 48).

The golden sample is one replay run of the real deployment path. The run uses the real entry point, a real deployment config and a real strategy. Only the broker is different: the simulated broker replaces IBKR (decision 6). The run is deterministic: the same input files give the same output bytes on every machine. Thus every pass and fail condition is an exact value.

### Input

| Input | What it is | Where it is |
|----|----|----|
| Entry point | `python run_strategies.py --config tests/golden/config.yaml --mode replay`, started as a child process. The entry point runs the same `Runtime.build` that paper and live use (decision 47). | `run_strategies.py` |
| Strategy | A frozen copy of the public example strategy `macd_rsi_rth_long` (signal `macd_signal_dag`, default params). The copy keeps the golden answer stable when a person edits the public example. | `tests/golden/strategies/macd_rsi_rth_long/` |
| Config | A real deployment config: the simulated broker, a `replay:` section, one slot that trades MES on MES, and `strategies_root` set to the frozen copy. | `tests/golden/config.yaml` |
| Data | A synthetic tape of 1-second bars from the scripted generator `golden_session`. The generator uses no random numbers. The tape holds one full NY_RTH session on the replay date, and the prior sessions that warmup needs. The simulated broker sends each replay bar to the Event Loop as one `bar_update` (decision 49). | A synthetic tape source behind the simulated broker ([Gateway feature 12](GATEWAY_MAP.md)) |
| Expected output | The frozen broker journal and the frozen tape fingerprint. | `tests/golden/expected/` |

The config file (the output folder comes from the environment variable `GOLDEN_OUT`, so each run writes to its own temporary folder):

``` yaml
system:
  bot_id: golden
  log_path: ${GOLDEN_OUT}
  strategies_root: tests/golden/strategies

gateway:
  name: simulation

replay:
  tape: synthetic:golden_session
  date: 2026-04-15
  session: NY_RTH
  pad_before_s: 0
  pad_after_s: 0
  fill_reference: close
  slippage_ticks: 0
  journal: ${GOLDEN_OUT}/broker_journal.jsonl

strategies:
  macd_rsi_rth_long:
    strat: macd_rsi_rth_long
    account_id: SIM
    signal_instrument: MES
    execution_instrument: MES
```

### Synthetic session

The generator draws the price path in phases. Each phase makes one part of the strategy do a known thing. The replay date 2026-04-15 is a Wednesday with a full session.

| Phase | Time (New York) | Bars | Price path | Designed effect |
|----|----|----|----|----|
| Prior sessions | The 60 sessions before the replay date | 23,400 each | Each session opens at 5000.00, rises to 5010.00, falls to 4990.00 and closes at 5000.00. | Each daily bar has a true range of 20.00, so the daily ATR(14) is exactly 20.00. The signal is warm on the first replay bar. |
| A · Quiet open | 09:30:00–09:39:59 | 600 | Flat at 5000.00. | The MACD line equals the signal line, so the strict condition `macd_line > macd_signal` is false. Phase A also covers `skip_open_bars` (300). No entry. |
| B · Rise | From 09:40:00 until the target is reached | Set by the builder | A zigzag rise: one bar up by *u* = 2.00, the next bar down by *d* = 1.00. The first bar of phase B goes up. | On bar 09:40:01 RSI(14) is about 65 and the MACD line is above its signal line: exactly one entry, filled at 5001.00. The price reaches the target (5001.00 + 0.5 × 20.00 = 5011.00) on bar 09:40:18, before the stop or `macd_below_signal` fires: from entry to target the MACD line stays at least 0.26 above its signal line. The builder confirms these bars with the frozen strategy copy. |
| C · Rest | From the end of phase B until 15:59:59 | The rest of the 23,400 | Flat at the last price of phase B. | `max_trades_per_day` (1) blocks a second entry. The book is flat, so the `end_of_day` exit and the session-end flatten send no order. |

The replay session has exactly 23,400 bars (09:30:00 to 15:59:59, one bar per second).

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Instrument | Fixed for now (one instrument keeps the answer easy to check by hand) | MES | MES |
| Session | Fixed for now | NY_RTH | NY_RTH |
| Replay date | Fixed (a change moves every timestamp in the expected journal) | 2026-04-15 | 2026-04-15 |
| Prior sessions | Fixed (enough for `warm_up_months` = 2 and ATR(14)) | 60 | 60 |
| Base price; daily range | Fixed | 5000.00; 20.00 | — |
| Zigzag steps *u*, *d* | Fixed (decision 57) | 2.00, 1.00 | 2.00, 1.00 |
| Fill reference; slippage | Fixed for the golden run | bar close; 0 ticks | bar close; 0 ticks |
| Fill bar of a MKT order | Fixed ([Gateway feature 6](GATEWAY_MAP.md)) | the bar that made the decision (bar N) | bar N |
| Flat-bar padding | Fixed for the golden run | off | off |

### Per component

| Component | Expected behaviour |
|----|----|
| Gateway | The simulated broker answers daily history and seed bars from the 60 prior sessions, delivers all 23,400 replay bars as `bar_update` messages in time order, then end of data. Each MKT order fills at the close of the bar that made the decision. The simulated broker accepts exactly two orders, returns one fill for each, and writes four journal rows. |
| Event Loop | Seeds the slot before the first replay bar. Delivers each of the 23,400 bars to the strategy exactly once, in time order. Delivers both fills. Stops after end of data with the book flat. |
| Aux Data | The strategy reads no auxiliary name: Aux Data reads no source, arms nothing and adds nothing to any bar. |
| Strategy | Sets `aux_ready` when the slot is built, because the signal reads no auxiliary name. Steps the signal on every bar. Books long 1 MES on the buy fill and flat on the sell fill. Ends flat. |
| StrategySignal | Is warm on the first replay bar, with daily ATR = 20.00. Entry is false in phase A and true on one bar in phase B. Exit `target` is the first exit that is true after the entry. |
| StrategyExecution | Stays flat in phase A. Decides one buy in phase B. Moves to in-position on the buy fill. Decides one sell when the target is reached. Returns to flat on the sell fill. Decides nothing in phase C. |
| Order Manager | Submits exactly two market orders, tracks each one to filled, and ends with no open or submitted order. |
| Safety | Replay uses the do-nothing stand-in (decision 46). Nothing to check in the golden run; Safety is proved by its own component tests. |

### End to end

The broker journal is the output that the test reads. The simulated broker writes one JSON line for each order that the simulated broker receives (time, order id, side, quantity, order type, instrument) and one JSON line for each fill that the simulated broker sends (time, order id, price, quantity).

| Journal rule | Value |
|----|----|
| Time | `ReplayClock` time in UTC, ISO-8601 with milliseconds. A fill row has the end time of the bar it filled on. |
| Keys | Sorted by name; no spaces after separators. |
| Prices | Written with 2 decimals. |
| Order ids | The simulated broker numbers orders from 1 in each run. |

- **Input** — The entry point, config, frozen strategy copy and synthetic tape above.
- **Expected** — One BUY 1 MES MKT in phase B and its fill; one SELL 1 MES MKT at the target and its fill; the book ends flat.
- **Passes when** — All of these checks are true:
  1. The process exit code is 0.
  2. The sha256 of the `bar_update` stream that the simulated broker sends equals `expected/tape.sha256`.
  3. The broker journal equals `expected/broker_journal.jsonl` byte for byte.
  4. The journal has exactly 4 rows: BUY order, BUY fill, SELL order, SELL fill, in this order. Each order is 1 MES, type MKT.
  5. The BUY order time is at or after 09:40:00 (phase B) and before 15:30:00 (`last_entry_bars = 1800`).
  6. SELL fill price − BUY fill price ≥ 10.00. Thus the realised profit is at least 10.00 × 5 USD = 50.00 USD.
  7. A second run with the same input gives a byte-identical broker journal.
  8. The log has 0 ERROR lines and 0 CRITICAL lines.
- **Fails when** — One or more of the checks is false. The test report names each false check; for check 3 it shows the first journal row that is different.

Checks 4 to 6 come from the designed price path, not from the frozen journal. Thus a wrong journal cannot become the expected answer by a careless refresh. The frozen journal and the tape fingerprint change only in a commit that says why they changed.

## Decisions already made

### Goal and boundary

#### 1. What is the system's one goal?

**Answer:** Execute a strategy book at the broker, placing the same orders on a simulated broker, a paper account or a live account.

#### 2. Who consumes the output?

**Answer:** The broker is the only trading consumer: only the broker receives orders. The operator is a second outside actor: the operator sends deploy requests and receives status lines, alerts and CRITICAL logs. Secondary outputs, read by people and outside tools: logs in a stable format (decision 29), bar and trade recordings, and in replay the broker journal.

*changed 2026-11-09 · was "the broker only"*

#### 3. Are strategy research, comparing results with research, a strategy/params registry or decay monitoring inside the boundary?

**Answer:** None of them. None of that work happens here, so they are left out of the map.

#### 4. How many components?

**Answer:** Eight: Gateway, Event Loop, Aux Data, Strategy, StrategySignal, StrategyExecution, Order Manager, Safety.

**Rejected:** Rejected: folding Safety into Gateway (it fails and recovers independently of trading); a separate Warmup component (not needed at this level).

*changed 2026-11-09: eight, with Aux Data (decision 44)*

#### 5. Where do strategy params and deployment config sit?

**Answer:** Deployment config is an external input, not a component. Strategy params sit inside each strategy's signal class (decision 42): they are code the strategy author writes, not config.

**Rejected:** Rejected: a Configuration component (the engine reads config, it does not own it); params in outside config (a second place that must agree with the code).

*changed 2026-11-07 · params were an external input*

#### 6. How do simulation/replay modes relate to the boundary?

**Answer:** A swap behind the Gateway: a simulated broker that replays data and fills orders sits behind the same interface as the real one, so every other component runs unchanged.

**Rejected:** Rejected: a separate Simulator component, a simulation path that feeds strategies directly, and leaving sim out.

#### 7. The broker is both source and sink — one Gateway, or split feed/orders?

**Answer:** Keep one Gateway per broker, serving every instrument the process trades there. The diagram's order arrow is labelled as leaving through the Gateway.

**Rejected:** Rejected: a Market Feed / Order Router split.

#### 8. What does Safety exchange?

**Answer:** Heartbeat and reconnect with Gateway, executions from Gateway, repaired fill events into the Event Loop.

**Rejected:** Rejected: detection-only.

### Golden sample

#### 9. Where does the golden sample sit?

**Answer:** A normal input through the Gateway on the normal path.

**Rejected:** Rejected: its own harness component.

#### 10. How detailed is its expected behaviour?

**Answer:** Both: one behaviour per component and one end-to-end result.

#### 11. What is its data?

**Answer:** One full synthetic NY_RTH session of 1-second bars, plus the 60 prior sessions that warmup needs, from a scripted generator that uses no random numbers. The price path has phases, and each phase makes the strategy do one known thing (Synthetic session).

**Rejected:** Rejected: a small real day (slower, and its right answer is not known by hand); a few minutes of hand-made prices (too short for warmup, the open skip and the last-entry window); a seeded random generator (its output can change with a library version).

*changed 2026-11-09 · was "a synthetic session" of a few minutes*

#### 12. Which strategy runs on it?

**Answer:** A frozen copy of the public example strategy `macd_rsi_rth_long` with its default params, in `tests/golden/strategies/`.

**Rejected:** Rejected: a trivial fixed rule (it does not run the calculation DAG, the daily warmup or the session conditions that a real strategy uses); the public example in place (an edit to the example would move the golden answer).

*changed 2026-11-09 · was a trivial fixed rule*

#### 48. How is the golden sample run and judged?

**Answer:** The test starts the real entry point `run_strategies.py` as a child process, with a real config and `--mode replay`. Only the broker is the simulated one (decision 6). The simulated broker writes a broker journal: one row per order received and one row per fill sent. The test passes only when the exit code is 0, the tape fingerprint and the journal equal the frozen files byte for byte, the checks from the designed price path hold, a second run gives the same journal, and the log has no ERROR or CRITICAL line (End to end).

**Rejected:** Rejected: building `Runtime` in the test process (it skips the entry point, argument parsing, config loading and logging setup); comparing log text (log wording changes); comparing with a tolerance (a deterministic run needs none).

*agreed 2026-11-09*

#### 13. Does it include a broker outage?

**Answer:** No; Safety's expected behaviour is to do nothing.

### Splitting the slot

#### 14. Why split StrategyExecution and Order Manager out of Strategy?

**Answer:** Separation of work: Order Manager handles the order lifecycle, StrategyExecution handles the execution state machine, and Strategy is the higher-level object that uses both to interact with the Event Loop and consume its updates.

*Resting broker-side exits (OCA groups) made both concerns the hard part.*

#### 15. Who talks to Order Manager?

**Answer:** Both: StrategyExecution sends entry and exit orders and cancels and queries its own orders; Strategy forwards order updates and checks ownership.

**Rejected:** Rejected: routing everything through StrategyExecution, and Strategy relaying Order Manager's answers to StrategyExecution in an order-context object.

#### 16. Who owns the position?

**Answer:** Strategy.

**Rejected:** Rejected: moving it into Order Manager.

#### 17. Does the golden sample change with the split?

**Answer:** No: the same bar-close round trip, with expected behaviour for StrategyExecution and Order Manager.

**Rejected:** Rejected: a resting-exit sample.

#### 18. Does the system's goal or external boundary change with the split?

**Answer:** No; only the internal split changes.

#### 19. Why split StrategySignal out of Strategy?

**Answer:** It is deterministic (bars and position data in, entry/exit answers out, no I/O), and it is where most warmup and signal bugs live; giving it its own boundary lets it be tested alone, from bars in to answers out. Strategy becomes the driver of StrategySignal, StrategyExecution and Order Manager, and stays the only component the Event Loop talks to.

#### 20. Who calls the signal?

**Answer:** Strategy and StrategyExecution: Strategy steps StrategySignal on each bar and rebuilds it; StrategyExecution asks it for the entry or exit answer in the phase that needs one. The Event Loop never reads the signal; it goes through Strategy.

**Rejected:** Rejected: Strategy evaluating both answers and handing them to StrategyExecution — evaluating exit while flat, or entry before warmup, reads calculations that do not exist yet, and choosing by phase would leak the state machine into Strategy.

#### 21. Does the signal see more than bars?

**Answer:** Yes, position data: Strategy passes strategy data (entry price, seconds since entry) into each step, and StrategyExecution times the exit-input freeze (entry-decision bar) and release (flat again). The signal never reads Strategy's position itself. It answers exit per condition as well as overall, so StrategyExecution can mask conditions a resting broker order already covers.

#### 22. Who owns the signal rebuild (cold reset + seed replay)?

**Answer:** Split: StrategySignal owns how; Strategy owns when — only while flat with no open or submitted orders, which needs position and Order Manager state the signal must not see.

#### 23. Who seeds strategies with warmup history (fetch, retry, auxiliary data)?

**Answer:** The Event Loop. It already holds the per-instrument plumbing and the loop the retries run on; Strategy decides only when a rebuild may happen (decision 22) and the signal how. Still no separate Warmup component (decision 4).

*changed 2026-11-09: auxiliary data moved to Aux Data (decision 44); only broker history stays here*

#### 24. What size does a slot trade?

**Answer:** Always 1 unit per entry: 1 contract for futures and options, 1 share for stocks. So an order fills whole or not at all: partially filled orders and partial exits are not handled anywhere (Order Manager, StrategyExecution, Safety's repair), and a design or review must not raise them as a gap. Changing the size reopens this decision.

*carried · wording widened from "1 contract" to "1 unit" for stocks and options, 2026-11-06*

### Agreed at finalize · 2026-11-09

These decisions were proposed during the design and accepted when v3 was finalized.

### Latency and concurrency (R3)

#### 25. What latency does R3 measure?

**Answer:** *Replaced by decision 55: feed latency and processing latency; the 5 ms budget applies to processing latency.* The 5 ms budget is **end-to-end latency**: from the broker timestamp of the tick that closes a bar to that bar's order leaving the Gateway for the broker, at p99. It is logged per minute as p50/p99 together with its two internal parts: **slot cost** (one strategy, one bar) and **dispatch latency** (a bar's closing event entering the event buffer → the last strategy on that instrument done). Because feed transit counts against the budget, the engine's own share is what is left after transit.

**Rejected:** Rejected: bar age measured from the bar's nominal end time — it includes the wait for the next tick that closes the bar (often hundreds of ms in a quiet market), which no engine design can remove. Rejected: counting only internal processing (slot cost or dispatch latency alone) — the budget is from the broker's time to the order.

*agreed 2026-11-09*

#### 26. How does the Event Loop learn there is work?

**Answer:** A drain wake-up: every producer of the event buffer raises it after appending (the broker's reader thread through a thread-safe call into the loop), and the loop waits on it with a timeout equal to the bar-flush cadence (20 ms), so the clock-driven bar flush still runs on time. The event buffer stays a deque. This removes a 0–20 ms queue wait on every event that a fixed polling timer would add. A producer raises the wake-up only when none is pending, the buffer drains a control lane before a data lane, and each pass has a batch budget ([Event Loop decision 8](EVENT_LOOP_MAP.md)).

**Rejected:** Rejected: a shorter timer (more idle wake-ups and still a floor); an asyncio queue everywhere (replay preloads the whole day into the buffer and puts fills at its front).

*agreed 2026-11-09*

#### 27. What happens when the event buffer backs up?

**Answer:** It stays lossless (order updates are never dropped), but a control-lane backlog over a threshold becomes an ERROR with an operator alert, and the size of each lane and the queue wait are logged with each drain pass. The data lane has no alert, because replay loads a whole day of bars into the data lane on purpose.

**Rejected:** Rejected: a hard size limit that drops events — it could drop a fill.

*agreed 2026-11-09*

#### 28. What concurrency model runs the strategies?

**Answer:** One process, one asyncio event loop, the broker's reader thread and one send thread; strategies on a bar are stepped one after another, in time order. One process keeps dispatch latency under 5 ms at p99 up to about 10 strategies active at once; R3's budget is on processing latency (decision 55), which adds queue wait to dispatch latency, so that count is an upper bound to be re-measured. The load target is 3 tick-by-tick streams and 10–15 strategies; whether one process carries it is decided by a measured scale trigger, not by a strategy count ([Event Loop decision 9](EVENT_LOOP_MAP.md)). Beyond that, split into process shards by strategy group, with one process still owning the single Gateway (decision 7 holds). Before sharding, Safety and its repair need a design that covers every shard.

**Rejected:** Rejected: a thread per strategy (the GIL serialises CPU-bound Python anyway); an asyncio task per strategy (no parallelism, and the time-order guarantee gets harder to keep); one actor per strategy and gateway with message ports between them (no global time order, and ports wired by hand); sharding before it is needed.

*agreed 2026-11-09*

#### 29. Where are per-bar logs written?

**Answer:** Off the loop thread: loggers hand records to a queue and a listener thread writes the files. Record content and field names are a stable format that outside tools may read.

**Rejected:** Rejected: dropping per-bar indicator logs to DEBUG — they are how a run is audited afterwards.

*agreed 2026-11-09*

#### 33. How is slot cost brought down before sharding?

**Answer:** Profile one strategy's bar first: logging is a small part of slot cost, so the rest is signal stepping and the execution state machine. Then consider computing a calculation once for all strategies on the same instrument when its registry name and params are identical. That would change who owns a calculation's state, so it needs its own decision first. This reduces sharding but does not replace it.

*agreed 2026-11-09*

### Extensibility (R2)

#### 30. What is the Gateway interface?

**Answer:** An abstract interface. Required methods have no body, so a gateway that misses one fails when it is built instead of returning nothing. Optional methods are gateway capabilities declared on the interface with safe defaults, never looked up by name at runtime. Gateways are picked by name from one fixed gateway table in the engine, `GATEWAYS`, because users do not write their own gateways. History is requested in broker-neutral units (months or dates), not broker-specific duration strings. See [Gateway](GATEWAY_MAP.md).

**Rejected:** Rejected: splitting the Gateway (decision 7 holds); a decorator registry for gateways (dropped 2026-11-10: only built-in gateways exist, so a fixed table is enough).

*agreed 2026-11-09*

#### 31. Where do exchange facts live?

**Answer:** In one exchange profile per exchange, in shared code: session names, calendar and early closes, the continuous data session, contract venue and currency, the broker's contract identity, the futures contract roll rule and session-end cutoffs. Every component reads it; no other file branches on an exchange name. CME, Eurex and NASDAQ (decision 54) are the first profiles.

**Rejected:** Rejected: one component owning the calendar — the Event Loop, Gateway, Safety and StrategySignal all read it; special cases for one exchange spread over the files that need them.

*agreed 2026-11-09*

#### 32. How do auxiliary data sources plug in?

**Answer:** Outside data sources are named on the boundary as a second external input, read only by Aux Data (decision 44), each through one data-source interface. A new feed or backup is one new class.

**Rejected:** Rejected: moving them behind the Gateway — the Gateway is the broker only (decision 7).

*agreed 2026-11-09 · changed 2026-11-07 · was the Event Loop's auxiliary data provider*

### Add a strategy without a restart (R5)

#### 34. Who owns the set of strategies the process runs?

**Answer:** The Event Loop, as one **runtime book**: strategies in dispatch order, per-instrument resamplers and bar buffers, contracts, the recording session maps and the background retry tasks. Only the drain-loop task changes it, and only at a commit boundary: another task hands it a book operation and awaits the result, so a bar or order dispatch already in flight never sees the book change. Everyone else reads membership from the book at call time — shutdown, Safety's repair and positions poll, the auxiliary retry, the recorders and the reconnect resubscribe — so a strategy added later is a full member. Cold start and hot-add go through the same steps: reserve instruments, stage, prepare, commit.

**Rejected:** Rejected: each consumer keeping its own copy of the strategy list made at start; a book changed from any task (a dispatch could see a half-added strategy).

*agreed 2026-11-09*

#### 35. How does an operator add a strategy to a running process?

**Answer:** A separate client script, `deploy_strategy.py --config FILE --key KEY`. It reads the same config file to find the socket and the sha256, sends one request and exits with the terminal status. The runner, in paper and live only, listens on a local socket next to its logs (directory 0700, socket 0600). One deploy request per connection names one strategy key and the config file's sha256; the runner streams status lines (preparing, then one of registered, noop, rejected, failed, busy, shutting_down) with a reason and the revision (the config file's sha256). Which version of a strategy's code runs is not tracked: version control of strategies is outside the system. Requests run one at a time. Every terminal status is logged with the same hash fields as the startup deploy fingerprint. The slot is first added to the config file, so the next cold start runs it too: the deploy request is the existing deployment-config input arriving while the process runs (decision 5 holds). Simulation never opens the socket.

**Rejected:** Rejected: a `--deploy` flag on the runner script (a short socket client and the long-running driver would share one parser and one `main`); a second service; the runner polling the config file.

*agreed 2026-11-09 · changed 2026-11-07 · was a `--deploy KEY` flag on the runner script*

#### 36. What may a deploy request do?

**Answer:** Add one new strategy key, nothing else, and only when it cannot disturb what is running. It is refused, before anything is touched, when the config file changed after the request was made (`stale_config`), the key is not in the config (`unknown_key`), the strategy folder fails to import (`import_error`), the signal's params fail their type check (`params_invalid`), the config differs anywhere but the new slot (`config_drift`), the key already runs (`conflict`), the strategy's session is open (`session_open`), its next session closes after this process exits (`after_process_exit`), no running strategy uses its account (`unknown_account_route`), or a position or order on its execution instrument belongs to no running strategy, compared per account (`unmanaged_exposure`; nothing is cancelled or flattened). A failure after that cleans only what the candidate created; shared feeds and running strategies are never touched. A committed strategy starts gated and can first enter at its next session open, once warmup and auxiliary data are ready.

**Rejected:** Rejected as out of scope: changing or removing a running strategy, reloading code or params, starting a strategy inside its own session, adding several strategies atomically, new account routes, changing when the process exits, automatic recovery from quarantine.

*agreed 2026-11-09*

### Multi-asset and multi-currency (R1)

#### 37. What is an instrument?

**Answer:** One shared value naming a tradable product: its asset class (decision 41), symbol (NQ, ES, FDAX), its exchange profile, its currency, multiplier (1 for a stock) and tick size. Every tick, bar, order, fill and position carries the instrument, so no component needs to guess an exchange or a currency from a symbol. For a future, the front-month contract is resolved from the instrument by its exchange profile's roll rule; a stock is its one listing and has no roll; how an option contract is resolved moves to a later version (decision 54).

*agreed 2026-11-09 · v3*

#### 38. How does a strategy name what it trades?

**Answer:** By two instrument lists in its deployment config: **signal instruments** (market data it subscribes to) and **execution instruments** (where its orders and position live). They are usually the same; they differ when a strategy reads one market and trades another.

*agreed 2026-11-09 · v3*

#### 39. What currency is money counted in?

**Answer:** Every money amount carries its currency, and Strategy books position and PnL in its execution instrument's currency (USD for NQ/ES, EUR for FDAX). The engine never converts between currencies; account cash per currency is read from the broker. Reporting a book in one base currency is outside the system.

**Rejected:** Rejected: converting to a base currency inside the engine (it needs a live FX rate source on the trading path, and no trading decision needs it).

*agreed 2026-11-09 · v3*

#### 40. Do sessions differ per instrument?

**Answer:** Yes. Session names, open and close times, early closes, the continuous data session that arms the market-data watchdog, and the end-of-session cutoffs all come from the instrument's exchange profile, each on its own local clock (New York for CME, Frankfurt for Eurex). One process can trade both exchanges at the same time.

*agreed 2026-11-09 · v3*

#### 41. Which asset classes does the engine trade?

**Answer:** *v3 scope narrowed by decision 54: futures and stocks; options later.* Futures, stocks and options, through the same eight components and one code path. The asset class is part of the instrument, and only the facts that differ by asset class branch on it — how the tradable contract is resolved (futures roll, stocks have none, options open) and the unit of size (decision 24). Signal, execution, order lifecycle and Safety work the same for all three.

**Rejected:** Rejected: futures only (the v3 starting scope).

*agreed 2026-11-09 · v3 · 2026-11-06*

### StrategySignal model

#### 42. How is a StrategySignal written?

**Answer:** As a Python `SignalDag` subclass, registered by name. It declares its params as a typed, frozen dataclass whose defaults are the values, and builds its DAG in `build_graph()` from registered calculations, inline Python expressions, named conditions and named exits. The `SignalDag` base class implements the whole abstract signal interface, so a strategy author writes only the params and the graph. The instrument comes from deployment config and the session from the exchange profile. See [StrategySignal](STRATEGY_SIGNAL_MAP.md).

**Rejected:** Rejected: a declarative signal definition file (a second format to parse and check; a Python class is checked at import and build); params in outside config (decision 5); shipping the interface only (every user would have to bring a signal layer).

*agreed 2026-11-09 · v3 · changed 2026-11-07*

### Custom strategies

#### 43. How does the engine load a custom strategy?

**Answer:** `run_strategies.py` is the only entry point that runs strategies. At start it imports every folder under `local/strategies/` by file path: `calculations.py`, `strategy_signal.py` and `strategy.py`. The three files do not import from each other; they connect by registry names. `register_calc` binds a calculation in its folder's scope; `register_signal_dag` and `register_strategy` bind global names. A slot's config names its strat (the registered strategy name); the strategy names its signal; the signal's graph names its calculations, looked up in the folder's scope first and then in the built-in library. Lookups run when a slot is built, after every import, so import order does not matter. A name bound twice in the same scope stops startup and names both files.

**Rejected:** Rejected: imports between the files of a strategy folder (the folder is not a package on the engine's import path); one global calculation namespace (two folders could not each have their own `rsi`).

*agreed 2026-11-09 · v3 · 2026-11-07*

### Smaller code (R2, R4)

#### 44. Is auxiliary data its own component?

**Answer:** Yes: [Aux Data](AUX_DATA_MAP.md), the eighth component. It has its own outside input, its own failure modes (source down, value not settled, provisional value changed, backup not confirmed) and its own clock (arming by each session's open, on each exchange's clock). It meets the rest of the engine at three points only: values on a bar (called by the Event Loop before dispatch), frames for a cold rebuild (read by Strategy), and ready / not ready (sent to Strategy). A group's arm session comes from the exchange profile. Decision 4 now reads "eight", and decision 23 keeps only broker history in the Event Loop.

**Rejected:** Rejected: keeping it in the Event Loop (its widest part, with most of its outside I/O); behind the Gateway (decision 7); a component for resampling too (a small pure module with no outside input; see the Event Loop map, decision 7).

*decided 2026-11-09 · proposed 2026-11-07*

#### 45. Who prepares replay data?

**Answer:** The simulated broker, on request ([Gateway feature 12](GATEWAY_MAP.md)). It reads the tape lazily, checks it against the session from the exchange profile with one generic check, and answers daily-history and prior-session requests like the broker gateway. Padding and synthetic tapes are simulated broker options. The Gateway builds daily bars in live and replay; the Event Loop only requests them. This follows decision 6.

**Rejected:** Rejected: the runner slicing the tape and writing history files before start (a second history path that must read strategies before they exist); one hand-written session check per exchange.

*agreed 2026-11-09 · 2026-11-07*

#### 46. How do components differ between replay and live?

**Answer:** Only through what the runner passes in. A shared `Clock` (`now()`, `wait(timeout)`) is a `WallClock` live and a `ReplayClock` in replay, moving forward on each bar close; the bar flush, session-end flatten, reconcile and the session-active flag read it. `RunMode` is built once by the runner, which uses it to choose components and do-nothing stand-ins (recorders, Safety, state stores). No component reads the mode or checks a collaborator for `None`.

**Rejected:** Rejected: checking a mode string inside components (it spreads, and a check that matches the wrong mode runs the wall clock against replayed data).

*agreed 2026-11-09 · 2026-11-07*

#### 47. What does `run_strategies.py` do?

**Answer:** It is a thin startup module. It parses `--config` and `--mode` only (the deploy client is `deploy_strategy.py`, decision 35); replay settings (tape, date, session) sit in a `replay:` section of the config. It builds a `Runtime` from the config and the mode: the gateway by name from the gateway table (decision 30), the clock, the stand-ins and each component with its own typed config section. Every long-lived part follows one `Service` protocol (`start()`, `stop()`); `Runtime` starts them in order and stops them in reverse, also when start-up fails, then runs the Event Loop. Logging is one queue-handler setup (decision 29) with a logger per module. Comparing results with research stays outside (decision 3).

``` python
async def main(cfg, mode):
    async with Runtime.build(cfg, mode) as rt:
        await rt.run()
```

**Rejected:** Rejected: building components one by one with a try/finally of None checks; a long list of replay flags on the command line; keyword arguments copied one by one from config into a component.

*agreed 2026-11-09 · 2026-11-07*

### Open questions decided 2026-11-09

#### 52. Which readiness state gates trading?

**Answer:** One gate: StrategyExecution's `ready_to_arm` = `warmup_ready` AND `aux_ready`. Each flag has one writer, through Strategy. `warmup_ready` is set when the Event Loop's seed state is READY *and* the signal reports warm. `aux_ready` is set when Aux Data arms the slot's group, or at build when the signal reads no auxiliary name. The Event Loop's seed state (PENDING, READY, QUARANTINED\_\*) only drives the seed retry; Aux Data's arming only drives `aux_ready`. Neither one is read by StrategyExecution directly.

**Rejected:** Rejected: StrategyExecution reading the seed state or the arm groups (a reach across two boundaries); one combined flag (a late aux value and a late warmup fetch are retried by different components).

*decided 2026-11-09*

#### 53. What happens to late warmup or aux data while a position is open?

**Answer:** Strategy keeps it as a pending rebuild and clears the matching ready flag at once, so the slot makes no new entry. The open trade keeps running on the current signal, and its exits stay live. When the slot is flat and idle, Strategy rebuilds the signal from the pending data and sets the flag again. A newer late value replaces an older pending one. At session end, a pending rebuild that never ran is logged at WARNING.

**Rejected:** Rejected: dropping the late data (the next entry would trade on a stale signal); rebuilding inside the trade (the exit logic would change under an open position, Strategy feature 3).

*decided 2026-11-09*

#### 54. Which asset classes does v3 trade, and how are stocks tested?

**Answer:** Futures and stocks. v3 adds a NASDAQ exchange profile (USD, regular session 09:30–16:00 New York, no roll), so stock contract resolution, share size and USD booking have real test cases. Options move to a later version, together with the two option questions: how an option contract is picked, and which feed gives option data. The instrument model keeps the asset class `option` so that the later version needs no model change.

**Rejected:** Rejected: keeping options in v3 (no feed gives tick-by-tick or 1-second option data at IBKR, and contract picking is not designed); testing stocks with a made-up profile only (the first real stock book would still be untested).

*decided 2026-11-09 · narrows decision 41*

#### 55. Which latencies does the engine measure, and which one has the 5 ms budget?

**Answer:** Two measures, both logged per minute as p50 and p99, per instrument.

1.  **Feed latency** — from the broker's timestamp on a message to the message's arrival in the engine. The Gateway's reader thread stamps `recv_ts` when each message arrives. Feed latency = `recv_ts` − broker timestamp. It is measured, not budgeted: the engine does not control it, and its precision is the broker's timestamp precision (IBKR stamps ticks to the second), so the log records that precision with the number.
2.  **Processing latency** — from a message's arrival (`recv_ts`) to each strategy's final decision on it: an order or cancel handed to the Gateway, or no action. A bar that the flush timer closes starts at the time the Event Loop closes it. The 5 ms p99 budget of R3 applies to processing latency. Its internal parts, queue wait, dispatch latency and slot cost, are logged with it.

**Rejected:** Rejected: one end-to-end number from the broker timestamp (it mixes a part the engine does not control with a part it does, and broker clock offset and second-level timestamps make it unmeasurable at 5 ms); ending at "order leaving the Gateway" only (bars with no order would go unmeasured).

*decided 2026-11-09 · changes decision 25*

#### 56. What stays fixed in v3?

**Answer:** Size stays 1 unit per entry (decision 24). Bar resolution stays 1 s only; other resolutions come later, chosen per strategy and built from 1-second bars. Order types stay MKT, STP and LMT with DAY time in force and outside-regular-hours allowed, fixed for every strategy; stop-limit is not planned. Every closed bar goes back through the data lane before dispatch, so one path orders all bars.

*decided 2026-11-09*

#### 57. How is the golden run wired?

**Answer:** The config key `gateway.name` picks the gateway by name from the gateway table (decision 30). The `golden_session` generator lives in the engine's synthetic tape registry, in `alchemist/gateways/simulation_gateway/synthetic_tapes.py`, registered under the name `golden_session`, so the real entry point finds it by name. The zigzag steps are u = 2.00 and d = 1.00.

*decided 2026-11-09*

#### 58. Which operational numbers are configurable?

**Answer:** Every operational number (timeouts, intervals, thresholds, budgets, counts) is configurable per process, in its component's typed config section, with the value on the component page as the default. No number is configurable per strategy, except the snapshot interval and the signal's own params. A number that a broker defines (error codes, the 48-character group name limit) or that a decision fixes (OCA type 2, 1 unit, 1 s) stays fixed.

**Rejected:** Rejected: hard-coded constants (a value cannot change after measurement without a code change); per-strategy overrides for engine numbers (one strategy could slow the whole process).

*decided 2026-11-09*

### Interface and ownership fixes

#### 49. Which market-data messages does the Gateway send?

**Answer:** Two message types: `tick_update` (one trade: time, price, size) and `bar_update` (one completed OHLCV bar at a stated resolution, with its broker timestamp). Each gateway declares, per instrument, which type it gives and, for bars, the resolution. The IBKR gateway gives ticks. The simulated broker gives bars from its tape, like any broker that serves klines. The Event Loop sends a `tick_update` to the Resampler and takes a `bar_update` as a completed bar; both then go through the same `emit_bar` and dispatch. A slot that needs a finer resolution than its instrument's feed gives stops startup with an error. A third type, `quote_update` (order-book changes), is reserved and out of scope for v3.

**Rejected:** Rejected: treating replay bars as a replay-only path that skips the Resampler (it hides a missing message type in the interface and breaks R4); turning each bar into made-up ticks (the ticks would be invented data).

*agreed 2026-11-09*

#### 50. Who runs the periodic stuck-order check?

**Answer:** Safety, on its own heartbeat loop, every check interval (60 s). Safety reads the runtime book's members, asks each slot through Strategy whether it is processing an order, queries the Gateway, and runs the same repair it runs after a reconnect. The Event Loop no longer calls Safety.

**Rejected:** Rejected: the Event Loop querying the Gateway and then calling Safety's repair (two components share one job, and the call goes against the boundary direction).

*agreed 2026-11-09*

#### 51. Who owns startup, shutdown and the deploy socket?

**Answer:** The runner, which is shared code, not a component: `run_strategies.py`, `Runtime.build`, `RunMode` and the deploy socket server. The runner builds and stops the components in a fixed order (see Shared, owned by no component) and hands each deploy request to the Event Loop as a book operation. The Event Loop still owns the runtime book and the admission steps (decisions 34–36).

**Rejected:** Rejected: a ninth Runner component (the runner has no outside input of its own and no failure mode beyond startup); the deploy socket server inside the Event Loop.

*agreed 2026-11-09*

### Component status

#### 59. Why are the Event Loop, StrategyExecution and Order Manager components?

**Answer:** By the human's decision. Each of the three has its own owner and its own frozen contract (component map) that other components build against. None of the three passes a component-test question (own failing outside source, independent recovery, extension point); they stay components anyway. StrategyExecution and Order Manager keep the split of decision 14.

**Rejected:** Rejected: folding StrategyExecution and Order Manager back into Strategy as modules, and the Event Loop into the runner (a large boundary change for no gain in v3).

*decided 2026-11-09*

## Open questions

None.
