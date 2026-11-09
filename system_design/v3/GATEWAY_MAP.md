# GATEWAY_MAP — Gateway · Alchemist v3

The engine's only line to the broker. Prices come in through the Gateway, orders go out through the Gateway, and fills come back through the Gateway. In simulation, a simulated broker sits behind the same interface.

Frozen contract for the Gateway component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `gateway_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-09.** **v3 final, 2026-11-09 — agreed.** The End-to-end scenarios in Tests were agreed on 2026-11-09. All features and decisions are agreed. Multi-asset: contract identity, venue and session clocks come from each instrument's exchange profile (feature 3). Change history: see `gateway_map.html`.

## Goal and scope

**What it is.** The Gateway is an abstract interface between the engine and one broker. Each broker gets one concrete subclass: the IBKR gateway for paper and live accounts, and the simulated broker for replay. One process runs one Gateway, and that Gateway serves every instrument the process trades ([System map decision 7](SYSTEM_MAP.md)).

**Why it exists.** Every broker has its own API, its own contract names, its own error codes and its own threads. The Event Loop, Order Manager and Safety must not know which broker sits behind the engine, so that replay, paper and live place the same orders ([System map decision 6](SYSTEM_MAP.md)). The Gateway hides the broker's API behind one set of operations, and each subclass translates those operations into one broker's API.

**Goal.** Give the engine one asynchronous interface to the broker: market data and order updates come in, orders and cancels go out, and the broker's executions, history and connection health are available on request, for every instrument the process trades.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Other components call the Gateway | Order Manager | Place an order; cancel an order | order: instrument, action, size, type (MKT, STP, LMT), price, OCA group and type; order id |
|  | Event Loop | Resolve a contract; subscribe and unsubscribe ticks; declare instrument sessions; request daily history and intraday seed bars; show a bar (the simulated broker fills against the bar) | instrument, history window (months or dates) and session, bar |
|  | Safety | Probe the socket; reconnect; read the last tick time; fetch executions, open orders and positions; take the resubscribe request | timeout, since-timestamp |
| The Gateway calls other components | Event Loop | Push events into the event buffer and raise the drain wake-up | `tick_update` or `bar_update`, order update, position update, balance update, `end_of_data` |
|  | Broker (outside the system) | Send orders, cancels, subscriptions and requests through the broker's API | broker messages |

### Scope

Owns, from the System map:

> All communication with the broker: market data in, orders and cancels out, fills and executions back, for every instrument the process trades. A replay source can stand in for the broker behind it.

| Not the Gateway's job | Owned by |
|----|----|
| Building bars from ticks; deciding which strategy receives an event | [Event Loop](EVENT_LOOP_MAP.md) |
| Deciding what to trade | [Strategy](STRATEGY_MAP.md), [StrategyExecution](STRATEGY_EXECUTION_MAP.md) |
| Tracking an order's life | [Order Manager](ORDER_MANAGER_MAP.md) |
| Noticing a dead connection; repairing strategies after an outage | [Safety](SAFETY_MAP.md) |

### Place in the system

Drawn in the HTML page; the flows are the Public interface table above and the [SYSTEM_MAP.md](SYSTEM_MAP.md) Diagram.

The System map's Gateway *Receives* and *Produces* lines list every operation on this page, including the bar from the Event Loop and the two market-data message types.

## Features

Each feature says which component needs it and why, what the broker can give, and how the Gateway closes the gap. Every number is a parameter with its supported values.

### Algorithm map

```
  ( Order Manager )
        │
        │ order (instrument, action, size, type, price, OCA group)
        ▼
  ┌─────────────────────────┐
  │ Check and round order   │
  │ rule: reject bad price  │
  └─────────────────────────┘
        │
        │ on-tick order
        ▼
  ┌─────────────────────────┐
  │ Assign order id         │
  │ rule: rising, unique    │
  └─────────────────────────┘
        │
        │ order + order id
        ▼
  ( broker or simulated broker )

  ( Order Manager )
        │
        │ cancel (order id)
        ▼
  ┌─────────────────────────┐
  │ Send cancel             │
  │ rule: active order id   │
  └─────────────────────────┘
        │
        │ cancel
        ▼
  ( broker or simulated broker )

  ( Event Loop )
        │
        │ instrument
        ▼
  ┌─────────────────────────┐
  │ Resolve contract        │
  │ rule: exchange profile  │
  └─────────────────────────┘
        │
        ├── writes ──▶ [ contracts by instrument ]
        ├── contract request, subscribe ──▶ ( broker )
        │   ◀── contract details
        │
        ▼
  ( Event Loop ): contract resolved

  ( broker or simulated broker )
        │
        │ tick
        ▼
  ┌─────────────────────────┐
  │ Append tick             │
  │ rule: keep time order   │
  └─────────────────────────┘
        │
        ├── appends events + raises drain wake-up ──▶ [ event buffer ]
        │
        │ tick_update / bar_update in the event buffer
        ▼
  ( Event Loop )

  ( broker or simulated broker )
        │
        │ order status
        ▼
  ┌─────────────────────────┐
  │ Translate status        │
  │ rule: reject → REJECTED │
  └─────────────────────────┘
        │
        ├── order update ──▶ [ event buffer ]
        │
        │ order update
        ▼
  ( Event Loop )

  ( broker or simulated broker )
        │
        │ account
        ▼
  ┌─────────────────────────┐
  │ Report account          │
  │ rule: with currency     │
  └─────────────────────────┘
        │
        ├── position, balance update ──▶ [ event buffer ]
        │
        │ position, balance update
        ▼
  ( Event Loop )

  ( Event Loop )
        │
        │ completed bar
        ▼
  ┌─────────────────────────┐
  │ Fill on bar (simulated) │
  │ rule: level; gap → open │
  └─────────────────────────┘
        │ fill
        └──▶ Translate status

  ( Event Loop )
        │
        │ history request (months or dates)
        ▼
  ┌─────────────────────────┐
  │ Build history frame     │
  │ rule: no look-ahead     │
  └─────────────────────────┘
        │
        ├── intraday bars request ──▶ ( broker )
        │   ◀── intraday bars
        │
        │ history frame
        ▼
  ( Event Loop )

  ( Safety )
        │
        │ heartbeat (timeout)
        ▼
  ┌─────────────────────────┐
  │ Probe socket            │
  │ rule: answer in timeout │
  └─────────────────────────┘
        │
        ├── time request ──▶ ( broker )
        │   ◀── time answer
        │
        │ alive?
        ▼
  ( Safety )

  ( Safety )
        │
        │ reconnect
        ▼
  ┌─────────────────────────┐
  │ Reconnect               │
  │ rule: every stream back │
  └─────────────────────────┘
        │
        │ connect, resubscribe
        ▼
  ( broker )

  ( Safety )
        │
        │ query (since-timestamp)
        ▼
  ┌─────────────────────────┐
  │ Fetch executions        │
  │ rule: row has order id  │
  └─────────────────────────┘
        │
        ├── execution request ──▶ ( broker )
        │   ◀── executions
        │
        │ executions
        ▼
  ( Safety )
```

Main path in colour: orders out, ticks and order updates in. The right edge is one broker: the IBKR gateway talks to IBKR, and the simulated broker plays a tape and fills orders itself (feature 12). Dashed boxes are shared data structures.

### Feature cards

### 1. Connect and reconnect with every stream back *(was behaviour 1)*

#### Need

The engine needs order-status, account and market-data streams for every instrument from start to exit. Safety needs a way to bring the connection back after an outage.

#### Upstream gives

The broker API accepts a connect and a disconnect, and drops the connection without notice. After a drop the broker sends nothing on the old subscriptions.

#### Algorithm

1.  Connect with the Gateway's configuration; on success subscribe order status, account updates and the tick stream of every resolved contract.
2.  On a reconnect request from Safety: disconnect, connect, increase the connection generation, and subscribe every stream again. Callbacks that carry an older generation are dropped.
3.  Keep the next order id across a reconnect; never let the next order id fall back to a lower value.

#### Parameters

None.

#### Invariant

After every successful connect or reconnect, the order-status, account and market-data streams are active for every instrument the process trades.

### 2. Market data reaches the event buffer in time order *(was behaviours 2, 12 · System map decisions 26, 49)*

#### Need

The Event Loop needs every market-data message, in time order, as soon as the broker sends it: trades to build bars from, or completed bars. Safety needs the time of the last market-data message to judge whether the feed is alive.

#### Upstream gives

A broker gives one of two kinds of market data per contract: tick-by-tick trades (time, price, size), or completed OHLCV bars (klines) at a fixed resolution. The IBKR broker sends ticks on its reader thread. The simulated broker gives 1-second bars from its tape (feature 12). Each exchange has its own data session; a closed exchange sends nothing.

#### Algorithm

1.  Declare, per instrument, which message type the gateway gives: `tick_update`, or `bar_update` with its resolution (feature 14).
2.  For each trade: build a `tick_update` with instrument, time, price and size. For each completed bar: build a `bar_update` with instrument, resolution, open, high, low, close, volume and the bar's broker timestamp. Append the message to the event buffer.
3.  Stamp `recv_ts` (monotonic clock) on each message on the reader thread, when the message arrives; the 5 ms budget starts there ([System map decision 55](SYSTEM_MAP.md)).
4.  Raise the Event Loop's drain wake-up through the loop's thread-safe call. A reader thread changes no loop state directly.
5.  Update the last tick time, from either message type, only for instruments whose exchange data session is open, so a closed Eurex feed does not look dead while CME trades.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Market data message type | Fixed per gateway subclass and instrument (the broker decides) | `tick_update`; `bar_update` (`quote_update` reserved, out of scope) | IBKR: `tick_update`; simulated broker: `bar_update` |
| Bar resolution of a `bar_update` | Fixed per gateway subclass | 1 s | 1 s |

#### Invariant

Market-data messages keep their prices, sizes and instrument, and their timestamps never go backwards in the buffer. One instrument's feed carries one message type only.

### 3. Contracts come from the exchange profile *(was behaviour 13 and the contract rules · System map decisions 31, 37)*

#### Need

Orders, subscriptions, history, fills and positions need the broker's exact contract for each engine instrument. Two products can share one broker symbol: at IBKR, FDAX, FDXM and FDXS all appear as DAX.

#### Upstream gives

The broker answers a contract request with contract details (contract id, minimum tick, trading class). The exchange profile gives the venue, currency, broker contract identity, roll rule and session clocks of each exchange.

#### Algorithm

1.  Read venue, currency, trading class and roll rule from the instrument's exchange profile. A future resolves to its current contract by the roll rule; a stock resolves to its one listing.
2.  Ask the broker for the contract details and store the resolved contract in the contracts table, keyed by the engine's instrument.
3.  Every later operation (subscribe, order, history, fill, position) looks the contract up by instrument.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Asset class | Configurable per instrument | future, stock (options in a later version, [System map decision 54](SYSTEM_MAP.md)) | — |
| Exchange profile | Configurable per instrument | CME, Eurex, NASDAQ | — |

#### Invariant

Two instruments that share a broker symbol never share a contract, a tick subscription, a fill or a position. A new exchange needs a new exchange profile, not a Gateway change.

### 4. Orders reach the broker valid, on tick and numbered *(was behaviour 3 · decision 5)*

#### Need

Order Manager sends entry and exit orders, including resting stop and limit exits in OCA groups. An order the broker would refuse, or a price off the contract's tick, must not leave the engine. Order Manager and Safety need one unique id per order to trace every fill.

#### Upstream gives

The broker accepts MKT, STP and LMT orders with an OCA group and OCA type, and refuses a price that is not on the contract's minimum tick. At IBKR a stop triggers on the last price.

#### Algorithm

1.  **Check.** Reject locally, before sending, a priced order whose price is missing, zero, negative or not a number, an order for a contract with no known minimum tick, and an order with an OCA group but no valid OCA type. The caller always chooses the OCA type; the Gateway has no default.
2.  **Round.** Round STP and LMT prices to the minimum tick: BUY STP and SELL LMT round up, SELL STP and BUY LMT round down. Write the rounded price back into the order. The simulated broker uses the same rounding helper.
3.  **Number.** Order Manager takes the order id from `next_order_id()` (under a lock) before it sends the order. `place_order` sends the order with that id and never numbers the order again.
4.  **Send.** Send the order with the time in force and the outside-regular-hours flag, and put the resulting order update on the event buffer.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Order types | Fixed for v3 ([System map decision 56](SYSTEM_MAP.md)) | MKT, STP, LMT | — |
| OCA type | Chosen by the caller per order | 1, 2, 3 | none (required) |
| Time in force | Fixed for v3 ([System map decision 56](SYSTEM_MAP.md)) | DAY | DAY |
| Outside regular hours | Fixed for v3 ([System map decision 56](SYSTEM_MAP.md)) | allowed | allowed |

#### Invariant

Every placed order gets a distinct, increasing order id and ends FILLED, CANCELLED or REJECTED. Every STP and LMT price sent to the broker is on the contract's tick.

### 5. Cancels end in one clear answer *(was behaviour 4 · decision 6)*

#### Need

Order Manager cancels orders and must learn one final answer: cancelled, or refused. A cancelled order must never fill afterwards.

#### Upstream gives

The broker accepts a cancel by order id, sends an intermediate pending-cancel status, then sends Cancelled, or refuses the cancel with an error (at IBKR, codes 10147, 10148 and 161).

#### Algorithm

1.  Send the cancel for an active order id.
2.  Drop the broker's pending-cancel status.
3.  On Cancelled, put a CANCELLED order update on the buffer. On a refusal code, put a CANCEL_REJECTED order update for that order id.
4.  The simulated broker cancels only a resting order (→ CANCELLED); any other order id gets no event.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Cancel-refusal codes | Fixed per broker subclass (the broker defines the codes) | IBKR: 10147, 10148, 161 | — |

#### Invariant

A cancelled order never produces a later fill. A refused cancel never produces a CANCELLED update.

### 6. The simulated broker fills like the broker *(was part of behaviour 3 · decision 2)*

#### Need

A replay must produce the same order updates as a paper or live run, so the rest of the engine runs unchanged ([System map decision 6](SYSTEM_MAP.md)).

#### Upstream gives

In replay there is no broker. The Event Loop shows the simulated broker each completed bar before the strategies see the bar.

#### Algorithm

1.  Fill a MKT order at the reference price of the current bar, plus slippage. The current bar is the bar on which the strategy made the decision (bar N): the Event Loop shows bar N to the simulated broker before the strategies, so an order sent while bar N is dispatched fills on bar N. The fill time is the end time of bar N.
2.  Rest STP and LMT orders and emit OPEN. On each later bar of the instrument: a touch fills at the level, a gap fills at the bar's open.
3.  When both legs of an OCA group fire on one bar, the stop wins; the other legs become CANCELLED.
4.  Reject an order into an OCA group that already filled. A MKT order into a working group cancels the group's legs first.
5.  A resting fill is booked after strategies have seen its bar: one bar later than live. This difference is accepted.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| MKT reference price | Configurable | bar close | bar close |
| Slippage (ticks) | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any integer ≥ 0 | 0 |

#### Invariant

For every supported reference price and slippage: the simulated broker emits the same order statuses, in the same order, as the broker gateway would for the same fills.

### 7. Executions and open orders on request *(was behaviour 5 · decision 3)*

#### Need

Safety's repair and the Event Loop's stuck-order check attribute broker fills to orders, even when several strategies trade the same instrument.

#### Upstream gives

The broker answers requests for execution rows (optionally since a time) and for open-order snapshots.

#### Algorithm

1.  Request the executions since the given time, and the open orders.
2.  Return each execution row with order id, instrument, side, fill quantity, average price, currency, fill time and execution id.

#### Parameters

None.

#### Invariant

Every execution row carries its order id, so each fill traces to exactly one order.

### 8. History and seed bars in broker-neutral units *(was behaviours 7, 11 · System map decisions 30, 45)*

#### Need

The Event Loop seeds each strategy with daily history and prior-session intraday bars before the strategy trades. The Event Loop must not know any broker's duration format.

#### Upstream gives

The broker returns intraday bars for a contract and a duration in the broker's own units. Near a roll, the new contract's history is thin.

#### Algorithm

1.  Take the window as months, or as start and end dates, plus the session. Each subclass turns the window into its broker's request.
2.  Build the daily frame from intraday bars over the full session, clipped and aggregated per trading date, with bar dates in UTC. The window ends at the last completed session's real close, on that session's own clock.
3.  Before a roll is complete, take liquidity-dependent history from the previous contract, and name the sessions where the feed is the new, still-thin contract.
4.  The simulated broker builds the same frame from its tape (feature 12).

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| History window unit | Chosen by the caller per request | months; start and end dates | — |
| Intraday bar size of the source | Fixed per subclass | IBKR: daily frame from 1-minute bars, seed bars as 1-second bars; simulated broker: its 1-second tape | as supported |

#### Invariant

History arrives in time order, with no bar after the requested window. A value the frame cannot serve exactly is never served silently.

### 9. Positions and balances per account and currency *(was behaviour 8)*

#### Need

Strategy books position and available funds per account, in the instrument's currency (USD for CME, EUR for Eurex). Safety and the deploy service read position snapshots.

#### Upstream gives

The broker streams position and account-value updates per account, with decimal amounts, and answers position snapshot requests.

#### Algorithm

1.  Turn each position update into a position event: signed size, side, average price, account; zero size is flat.
2.  Turn each available-funds update into a balance event per account and per currency. Broker decimals become floats.
3.  Answer a position snapshot for the configured accounts; on timeout, raise an error and remove the waiter.
4.  The simulated broker reports from its simulated book.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Position snapshot timeout | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any positive seconds | 5 s |

#### Invariant

Every update carries its account, and every money amount carries its currency. A timed-out snapshot leaves no waiter behind.

### 10. Broker status messages become events or nothing *(was behaviour 9)*

#### Need

Order Manager must learn when the broker rejects an order, even when the broker sends no order status for that reject. Safety must learn when a data farm recovers, so that Safety can restore subscriptions.

#### Upstream gives

The broker sends error and status messages with numeric codes, for orders, for data requests and for connectivity.

#### Algorithm

1.  Ignore harmless codes.
2.  Track connectivity and each data farm's status; on recovery, raise one resubscribe request.
3.  Turn an order-reject code into a REJECTED order update by order id, never for a data request id. Code 401 (an order field failed validation) gets no order status from IBKR, so without this rule the order would wait, unprotected, until Safety's repair declared the order dead.
4.  Resolve or fail pending history requests by their codes.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Order-reject codes | Fixed per broker subclass (the broker defines the codes) | IBKR: 110, 200, 201, 203, 321, 401, 512 | — |

#### Invariant

No status message stops the engine, and no status message creates an order update for an order the broker did not reject.

### 11. The socket heartbeat answers within a timeout *(was behaviour 6)*

#### Need

Safety must tell a dead socket from a quiet market. A quiet market sends no ticks, so tick silence alone cannot prove the socket is dead.

#### Upstream gives

The broker answers a cheap request (at IBKR, the current-time request) while the socket is alive.

#### Algorithm

1.  Send the cheap request and wait up to the heartbeat timeout.
2.  Return True when the broker answers within the timeout, otherwise False.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Heartbeat timeout | Configurable per call; Safety passes its config value | any positive seconds | 3.0 s |

#### Invariant

For every timeout value: the heartbeat returns True if and only if the socket answers within that timeout.

### 12. The simulated broker prepares its own replay data *(was behaviour 15 · System map decision 45 · decision 7)*

#### Need

A replay needs ticks, daily history and seed bars with the same definitions as the broker's. The runner must write no data files and must read no strategy before the strategy exists.

#### Upstream gives

The config names a tape source, a replay date and a session. The tape is a file (or a synthetic generator) much larger than one day.

#### Algorithm

1.  Read the tape lazily, and collect only the rows each request needs.
2.  Before replay starts, check that every row in the replay window and in the history the tape serves falls inside the session, using the exchange profile's session hours, calendar and early closes. Refuse a tape with rows outside the session, and a day-session-only tape given for an overnight session.
3.  Answer daily history and seed-bar requests from the tape, like the broker gateway; return an empty list when the tape has no prior session.
4.  Pad the replay window before and after the session with flat bars, when the option is on.
5.  A synthetic generator is a second tape source behind the same simulated broker.
6.  Send each replay bar as one `bar_update` (feature 2).
7.  When the config sets `replay.journal`, write the broker journal: one row per order received and one row per fill sent, in the journal format of the [golden sample](SYSTEM_MAP.md).

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Tape source | Configurable | Parquet file, synthetic generator | — |
| Flat-bar padding before / after the session | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) (`replay.pad_before_s`, `replay.pad_after_s`) | any seconds ≥ 0; 0 means off | 0 (off) |

#### Invariant

The runner writes no data file. Replay history has the same definitions as broker history (feature 8).

### 13. Serve a strategy added while running *(was behaviour 14 · System map decisions 34–36)*

#### Need

The Event Loop adds a strategy to a running paper or live process ([Event Loop feature 12](EVENT_LOOP_MAP.md)), and the strategies already running must keep their feeds.

#### Upstream gives

The broker accepts new subscriptions and cancels existing ones while the connection is up.

#### Algorithm

1.  Resolve a contract and subscribe ticks for an instrument new to the process, while existing feeds keep running.
2.  Release a tick subscription when a failed candidate's new instrument is cleaned up. The simulated broker does nothing here.
3.  Declare instrument sessions again from the runtime book when a new instrument is reserved, so a new Eurex feed is judged on the Frankfurt clock.
4.  Answer the candidate's admission check from the positions and open-orders reads; a broker that does not answer makes the Event Loop refuse the request with `unmanaged_exposure`.

#### Parameters

None.

#### Invariant

Adding a strategy never subscribes again, or drops, a feed that a running strategy uses.

### 14. One explicit interface, picked by name *(was behaviour 10 · System map decision 30)*

#### Need

The runner chooses a broker by config, and a new broker must plug in without changes to the Event Loop, Order Manager or Safety. A broker subclass that misses an operation must fail at start, not return nothing during trading.

#### Upstream gives

Brokers differ in capability: some report feed liveness, commissions or roll mismatches, and some do not.

#### Algorithm

1.  Declare required operations (connect, disconnect, reconnect, orders, cancels, contracts, subscriptions, history, executions, positions) with no body; building a subclass that misses one fails at once.
2.  Declare capabilities with a safe default: the market-data message type per instrument (feature 2; default `tick_update`), feed liveness, the resubscribe request, market-data readiness, roll-mismatch dates, commissions, the current bar for simulated fills, session declaration, and whether a failed intraday seed is fatal. A subclass overrides a capability only when the broker has it.
3.  Register each subclass under a name; the runner picks the subclass by that name.

#### Parameters

| Parameter    | Configurable? | Supported                      | Default |
|--------------|---------------|--------------------------------|---------|
| Gateway name | Configurable  | IBKR gateway, simulated broker | —       |

#### Invariant

No caller looks a Gateway operation up by name at runtime.

## Tests

The goal and features as measurable cases. A test with parameter cases runs once per supported value. "Both subclasses" means the case runs on the IBKR gateway (against a broker stub) and on the simulated broker. No Gateway test exists in v3 yet; every row is not written yet.

**End to end · the sample (1 test)**

|  |  |
|----|----|
| Test | `tests/gateway/test_gateway_sample.py::test_golden_replay_delivers_every_bar_then_end_of_data_and_fills_two_orders` |
| Input | The system golden sample on the simulated broker: the `golden_session` synthetic tape for MES on 2026-04-15 (NY_RTH, 60 prior sessions), and two scripted MKT orders, a BUY 1 and a SELL 1, sent on the phase B entry bar and the target bar ([System map golden sample](SYSTEM_MAP.md)). |
| Parameter cases | subclass ∈ {simulated broker} |
| Passes when | Daily history gives ATR(14) = 20.00 from the 60 prior sessions; 23,400 `bar_update` messages are in the buffer with time never going backwards, followed by `end_of_data`; each MKT order fills at the close of the bar it was sent on; exactly two FILLED order updates; the executions query returns two rows whose order ids match the two orders; the journal has the 4 rows in the journal format of the golden sample. |
| Fails when | A bar is dropped, duplicated or out of order; an order fills on another bar; an order produces no order update; an execution row has no order id; or a journal row breaks the journal format. |

### End-to-end scenarios *(agreed 2026-11-09)*

Each scenario drives the component only through its public interface, from a known start state, and checks one exact, observable output. A scenario tells one story across several features; the feature tables below keep the single rules. File: `tests/gateway/test_gateway_scenarios.py`. A recording event buffer stands in for the Event Loop; the test plays Order Manager (it takes each id from `next_order_id()`) and Safety. IBKR cases run on an IBKR paper account, only while the instrument's session is open, under the test marker `ibkr_paper`, outside the default test run. The paper account comes from the test `.env` file, and the test instruments (MES, FDAX, FDXM, ES and the NASDAQ stock) come from a test instrument config. A resting LMT or STP order sits the distance away from the last price: 2% of the last price, in whole ticks, so the order does not fill during the rest check. Waits (configurable in the test config): the first-message wait (30 s) is how long the test waits for the first market-data message after a subscribe; the fill wait (30 s) is how long the test waits for FILLED after a marketable order; the rest check (10 s) is how long the test watches a far-away order stay OPEN with no fill. Simulated cases run on a scripted tape in the default test run. A fill price on the paper account is not known in advance, so an IBKR case checks the order status sequence, quantities, ids and tick alignment; a simulated case also checks the exact price.

**S1. Subscribe to a market-data stream (features 1, 2, 3)**

|  |  |
|----|----|
| Test | `test_scenario_subscribe_stream` |
| Given | Connected. The instrument's exchange data session is open (IBKR), or a scripted tape of 10 one-second bars is loaded (simulated). |
| When | Resolve the contract for the instrument, subscribe market data, and wait up to the first-message wait. |
| Parameter cases | subclass ∈ {IBKR paper → `tick_update`, simulated broker → `bar_update`} × instrument ∈ {MES (CME future), one NASDAQ stock named in the test config} |
| Passes when | IBKR: at least one `tick_update` for the instrument arrives; every price is a whole multiple of the contract's minimum tick; every size is \> 0; `recv_ts` never goes backwards; the last tick time moves forward. Simulated: exactly 10 `bar_update` messages at resolution 1 s, in time order, then `end_of_data`. |
| Fails when | No message within the first-message wait while the session is open; a price off the tick; a message carries another instrument; `recv_ts` or bar time goes backwards; a simulated bar is missing or repeated. |

**S2. Resolve contracts from the exchange profile (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_resolve_contracts` |
| Given | Connected to the IBKR paper account. |
| When | Resolve MES, FDAX, FDXM and the NASDAQ stock. |
| Parameter cases | subclass ∈ {IBKR paper} |
| Passes when | MES resolves to the contract that the CME roll rule names for today; FDAX and FDXM get different contract ids although both appear as DAX at IBKR; the stock resolves to its one listing; each contract has a known minimum tick. |
| Fails when | Two instruments share a contract id; a future resolves to a contract other than the roll rule's; a contract has no minimum tick. |

**S3. Place a market order (features 4, 6, 7, 9)**

|  |  |
|----|----|
| Test | `test_scenario_market_order_fills` |
| Given | Connected; the paper account is flat on the instrument (IBKR), or bar N of the tape is being shown (simulated). |
| When | Take id A; place BUY 1 MKT with id A; wait up to the fill wait. Then take id B; place SELL 1 MKT with id B; wait up to the fill wait. |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker} × instrument ∈ {MES, NASDAQ stock}; simulated slippage ∈ {0, 2 ticks} |
| Passes when | The order updates for A end in FILLED, filled quantity 1, average price \> 0 and on the tick; the same for B. B \> A. Executions read after the fills: exactly one row for A (BUY, 1, currency USD) and one for B (SELL, 1), each with its order id. Position updates for the account: +1 after A, 0 after B. Simulated: each fill price is the reference price (bar close) of bar N with the configured slippage, and the fill time is the end time of bar N. |
| Fails when | No FILLED within the fill wait; a filled quantity other than 1; more or fewer than one execution row per order; a row without order id; the position is not +1 then 0; a simulated fill on another bar or at another price. |

**S4. Place a resting limit order (features 4, 7)**

|  |  |
|----|----|
| Test | `test_scenario_limit_order_rests_rounded` |
| Given | Connected; the last price L is known. |
| When | Place BUY LMT 1 at L less the distance, plus 0.4 tick (off the tick); place SELL LMT 1 at L plus the distance, plus 0.4 tick. Wait the rest check. Read open orders. |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker (tape never reaches either price)} × instrument ∈ {MES, NASDAQ stock} |
| Passes when | The BUY price sent is rounded down to the tick and the SELL price rounded up. Each order gets an OPEN update and no fill. The open-orders read lists both ids with the rounded prices. (Cleanup: the cancel scenario.) |
| Fails when | A price sent off the tick, or rounded the wrong way; no OPEN update; a fill; an order missing from the open-orders read. |

**S5. Place a limit order that fills (features 4, 6)**

|  |  |
|----|----|
| Test | `test_scenario_limit_order_fills` |
| Given | Connected; the last price L is known. |
| When | IBKR: place BUY LMT 1 at L + 5 ticks (marketable), then close with SELL 1 MKT. Simulated: place BUY LMT 1 at level P; the next bar touches P (case touch) or opens below P (case gap). |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker} × simulated case ∈ {touch, gap} |
| Passes when | IBKR: FILLED 1 at an average price ≤ the limit, with one execution row for the id. Simulated: the order is OPEN on its bar and FILLED on the next bar, at P (touch) or at that bar's open (gap). |
| Fails when | No fill; a fill above the limit; a simulated fill at another price or on the order's own bar. |

**S6. Place a stop order (features 4, 5, 6)**

|  |  |
|----|----|
| Test | `test_scenario_stop_order` |
| Given | Connected; long 1 for the SELL case (simulated: after a scripted BUY fill). |
| When | IBKR: place SELL STP 1 at L less the distance, less 0.4 tick, and BUY STP 1 at L plus the distance, plus 0.4 tick; wait the rest check; then cancel both. Simulated: place SELL STP 1 at S; the next bar touches S (case touch) or opens below S (case gap). |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker} × simulated case ∈ {touch, gap} |
| Passes when | IBKR: the SELL STP price is rounded down and the BUY STP price up; both orders are OPEN with no fill; each cancel ends in one CANCELLED. Simulated: FILLED at S (touch) or at the bar's open (gap), on the bar after placement. |
| Fails when | A stop price off the tick or rounded the wrong way; a fill while the market is away from the stop (IBKR); a simulated fill at another price or bar. |

**S7. Cancel a resting order (features 5, 7)**

|  |  |
|----|----|
| Test | `test_scenario_cancel_resting_order` |
| Given | A resting LMT order with id A (from the resting-limit scenario). |
| When | Cancel A; wait the rest check; read open orders. |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker} |
| Passes when | Exactly one CANCELLED update for A; no pending-cancel status in the buffer; no fill for A after the CANCELLED; the open-orders read no longer lists A. |
| Fails when | No CANCELLED, or two; a pending-cancel status forwarded; a fill for A after the cancel. |

**S8. Cancel a filled order (features 5, 10)**

|  |  |
|----|----|
| Test | `test_scenario_cancel_filled_order` |
| Given | A MKT order with id A that is FILLED (from the market-order scenario). |
| When | Cancel A. |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker} |
| Passes when | IBKR: exactly one CANCEL_REJECTED update for A (refusal code 10147, 10148 or 161) and no CANCELLED. Simulated: no event for A. |
| Fails when | A CANCELLED update for A; the engine stops; any simulated event for A. |

**S9. Place an OCA group (features 4, 6)**

|  |  |
|----|----|
| Test | `test_scenario_oca_group` |
| Given | Long 1 (after a BUY 1 MKT fill). |
| When | Place SELL STP 1 (far below) and SELL LMT 1 (far above) with one OCA group and OCA type 2. IBKR: wait the rest check, cancel both legs, then close with SELL 1 MKT. Simulated: play the case. |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker} × simulated case ∈ {LMT touched; both legs touched on one bar; MKT into the working group; new order into a group that already filled} |
| Passes when | IBKR: both legs OPEN with the same OCA group; each cancel ends in one CANCELLED; the position ends at 0. Simulated: LMT touched → LMT FILLED at its level and STP CANCELLED; both touched → STP FILLED and LMT CANCELLED; MKT into the group → both legs CANCELLED before the MKT is FILLED; order into a filled group → REJECTED. |
| Fails when | A leg without the OCA group; two legs of one group filled; a MKT filled while a leg of its group still works; an order into a filled group accepted. |

**S10. Refuse an invalid order before sending (features 4)**

|  |  |
|----|----|
| Test | `test_scenario_invalid_order_refused_locally` |
| Given | Connected. |
| When | Place each invalid order of the case. |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker} × case ∈ {LMT price missing, 0, −1, NaN; OCA group with no OCA type; contract with no known minimum tick} |
| Passes when | Each place call raises; nothing reaches the broker (the open-orders read is unchanged; the simulated journal has no new row); no order update is put on the buffer. |
| Fails when | A place call returns normally; the broker or the journal sees the order; an order update appears. |

**S11. Read positions and balances (features 9)**

|  |  |
|----|----|
| Test | `test_scenario_positions_and_balances` |
| Given | Long 1 MES after a BUY 1 MKT fill at average price P. |
| When | Ask for a position snapshot for the configured accounts. Read the balance updates. |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker}; snapshot timeout ∈ {5 s} |
| Passes when | The snapshot returns +1 MES with average price P for the account within the timeout. Each balance update carries its account and the currency USD. |
| Fails when | The snapshot times out while connected; a size other than +1; a balance update without account or currency. |

**S12. Fetch history and seed bars (features 8, 12)**

|  |  |
|----|----|
| Test | `test_scenario_history_and_seed_bars` |
| Given | Connected (IBKR) or the golden tape loaded (simulated). |
| When | Request daily history for MES over the window; request seed bars for the prior session. |
| Parameter cases | subclass ∈ {IBKR paper, simulated broker} × window ∈ {2 months, start and end dates}; simulated tape ∈ {golden tape, tape with no prior session} |
| Passes when | Daily bars are in time order with dates in UTC; the last daily bar is the last completed session; no bar falls after the window. Seed bars are 1-second bars inside the prior session. Simulated golden tape: daily ATR(14) = 20.00. Tape with no prior session: seed bars are an empty list. |
| Fails when | A bar out of order or after the window; a daily bar for a session that is not complete; a seed bar outside the prior session; a simulated ATR(14) other than 20.00. |

**S13. Heartbeat and reconnect (features 1, 11)**

|  |  |
|----|----|
| Test | `test_scenario_heartbeat_and_reconnect` |
| Given | Connected, streaming MES, with the last order id K. |
| When | Heartbeat with the timeout; reconnect; wait up to the first-message wait; place BUY 1 MKT and SELL 1 MKT. Then disconnect and heartbeat again. |
| Parameter cases | subclass ∈ {IBKR paper}; heartbeat timeout ∈ {3.0 s} |
| Passes when | The first heartbeat returns True within the timeout. After the reconnect: MES ticks arrive again, both orders give order updates and fills, the first new order id is \> K, and no event carries the old connection generation. After the disconnect the heartbeat returns False within the timeout. |
| Fails when | True from a disconnected socket, or False from a connected one; a stream missing after the reconnect; an order id ≤ K; an event from the old generation. |

**S14. Add an instrument while streaming (features 13)**

|  |  |
|----|----|
| Test | `test_scenario_add_instrument_while_streaming` |
| Given | Connected, streaming MES. |
| When | Resolve and subscribe ES; wait up to the first-message wait. Then release the ES subscription. |
| Parameter cases | subclass ∈ {IBKR paper} |
| Passes when | ES ticks arrive; MES is subscribed exactly once during the whole run and its ticks keep arriving. After the release no ES tick arrives and MES still streams. |
| Fails when | MES is subscribed again or stops; no ES tick; an ES tick after the release. |

**1. Connect and reconnect (3 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_reconnect_restores_order_account_and_tick_streams` | subclass ∈ {IBKR, simulated} | active streams after reconnect | order status, account and every instrument's ticks |
| `test_stale_subscription_dropped_after_reconnect` | — | events from an older connection generation | 0 |
| `test_order_ids_never_regress_on_reconnect` | — | first order id after reconnect | \> last order id before |

**2. Market data reaches the event buffer in time order (4 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_market_data_appended_in_time_order` | subclass ∈ {IBKR (`tick_update`), simulated (`bar_update`)} | timestamps in the buffer | never decreasing; prices, sizes, instrument unchanged |
| `test_declared_message_type_matches_messages_sent` | — | messages of a type other than the declared one, per instrument | 0 |
| `test_reader_thread_wakes_loop_through_thread_safe_call` | — | loop state written from the reader thread | 0 writes; one wake-up call per burst |
| `test_last_tick_ts_ignores_instrument_whose_data_session_is_closed` | — | last tick time while Eurex is closed and CME trades | follows CME ticks only |

**3. Contracts come from the exchange profile (2 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_contract_resolved_from_exchange_profile` | (exchange, asset class) ∈ {(CME, future), (Eurex, future), (NASDAQ, stock)} | venue, currency, trading class of the resolved contract | equal to the exchange profile |
| `test_shared_symbol_instruments_keyed_apart` | pair ∈ {FDAX/FDXS, FDAX/FDXM} | contracts, subscriptions, fills, positions that collide | 0 |

**4. Orders reach the broker valid, on tick and numbered (4 tests · 19 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_stop_and_limit_prices_rounded_by_side_and_type` | type ∈ {STP, LMT} × side ∈ {BUY, SELL} | price sent vs. price given | BUY STP, SELL LMT up; SELL STP, BUY LMT down; on tick |
| `test_priced_order_without_valid_price_rejected_locally` | price ∈ {missing, 0, negative, NaN, text} × type ∈ {STP, LMT} | orders reaching the broker | 0 |
| `test_oca_group_needs_valid_type` | OCA type ∈ {none, 1, 2, 3} | local reject | none → rejected; 1, 2, 3 → sent |
| `test_order_ids_distinct_and_increasing` | orders ∈ {1000} | duplicate or decreasing ids | 0 |

**5. Cancels end in one clear answer (3 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_refused_cancel_becomes_cancel_rejected_never_cancelled` | code ∈ {10147, 10148, 161} | update for the order id | CANCEL_REJECTED; no CANCELLED |
| `test_pending_cancel_status_never_forwarded` | — | pending-cancel updates in the buffer | 0 |
| `test_sim_cancel_of_non_resting_order_emits_nothing` | — | events for that order id | 0 |

**6. The simulated broker fills like the broker (4 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_sim_resting_order_fills_on_touch_or_gap` | case ∈ {touch, gap} | fill price | touch → level; gap → bar open |
| `test_sim_stop_wins_when_both_legs_touch` | — | filled leg; other leg's status | stop; CANCELLED |
| `test_sim_order_into_filled_group_rejected` | — | status of the new order | REJECTED |
| `test_sim_market_fill_at_reference_price_plus_slippage` | reference ∈ {bar close} × slippage ∈ {0, 1, 2} ticks | fill price | reference ± slippage × tick, exact |

**7. Executions and open orders on request (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_every_execution_row_carries_order_id_and_currency` | subclass ∈ {IBKR, simulated} | rows missing a field | 0 |

**8. History and seed bars in broker-neutral units (3 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_daily_frame_has_no_look_ahead_past_last_completed_close` | exchange ∈ {CME, Eurex} | bars after the last completed close, on the session's clock | 0 |
| `test_history_window_in_months_or_dates` | unit ∈ {months, dates} | broker request built | covers exactly the window |
| `test_roll_history_from_previous_contract` | — | contract used before the roll completes | the previous contract |

**9. Positions and balances per account and currency (2 tests · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_balance_update_per_currency` | currency ∈ {USD, EUR} | balance events per currency | one per currency, never summed |
| `test_position_snapshot_timeout_leaves_no_waiter` | — | waiters after timeout | 0; error raised |

**10. Broker status messages become events or nothing (2 tests · 8 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_order_reject_codes_become_rejected_by_order_id` | code ∈ {110, 200, 201, 203, 321, 401, 512} | REJECTED updates for an order id; for a data request id | 1; 0 |
| `test_data_farm_recovery_raises_one_shot_resubscribe` | — | resubscribe requests per recovery | 1 |

**11. The socket heartbeat answers within a timeout (1 test · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_heartbeat_true_iff_answer_within_timeout` | answer delay ∈ {0.5 × timeout, 2 × timeout, never} at timeout 3.0 s | return value | True; False; False |

**12. The simulated broker prepares its own replay data (3 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_replay_refuses_tape_rows_outside_session` | exchange ∈ {CME, Eurex} | replay start with an out-of-session row | refused |
| `test_replay_daily_history_matches_broker_definitions` | tape ∈ {Parquet, synthetic} | daily frame vs. the broker gateway's frame for the same bars | equal |
| `test_replay_no_prior_session_returns_empty_seed` | — | seed bars returned | empty list |

**13. Serve a strategy added while running (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_new_instrument_subscribed_without_touching_running_feeds` | — | re-subscriptions or drops of existing feeds | 0 |
| `test_sessions_declared_again_for_new_instrument` | — | clock used for a new Eurex feed | Frankfurt |

**14. One explicit interface, picked by name (2 tests · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_subclass_missing_required_operation_fails_at_build` | — | build of an incomplete subclass | raises |
| `test_gateway_picked_by_registered_name` | name ∈ {IBKR gateway, simulated broker} | class built by the runner | the registered class |

## Open questions

Gaps found while writing the features. Each one needs a human answer; the answer goes into the feature's Parameters or Algorithm.

All decided on 2026-11-09.

1.  **Market data type (feature 2).** `tick_update` and `bar_update`. Options and their feed move to a later version, and `quote_update` comes into scope with them ([System map decision 54](SYSTEM_MAP.md)).
2.  **Order types and time in force (feature 4).** MKT, STP, LMT; DAY; outside regular hours allowed. Fixed for v3; no stop-limit ([System map decision 56](SYSTEM_MAP.md)).
3.  **Simulated slippage (feature 6).** Any integer ≥ 0 ticks, configurable per process; default 0.
4.  **History source bar size (feature 8).** IBKR builds the daily frame from 1-minute bars and serves seed bars as 1-second bars; the simulated broker uses its 1-second tape.
5.  **Position snapshot timeout (feature 9).** Configurable per process; default 5 s.
6.  **Flat-bar padding (feature 12).** `replay.pad_before_s` and `replay.pad_after_s`, any seconds ≥ 0; default 0 (off).
7.  **Heartbeat timeout (feature 11).** Set by Safety's config (`heartbeat_timeout_s`); default 3.0 s.

### Open questions on the end-to-end scenarios

1.  **Scenario waits (End-to-end scenarios).** Decided 2026-11-09: first-message wait 30 s, fill wait 30 s, rest check 10 s; configurable in the test config with these defaults.
2.  **Resting distance (End-to-end scenarios).** Decided 2026-11-09: 2% of the last price, in whole ticks.
3.  **Paper account and stock (End-to-end scenarios).** Decided 2026-11-09: the account comes from the test `.env` file; the instruments come from a test instrument config.

## Reference

Implementation detail and history. The skeleton still cites the old behaviour numbers; each feature card lists the behaviours it replaces.

### Step → code

Each box of the algorithm map and the code that carries it. `Gateway` is in `gateways/gateway.py`; the simulated broker is in `gateways/simulation_gateway.py`; the IBKR gateway is Planned.

| Step | Code |
|----|----|
| Check and round order | `Gateway.place_order()`; `gateways/utils.py` · `round_price()` |
| Assign order id | `Gateway.next_order_id()` |
| Send cancel | `Gateway.cancel_order()` |
| Resolve contract | `Gateway.resolve_contract()`, `subscribe_tick()`, `unsubscribe_tick()`, `set_instrument_sessions()` |
| \[ contracts by instrument \] | IBKR gateway state · Planned |
| Append tick | IBKR gateway tick callback · Planned; `Gateway.last_tick_ts` |
| Fill on bar (simulated) | `SimulationGateway.on_bar()`; `OrderFillingEngine.try_fill()` |
| Translate status | IBKR gateway error handler · Planned; `Gateway.consume_resubscribe_request()` |
| Report account | `Gateway.subscribe_account_update()`, `get_positions()` |
| Build history frame | `Gateway.get_daily_history()`, `get_intraday_seed_bars()`, `roll_mismatch_dates()`; tape: `TapeSource`, `ParquetTape`, `SyntheticTape`, `check_tape_in_session()` |
| Probe socket | `Gateway.heartbeat(timeout=3.0)` |
| Reconnect | `Gateway.connect()`, `disconnect()`, `reconnect()`, `subscribe_order_update()` |
| Fetch executions | `Gateway.get_executions(since)`, `get_open_orders()`, `get_commissions()` |
| Interface and registry | `Gateway`, `register_gateway()` |

### Skeleton code

The Gateway interface for v3, with two implementations: a broker gateway (IBKR first) and the simulated broker. Orders and execution rows carry the keys named in features 4 and 7; contracts are resolved from an `Instrument` (System map decision 37).

### Major state

| Where | State | What it holds |
|----|----|----|
| Interface | `event_buffer` | The one shared queue the Event Loop drains. Every tick, order update and balance/position update is appended as `{event_type, data, enqueued_at}`; event types are `tick`, `order_update`, `position_update`, `balance_update`, `end_of_data`. |
| Interface | `last_tick_ts` | Time of the last tick on an instrument whose data session is open. Safety's feed-liveness input. |
| Broker gateway | contracts | Resolved contracts by instrument. |
| Broker gateway | instrument sessions | Instrument → session, so liveness skips instruments whose data session is closed. |
| Broker gateway | connection generation | Bumped on each (re)connect; stale subscriptions are dropped by it. |
| Broker gateway | next order id | Guarded by a lock; never regresses on reconnect. |
| Broker gateway | request waiters | Futures for executions, open orders, positions, history and contract-details requests in flight. |
| Simulated broker | tape, current bars | The replay tape; the latest bar per instrument, which fills are priced off. |
| Simulated broker | positions, executions, balances | The simulated book, with balances per currency. |
| Simulated broker | resting orders, filled groups | Resting STP/LMT orders by id; OCA groups already filled. |
| Simulated broker | fill engine | Prices fills: reference price (bar close by default) and slippage in ticks. |

### Methods by behaviour

| Old behaviour | Methods |
|----|----|
| 1\. Connection lifecycle | `connect()`, `disconnect()`, `reconnect()`, `subscribe_order_update()` |
| 2\. Market data | `subscribe_tick()`, `last_tick_ts`, `market_data_ready()`, `set_instrument_sessions()` |
| 3\. Placing orders | `next_order_id()`, `place_order()`, `resolve_contract()`, `on_bar()` |
| 4\. Cancelling | `cancel_order()` |
| 5\. Queries | `get_executions(since)`, `get_open_orders()`, `get_commissions()` |
| 6\. Heartbeat | `heartbeat(timeout=3.0)` |
| 7\. History | `get_daily_history()`, `get_intraday_seed_bars()`, `roll_mismatch_dates()` |
| 8\. Account updates | `subscribe_account_update()`, `get_positions()` |
| 9\. Status messages | `consume_resubscribe_request()` |
| 14\. Hot-add | `unsubscribe_tick()` |

**Full skeleton code (python)**

``` python
"""Skeleton contract for the Gateway component (Alchemist v3)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from datetime import date
from typing import Any

from alchemist.instruments import Instrument

class Gateway(ABC):
    """Base of every broker gateway and of the simulated broker."""

    event_buffer: deque
    #   the one shared queue the Event Loop drains; entries are
    #   {'event_type', 'data', 'enqueued_at'} with event_type in
    #   tick_update, bar_update, order_update, position_update,
    #   balance_update, end_of_data

    # ---- required (Behaviours 1-9) ----
    @abstractmethod
    async def connect(self) -> None:
        """Behaviour 1: connect to the broker or replay source."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Behaviour 1: close the connection and release resources."""

    @abstractmethod
    async def reconnect(self) -> None:
        """Behaviour 1: re-establish the connection and restore subscriptions."""

    @abstractmethod
    async def subscribe_order_update(self) -> None:
        """Behaviour 1: subscribe to order status updates."""

    @abstractmethod
    async def resolve_contract(self, instrument: Instrument) -> Any:
        """Behaviour 3: qualify the instrument's current contract (exchange profile roll rule)."""

    @abstractmethod
    async def subscribe_tick(self, contract: Any) -> None:
        """Behaviour 2: subscribe to real-time ticks."""

    @abstractmethod
    def next_order_id(self) -> int:
        """Behaviour 3: a monotonic, unique order id."""

    @abstractmethod
    async def place_order(self, order: dict[str, Any]) -> None:
        """Behaviour 3: round, validate, send; queue the order update."""

    @abstractmethod
    async def cancel_order(self, order_id: int) -> None:
        """Behaviour 4: cancel a pending order."""

    @abstractmethod
    async def get_executions(self, since: float | None = None) -> list[dict[str, Any]]:
        """Behaviour 5: execution rows, each with its order_id and currency."""

    @abstractmethod
    async def get_open_orders(self, timeout: float = 5.0) -> list[dict[str, Any]]:
        """Behaviour 5: orders still working at the broker."""

    @abstractmethod
    async def heartbeat(self, timeout: float = 3.0) -> bool:
        """Behaviour 6: True iff the socket answers within the timeout."""

    @abstractmethod
    async def get_daily_history(self, instrument: Instrument, start: date,
                                end: date, session: str) -> Any:
        """Behaviours 7, 11: daily frame in broker-neutral units, no look-ahead."""

    @abstractmethod
    async def get_intraday_seed_bars(self, instrument: Instrument, session: str) -> list:
        """Behaviour 7: prior-session 1-second bars for intraday seeding."""

    @abstractmethod
    async def subscribe_account_update(self) -> None:
        """Behaviour 8: account, position and balance streams."""

    @abstractmethod
    async def get_positions(self, timeout: float = 5.0) -> list[dict]:
        """Behaviour 8: position snapshot for the configured accounts."""

    # ---- capabilities with safe defaults (Behaviour 10) ----
    @property
    def last_tick_ts(self) -> float | None:
        """Behaviour 2: last tick on an instrument whose data session is open."""
        return None

    def set_instrument_sessions(self, sessions: dict[Instrument, str]) -> None:
        """Behaviours 2, 14: which data session judges each instrument's feed."""

    def market_data_type(self, instrument: Instrument) -> tuple[str, float | None]:
        """Feature 2: ('tick_update', None) or ('bar_update', resolution_s)."""
        return ("tick_update", None)
    def market_data_ready(self) -> bool:
        return True

    def consume_resubscribe_request(self) -> bool:
        """Behaviour 9: read and reset the one-shot resubscribe request."""
        return False

    def roll_mismatch_dates(self) -> frozenset:
        """Behaviour 7: sessions served by a newly rolled, still-thin contract."""
        return frozenset()

    async def get_commissions(self) -> dict[str, dict]:
        return {}

    def on_bar(self, instrument: Instrument, bar: dict[str, Any]) -> None:
        """Behaviour 3: current bar from the Event Loop; drives simulated fills."""

    def intraday_seed_failure_is_fatal(self) -> bool:
        return False

    async def unsubscribe_tick(self, contract: Any) -> None:
        """Behaviour 14: release a failed hot-add candidate's new feed."""

GATEWAYS: dict[str, type[Gateway]] = {}   # Behaviour 10: picked by name
```

### Decisions already made

### 1. One Gateway interface for live and simulation?

**Answer:** Yes — one Gateway interface covers both the live broker and simulation/replay.

**Rejected:** Rejected: splitting it into separate market-feed and order-router components.

### 2. Where does simulated execution live?

**Answer:** Inside the simulated broker, which holds a fill engine (bar reference price, optional slippage in ticks) and presents exactly the same asynchronous interface and events as the broker gateway.

### 3. How are executions attributed?

**Answer:** Every execution row carries its `order_id`, so Safety's repair can attribute fills correctly even when several strategies trade the same instrument at once.

### 4. How is data liveness tracked?

**Answer:** The last-tick time is a property on the Gateway, so Safety can watch feed liveness separately from the socket heartbeat.

### 5. Who rounds resting prices?

**Answer:** The Gateway, silently, choosing the direction from side and type, with one shared rounding helper the simulated broker also uses.

### 6. What about the broker's pending-cancel status and refused cancels?

**Answer:** Pending-cancel is dropped: Order Manager learns of a cancel only from the final Cancelled. A refused cancel becomes a CANCEL_REJECTED update by order id.

### Agreed at finalize · 2026-11-09

### 7. Who prepares replay data?

**Answer:** The simulated broker (`SimulationGateway`), on request, from a tape source (feature 12). The runner only names the tape, the date and the session in config.

**Rejected:** Rejected: the runner slicing the tape and writing history files before start (a second history path beside the Gateway, and it must read strategies before they exist to know which columns to build).

*agreed 2026-11-09 · 2026-11-07*
