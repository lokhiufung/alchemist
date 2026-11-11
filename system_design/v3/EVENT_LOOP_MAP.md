# EVENT_LOOP_MAP — Event Loop · Alchemist v3

The engine's heartbeat. It drains one shared queue of events, packs ticks into bars at each strategy's resolution, and hands every bar and order update to the right strategies — in time order, exactly once — while keeping one broken strategy from stopping the rest.

Frozen contract for the Event Loop component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `event_loop_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-10.** **v3 final, 2026-11-09 — agreed.** The End-to-end scenarios in Tests were agreed on 2026-11-09. All features and decisions are agreed. Multi-asset: sessions and cutoffs follow each instrument's exchange profile, each on its own clock. Change history: see `event_loop_map.html`.

## Goal and scope

**What it is.** The Event Loop is the only consumer of the event buffer and the only scheduler of strategy calls. The Event Loop delivers every input a strategy receives: bars, order updates, balance updates, warmup history and session-end flatten requests. The Event Loop also adds new strategies to the runtime book. The Event Loop is one concrete component, not an interface with subclasses: the replay runner and the live runner give the Event Loop a different clock and different collaborators, and nothing else differs (feature 13).

**Why it exists.** Four producers write events from different threads: the Gateway feed, Safety, the bar flush and the deploy service. Each strategy must receive those events in one time order, exactly once, and an exception in one strategy must not stop the other strategies. Strategies consume bars, but the broker sends ticks. The Event Loop owns the time order, the tick-to-bar conversion and the fault isolation, so that no other component must do these three jobs.

**Goal.** Take events from the event buffer, turn ticks into completed bars at each strategy's resolution, and deliver bars, order updates and balance updates to the right strategies, in time order, exactly once, with fault isolation.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Other components call the Event Loop | Gateway feed, Safety, bar flush | Push an event into the buffer (Safety pushes repaired fills, Rejected updates and position updates); wait until the buffer is idle | event: type, data, enqueued_at |
|  | Replay runner | Push a fill to the front of the buffer | fill event |
|  | Any task off the loop | Run a book operation and await its result | an operation on the runtime book → result or error |
|  | Operator (local socket) | Deploy one strategy | strategy key → one status line per step |
| The Event Loop calls other components | Strategy | Step on a bar; book an order or balance update; seed with warmup history; request flatten; ask "flat and idle?" and "processing an order?"; quarantine | bar, update, history, flatten reason |
|  | Gateway | Request daily history and seed bars; read each instrument's market-data message type; resolve contracts, subscribe and unsubscribe ticks | history request, query, instrument |
|  | Aux Data | Annotate a bar | bar → bar + aux values |
|  | Safety | Check in from the loop (freeze detector) | check-in |
|  | Clock | Read the time; wait | now, wait |

### Scope

Owns, from the System map:

> Turning ticks into completed bars and delivering bars and order updates to the right strategies, in time order, exactly once; and seeding each strategy with the broker warmup history it needs (daily and prior-session), fetched through Gateway and retried until it lands ([System map decision 23](SYSTEM_MAP.md); auxiliary series move to Aux Data, [decision 44](SYSTEM_MAP.md), proposed).

| Not the Event Loop's job | Owned by |
|----|----|
| The broker connection; building daily bars from intraday history | [Gateway](GATEWAY_MAP.md) |
| Auxiliary series, their arming and their values on bars | [Aux Data](AUX_DATA_MAP.md) |
| Entry/exit decisions; which orders a strategy owns | [Strategy](STRATEGY_MAP.md), [StrategyExecution](STRATEGY_EXECUTION_MAP.md), [Order Manager](ORDER_MANAGER_MAP.md) |
| Recovering from an outage; repairing an order | [Safety](SAFETY_MAP.md) |

### Place in the system

Drawn in the HTML page; the flows are the Public interface table above and the [SYSTEM_MAP.md](SYSTEM_MAP.md) Diagram.

The System map's Event Loop *Receives* and *Produces* lines list every operation on this page. The Event Loop no longer calls Safety ([System map decision 50](SYSTEM_MAP.md)).

## Features

Each feature says who needs it and why, what the upstream can give, and how the Event Loop closes the gap. Every number is a parameter with its supported values.

### Algorithm map

```
  ( Gateway ): tick · order update     ( Safety ): repaired fill
        │
        │ event
        ▼
  ┌─────────────────────────┐
  │ Route event to a lane   │
  │ rule: fills → control   │
  └─────────────────────────┘
        │
        ├── event + enqueued_at ──▶ [ buffer: control lane │ data lane ]
        │
        │ events
        ▼
  ┌─────────────────────────┐
  │ Drain one pass          │
  │ rule: control 1st ≤2 ms │
  └─────────────────────────┘
        │
        │ events
        ▼
  ┌─────────────────────────┐
  │ Dispatch by event type  │
  │ rule: handler per type  │
  └─────────────────────────┘
        │
        ├── tick (ts, px, size) ──▶ ┌─────────────────────────┐
        │                           │ Bucket ticks by second  │
        │                           │ rule: reject ts < last  │
        │   ◀── open bucket         └─────────────────────────┘
        │
        ├── open bucket ──▶ ┌─────────────────────────┐
        │                   │ Close the bar           │
        │                   │ rule: rollover or grace │
        │   ◀── bar → data lane └─────────────────────────┘
        │
        ├── book op ──▶ [ runtime book ]
        │
        │ bar
        ▼
  ┌─────────────────────────┐
  │ Annotate the bar        │
  │ rule: Aux, then Gateway │
  └─────────────────────────┘
        │
        ├── bar ──▶ ( Aux Data )
        │   ◀── bar + aux values
        ├── bar ──▶ ( Gateway )
        │
        │ bar + aux values
        ▼
  ┌─────────────────────────┐
  │ Fan out to strategies   │
  │ rule: quarantine at 3   │
  └─────────────────────────┘
        │
        ├── bar, fill ──▶ ┌─────────────────────────┐
        │                 │ Record bars, trades     │
        │                 │ rule: daily files       │
        │                 └─────────────────────────┘
        │
        ├── reads ──▶ [ runtime book: strategies, resamplers ]
        │
        │ bar · order / position / balance update
        ▼
  ( Strategy )

  ( operator, through the runner )
        │
        │ deploy request
        ▼
  ┌─────────────────────────┐
  │ Deploy one strategy     │
  │ rule: fail → no change  │
  └─────────────────────────┘
        │
        ├── contract, subscribe request ──▶ ( Gateway )
        │
        │ book op (control lane)
        ▼
  ┌─────────────────────────┐
  │ Change the book         │
  │ rule: between events    │
  └─────────────────────────┘
        │
        ├── commit / release ──▶ [ runtime book ]
        │
        │ status lines
        ▼
  ( operator, through the runner )

  [ runtime book ]
        │
        │ slot to seed
        ▼
  ┌─────────────────────────┐
  │ Seed a strategy         │
  │ rule: shared, 1 retry   │
  └─────────────────────────┘
        │
        ├── history request ──▶ ( Gateway )
        │   ◀── warmup history
        │
        │ warmup history
        ▼
  ( Strategy )

  [ clock: now, wait ]
        │
        │ session cutoff from the exchange profile
        ▼
  ┌─────────────────────────┐
  │ Flatten at cutoff       │
  │ rule: cutoff − 1 min    │
  └─────────────────────────┘
        │
        │ flatten
        ▼
  ( Strategy )
```

Main path in colour. A tick never reaches a strategy: it is bucketed, the closed bar goes back into the data lane, and a later drain pass dispatches it as a bar. Dashed boxes are shared data structures; dashed accent arrows are the clock.

### Feature cards

### 1. Bars from ticks or from bar updates *(was behaviours 1, 2 · System map decision 49)*

#### Need

Strategies make their signals on completed bars at a fixed resolution. A strategy must never see a partial bar, or the same bar twice.

#### Upstream gives

The Gateway gives one market-data stream per instrument, of the type the Gateway declares for that instrument: either `tick_update` messages (ts, price, size) at an uneven rate, sometimes out of order, and nothing at all in quiet periods; or `bar_update` messages, each one completed bar at a stated resolution.

#### Algorithm

1.  At build, read each instrument's declared message type and resolution from the Gateway. If a slot needs a finer resolution than its instrument's `bar_update` feed gives, stop startup with an error that names the slot.
2.  For a `bar_update` at the slot's resolution: reject the bar if its ts is not later than the last bar's; otherwise hand the bar to `emit_bar` as a closed bar. The Resampler is not used for that instrument.
3.  For a `tick_update`, keep one open bucket per (instrument, resolution) in the runtime book.
4.  For each tick: reject it if its ts is older than the last tick's; drop it if its bucket was already emitted; otherwise update open, high, low, close and add its size to volume.
5.  Close the bucket when a tick for a later bucket arrives (rollover), when the clock passes bucket end + flush grace (close = last price), or on `end_of_data`.
6.  Append the closed bar to the data lane (feature 4). The closed bar reaches strategies through feature 2.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Bar resolution | Fixed for v3 ([System map decision 56](SYSTEM_MAP.md)) | 1 s | 1 s |
| Flush grace | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 1.0 s | 1.0 s |

#### Invariant

For every supported resolution r and both message types: at most one bar per bucket, and every bar goes through `emit_bar`. From ticks: a bar's OHLC covers every accepted tick in \t, t + r), and its volume is exactly the sum of their sizes; in a quiet period a bar is late by at most the flush grace. From bar updates: the bar equals the `bar_update`, field for field.

### 2. Bars delivered in time order, exactly once *(was behaviours 3, 13)*

#### Need

A signal is right only if its bars arrive in time order, with no gaps and no repeats. The simulated broker must fill against a bar before strategies react to it, and Aux Data's values must be on the bar when a strategy reads it.

#### Upstream gives

Bar events from the data lane ([feature 1), Aux Data values per bar, and the runtime book's strategies per instrument (feature 11).

#### Algorithm

1.  Dispatch every event through one table keyed by event type. The main drain and the final drain after stop use the same table.
2.  For a bar: Aux Data adds its values, then the Gateway sees the bar, then the bar fans out to the instrument's strategies, one after another, in book order.
3.  One function makes every bar (rollover, flush, end of data); one helper does every fan-out (bars and updates).

#### Parameters

None of its own.

#### Invariant

Per instrument, each strategy gets bars in increasing ts order, exactly once per bucket. Aux Data, then the Gateway, then strategies, always in that order.

### 3. Order and balance updates reach every strategy *(was behaviour 4)*

#### Need

A strategy that has just closed, or is flattening, must still book its closing fill. Which orders a strategy owns is decided by Strategy and Order Manager, not by the loop.

#### Upstream gives

Order and balance updates from the Gateway and repaired fills from Safety, all on the control lane.

#### Algorithm

1.  Fan every update out to all active strategies in every session state: in session, flattening and session ended. Each strategy filters by order-ID ownership.
2.  When a round trip closes, flush the trade recorder (feature 10).

#### Parameters

None.

#### Invariant

No update is ever dropped for any strategy.

### 4. Fills never wait behind ticks *(was behaviours 11, 12 · System map decisions 26, 27 · decision 8)*

#### Need

A fill must reach its strategy fast, within the processing latency budget ([System map R3](SYSTEM_MAP.md)), even during a tick burst. The loop must use no CPU when idle. No event may be dropped (decision 3).

#### Upstream gives

Producers on different threads: the Gateway feed (the broker's reader thread), the bar flush, Safety's repairs and book operations. The peak tick rate is not measured yet, so bursts have an unknown size.

#### Algorithm

1.  Two lanes. Control: `order_update`, `balance_update`, `repaired_fill`, book operations, `end_of_data`. Data: `tick_update`, `bar_update`, `bar`.
2.  A producer appends to its lane, then raises the drain wake-up only if `wake_pending` is clear, and sets it. A producer on another thread wakes the loop only through the loop's thread-safe call.
3.  The drain clears the wake-up and `wake_pending` *before* each pass, never after; empties the control lane, then takes data events; and yields when the batch budget is spent. Leftovers go to the next pass without a new wake-up.
4.  With nothing to do, wait on the wake-up with the wait timeout, so the clock-driven flush (feature 1) still runs.
5.  Each pass logs the size of each lane and the p99 queue wait. When the control lane is above the backlog threshold, raise an ERROR with an operator alert. The data lane raises no alert. Never drop.
6.  The buffer is idle when the drain finds both lanes empty, which is only after every event the drain took has been handled. A push clears the idle state. A producer can wait until the buffer is idle; the replay feed does this before each bar (Gateway feature 12).

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Batch budget per pass | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 2 ms | 2 ms |
| Wait timeout | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 20 ms | 20 ms |
| Backlog alert threshold | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 5000 events | 5000 events |

#### Invariant

Queue wait is close to 0, not 0 to the wait timeout. A burst costs one wake-up, not one per event. An event appended while a pass ends never waits for the timeout. Each lane drains in arrival order, each event exactly once, and nothing is dropped. The buffer is never idle while an event that the drain took is still being handled.

**Why: the load and where it costs time (load target, cost view, burst example, race rule)**

| Item | Target | Why it matters |
|----|----|----|
| Tick streams | **3** simultaneous IB `reqTickByTickData` streams | IB allows tick-by-tick streams equal to 5% of the account's market data lines (about 5 at 100 lines), and 1 request per instrument every 15 s. Options have no tick-by-tick data, so an option instrument needs another feed type. |
| Strategies | **10–15** active at the same time, in one process | This is more than the "about 10" that [System map decision 28](SYSTEM_MAP.md) gives for one process. The fan-out sets the real limit. |
| Ticks per second | Not known yet. Measure the peak from recorded tick data (market open, news). | No IB message-rate cap and no trusted ES peak figure was found. The design must not depend on a guessed rate. |
| Bars per second after resampling | At most one per stream per resolution period (3 at 1 s) | A tick burst never reaches the strategies. The strategies receive bars only. |
| Latency | 5 ms processing latency at p99 ([System map R3, decision 55](SYSTEM_MAP.md)) | Queue wait, GIL wait and the sum of slot costs all count against it. |

```
  INGEST · the burst lives here                FAN-OUT · strategy count lives here

  ( Gateway feed: 3 tick streams ) ─┐                        ┌──▶ strategy 1
  ( bar flush )                     ├─▶ control lane          │    strategy 2
  ( Safety repairs )                │   (fills · book ops)    │    ⋮
                                    └─▶ data lane ─▶ Drain loop ─▶ Resampler ─┴──▶ strategy 15
                                        (ticks · bars)  control first     ≤ 3 bars/s
                                    one wake-up         ≤ 2 ms / pass
                                    per burst                             stepped one after another
                                                                          dispatch = Σ slot costs
```

Ingest: producers append to two lanes, the drain loop takes control first, and the resampler folds ticks into bars. Fan-out: each bar steps the strategies on its instrument one after another.

| Layer | What costs time | Answer in this design |
|----|----|----|
| Ingest | A wake-up per event; a fill waiting behind queued ticks; the IB reader thread decoding in pure Python while it holds the GIL, so the loop thread can wait up to the GIL switch interval (5 ms by default, about 10 ms in the worst case). | One wake-up per burst, a control lane drained first, a batch budget (this feature, decision 8); a 0.5 ms switch interval (decision 10). |
| Fan-out | Strategies on one bar run one after another, so dispatch latency is the sum of their slot costs. Example: 300 µs × 15 strategies = 4.5 ms, which alone breaks R3. | Measure the summed slot cost per instrument (feature 5). Above the scale trigger, move to strategy node processes (decision 9). |

**One wake-up per burst.** While the loop is awake and draining, it sees a new event anyway, so another wake-up is wasted work that also takes GIL time from the loop. A thread-safe wake-up costs a lock and one system call. Example, a burst of 1,000 ticks:

| Time | Reader thread | Drain loop | Wake-up sent? |
|----|----|----|----|
| t0 | append tick 1 | asleep | **Yes** — the loop is asleep |
| t0 + 5 µs | append tick 2 | waking up | No — a wake-up is pending |
| t0 + 50 µs | append ticks 3–40 | drains ticks 1–40 | No — the loop is awake |
| … | append ticks 41–1000 | keeps draining | No |
| end | — | both lanes empty, goes to sleep | — |
| later | append tick 1001 | asleep | **Yes** |

The burst costs 2 wake-ups instead of 1,001.

**Clear the flag before the drain, never after.** If the flag were cleared after: (1) the consumer pops the last event and sees both lanes empty; (2) a producer appends a fill, sees `wake_pending` still set, and sends no wake-up; (3) the consumer clears the flag and sleeps; (4) the fill waits for the wait timeout. Cleared first, the worst case is one extra wake-up that finds an empty buffer. The flag needs no lock: an extra wake-up costs nothing, and a missed one is caught by the timeout.

### 5. Latency is measured against the budget *(was behaviour 14 · System map decisions 25, 28, 29)*

#### Need

R3 says each strategy's decision on a message must be made within the 5 ms processing latency budget of the message's arrival (System map decision 55). We need measured numbers to know when one process is no longer enough (decision 9).

#### Upstream gives

The broker timestamp on each tick, the time each strategy step takes, and the reader thread's decode time.

#### Algorithm

1.  Per message: record **processing latency**, from the message's `recv_ts` (or the flush time for a bar the clock closed) to each strategy's final decision on it (an order or cancel handed to the Gateway, or no action), with its parts: queue wait, dispatch latency and slot cost ([System map decision 55](SYSTEM_MAP.md)).
2.  Per message: record **feed latency**, `recv_ts` − broker timestamp, with the broker's timestamp precision.
3.  Per minute and instrument: p50 and p99 of processing latency and of feed latency.
4.  Per minute: p50 and p99 of the summed slot cost of each instrument's strategies, and of the reader thread's decode time.
5.  Write per-bar logs off the loop thread.
6.  A rebuild replaying seed bars yields within a time budget, not a fixed chunk, so live bars for other slots do not wait behind it.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Processing latency budget (p99) | Fixed by System map R3 | 5 ms | 5 ms |
| Scale trigger: summed slot cost p99 | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 3 ms | 3 ms |
| GIL switch interval | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 0.5 ms | 0.5 ms |
| Rebuild yield budget | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any positive ms | 1 ms |

#### Invariant

Strategies on a bar are still stepped one after another in time order. Measuring never blocks the loop.

### 6. One broken strategy never stops the others *(was behaviour 5 · decision 4)*

#### Need

Many strategies share one process. A bug in one must not stop trading for the rest.

#### Upstream gives

Exceptions raised by a strategy's step on a bar.

#### Algorithm

1.  The fan-out catches and logs each strategy's exception separately.
2.  Count consecutive errors per strategy. At the threshold, move it to the ERROR state (quarantine); later fan-outs skip it.
3.  All other strategies carry on with the same bar.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Quarantine threshold (consecutive errors) | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 3 | 3 |

#### Invariant

An exception in one strategy never ends the loop and never stops other strategies getting that bar.

### 7. Strategies are seeded before they trade *(was behaviours 8, 18 · decision 5)*

#### Need

A signal needs history (daily bars and prior-session bars) to be warm before its first live decision. A slot must never trade on history that did not arrive.

#### Upstream gives

The Gateway builds daily bars per session (holidays, early closes, contract roll) and gives prior-session intraday bars on request ([System map decision 45](SYSTEM_MAP.md)). A fetch can fail or come back empty. Auxiliary series come from [Aux Data](AUX_DATA_MAP.md), not from here.

#### Algorithm

1.  One attempt per slot: fetch daily history and prior-session bars through one shared fetch per key (one request in flight, kept for a time-to-live); hand them to the strategy in its fixed order; on a mid-session start, replay today's recorded bars (feature 10); return the slot's seed state.
2.  Any result other than `READY` starts one retry task for that slot, which runs the same attempt under a jittered, capped backoff.
3.  Seed state: `PENDING` ─seed lands─▶ `READY`; `PENDING` ─seed fails─▶ `QUARANTINED_RECOVERABLE` ─retry lands─▶ `READY`; terminal failure ─▶ `QUARANTINED_TERMINAL`, which ends the retry. Re-read the state after every await.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Backoff base, max, jitter | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | base, max: any positive seconds; jitter: 0 to 1 (fraction of the delay) | 5 s, 300 s, ±20% |
| Warmup window min / max | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | min: any count of trading days ≥ 1; max: any count of calendar days | 1 trading day; 400 calendar days |
| Shared fetch time-to-live | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any positive seconds | 300 s |

#### Invariant

A slot is `READY` only after its seed landed. Two tasks never seed one slot. A terminal state is never overwritten. One slot's failure never affects another.

### 8. Session end and shutdown leave the book flat *(was behaviours 7, 9)*

#### Need

Positions must not stay open past a session's end. At exit every configured position must be flat or flagged, and no queued event may be lost.

#### Upstream gives

The clock; each instrument's exchange profile (session calendar, early closes, time zone); shutdown signals; `end_of_data` in replay.

#### Algorithm

1.  At each session's cutoff, on that session's own clock (early closes included, never at a session open), ask each strategy of that session to flatten.
2.  Repeat on every bar of the instrument until the strategy is flat and idle, a SIGINT/SIGTERM arrives, or the flatten timeout passes. A flattening slot gets no bars. Keep going when a flatten raises.
3.  When every slot has ended but an exchange's data session is still open, keep running and recording until it closes.
4.  Before exit: cancel working orders on configured instruments and wait; force-flatten any leftover position that has no working order; otherwise page and exit non-zero. If open orders cannot be read, treat every configured instrument as blocked.
5.  Clean shutdown: flush buffered bars, drain the recorders, tell the strategies, leave the loop.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Cutoff offset before session end | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 1 min | 1 min |
| Flatten timeout | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 10 s | 10 s |

#### Invariant

At exit every configured position is flat or flagged. An instrument the engine does not trade is never touched. No market order is sent at shutdown on an instrument that has, or may have, a working order. A normal shutdown leaves no queued event and no open file.

### 9. Stuck orders: moved to Safety *(was behaviour 6 · System map decision 50)*

#### Need

The periodic stuck-order check now belongs to Safety ([Safety feature 11](SAFETY_MAP.md)). The number 9 is kept so references stay valid.

#### Algorithm

The Event Loop does nothing for this feature. The Event Loop does not query the Gateway for executions and does not call Safety.

### 10. Bars and trades are recorded *(was behaviour 10)*

#### Need

A run must be auditable afterwards, and a mid-session restart needs today's bars (feature 7).

#### Upstream gives

Closed bars and fills, in live and paper mode.

#### Algorithm

1.  Append every bar to the session day's file per instrument (an overnight session is not split at midnight); roll it up at day end or on finalize.
2.  Pair fills into per-strategy round trips (weighted average price for multi-fill legs), with currency; flush them, and repair a pending commission later.
3.  Replay passes do-nothing recorders (feature 13).

#### Parameters

None.

#### Invariant

A recorded bar is durable before finalize. Recording never blocks or changes trading.

### 11. One runtime book, changed only between events *(was behaviours 15, 17 · System map decision 34)*

#### Need

Dispatch, recorders, Aux Data, Safety and flatten must all agree which strategies exist, and a dispatch must not see the list change half-way.

#### Upstream gives

The strategies to run (from cold start or a deploy), with their instruments and sessions.

#### Algorithm

1.  One book holds the strategies (dispatch order), resamplers and bar buffers per signal instrument, contracts, fed instruments, session maps and retry tasks. Each container is made once and changed in place, so holders of a reference see additions.
2.  Reserve an instrument before any strategy on it is committed. Reserving is idempotent and keeps an existing recording partition.
3.  Commit is synchronous: it adds the strategy and its session maps in one call, or raises on a duplicate key and leaves the book unchanged.
4.  Only the drain loop changes the book. Another task enqueues a book operation and awaits its future. An operation after shutdown is refused; one still queued at stop fails with "book closed".
5.  A committed strategy is a full member from the next event; its session-end flatten uses its own calendar; Aux Data reads members from the book.

#### Parameters

None.

#### Invariant

A dispatch sees one strategy list for its whole run. No consumer keeps its own copy.

### 12. Add a strategy while running *(was behaviour 16 · System map decisions 35, 36)*

#### Need

The operator adds a strategy in paper or live without a restart, and without touching the strategies already running.

#### Upstream gives

A deploy request on the runner's local socket, the config file, and the broker's positions and open orders per account.

#### Algorithm

1.  **Pre-flight** (nothing exists yet): refuse `stale_config`, `config_drift`, `unknown_key`, `conflict`, `import_error`, `params_invalid`, `session_open`, `after_process_exit`, `unknown_account_route` or `unmanaged_exposure`.
2.  **Reserve and subscribe** each new instrument through a book operation; subscribe ticks only for a new signal instrument.
3.  **Prepare**: warm up the candidate alone (feature 7); a fetch miss is not a failure.
4.  **Commit** through a book operation that re-checks shutdown and the session gate; reply `registered`.
5.  Another request in flight → `busy`; same revision → `noop`; after shutdown begins → `shutting_down`. On failure after pre-flight, release only instruments no running strategy uses.

#### Parameters

None.

#### Invariant

Every outcome other than `registered` leaves the book as it was. Running strategies keep their connection, positions, orders, indicators and partly built bars.

### 13. Replay and live run the same code *(was behaviour 19 · System map decision 46)*

#### Need

A replay result is evidence about live trading only if the same lines run in both.

#### Upstream gives

A clock (wall or replay) and the collaborators (recorders, Safety, stores) as objects.

#### Algorithm

1.  Every time read goes through the clock: bar flush, session-active flag, flatten, and the drain's wait.
2.  `emit_bar` moves the `ReplayClock` forward to the bar's end time before the bar is dispatched. In replay, `wait(timeout)` returns at once.
3.  Replay passes do-nothing collaborators.

#### Parameters

None.

#### Invariant

No Event Loop code reads the run mode or checks a collaborator for `None`.

## Tests

The goal and features as measurable cases. A test with parameter cases runs once per supported value; when a value is added to a feature's Parameters, its cases grow with it. No Event Loop test exists in v3 yet; every row is not written yet.

**End to end · the sample (1 test)**

|  |  |
|----|----|
| Test | `tests/event_loop/test_event_loop_sample.py::test_golden_bar_updates_fills_and_end_of_data` |
| Input | The golden session from a Gateway stub: 23,400 `bar_update` messages for MES on 2026-04-15 (NY_RTH), the daily history and seed bars of the 60 prior sessions, two fills (the BUY in phase B, the SELL at the target), then `end_of_data` ([System map golden sample](SYSTEM_MAP.md)). |
| Parameter cases | message type ∈ {`bar_update`} × resolution ∈ {1 s} |
| Passes when | The slot is seeded before the first replay bar; the strategy gets all 23,400 bars in sequence, with no duplicate or missing timestamp; the Resampler is never called; the `ReplayClock` equals each bar's end time when the bar is dispatched; the strategy gets both fills; no ERROR line is logged; and the loop ends with no unhandled exception. |
| Fails when | A bar is out of order, skipped or repeated; an order update is dropped; an exception in one strategy stops bars reaching the others; or the loop keeps running after `end_of_data`. |

### End-to-end scenarios *(agreed 2026-11-09)*

Each scenario drives the component only through its public interface, from a known start state, and checks one exact, observable output. A scenario tells one story across several features; the feature tables below keep the single rules. File: `tests/event_loop/test_event_loop_scenarios.py`. A Gateway stub plays scripted messages and records what the Event Loop asks of it; recording strategies stand in for Strategy slots and record every bar and update they get; an Aux Data stub adds nothing to bars; a controlled clock (a `ReplayClock` or a fake wall clock) moves time. Times are New York unless stated.

**S1. Ticks become 1-second bars (features 1, 2)**

|  |  |
|----|----|
| Test | `test_scenario_ticks_to_bars` |
| Given | The Gateway stub declares `tick_update` for MES; one recording strategy on MES. |
| When | Send ticks at 09:30:00.1 (5000.00, 1), 09:30:00.5 (5001.00, 2), 09:30:00.9 (4999.00, 1), 09:30:01.2 (5000.50, 3), then a tick at 09:30:00.7 (5002.00, 1), then `end_of_data`. |
| Parameter cases | resolution ∈ {1 s} |
| Passes when | The strategy gets exactly two bars, in order: 09:30:00 with O 5000.00, H 5001.00, L 4999.00, C 4999.00, V 4 (closed by the rollover), and 09:30:01 with O = H = L = C = 5000.50, V 3 (closed by `end_of_data`). The tick at 09:30:00.7 is rejected and changes no bar. |
| Fails when | A wrong OHLCV value; the late tick changes a bar; a bar is missing or repeated. |

**S2. A quiet market closes the bar by the clock (features 1, 4, 13)**

|  |  |
|----|----|
| Test | `test_scenario_quiet_market_flush` |
| Given | The Gateway stub declares `tick_update` for MES. |
| When | Send one tick at 09:30:00.2 (5000.00, 1); send nothing more; move the clock forward. |
| Parameter cases | flush grace ∈ {1.0 s}; wait timeout ∈ {20 ms} |
| Passes when | The bar 09:30:00 (C 5000.00, V 1) reaches the strategy once, at a clock time ≥ 09:30:02.0 (bucket end + flush grace) and ≤ 09:30:02.0 + the wait timeout. |
| Fails when | The bar never closes; the bar closes before bucket end + grace; the bar arrives twice. |

**S3. Bar updates are taken as bars (features 1, 2, 13)**

|  |  |
|----|----|
| Test | `test_scenario_bar_updates` |
| Given | The Gateway stub declares `bar_update` at 1 s for MES; a `ReplayClock`. |
| When | Send `bar_update` for 09:30:00 to 09:30:09, then a second 09:30:05 and a 09:30:03, then `end_of_data`. |
| Parameter cases | message type ∈ {`bar_update`} × resolution ∈ {1 s} |
| Passes when | The strategy gets the 10 bars 09:30:00 to 09:30:09 once each, in order; the repeated and the older bar are rejected; the Resampler is never called; at each dispatch the `ReplayClock` equals the bar's end time; the loop stops after `end_of_data`. |
| Fails when | A bar out of order, missing or repeated; the Resampler is called; the clock differs from the bar end time; the loop runs on after `end_of_data`. |

**S4. A fill jumps a burst of ticks (features 4)**

|  |  |
|----|----|
| Test | `test_scenario_fill_jumps_tick_burst` |
| Given | The Gateway stub declares `tick_update` for MES; one recording strategy. |
| When | From another thread, append 5000 `tick_update` messages in one burst, then one `order_update`. |
| Parameter cases | batch budget ∈ {2 ms} |
| Passes when | The strategy gets the `order_update` before the data events that are still queued when the update arrives; every tick is processed exactly once; nothing is dropped; the burst raises one drain wake-up. |
| Fails when | The update waits behind the data lane; a tick is dropped or processed twice; one wake-up per event. |

**S5. Order updates reach every strategy (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_updates_reach_every_state` |
| Given | Three recording strategies on MES: one in session, one flattening, one whose session ended. |
| When | Send one `order_update` and one `balance_update`. |
| Parameter cases | — |
| Passes when | Each of the three strategies gets both updates exactly once. |
| Fails when | A strategy misses an update or gets one twice. |

**S6. One broken strategy is quarantined (features 6)**

|  |  |
|----|----|
| Test | `test_scenario_broken_strategy_quarantined` |
| Given | Strategies A, B and C on MES, in that book order; B raises on every bar. |
| When | Send 5 bars. |
| Parameter cases | quarantine threshold ∈ {3} |
| Passes when | A and C get all 5 bars. B gets bars 1 to 3, is in ERROR after bar 3, and gets neither bar 4 nor bar 5. Three exceptions are logged, each for B. The loop keeps running. |
| Fails when | A or C misses a bar; B gets bar 4; the loop stops. |

**S7. Seeding retries once, then lands (features 7)**

|  |  |
|----|----|
| Test | `test_scenario_seed_retry` |
| Given | Two slots on MES with the same history key; the Gateway stub fails the first daily-history fetch and answers the second. |
| When | Start the loop and run until both slots are seeded. |
| Parameter cases | backoff ∈ {base 5 s, max 300 s, jitter ±20%}; case ∈ {fetch fails once; terminal failure} |
| Passes when | The two slots share one fetch request. Each slot goes PENDING → QUARANTINED_RECOVERABLE → READY; one retry task per slot, started 4 s to 6 s after the failure. Terminal case: the slot ends QUARANTINED_TERMINAL and no further fetch is made for that slot. |
| Fails when | Two requests for one key; a slot READY before its seed landed; a second retry task for one slot; a retry after a terminal state. |

**S8. Session end flattens the book (features 8)**

|  |  |
|----|----|
| Test | `test_scenario_session_end_flatten` |
| Given | One strategy on MES (NY_RTH) and one on FDAX, each in position; each recording strategy becomes flat only on its third flatten request (case normal); one never becomes flat (case timeout). |
| When | Run the clock across each session's cutoff, with one bar per second. |
| Parameter cases | exchange ∈ {CME, Eurex} × day ∈ {normal, early close} × case ∈ {flat on third request, never flat}; cutoff offset ∈ {1 min}; flatten timeout ∈ {10 s} |
| Passes when | Each strategy gets its first flatten request at its session end less 1 min on its own clock (CME: 15:59:00 New York; Eurex: on the Frankfurt clock; early close: the early close less 1 min), then one request per bar until flat; a flattening strategy gets no bars. Never-flat case: requests stop 10 s after the first. |
| Fails when | A flatten at the wrong clock time or at a session open; a flattening strategy gets a bar; requests go on after the strategy is flat or after the timeout. |

**S9. Shutdown leaves the book flat (features 8)**

|  |  |
|----|----|
| Test | `test_scenario_shutdown_flat` |
| Given | Configured instruments MES and ES. MES has a working order; ES has a position of +1 and no working order; the broker also holds a position in an instrument the engine does not trade. |
| When | Send SIGTERM. |
| Parameter cases | case ∈ {open orders readable, open orders unreadable} |
| Passes when | Readable: the MES order is cancelled and its final status awaited; one MKT SELL 1 flattens ES; the untraded instrument is not touched; no event stays queued and no file stays open; exit code 0. Unreadable: no market order is sent on any configured instrument; an operator alert is sent; the exit code is non-zero. |
| Fails when | A market order on an instrument with a working order; the untraded instrument is touched; an event left queued; exit code 0 in the unreadable case. |

**S10. Add a strategy while running (features 11, 12)**

|  |  |
|----|----|
| Test | `test_scenario_deploy_strategy` |
| Given | A paper process runs strategy A on MES; the config file already holds slot B on ES; B's session is closed. |
| When | Send a deploy request for B; then send each refusal case as its own request. |
| Parameter cases | case ∈ {registered; noop (same revision); busy (a request in flight); session_open; conflict; unknown_key; stale_config} |
| Passes when | Registered: status lines `preparing` then `registered`; ES is subscribed once; MES is not subscribed again; A gets every MES bar with no gap; B gets events from the next event on. Each other case: that terminal status, and the book, the subscriptions and A are unchanged. |
| Fails when | A refused request changes the book or a subscription; MES is subscribed again; A misses a bar; a status other than the case's. |

**S11. Bars and trades are recorded (features 10)**

|  |  |
|----|----|
| Test | `test_scenario_recording` |
| Given | A paper process on MES, one strategy that makes one round trip; the session crosses midnight (overnight case). |
| When | Run the session and finalize. |
| Parameter cases | session ∈ {NY_RTH, overnight} |
| Passes when | One bar file per instrument for the session day holds every bar of the session (the overnight session is not split at midnight); one round-trip row holds the entry and exit prices and the currency USD. |
| Fails when | A bar missing from the file; an overnight session split into two files; no round-trip row, or a row without currency. |

**S12. Replay and live give the same dispatch (features 13)**

|  |  |
|----|----|
| Test | `test_scenario_replay_equals_live` |
| Given | One scripted session of 60 ticks and 2 order updates. |
| When | Run it once with a `ReplayClock` and once with a fake wall clock. |
| Parameter cases | clock ∈ {ReplayClock, fake wall clock} |
| Passes when | The recording strategy gets the same sequence of bars and updates, with the same values, in both runs. |
| Fails when | The two sequences differ in any event, value or order. |

**S13. Load target meets the latency budget (features 5)**

|  |  |
|----|----|
| Test | `test_scenario_load_target_latency` |
| Given | Three tick-by-tick streams and 15 strategies in one process, replayed at 500 ticks per second per stream (1,500 ticks per second in total), constant for the whole run. |
| When | Replay 10 minutes. |
| Parameter cases | strategies ∈ {10, 15}; streams ∈ {3}; tick rate ∈ {500 per second per stream} |
| Passes when | Processing latency p99 ≤ 5 ms per instrument; one per-minute line per instrument with p50 and p99 of processing latency and of feed latency, and the timestamp precision. |
| Fails when | p99 over 5 ms; a missing per-minute line. |

**1. Bars from ticks or from bar updates (5 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_bar_ohlcv_matches_ticks_in_bucket` | resolution ∈ {1 s} | OHLC and volume vs. a hand count, over ticks spanning 3 buckets | exact match |
| `test_out_of_order_tick_rejected` | resolution ∈ {1 s} | bar after a tick with ts \< last ts | unchanged by that tick |
| `test_late_tick_for_emitted_bucket_ignored` | resolution ∈ {1 s} | bars emitted for that bucket | exactly 1 |
| `test_quiet_bucket_flushed_after_grace` | resolution ∈ {1 s} × grace ∈ {1.0 s} | replay-clock time from bucket end to bar emitted; close price | ≤ grace; close = last price |
| `test_end_of_data_flushes_open_bucket` | resolution ∈ {1 s} | bars emitted for the open bucket | exactly 1 |

**2. Bars delivered in time order, exactly once (4 tests · 7 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_bars_in_order_exactly_once` | strategies per instrument ∈ {1, 15} | each strategy's bar timestamps | strictly increasing, no gap, no repeat |
| `test_aux_then_strategies` | — | call order on one bar | Aux Data, strategies; the Gateway is not called |
| `test_final_drain_uses_same_handlers` | — | handler per event type after stop | same as main drain |
| `test_every_bar_close_passes_one_function` | close cause ∈ {rollover, flush, end of data, `bar_update`} | bars made outside that function | 0 |
| `test_bar_update_feed_bypasses_resampler_and_finer_resolution_refused` | slot resolution ∈ {1 s on a 1 s feed, finer than the feed} | Resampler calls; startup result | 0, starts; refused, names the slot |

**3. Order and balance updates reach every strategy (2 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_order_update_reaches_every_strategy` | strategy state ∈ {in session, flattening, session ended} | strategies that received the update | all |
| `test_round_trip_close_flushes_trade_recorder` | — | recorder flushes per closed round trip | exactly 1 |

**4. Fills never wait behind ticks (7 tests · 8 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_control_lane_drained_before_data_lane` | ticks queued ahead ∈ {0, 2000} | dispatch position of a fill pushed after the ticks | first |
| `test_idle_only_after_every_popped_event_is_handled` | — | idle state after push, after the last pop, after the empty pop | clear; clear; set |
| `test_burst_sends_one_wakeup_while_loop_awake` | burst size ∈ {1000} | wake-up calls during one drain | 1 |
| `test_push_racing_end_of_drain_is_not_left_for_timeout` | wait timeout ∈ {20 ms} | queue wait of a push between last pop and sleep | \< wait timeout |
| `test_drain_pass_yields_after_batch_budget` | budget ∈ {2 ms} | pass length; wake-ups for leftovers | ≤ budget + one event; 0 |
| `test_drain_starts_on_wake_without_timer` | — | time from push to drain start | \< wait timeout |
| `test_backlog_over_threshold_raises_error_drops_nothing` | threshold ∈ {5000} × lane ∈ {control, data} | ERROR raised; events dispatched | control: at threshold + 1, data: never; all |

**5. Latency is measured against the budget (3 tests · 7 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_processing_and_feed_latency_logged_per_message` | message ∈ {`tick_update` closing a bar, `bar_update`, flushed bar, order update} | fields per record | processing latency (to each strategy's decision, also with no order), queue wait, dispatch latency, slot cost; feed latency with its precision (none for a flushed bar) |
| `test_slot_cost_sum_and_decode_time_logged_per_instrument` | instruments ∈ {1, 3} | records per minute | p50 and p99 for each instrument |
| `test_rebuild_replay_yields_within_budget` | yield budget ∈ {1 ms} | longest blocking slice during a rebuild | ≤ yield budget |

**6. One broken strategy never stops the others (2 tests · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_strategy_quarantined_at_threshold_others_continue` | threshold ∈ {3} | errors before ERROR; bars reaching the others | exactly threshold; all |
| `test_fan_out_logs_and_counts_each_strategy_exception` | failing strategies ∈ {1, all} | log lines and error counts | one per exception |

**7. Strategies are seeded before they trade (5 tests · 6 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_one_seed_fetch_shared_between_slots` | slots per key ∈ {1, 5} | broker fetches | 1 per key |
| `test_failed_seed_quarantines_only_that_slot` | — | other slots' seed state | `READY` |
| `test_startup_and_retry_run_the_same_attempt` | — | function run at start and on retry | the same |
| `test_retry_backoff_jittered_and_capped` | (base, max, jitter) ∈ {(5 s, 300 s, 0.2)} | retry delays | ≥ base and ≤ max |
| `test_terminal_quarantine_never_overwritten_by_retry` | — | state after a late retry lands | `QUARANTINED_TERMINAL` |

**8. Session end and shutdown leave the book flat (5 tests · 10 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_cutoff_on_session_clock` | exchange ∈ {CME, Eurex} × session ∈ {day, overnight} | time of first flatten request | session end − cutoff offset (1 min), on the session's clock |
| `test_early_close_cutoff_from_exchange_profile` | — | time of first flatten request | early close − cutoff offset |
| `test_flatten_repeats_until_flat_signal_or_timeout` | outcome ∈ {flat, signal, timeout} | when repeating stops | at flat; at signal; ≤ flatten timeout (10 s) |
| `test_unreadable_open_orders_block_force_flatten` | — | market orders sent at exit | 0 |
| `test_end_of_data_leaves_no_queued_event` | — | events left; files left open | 0; 0 |

**9. Stuck orders: moved to Safety (1 test · 1 case)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_event_loop_never_calls_safety_repair` | — | calls from Event Loop code to Safety's repair or to the Gateway's executions query | 0 |

**10. Bars and trades are recorded (3 tests · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_overnight_session_bars_not_split_at_midnight` | — | files per overnight session | 1 |
| `test_round_trip_weighted_average_for_multi_fill_leg` | — | leg price | size-weighted mean, exact |
| `test_recorded_bar_durable_before_finalize` | — | bars on disk after a crash before finalize | every bar emitted before the crash |

**11. One runtime book, changed only between events (4 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_book_op_runs_between_events_and_resolves_its_future` | — | events interleaved with the operation; future result | 0; resolved |
| `test_duplicate_key_commit_leaves_book_unchanged` | — | book contents after the raise | unchanged |
| `test_dispatch_sees_one_strategy_list` | — | strategies called by a fan-out during which a commit is queued | the list from the fan-out's start |
| `test_day_strategy_added_in_evening_not_due_that_evening` | — | first flatten request | at the next day-session cutoff |

**12. Add a strategy while running (3 tests · 14 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_each_refusal_reason_touches_nothing` | reason ∈ the 10 pre-flight refusals | book and subscriptions after the reply | unchanged |
| `test_failed_candidate_releases_only_its_own_instruments` | — | instruments released | only those no running strategy uses |
| `test_busy_noop_shutting_down_replies` | case ∈ {request in flight, same revision, after shutdown} | reply | `busy`; `noop` with the original ID; `shutting_down` |

**13. Replay and live run the same code (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_replay_clock_drives_flatten_and_flush` | — | flush and flatten times under the replay clock | equal to the replay times, not wall time |
| `test_no_run_mode_read_in_event_loop` | — | reads of the run mode in Event Loop code | 0 |

## Open questions

Gaps found while writing the features. Each one needs a human answer; the answer goes into the feature's Parameters or Algorithm.

All decided on 2026-11-09.

1.  **Bar resolution (feature 1).** 1 s only in v3. Other resolutions come later, chosen per strategy and built from 1-second bars ([System map decision 56](SYSTEM_MAP.md)).
2.  **Flush grace (feature 1).** Configurable per process; default 1.0 s; it does not depend on the resolution while only 1 s exists.
3.  **Buffer numbers (feature 4).** Batch budget 2 ms, wait timeout 20 ms, control-lane backlog threshold 5000 events: configurable per process, with these defaults.
4.  **Latency numbers (feature 5).** Scale trigger 3 ms, GIL switch interval 0.5 ms and rebuild yield budget 1 ms: configurable per process.
5.  **Operational numbers (features 6, 8).** Quarantine threshold 3, cutoff offset 1 min, flatten timeout 10 s: configurable per process, never per strategy ([System map decision 58](SYSTEM_MAP.md)).
6.  **Seeding numbers (feature 7).** Backoff base 5 s, max 300 s, jitter ±20%; warmup window from 1 trading day to 400 calendar days; shared fetch time-to-live 300 s.
7.  **Bars through the data lane (features 1, 2).** Confirmed: every closed bar goes back through the data lane, so one path orders all bars.

### Open questions on the end-to-end scenarios

1.  **Target tick rate (load scenario).** Decided 2026-11-09: 500 ticks per second per stream, constant. The real peak rate is still not measured (feature 4); change this number when it is.

## Reference

Implementation detail and history. The skeleton and decisions still cite the old behaviour numbers; each feature card lists the behaviours it replaces.

### Step → code

Each box of the algorithm map and the code that carries it.

| Step | Code |
|----|----|
| Route event to a lane | `EventBuffer.push()` · Planned |
| Drain one pass | `EventBuffer.drain_pass()` · Planned; `event_loop/loop.py` · `EventLoop.run()` |
| Dispatch by event type | `event_loop/loop.py` · `EventLoop.dispatch()`, `handles`, `on_*` handlers |
| Bucket ticks by second | `event_loop/resampler.py` · `Resampler` |
| Close the bar | `event_loop/loop.py` · `EventLoop.emit_bar()`, `fire_due_bars()` |
| Annotate the bar | `event_loop/loop.py` · `on_bar`, `EventLoop.dispatch_bar()` |
| Fan out to strategies | `event_loop/loop.py` · `EventLoop.fan_out()` |
| Change the book | `event_loop/book.py` · `RuntimeBook`, `enqueue_book_op()` |
| Seed a strategy | `event_loop/seeder.py` · `Seeder`, `SharedFetch`, `Backoff`, `SeedState` |
| Deploy one strategy | `event_loop/deploy.py` · `DeployService` |
| Flatten at cutoff | `event_loop/shutdown.py` · `ShutdownCoordinator` |
| Record bars, trades | `event_loop/recorders.py` · `BarRecorder`, `TradeRecorder` |
| \[ clock \] | `clock.py` · `Clock`, `WallClock`, `ReplayClock` |

### Skeleton code

The loop is one async function with helper classes around it.

| Piece | Role | Old behaviours |
|----|----|----|
| `EventBuffer` | Two lanes (control, data), the `wake_pending` flag and the drain wake-up. `push()` appends and wakes the loop only when no wake-up is pending; `drain_pass()` clears the flag first, then yields control events before data events within the batch budget. | 11, 12 |
| `run_event_loop()` | Drains the buffer until end of data. Holds the event buffer, the runtime book and the recorders. Reads time only from its `Clock`. Sets the GIL switch interval at start. | 1, 3, 4, 7, 11, 13, 19 |
| `HANDLERS`, `dispatch()` | One handler per event type; the main drain and the final drain both call `dispatch()`. | 4, 13 |
| `Resampler` | Ticks → 1-second bars. Rejects out-of-order ticks and drops late ticks for an already-emitted bucket, so no duplicate bar is made. A pure module with no I/O. | 1, 2 |
| `emit_bar()`, `fire_due_bars()` | The one place a bar is made: tick rollover, quiet-bucket flush and end-of-data flush. | 2, 13 |
| `dispatch_bar()`, `fan_out()` | Let Aux Data annotate, tell the gateway, then step strategies; one fan-out with quarantine for bars and updates. | 3, 4, 5, 13 |
| `ShutdownCoordinator` | Per-slot session-end flatten, shutdown requests, the final flat check before exit. | 7, 9 |
| `Seeder`, `SharedFetch`, `Backoff`, `SeedState` | One seed attempt and one retry per slot; one shared fetch per key; replay recorded bars mid-session. | 8, 18 |
| `BarRecorder`, `TradeRecorder` | One file per instrument per session day; per-strategy round trips. | 10 |
| `RuntimeBook`, `enqueue_book_op()` | The book and its commit boundary. | 15, 17 |
| `DeployService` | Socket server, staging, admission, commit, cleanup. | 16 |

**Full skeleton code (python)**

``` python
"""Skeleton contract for the Event Loop component (Alchemist v3)."""

from __future__ import annotations

import asyncio
import enum
from collections import deque
from typing import Any, Awaitable, Callable

from alchemist.clock import Clock
from alchemist.instruments import Instrument

ON_BAR_ERROR_QUARANTINE = 3       # Behaviour 5
BAR_FLUSH_GRACE_S = 1.0           # Behaviour 2, decision 2
FLATTEN_WAIT_MAX_S = 10.0         # Behaviour 9
DRAIN_TIMEOUT_S = 0.020           # Behaviour 11
DRAIN_BATCH_BUDGET_S = 0.002      # Behaviour 11, decision 8
GIL_SWITCH_INTERVAL_S = 0.0005    # decision 10
SCALE_TRIGGER_SLOT_SUM_P99_S = 0.003  # decision 9
BACKLOG_ALERT = 5000              # decision 3, Behaviour 12

CONTROL_EVENTS = frozenset({"order_update", "balance_update", "repaired_fill",
                            "book_op", "end_of_data"})

class SeedState(enum.Enum):
    """Behaviour 18."""
    PENDING = "pending"
    READY = "ready"
    QUARANTINED_RECOVERABLE = "quarantined_recoverable"
    QUARANTINED_TERMINAL = "quarantined_terminal"

class Backoff:
    """Behaviour 18: jittered, capped delay; never shorter than base."""
    def __init__(self, base_s: float, max_s: float, jitter: float) -> None: ...
    def next_delay(self) -> float: ...

class SharedFetch:
    """Behaviour 18: one in-flight fetch per key, kept for a time-to-live."""
    async def get(self, key: tuple, fetch: Callable[[], Awaitable[Any]]) -> Any: ...

class Seeder:
    """Behaviours 8, 18."""
    async def attempt(self, strategy) -> SeedState: ...
    async def retry(self, strategy) -> None: ...

HANDLERS: dict[str, Callable[[dict], Awaitable[None]]] = {}   # Behaviour 13

async def fan_out(strategies: list, method: str, payload: Any) -> None:
    """Behaviours 4, 5, 13: call method on each strategy; log and count exceptions."""

async def emit_bar(instrument: Instrument, bar: dict, source: str) -> None:
    """Behaviours 1, 2, 13: the one place a completed bar enters dispatch."""

class Resampler:
    """Behaviours 1-2: ticks -> completed 1-second OHLCV bars."""
    def update_from_tick(self, ts: float, price: float, size: float) -> dict | None: ...
    def flush_due(self, now: float) -> dict | None: ...

class RuntimeBook:
    """Behaviours 15, 17: the one registry of strategies and per-instrument resources."""
    strategies: list                     # dispatch order
    resamplers: dict[Instrument, Resampler]
    bars: dict[Instrument, deque]
    contracts: dict[Instrument, Any]
    sessions_by_instrument: dict[Instrument, str]
    def reserve_instrument(self, instrument: Instrument, contract: Any) -> None: ...
    def release_instrument(self, instrument: Instrument) -> None: ...
    def commit(self, strategy, retry_tasks: list) -> None: ...
    def strategies_for(self, instrument: Instrument) -> list: ...

async def enqueue_book_op(fn: Callable[[RuntimeBook], Any]) -> Any:
    """Behaviour 15: run fn(book) on the drain loop between events."""

class EventBuffer:
    """Behaviours 11-12, decision 8: two lanes, one wake-up per burst."""
    control: deque
    data: deque
    wake_pending: bool
    drain_wakeup: asyncio.Event

    def push(self, event: dict) -> None:
        """Append to its lane; wake the loop only if no wake-up is pending."""
    def push_front(self, event: dict) -> None:
        """Replay: put a fill at the front of the control lane."""
    async def wait(self, clock: Clock) -> None:
        """Wait on drain_wakeup with timeout DRAIN_TIMEOUT_S."""
    def drain_pass(self, clock: Clock) -> list[dict]:
        """Clear the wake-up and wake_pending first; control events, then data
        events, until both lanes are empty or DRAIN_BATCH_BUDGET_S is spent."""
    def __len__(self) -> int: ...

async def run_event_loop(event_buffer: EventBuffer,
                         book: RuntimeBook, gateway, aux, safety,
                         recorders, shutdown, clock: Clock) -> None:
    """Behaviours 1-7, 11, 13, 19: set GIL_SWITCH_INTERVAL_S, drain on wake-up,
    resample, dispatch in time order exactly once, quarantine,
    flatten, shut down cleanly."""
```

### Decisions already made

### 1. Who gets order updates?

**Answer:** Every strategy. Each one filters by order-ID ownership, so a strategy that has just closed can still book its closing fill.

### 2. How long can a quiet bar wait?

**Answer:** A 1.0 s clock-driven grace flush bounds the delay in illiquid periods without waiting for the next tick.

### 3. Is the event queue bounded?

**Answer:** No — it is unbounded so order updates are never dropped. A backlog past 5000 items is reported (feature 4).

### 4. When is a strategy quarantined?

**Answer:** After 3 consecutive `on_bar` exceptions it goes to the ERROR state, protecting the whole process.

### Agreed at finalize · 2026-11-09

### 5. How many seeding paths are there?

**Answer:** One: `Seeder.attempt()`, run once at start and again by `Seeder.retry()`. A failed daily fetch and a failed seed are the same retry, with one shared fetch and one backoff. The slot's seed state is one value with four states (feature 7).

**Rejected:** Rejected: a startup path plus separate warmup-retry and seed-retry loops (three copies of one sequence that drift apart); a cache that is used at start but not by retries.

*agreed 2026-11-09 · 2026-11-07*

### 6. How is an event routed to its handler?

**Answer:** Through one table keyed by event type, used by the main drain and the final drain. Every fan-out to strategies goes through `fan_out()`, and every bar through `emit_bar()` (feature 2).

**Rejected:** Rejected: an if/elif chain per drain, and a gather-and-log block copied for each update type.

*agreed 2026-11-09 · 2026-11-07*

### 7. Is the resampler its own component?

**Answer:** No. It is a small pure module (ticks in, bars out, no I/O) under feature 1, tested alone. It stays out of the Gateway because the clock-driven flush needs the loop's clock and the broker's reader thread must stay thin (System map R3).

**Rejected:** Rejected: a separate bar-building component (no outside input and no failure mode of its own); building bars in the Gateway.

*agreed 2026-11-09 · 2026-11-07*

### 8. How does the event buffer wake the loop, and in what order does it drain?

**Answer:** One wake-up per burst: a producer raises the drain wake-up only when `wake_pending` is clear, and the drain loop clears the flag before each pass. The buffer has two deques: a control lane (fills, balance updates, repaired fills, book operations, end of data) drained first, and a data lane (ticks, bars). A pass yields after a 2 ms batch budget. The 20 ms timeout stays as a safety net and the bar-flush cadence (feature 4).

**Rejected:** Rejected: a wake-up on every append (a lock and a system call per tick, almost all wasted during a burst); clearing the flag after the drain (a fill appended at the end of a pass waits up to 20 ms); one lane in arrival order (a fill waits behind a tick burst); one ring per tick stream with a round-robin drain (more parts for a fold that is already cheap, and it does not move decode off the GIL); a lock-free or shared-memory ring of the Disruptor kind (Python has no atomic operations, so its pure-Python versions use locks).

*agreed 2026-11-09 · 2026-11-08*

### 9. What load must one process carry, and when do we scale?

**Answer:** One process carries 3 tick-by-tick streams and 10–15 strategies: one asyncio loop, the official IB reader thread, and the buffer of decision 8. Measure first (feature 5). **Scale trigger:** when p99 of the summed slot cost on the busiest instrument is above 3 ms (room left for queue wait inside R3's 5 ms processing budget), move to one Gateway process (IB socket, decoding, resampler, order send; [System map decision 7](SYSTEM_MAP.md) holds) plus strategy node processes of 5–8 strategies each, joined by a local socket or pipe. After resampling the bus carries only about 3 bars/s plus order updates. Shard by strategy group, not by instrument, because many strategies share each of the 3 instruments. Before that move, Safety and its repair need a design that covers every node ([System map decision 28](SYSTEM_MAP.md)). **Gateway trigger:** if the reader thread's decode time or the GIL wait shows in p99 after decision 10, move the Gateway to an asyncio-native protocol with no reader thread.

**Rejected:** Rejected for now: an asyncio-native Gateway with no reader thread (decoding then runs on the loop thread next to strategy work, and a reimplementation must follow IB's per-message move to protobuf); strategy node processes before the trigger fires (an extra hop for each bar and order, and Safety across nodes is not designed yet); free-threaded Python 3.14 (5–10% slower on one thread, and a C extension can turn the GIL back on).

*agreed 2026-11-09 · 2026-11-08*

### 10. What GIL switch interval does the process use?

**Answer:** 0.5 ms, set once at start with `sys.setswitchinterval(0.0005)`. While the IB reader thread decodes a burst it holds the GIL, and at the default 5 ms the loop thread can wait that long (about 10 ms in the worst case), which is all of R3's budget. The cost is more thread switches; feature 5 measures whether it is enough (decision 9, Gateway trigger).

**Rejected:** Rejected: keeping the 5 ms default (one GIL wait can use the whole latency budget); moving decoding off the GIL now (see decision 9).

*agreed 2026-11-09 · 2026-11-08*
