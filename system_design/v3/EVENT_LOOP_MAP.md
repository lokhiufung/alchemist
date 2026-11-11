# EVENT_LOOP_MAP — Event Loop · Alchemist v3

The engine's heartbeat. It drains one shared queue of events, packs ticks into bars at each strategy's resolution, and hands every bar and order update to the right strategies — in time order, exactly once — while keeping one broken strategy from stopping the rest.

Frozen contract for the Event Loop component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `event_loop_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-11.** **Streaming core agreed 2026-11-11.** One inbound and one outbound buffer, event time and watch time in place of a shared clock, recorders as sinks and deploy requests as events (System map decisions 61–70). **v3 final, 2026-11-09 — agreed.** The End-to-end scenarios in Tests were agreed on 2026-11-09. All features and decisions are agreed. Multi-asset: sessions and cutoffs follow each instrument's exchange profile, each on its own clock. Change history: see `event_loop_map.html`.

## Goal and scope

**What it is.** The Event Loop is the only consumer of the event buffer and the only scheduler of strategy calls. The Event Loop delivers every input a strategy receives: bars, order updates, balance updates, warmup history and session-end flatten requests. The Event Loop also adds new strategies to the runtime book. The Event Loop is one concrete component, not an interface with subclasses, and it is the same code in every mode; only the adapters at the edges differ (feature 13).

**Why it exists.** Four producers write events from different threads: the Gateway, Safety, the bar flush and the deploy socket. Each strategy must receive those events in one time order, exactly once, and an exception in one strategy must not stop the other strategies. Strategies consume bars, but the broker sends ticks. The Event Loop owns the time order, the tick-to-bar conversion and the fault isolation, so that no other component must do these three jobs.

**Goal.** Take events from the inbound buffer, turn ticks into completed bars at each strategy's resolution, and deliver bars, order updates and balance updates to the right strategies, in time order, exactly once, with fault isolation.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Producers push to the inbound buffer | Gateway, Safety, bar flush | Push an event (the Gateway pushes market data, order updates, connection events and replies; Safety pushes repaired fills and Rejected updates); wait until the buffer is idle | event: type, data, enqueued_at |
|  | Replay runner | Push a fill to the front of the buffer | fill event |
|  | Runner | Give each instrument's declared market-data message type at build | instrument → message type, resolution |
|  | Deploy socket | Push a `deploy_request` event | strategy key, config sha256 |
| The Event Loop calls or produces | Strategy | Step on a bar; book an order or balance update; seed with warmup history; request flatten; ask "flat and idle?" and "processing an order?"; quarantine | bar, update, history, flatten reason |
|  | Outbound buffer | Push commands: history and seed-bar requests, contract resolution, subscribe and unsubscribe, `deploy_result` status lines | command with correlation id |
|  | Aux Data | Annotate a bar | bar → bar + aux values |
|  | Safety | Check in from the loop (freeze detector) | check-in |
|  | Recorders | Hand bar and fill events to the sinks | bar, fill, with session and strategy key |

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

Each feature says who needs it and why, what the upstream can give, and how the Event Loop closes the gap. Every number is a parameter with its supported values. Click a box in the map to jump to its feature.

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
        │ bar
        ▼
  ┌─────────────────────────┐
  │ Annotate the bar        │
  │ rule: Aux values        │
  └─────────────────────────┘
        │
        ├── bar ──▶ ( Aux Data )
        │   ◀── bar + aux values
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

  ( deploy socket )
        │
        │ deploy_request event
        ▼
  ┌─────────────────────────┐
  │ Deploy one strategy     │
  │ rule: fail → no change  │
  └─────────────────────────┘
        │
        ├── contract, subscribe command ──▶ ( outbound buffer )
        │
        │ deploy step (on the loop)
        ▼
  ┌─────────────────────────┐
  │ Change the book         │
  │ rule: between events    │
  └─────────────────────────┘
        │
        ├── commit / release ──▶ [ runtime book ]
        │
        │ deploy_result command
        ▼
  ( outbound buffer → deploy socket )

  [ runtime book ]
        │
        │ slot to seed
        ▼
  ┌─────────────────────────┐
  │ Seed a strategy         │
  │ rule: shared, 1 retry   │
  └─────────────────────────┘
        │
        ├── history request ──▶ ( outbound buffer )
        │   ◀── warmup history reply
        │
        │ warmup history
        ▼
  ( Strategy )

  [ watch time ]
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

Main path in colour. A tick never reaches a strategy: it is bucketed, the closed bar goes back into the data lane, and a later drain pass dispatches it as a bar. Dashed boxes are shared data structures; dashed accent arrows are the watch time.

### Feature cards

### 1. Bars from ticks or from bar updates *(settled · was behaviours 1, 2 · System map decision 49)*

#### Need

Strategies make their signals on completed bars at a fixed resolution. A strategy must never see a partial bar, or the same bar twice.

#### Upstream gives

The Gateway gives one market-data stream per instrument, of the type the Gateway declares for that instrument: either `tick_update` messages (ts, price, size) at an uneven rate, sometimes out of order, and nothing at all in quiet periods; or `bar_update` messages, each one completed bar at a stated resolution.

#### Algorithm

1.  At build, read each instrument's declared message type and resolution from the Gateway. If a slot needs a finer resolution than its instrument's `bar_update` feed gives, stop startup with an error that names the slot.
2.  For a `bar_update` at the slot's resolution: reject the bar if its ts is not later than the last bar's; otherwise hand the bar to `emit_bar` as a closed bar. The Resampler is not used for that instrument.
3.  For a `tick_update`, keep one open bucket per (instrument, resolution) in the runtime book.
4.  For each tick: reject it if its ts is older than the last tick's; drop it if its bucket was already emitted; otherwise update open, high, low, close and add its size to volume.
5.  Close the bucket when a tick for a later bucket arrives (rollover), when the watch time reaches bucket end + flush grace (close = last price), or on `end_of_data`. The watch time is the event time of the last event plus the system time passed since that event arrived ([System map decision 68](SYSTEM_MAP.md)); the loop checks it on every pass and on every wait timeout.
6.  Append the closed bar to the data lane (feature 4). The closed bar reaches strategies through feature 2.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Bar resolution | Fixed for v3 ([System map decision 56](SYSTEM_MAP.md)) | 1 s | 1 s |
| Flush grace | Configurable per process ([System map decision 58](SYSTEM_MAP.md)); measured in watch time; stays 1.0 s until a live measurement sets it ([System map decision 72](SYSTEM_MAP.md)) | any positive seconds | 1.0 s |

#### Invariant

For every supported resolution r and both message types: at most one bar per bucket, and every bar goes through `emit_bar`. From ticks: a bar's OHLC covers every accepted tick in \[t, t + r), and its volume is exactly the sum of their sizes; in a quiet period a bar is late by at most the flush grace, in watch time. From bar updates: the bar equals the `bar_update`, field for field.

### 2. Bars delivered in time order, exactly once *(settled · was behaviours 3, 13)*

#### Need

A signal is right only if its bars arrive in time order, with no gaps and no repeats. The simulated broker must fill against a bar before strategies react to it, and Aux Data's values must be on the bar when a strategy reads it.

#### Upstream gives

Bar events from the data lane (feature 1), Aux Data values per bar, and the runtime book's strategies per instrument (feature 11).

#### Algorithm

1.  Dispatch every event through one table keyed by event type. The main drain and the final drain after stop use the same table.
2.  For a bar: Aux Data adds its values, then the bar fans out to the instrument's strategies, one after another, in book order.
3.  One function makes every bar (rollover, flush, end of data); one helper does every fan-out (bars and updates).

#### Parameters

None of its own.

#### Invariant

Per instrument, each strategy gets bars in increasing ts order, exactly once per bucket. Aux Data, then the Gateway, then strategies, always in that order.

### 3. Order and balance updates reach every strategy *(settled · was behaviour 4)*

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

### 4. Fills never wait behind ticks *(settled · was behaviours 11, 12 · System map decisions 26, 27 · decision 8)*

#### Need

A fill must reach its strategy fast, within the processing latency budget ([System map R3](SYSTEM_MAP.md)), even during a tick burst. The loop must use no CPU when idle. No event may be dropped (decision 3).

#### Upstream gives

Producers on different threads: the Gateway (the broker's reader thread), the bar flush, Safety's watcher thread and the deploy socket. The peak tick rate is not measured yet, so bursts have an unknown size.

#### Algorithm

1.  Two lanes. Control: `order_update`, `balance_update`, `repaired_fill`, `deploy_request`, replies, connection events, `end_of_data`. Data: `tick_update`, `bar_update`, `bar`, `heartbeat`.
2.  A producer appends to its lane, then raises the drain wake-up only if `wake_pending` is clear, and sets it. A producer on another thread wakes the loop only through the loop's thread-safe call.
3.  The drain clears the wake-up and `wake_pending` *before* each pass, never after; empties the control lane, then takes data events; and yields when the batch budget is spent. Leftovers go to the next pass without a new wake-up.
4.  With nothing to do, wait on the wake-up with the wait timeout, so the watch-time flush (feature 1) and the shutdown watch (feature 8) still run.
5.  Each pass logs the size of each lane and the p99 queue wait. When the control lane is above the backlog threshold, raise an ERROR with an operator alert. The data lane raises no alert. Never drop.
6.  The buffer is idle when the drain finds both lanes empty, which is only after every event the drain took has been handled. A push clears the idle state. A producer can wait until the buffer is idle; the replay feed does this before each bar ([Gateway feature 12](GATEWAY_MAP.md)).

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

Drawn in the HTML page.

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

### 5. Latency is measured against the budget *(settled · was behaviour 14 · System map decisions 25, 28, 29)*

#### Need

R3 says each strategy's decision on a message must be made within the 5 ms processing latency budget of the message's arrival (System map decision 55). We need measured numbers to know when one process is no longer enough (decision 9).

#### Upstream gives

The broker timestamp on each tick, the time each strategy step takes, and the reader thread's decode time.

#### Algorithm

1.  Per message: record **processing latency**, from the message's `recv_ts` (or the flush time for a bar the watch-time flush closed) to each strategy's final decision on it (an order or cancel handed to the Gateway, or no action), with its parts: queue wait, dispatch latency and slot cost ([System map decision 55](SYSTEM_MAP.md)).
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

### 6. One broken strategy never stops the others *(settled · was behaviour 5 · decision 4)*

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

### 7. Strategies are seeded before they trade *(settled · was behaviours 8, 18 · decision 5)*

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

### 8. Session end and shutdown leave the book flat *(settled · was behaviours 7, 9)*

#### Need

Positions must not stay open past a session's end. At exit every configured position must be flat or flagged, and no queued event may be lost.

#### Upstream gives

The watch time ([System map decision 68](SYSTEM_MAP.md)); each instrument's exchange profile (session calendar, early closes, time zone); shutdown signals; `end_of_data` in replay.

#### Algorithm

1.  When a strategy joins the runtime book, compute its cutoff one time: its session's end on the run's trading date ([System map decision 60](SYSTEM_MAP.md)), on that session's own clock with early closes included, minus the cutoff offset. If the run's trading date is not a trading day for the strategy's exchange, the strategy has no cutoff in this run.
2.  The shutdown runs its own watch: on every loop pass and on every wait timeout, it compares the watch time with each stored cutoff. At a strategy's cutoff, it asks the strategy to flatten, whether bars arrive or not ([System map decision 64](SYSTEM_MAP.md)). The watch never computes a session window on a loop pass.
3.  Repeat on every bar of the instrument until the strategy is flat and idle, a SIGINT/SIGTERM arrives, or the flatten timeout passes in watch time. A flattening slot gets no bars. Keep going when a flatten raises.
4.  When every slot has ended but an exchange's data session is still open, keep running and recording until it closes.
5.  When the watch time reaches the run end, the shutdown requests itself, the same as a SIGINT/SIGTERM. The watch checks the run end on every pass, and an idle loop wakes at least once per drain timeout, so the process exits at the run end without an outside signal.
6.  Before exit: cancel working orders on configured instruments and wait; force-flatten any leftover position that has no working order; otherwise page and exit non-zero. If open orders cannot be read, treat every configured instrument as blocked.
7.  Clean shutdown: flush buffered bars, drain the recorders, tell the strategies, leave the loop.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Cutoff offset before session end | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 1 min | 1 min |
| Run end and its time zone | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any time of day; any IANA time zone | 17:00, America/New_York |
| Flatten timeout | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 10 s | 10 s |

#### Invariant

At exit every configured position is flat or flagged. An instrument the engine does not trade is never touched. No market order is sent at shutdown on an instrument that has, or may have, a working order. A normal shutdown leaves no queued event and no open file.

### 9. Stuck orders: moved to Safety *(settled · was behaviour 6 · System map decision 50)*

#### Need

The periodic stuck-order check now belongs to Safety ([Safety feature 11](SAFETY_MAP.md)). The number 9 is kept so references stay valid.

#### Algorithm

The Event Loop does nothing for this feature. The Event Loop does not query the Gateway for executions and does not call Safety.

### 10. Bars and trades are recorded *(changed 2026-11-11 · was behaviour 10 · System map decision 67)*

#### Need

A run must be auditable afterwards, a mid-session restart needs today's bars (feature 7), and a replay's trades must be comparable with research.

#### Upstream gives

Bar and fill events, in every mode. Each bar event carries its instrument and session; each fill event carries its strategy key.

#### Algorithm

1.  The recorders are sinks: the loop hands each closed bar and each fill event to them, and they hold no reference to the runtime book or the Gateway.
2.  Append every bar to the session day's file per instrument (an overnight session is not split at midnight); roll it up at day end or on finalize.
3.  Pair fills into per-strategy round trips by strategy key (weighted average price for multi-fill legs), with currency; flush them, and repair a pending commission later.
4.  Write under the run's `log_path`. A replay run uses its own `log_path`, so it reads no recording of an earlier run.

#### Parameters

None.

#### Invariant

A recorded bar is durable before finalize. Recording never blocks or changes trading. The recorders are the same code in every mode.

### 11. One runtime book, changed only between events *(settled · was behaviours 15, 17 · System map decision 34)*

#### Need

Dispatch, Aux Data and flatten must all agree which strategies exist, and a dispatch must not see the list change half-way.

#### Upstream gives

The strategies to run (from cold start or a deploy), with their instruments and sessions.

#### Algorithm

1.  One book holds the strategies (dispatch order), resamplers and bar buffers per signal instrument, contracts, fed instruments, session maps and retry tasks. Each container is made once and changed in place, so holders of a reference see additions.
2.  Reserve an instrument before any strategy on it is committed. Reserving is idempotent and keeps an existing recording partition.
3.  Commit is synchronous: it adds the strategy and its session maps in one call, or raises on a duplicate key and leaves the book unchanged.
4.  Only the drain loop changes the book, between events. A change starts from an event (a `deploy_request`, or a reply to a deploy step); no other task changes the book ([System map decision 67](SYSTEM_MAP.md)). A deploy request after shutdown begins is refused.
5.  A committed strategy is a full member from the next event; its session-end flatten uses its own calendar; the commit sends a member event to Aux Data ([System map decision 71](SYSTEM_MAP.md)).

#### Parameters

None.

#### Invariant

A dispatch sees one strategy list for its whole run. No consumer keeps its own copy.

### 12. Add a strategy while running *(settled · was behaviour 16 · System map decisions 35, 36)*

#### Need

The operator adds a strategy in paper or live without a restart, and without touching the strategies already running.

#### Upstream gives

A `deploy_request` event from the deploy socket, the config file, and the Gateway's replies with positions and open orders per account.

#### Algorithm

1.  **Pre-flight** (nothing exists yet): refuse `stale_config`, `config_drift`, `unknown_key`, `conflict`, `import_error`, `params_invalid`, `session_open`, `after_process_exit`, `unknown_account_route` or `unmanaged_exposure`. `after_process_exit`: take the candidate's session on the run's trading date ([System map decision 60](SYSTEM_MAP.md)); refuse unless that session starts after now and ends at or before the run end.
2.  **Reserve and subscribe** each new instrument on the loop; push a subscribe command only for a new signal instrument.
3.  **Prepare**: warm up the candidate alone (feature 7); history requests are commands, and each reply, matched by correlation id, resumes the deploy. A fetch miss is not a failure.
4.  **Commit** on the loop between events, after a re-check of shutdown and the session gate; push a `deploy_result` command with `registered`. Every status line is a `deploy_result` command that the deploy socket writes to the operator.
5.  Another request in flight → `busy`; same revision → `noop`; after shutdown begins → `shutting_down`. On failure after pre-flight, release only instruments no running strategy uses.

#### Parameters

None.

#### Invariant

Every outcome other than `registered` leaves the book as it was. Running strategies keep their connection, positions, orders, indicators and partly built bars.

### 13. Replay and live run the same code *(changed 2026-11-11 · was behaviour 19 · System map decisions 46, 62, 68)*

#### Need

A replay result is evidence about live trading only if the same lines run in both.

#### Upstream gives

Events with their event time on the inbound buffer. The mode selects only the adapters at the edges: the live or simulated gateway, and whether the deploy socket and the OS signal handlers are on.

#### Algorithm

1.  All logic reads event time. The bar flush, the session-active flag, the reconcile and the shutdown watch read the watch time when no event arrives ([System map decision 68](SYSTEM_MAP.md)).
2.  No shared `Clock` and no `clock_advance`: the watch time moves with the events in every mode.
3.  Recorders, Safety and stores are the same objects in every mode; there are no do-nothing stand-ins.

#### Parameters

None.

#### Invariant

No Event Loop code reads the run mode or checks a collaborator for `None`. The Event Loop holds no reference to the Gateway.

### 14. One inbound and one outbound buffer, each with taps *(new 2026-11-11 · System map decisions 61, 63)*

#### Need

Components must exchange data without holding references to each other, in both directions. Safety must see every item without slowing the consumers.

#### Upstream gives

Producers on several threads: the Gateway, Safety, the bar flush and the deploy socket push events; Strategy (through Order Manager), the Event Loop and Safety push commands.

#### Algorithm

1.  The inbound buffer is the event buffer of feature 4, with its control and data lanes; the Event Loop is its only consumer.
2.  The outbound buffer is one queue of commands, drained in push order by the Gateway's command consumer ([Gateway feature 15](GATEWAY_MAP.md)). The outbound buffer is idle when the consumer has run every command it took.
3.  Each buffer keeps a list of taps. A push appends the item to its lane or queue and to every tap queue. A tap append is a thread-safe deque append and never waits.

#### Parameters

None.

#### Invariant

A tap never changes what the consumer receives or its order. A push with taps costs the same as a push without taps, within 10 %.

## Tests

The goal and features as measurable cases. A test with parameter cases runs once per supported value; when a value is added to a feature's Parameters, its cases grow with it. Every row has a test under `tests/event_loop/` (streaming core built 2026-11-11).

**End to end · the sample (1 test)**

|  |  |
|----|----|
| Test | `tests/event_loop/test_event_loop_sample.py::test_golden_bar_updates_fills_and_end_of_data` |
| Input | The golden session from a Gateway stub: 23,400 `bar_update` messages for MES on 2026-04-15 (NY_RTH), the daily history and seed bars of the 60 prior sessions, two fills (the BUY in phase B, the SELL at the target), then `end_of_data` ([System map golden sample](SYSTEM_MAP.md)). |
| Parameter cases | message type ∈ {`bar_update`} × resolution ∈ {1 s} |
| Passes when | The slot is seeded before the first replay bar; the strategy gets all 23,400 bars in sequence, with no duplicate or missing timestamp; the Resampler is never called; the watch time equals each bar's end time when the bar is dispatched; the strategy gets both fills; no ERROR line is logged; and the loop ends with no unhandled exception. |
| Fails when | A bar is out of order, skipped or repeated; an order update is dropped; an exception in one strategy stops bars reaching the others; or the loop keeps running after `end_of_data`. |

### End-to-end scenarios *(agreed 2026-11-09)*

Each scenario drives the component only through its public interface, from a known start state, and checks one exact, observable output. A scenario tells one story across several features; the feature tables below keep the single rules. File: `tests/event_loop/test_event_loop_scenarios.py`. A Gateway stub plays scripted messages and records what the Event Loop asks of it; recording strategies stand in for Strategy slots and record every bar and update they get; an Aux Data stub adds nothing to bars; a controlled system-time source moves the watch time. Times are New York unless stated.

**S1. Ticks become 1-second bars (features 1, 2)**

|  |  |
|----|----|
| Test | `test_scenario_ticks_to_bars` |
| Given | The Gateway stub declares `tick_update` for MES; one recording strategy on MES. |
| When | Send ticks at 09:30:00.1 (5000.00, 1), 09:30:00.5 (5001.00, 2), 09:30:00.9 (4999.00, 1), 09:30:01.2 (5000.50, 3), then a tick at 09:30:00.7 (5002.00, 1), then `end_of_data`. |
| Parameter cases | resolution ∈ {1 s} |
| Passes when | The strategy gets exactly two bars, in order: 09:30:00 with O 5000.00, H 5001.00, L 4999.00, C 4999.00, V 4 (closed by the rollover), and 09:30:01 with O = H = L = C = 5000.50, V 3 (closed by `end_of_data`). The tick at 09:30:00.7 is rejected and changes no bar. |
| Fails when | A wrong OHLCV value; the late tick changes a bar; a bar is missing or repeated. |

**S2. A quiet market closes the bar by the watch time (features 1, 4, 13)**

|  |  |
|----|----|
| Test | `test_scenario_quiet_market_flush` |
| Given | The Gateway stub declares `tick_update` for MES. |
| When | Send one tick at 09:30:00.2 (5000.00, 1); send nothing more; move system time forward. |
| Parameter cases | flush grace ∈ {1.0 s}; wait timeout ∈ {20 ms} |
| Passes when | The bar 09:30:00 (C 5000.00, V 1) reaches the strategy once, at a watch time ≥ 09:30:02.0 (bucket end + flush grace) and ≤ 09:30:02.0 + the wait timeout. |
| Fails when | The bar never closes; the bar closes before bucket end + grace; the bar arrives twice. |

**S3. Bar updates are taken as bars (features 1, 2, 13)**

|  |  |
|----|----|
| Test | `test_scenario_bar_updates` |
| Given | The Gateway stub declares `bar_update` at 1 s for MES. |
| When | Send `bar_update` for 09:30:00 to 09:30:09, then a second 09:30:05 and a 09:30:03, then `end_of_data`. |
| Parameter cases | message type ∈ {`bar_update`} × resolution ∈ {1 s} |
| Passes when | The strategy gets the 10 bars 09:30:00 to 09:30:09 once each, in order; the repeated and the older bar are rejected; the Resampler is never called; at each dispatch the watch time equals the bar's end time; the loop stops after `end_of_data`. |
| Fails when | A bar out of order, missing or repeated; the Resampler is called; the watch time differs from the bar end time; the loop runs on after `end_of_data`. |

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
| When | Run the watch time across each session's cutoff, with one bar per second. |
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
| When | Run it once behind the simulated broker and once behind a fake live gateway that stamps the same event times. |
| Parameter cases | gateway ∈ {simulated broker, fake live gateway} |
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

**1. Bars from ticks or from bar updates (6 tests · 6 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_bar_ohlcv_matches_ticks_in_bucket` | resolution ∈ {1 s} | OHLC and volume vs. a hand count, over ticks spanning 3 buckets | exact match |
| `test_out_of_order_tick_rejected` | resolution ∈ {1 s} | bar after a tick with ts \< last ts | unchanged by that tick |
| `test_late_tick_for_emitted_bucket_ignored` | resolution ∈ {1 s} | bars emitted for that bucket | exactly 1 |
| `test_quiet_bucket_flushed_after_grace` | resolution ∈ {1 s} × grace ∈ {1.0 s} | watch time from bucket end to bar emitted; close price | ≤ grace; close = last price |
| `test_end_of_data_flushes_open_bucket` | resolution ∈ {1 s} | bars emitted for the open bucket | exactly 1 |
| `test_bar_update_feed_bypasses_resampler` | — | `harness.book.resamplers; strategy.bars` | `= {}; = [OPEN]` |

**2. Bars delivered in time order, exactly once (5 tests · 8 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_bars_in_order_exactly_once` | strategies per instrument ∈ {1, 15} | each strategy's bar timestamps | strictly increasing, no gap, no repeat |
| `test_aux_then_strategies` | — | call order on one bar | Aux Data, strategies; the Gateway is not called |
| `test_final_drain_uses_same_handlers` | — | handler per event type after stop | same as main drain |
| `test_every_bar_close_passes_one_function` | close cause ∈ {rollover, flush, end of data, `bar_update`} | bars made outside that function | 0 |
| `test_bar_update_feed_bypasses_resampler_and_finer_resolution_refused` | slot resolution ∈ {1 s on a 1 s feed, finer than the feed} | Resampler calls; startup result | 0, starts; refused, names the slot |
| `test_unknown_event_type_is_logged_and_skipped` | — | `any((getattr(r, 'event_type', None) == 'mystery' for r in caplog.records))` | `true` |

**3. Order and balance updates reach every strategy (4 tests · 6 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_order_update_reaches_every_strategy` | strategy state ∈ {in session, flattening, session ended} | strategies that received the update | all |
| `test_round_trip_close_flushes_trade_recorder` | — | recorder flushes per closed round trip | exactly 1 |
| `test_position_update_and_repaired_fill_reach_every_strategy` | — | `strategy.events == [('order', 9, 'FILLED'), ('position', 2)] or sorted(strategy.events) =…` | `true` |
| `test_repair_event_reaches_only_its_strategy` | — | `owner.events; other.events` | `= [('order', 42, 'FILLED')]; = []` |

**4. Fills never wait behind ticks (13 tests · 15 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_control_lane_drained_before_data_lane` | ticks queued ahead ∈ {0, 2000} | dispatch position of a fill pushed after the ticks | first |
| `test_idle_only_after_every_popped_event_is_handled` | — | idle state after push, after the last pop, after the empty pop | clear; clear; set |
| `test_burst_sends_one_wakeup_while_loop_awake` | burst size ∈ {1000} | wake-up calls during one drain | 1 |
| `test_push_racing_end_of_drain_is_not_left_for_timeout` | wait timeout ∈ {20 ms} | queue wait of a push between last pop and sleep | \< wait timeout |
| `test_drain_pass_yields_after_batch_budget` | budget ∈ {2 ms} | pass length; wake-ups for leftovers | ≤ budget + one event; 0 |
| `test_drain_starts_on_wake_without_timer` | — | time from push to drain start | \< wait timeout |
| `test_backlog_over_threshold_raises_error_drops_nothing` | threshold ∈ {5000} × lane ∈ {control, data} | ERROR raised; events dispatched | control: at threshold + 1, data: never; all |
| `test_each_lane_drains_in_arrival_order_and_nothing_is_dropped` | — | `[e['data']['broker_ts'] for e in events if e['event_type'] == 'tick_update']; len(events)` | `= [100.0 + i for i in range(50)]; = 100` |
| `test_end_of_data_waits_for_the_data_lane` | — | `[e['event_type'] for e in drain_all(buffer)]` | `= ['tick_update', 'end_of_data']` |
| `test_fill_behind_a_held_end_of_data_still_jumps_the_data_lane` | — | `[e['event_type'] for e in events][0]; [e['event_type'] for e in events][-1]` | `= 'order_update'; = 'end_of_data'` |
| `test_no_error_when_control_lane_is_at_threshold` | — | `buffer.control_backlog_exceeded(); buffer.control_backlog_exceeded()` | `is False; is True` |
| `test_reader_thread_wakes_loop_through_thread_safe_call` | 2 cases | `seen_threads; len(buffer.of_type('tick_update'))` | `= ['reader']; = 1` |
| `test_safety_checked_in_every_pass` | — | `harness.safety.check_ins` | `≥ 1` |

**5. Latency is measured against the budget (8 tests · 12 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_processing_and_feed_latency_logged_per_message` | message ∈ {`tick_update` closing a bar, `bar_update`, flushed bar, order update} | fields per record | processing latency (to each strategy's decision, also with no order), queue wait, dispatch latency, slot cost; feed latency with its precision (none for a flushed bar) |
| `test_slot_cost_sum_and_decode_time_logged_per_instrument` | instruments ∈ {1, 3} | records per minute | p50 and p99 for each instrument |
| `test_rebuild_replay_yields_within_budget` | yield budget ∈ {1 ms} | longest blocking slice during a rebuild | ≤ yield budget |
| `test_maybe_flush_runs_once_a_minute` | — | `len(latency_rows(caplog))` | `= 1` |
| `test_metrics_reset_after_a_flush` | — | `latency_rows(caplog)` | `= []` |
| `test_percentiles_per_instrument_per_minute_are_logged` | — | `{'feed_p50', 'feed_p99', 'proc_p50', 'proc_p99', 'queue_wait_p99', 'dispatch_p99', 'slot_…` | `≤ set(row); = pytest.approx(0.05, abs=0.002); ≤ 0.0101` |
| `test_sample_memory_and_flush_cost_stay_bounded_under_a_tick_flood` | — | `time_module.perf_counter() - started; row['feed_p50']; row['feed_p99']` | `< 0.005; = pytest.approx(0.05, abs=0.01); = pytest.approx(0.099, abs=0.005)` |
| `test_timestamp_precision_follows_the_broker_timestamps` | — | `metrics.precision(MES); metrics.precision(MES)` | `= 1.0; = 0.001` |

**6. One broken strategy never stops the others (3 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_strategy_quarantined_at_threshold_others_continue` | threshold ∈ {3} | errors before ERROR; bars reaching the others | exactly threshold; all |
| `test_fan_out_logs_and_counts_each_strategy_exception` | failing strategies ∈ {1, all} | log lines and error counts | one per exception |
| `test_success_resets_the_consecutive_error_count` | — | `flaky.quarantined; len(flaky.bars)` | `false; = 3` |

**7. Strategies are seeded before they trade (12 tests · 13 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_one_seed_fetch_shared_between_slots` | slots per key ∈ {1, 5} | broker fetches | 1 per key |
| `test_failed_seed_quarantines_only_that_slot` | — | other slots' seed state | `READY` |
| `test_startup_and_retry_run_the_same_attempt` | — | function run at start and on retry | the same |
| `test_retry_backoff_jittered_and_capped` | (base, max, jitter) ∈ {(5 s, 300 s, 0.2)} | retry delays | ≥ base and ≤ max |
| `test_terminal_quarantine_never_overwritten_by_retry` | — | state after a late retry lands | `QUARANTINED_TERMINAL` |
| `test_cache_serves_a_later_attempt_within_ttl_only_for_non_empty_results` | — | `gateway.daily_calls` | `= 1` |
| `test_empty_needed_history_is_not_ready` | — | `seeder.state_of('a')` | `is SeedState.QUARANTINED_RECOVERABLE` |
| `test_fatal_seed_failure_is_terminal` | — | `seeder.state_of('a')` | `is SeedState.QUARANTINED_TERMINAL` |
| `test_history_handed_over_in_fixed_order_then_recorded_bars_replayed` | — | `order; strategy.histories[0]['daily']; len(strategy.histories[0]['intraday'])` | `= ['warmup', 'replay3', 'replay1', 'ready']; true; = 3` |
| `test_shared_fetch_does_not_keep_empty_results` | — | `len(calls)` | `= 2` |
| `test_slot_without_history_needs_still_gets_ready` | — | `seeder.state_of('a'); gateway.daily_calls` | `is SeedState.READY; = 0` |
| `test_two_attempts_never_seed_one_slot_at_once` | — | `len(strategy.histories)` | `= 1` |

**8. Session end and shutdown leave the book flat (12 tests · 19 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_cutoff_on_session_clock` | exchange ∈ {CME, Eurex} × session ∈ {day, overnight} | time of first flatten request | session end − cutoff offset (1 min), on the session's clock |
| `test_early_close_cutoff_from_exchange_profile` | — | time of first flatten request | early close − cutoff offset |
| `test_flatten_repeats_until_flat_signal_or_timeout` | outcome ∈ {flat, signal, timeout} | when repeating stops | at flat; at signal; ≤ flatten timeout (10 s) |
| `test_run_exits_at_the_first_run_end_after_start` | start ∈ {18:00, 10:00} New York | run end | 17:00 New York on the next day; 17:00 New York on the same day |
| `test_process_due_does_not_recompute_session_windows_on_every_call` | 15 strategies × 500 loop passes | session window computations | ≤ 3 per strategy |
| `test_loop_exits_by_itself_at_run_end` | overnight strategy, watch time runs past 17:00 New York, no signal, no `end_of_data` | shutdown requested; loop ended | yes; yes |
| `test_unreadable_open_orders_block_force_flatten` | — | market orders sent at exit | 0 |
| `test_end_of_data_leaves_no_queued_event` | — | events left; files left open | 0; 0 |
| `test_is_flat_reads_the_gateway_positions_of_configured_instruments` | — | `await rig.shutdown.is_flat(); await rig.shutdown.is_flat()` | `is False; is True` |
| `test_leftover_position_with_working_order_is_flagged_not_forced` | — | `gateway.placed; rig.shutdown.not_flat_at_shutdown` | `= []; is True` |
| `test_os_signal_event_requests_shutdown` | — | `harness.shutdown.shutdown_requested` | `true` |
| `test_overnight_session_cutoff_on_its_own_clock` | — | `cutoff <= strategy.request_times[0] <= cutoff + 1.0` | `true` |

**9. Stuck orders: moved to Safety (1 test · 1 case)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_event_loop_never_calls_safety_repair` | — | calls from Event Loop code to Safety's repair or to the Gateway's executions query | 0 |

**10. Bars and trades are recorded (10 tests · 11 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_overnight_session_bars_not_split_at_midnight` | — | files per overnight session | 1 |
| `test_round_trip_weighted_average_for_multi_fill_leg` | — | leg price | size-weighted mean, exact |
| `test_recorded_bar_durable_before_finalize` | — | bars on disk after a crash before finalize | every bar emitted before the crash |
| `test_bars_are_recorded` | — | `harness.bar_recorder.bars` | `= [OPEN, OPEN + 1]` |
| `test_next_overnight_session_gets_its_own_file` | — | `len(files(tmp_path))` | `= 2` |
| `test_open_position_is_not_written_until_closed` | — | `list((tmp_path / 'trades').glob('*.jsonl'))` | `= []` |
| `test_recorded_bars_load_back_for_a_mid_session_start` | — | `[b['close'] for b in loaded]; loaded[0]['instrument']; load_recorded_bars(tmp_path / 'mis…` | `= [100.0 + i for i in range(5)]; = MES; = []` |
| `test_scenario_recording_bars` | 2 cases | `len(files(tmp_path)); len(rows); rows[0]['instrument']; rows[0]['close']` | `= 1; = 180; = 'MES'; = 100.0` |
| `test_scenario_recording_round_trip_row_has_prices_and_currency` | — | `(row['strategy'], row['currency'], row['entry_price'], row['exit_price'], row['qty'])` | `= ('slot', 'USD', 5001.0, 5011.0, 1)` |
| `test_unowned_updates_are_ignored` | — | `recorder.pending` | `= []` |

**11. One runtime book, changed only between events (12 tests · 12 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_book_op_runs_between_events_and_resolves_its_future` | — | events interleaved with the operation; future result | 0; resolved |
| `test_duplicate_key_commit_leaves_book_unchanged` | — | book contents after the raise | unchanged |
| `test_dispatch_sees_one_strategy_list` | — | strategies called by a fan-out during which a commit is queued | the list from the fan-out's start |
| `test_day_strategy_added_in_evening_not_due_that_evening` | — | first flatten request | at the next day-session cutoff |
| `test_admit_on_tick_feed_creates_a_resampler_and_subscribes_once` | — | `gateway.subscribed; len(book.strategies); MES` | `= [MES]; = 2; in book.sessions_by_instrument` |
| `test_admit_subscribes_only_new_signal_instruments_and_reserves_execution_instrument` | — | `gateway.subscribed; NQ; NQ` | `= [MES]; in book.contracts; not in book.fed_instruments` |
| `test_admit_with_a_fetch_miss_still_commits_and_retries` | — | `book.strategies; 'slot'; book.retry_tasks['slot']` | `= [strategy]; in book.retry_tasks; true` |
| `test_commit_records_session_maps` | — | `book.sessions_by_strategy['a']; book.sessions_by_execution_instrument[NQ]; book.strategie…` | `= 'NY_RTH'; = 'NY_RTH'; = book.strategies; = []` |
| `test_commit_sends_a_member_event_to_each_listener` | — | `joined` | `= ['a']` |
| `test_failed_admit_releases_only_what_it_reserved` | — | `MES; NQ; len(book.strategies)` | `in book.contracts; not in book.contracts; = 1` |
| `test_release_only_when_no_strategy_uses_the_instrument` | — | `MES; ES; ES` | `in book.contracts; not in book.contracts; not in book.fed_instruments` |
| `test_reserve_is_idempotent_and_keeps_the_existing_partition` | — | `book.reserve_instrument(MES, {'c': 2}, fed=True); list(book.bars[MES]); book.contracts[ME…` | `is False; = [{'start_ts': 1}]; = {'c': 1}` |

**12. Add a strategy while running (17 tests · 35 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_each_refusal_reason_touches_nothing` | reason ∈ the 10 pre-flight refusals | book and subscriptions after the reply | unchanged |
| `test_failed_candidate_releases_only_its_own_instruments` | — | instruments released | only those no running strategy uses |
| `test_overnight_deploy_needs_the_session_to_close_before_run_exit` | deploy at 18:00 New York of an RTH strategy × run end ∈ {17:00, 15:00} New York next day | reply | `registered`; `rejected` with `after_process_exit` |
| `test_busy_noop_shutting_down_replies` | case ∈ {request in flight, same revision, after shutdown} | reply | `busy`; `noop` with the original ID; `shutting_down` |
| `test_a_fetch_miss_while_preparing_is_not_a_failure` | — | `lines[-1]['status']` | `= 'registered'` |
| `test_client_exit_code_follows_the_terminal_status` | 4 cases | `await serve_deploy(folder, lines_for, lambda: send_deploy_request(folder / 'deploy.sock',…` | `= code` |
| `test_client_without_a_server_exits_with_failure` | — | `await send_deploy_request(tmp_path / 'missing.sock', 'b', 'abc')` | `= 1` |
| `test_commit_refused_when_shutdown_begins_during_preparation` | — | `lines[-1]['status']; [s.name for s in rig.book.strategies]` | `= 'shutting_down'; = ['a']` |
| `test_deploy_request_event_goes_to_the_deploy_service` | — | `harness.deploy.requests` | `= [request]` |
| `test_every_deploy_status_line_is_a_deploy_result_command` | — | `[(data['request_id'], data['line']['status'], data['final']) for data in results]` | `= [(9, DeployStatus.PREPARING, False), (9, DeployStatus.REGISTERED, True)]` |
| `test_exposure_managed_by_a_running_strategy_on_the_same_account_is_allowed` | — | `statuses(await rig.deploy('b'))[-1]` | `= ('registered', None)` |
| `test_scenario_deploy_strategy_busy` | — | `statuses(second); statuses(first_lines)[-1]` | `= [('busy', None)]; = ('registered', None)` |
| `test_scenario_deploy_strategy_refusals` | 4 cases | `statuses(lines); rig.snapshot()` | `= [('rejected', reason)]; = before` |
| `test_scenario_deploy_strategy_registered_then_noop` | — | `rig.gateway.subscribed.count(MES); rig.gateway.subscribed.count(ES); statuses(again); aga…` | `= 1; = 1; = [('noop', None)]; = rig.service.file_sha()` |
| `test_socket_permissions` | — | `stat.S_IMODE(os.stat(folder).st_mode); stat.S_IMODE(os.stat(folder / 'deploy.sock').st_mo…` | `= 448; = 384` |
| `test_socket_round_trip_streams_status_lines` | — | `code; [o['status'] for o in out]` | `= 0; = ['preparing', 'registered']` |
| `test_unreadable_broker_refuses_with_unmanaged_exposure` | — | `statuses(await rig.deploy('b'))` | `= [('rejected', 'unmanaged_exposure')]` |

**13. Replay and live run the same code (9 tests · 9 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_watch_time_drives_flatten_and_flush` | — | flush and flatten times in replay | equal to the watch time from the tape, not wall time |
| `test_flatten_starts_with_no_bars` | no bar after 15:58:30 New York | flatten requests at the cutoff | 1, when the watch time reaches 15:59:00 |
| `test_tap_never_changes_consumer_order` | taps ∈ {0, 2} | events the consumer receives, in order | identical |
| `test_no_run_mode_read_in_event_loop` | — | reads of the run mode in Event Loop code | 0 |
| `test_golden_bar_updates_fills_and_end_of_data` | — | `[r for r in caplog.records if r.levelno >= logging.ERROR]; len(buffer); loop.end_of_data_…` | `false; = 0; true` |
| `test_moment_is_the_watch_time_as_utc_datetime` | — | `watch.moment()` | `= datetime.fromtimestamp(1002.0, timezone.utc)` |
| `test_new_event_resets_the_watch_to_its_event_time` | — | `watch.now(system_now=51.0)` | `= 1010.0` |
| `test_older_event_never_moves_the_watch_back` | — | `watch.now(system_now=51.0)` | `= 1011.0` |
| `test_ready_waits_for_the_first_event` | — | `waiter.done()` | `false` |
| `test_watch_time_is_last_event_time_plus_system_time_since_arrival` | — | `watch.now(system_now=52.5)` | `= 1002.5` |
| `test_watch_time_is_none_before_the_first_event` | — | `WatchTime().now(system_now=10.0)` | `is None` |

**14. One inbound and one outbound buffer, each with taps (6 tests · 6 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_connection_restored_raises_the_order_id_floor` | — | `harness.requests.next_order_id()` | `= 500` |
| `test_consumer_gets_commands_in_push_order` | — | `taken` | `= pushed` |
| `test_every_tap_sees_every_pushed_event_in_push_order` | — | `list(first); list(second)` | `= pushed + [front]; = pushed + [front]` |
| `test_get_waits_for_a_push_from_another_thread` | — | `taken['correlation_id']` | `= 9` |
| `test_idle_only_after_consumer_ran_every_command_it_took` | — | `buffer.idle.is_set(); buffer.idle.is_set()` | `false; true` |
| `test_reply_event_resolves_the_waiting_request` | — | `await waiting` | `= [{'size': 0}]` |

## Open questions

Gaps found while writing the features. Each one needs a human answer; the answer goes into the feature's Parameters or Algorithm.

All decided on 2026-11-09.

1.  **Bar resolution (feature 1).** 1 s only in v3. Other resolutions come later, chosen per strategy and built from 1-second bars ([System map decision 56](SYSTEM_MAP.md)).
2.  **Flush grace (feature 1).** Configurable per process; default 1.0 s; it does not depend on the resolution while only 1 s exists. Measured in watch time; the default stays until a live measurement sets it (decided 2026-11-11, [System map decision 72](SYSTEM_MAP.md)).
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

Each box of the algorithm map and the code that carries it. The Event Loop is in `event_loop/`; the request client that turns calls into commands with a correlation id is `requests.py` · `Requests`.

| Step | Code |
|----|----|
| Route event to a lane | `event_loop/buffer.py` · `EventBuffer.push()`, `lane_for()`, `push_front()` |
| Drain one pass | `event_loop/buffer.py` · `EventBuffer.begin_pass()`, `pop()`, `wait()`; `event_loop/loop.py` · `EventLoop.run()`, `drain_pass()` |
| Dispatch by event type | `event_loop/loop.py` · `EventLoop.dispatch()`, `handles`, `on_*` handlers; `on_reply` resolves a request by correlation id |
| Bucket ticks by second | `event_loop/resampler.py` · `Resampler` |
| Close the bar | `event_loop/loop.py` · `EventLoop.emit_bar()`, `fire_due_bars()` (reads the watch time) |
| Annotate the bar | `event_loop/loop.py` · `EventLoop.dispatch_bar()` |
| Fan out to strategies | `event_loop/loop.py` · `EventLoop.fan_out()`; `on_order_update` gives an event with a strategy key to that strategy only |
| Change the book | `event_loop/book.py` · `RuntimeBook.admit()`, `commit()`, `release_instrument()`; `commit()` sends a member event to `RuntimeBook.member_listeners` |
| Seed a strategy | `event_loop/seeder.py` · `Seeder`, `SharedFetch`, `Backoff`, `SeedState`; history requests through `Requests.request()` |
| Deploy one strategy | `event_loop/deploy.py` · `DeployService.handle()`, `deploy()`; `on_deploy_request`; status lines as `deploy_result` commands; socket adapter `deploy_socket.py` · `DeploySocketServer` |
| Flatten at cutoff | `event_loop/shutdown.py` · `ShutdownCoordinator.process_due()`, `flatten_on_bar()`, `finalize()` (reads the watch time); `on_os_signal` |
| Record bars, trades | `event_loop/recorders.py` · `BarRecorder.record(instrument, bar, session)`, `TradeRecorder.record_update(update, strategy_key, order)` |
| \[ watch time \] | `event_loop/watch_time.py` · `WatchTime.observe()`, `now()`, `moment()`, `ready()` |
| \[ two buffers \] | `event_loop/buffer.py` · `EventBuffer` (inbound), `CommandBuffer` (outbound), `add_tap()` |

### Skeleton code

The loop is one async function with helper classes around it.

| Piece | Role | Old behaviours |
|----|----|----|
| `EventBuffer` | Two lanes (control, data), the `wake_pending` flag and the drain wake-up. `push()` appends and wakes the loop only when no wake-up is pending; `drain_pass()` clears the flag first, then yields control events before data events within the batch budget. | 11, 12 |
| `run_event_loop()` · Planned (the code today is `EventLoop.run()`) | Drains the buffer until end of data. Holds the inbound buffer, the outbound buffer, the runtime book and the recorders. Reads event time and the watch time only. Sets the GIL switch interval at start. | 1, 3, 4, 7, 11, 13, 19 |
| `HANDLERS`, `dispatch()` | One handler per event type; the main drain and the final drain both call `dispatch()`. | 4, 13 |
| `Resampler` | Ticks → 1-second bars. Rejects out-of-order ticks and drops late ticks for an already-emitted bucket, so no duplicate bar is made. A pure module with no I/O. | 1, 2 |
| `emit_bar()`, `fire_due_bars()` | The one place a bar is made: tick rollover, quiet-bucket flush and end-of-data flush. | 2, 13 |
| `dispatch_bar()`, `fan_out()` | Let Aux Data annotate, tell the gateway, then step strategies; one fan-out with quarantine for bars and updates. | 3, 4, 5, 13 |
| `ShutdownCoordinator` | Per-slot session-end flatten, shutdown requests, the final flat check before exit. | 7, 9 |
| `Seeder`, `SharedFetch`, `Backoff`, `SeedState` | One seed attempt and one retry per slot; one shared fetch per key; replay recorded bars mid-session. | 8, 18 |
| `BarRecorder`, `TradeRecorder` | One file per instrument per session day; per-strategy round trips. | 10 |
| `RuntimeBook` | The book and its commit boundary; changed only between events. | 15, 17 |
| `DeployService` | Socket server, staging, admission, commit, cleanup. | 16 |

**Full skeleton code (python)**

``` python
"""Skeleton contract for the Event Loop component (Alchemist v3)."""

from __future__ import annotations

import asyncio
import enum
from collections import deque
from typing import Any, Awaitable, Callable

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
                            "deploy_request", "reply", "connection_lost",
                            "connection_restored", "end_of_data"})

class WatchTime:
    """Feature 1, 8: event time of the last event + system time since it arrived."""
    def observe(self, event_time: float, arrived_at: float) -> None: ...
    def now(self, system_now: float) -> float: ...

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

class CommandBuffer:
    """Feature 14: the outbound buffer; drained in push order by the Gateway."""
    def push(self, command: dict) -> None: ...
    def add_tap(self, tap: deque) -> None: ...

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
    def add_tap(self, tap: deque) -> None:
        """Feature 14: every push also appends to each tap; never waits."""
    async def wait(self) -> None:
        """Wait on drain_wakeup with timeout DRAIN_TIMEOUT_S."""
    def drain_pass(self) -> list[dict]:
        """Clear the wake-up and wake_pending first; control events, then data
        events, until both lanes are empty or DRAIN_BATCH_BUDGET_S is spent."""
    def __len__(self) -> int: ...

async def run_event_loop(inbound: EventBuffer, outbound: CommandBuffer,  # Planned; today EventLoop.run()
                         book: RuntimeBook, aux, recorders, shutdown,
                         watch_time: WatchTime) -> None:
    """Behaviours 1-7, 11, 13, 19: set GIL_SWITCH_INTERVAL_S, drain on wake-up,
    resample, dispatch in time order exactly once, quarantine,
    flatten, shut down cleanly."""
```

### Decisions already made

### 1. Who gets order updates?

**Answer:** Every strategy. Each one filters by order-ID ownership, so a strategy that has just closed can still book its closing fill.

### 2. How long can a quiet bar wait?

**Answer:** A 1.0 s grace flush on the watch time bounds the delay in illiquid periods without waiting for the next tick ([System map decision 68](SYSTEM_MAP.md)).

*changed 2026-11-11 · was a clock-driven flush*

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

**Answer:** No. It is a small pure module (ticks in, bars out, no I/O) under feature 1, tested alone. It stays out of the Gateway because the watch-time flush needs the loop's watch time and the broker's reader thread must stay thin (System map R3).

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
