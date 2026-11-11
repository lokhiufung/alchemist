# SAFETY_MAP — Safety · Alchemist v3

The watchdog. Safety watches both buffers through taps, asks the Gateway for a ladder of fixes when the socket or the feed looks dead, and repairs every strategy left waiting on an order whose outcome the engine missed.

Frozen contract for the Safety component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `safety_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-11.** **Streaming core agreed 2026-11-11.** Safety reads both buffers through taps and acts only through requests and repair events (System map decisions 61–70). **v3 final, 2026-11-09 — agreed.** The End-to-end scenarios in Tests were agreed on 2026-11-09. All features and decisions are agreed. Multi-asset: the feed watchdog arms on every configured exchange's data session. Change history: see `safety_map.html`.

## Goal and scope

**What it is.** Safety is the watchdog of the process. Safety runs a concurrent watcher thread beside the Event Loop. The watcher reads the events and commands that pass through the inbound and the outbound buffer, and it never blocks their consumers. From what the watcher reads, Safety decides that the broker socket is dead, that the feed is stale, that an order is stuck or that a fill was missed. Safety acts only by sending requests to the Gateway on the outbound buffer, and repair events to the strategies on the inbound buffer ([System map decision 61](SYSTEM_MAP.md)).

**Why it exists.** A broker connection drops, a data farm goes quiet, and a fill can happen while the engine is disconnected. Without repair, a strategy waits forever on that order. This work has its own failure modes and its own recovery, independent of trading, so Safety is a separate component ([System map decision 4](SYSTEM_MAP.md), [decision 8](SYSTEM_MAP.md)). The Gateway does the connection work itself ([System map decision 65](SYSTEM_MAP.md)); Safety decides when recovery is needed and checks the result.

**Goal.** Watch the broker socket and the market-data feed in system time; ask the Gateway for escalating recovery when a check fails; and repair strategies stranded during a disconnection or by a lost update, by attributing missed fills from the broker's executions, by order id.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Safety reads | Inbound buffer (watcher tap) | Feed events, `heartbeat`, `connection_lost`, `connection_restored`, order updates, replies | event time, arrival system time, correlation id |
|  | Outbound buffer (watcher tap) | Subscribe and unsubscribe commands, order and cancel commands | instrument, order id, strategy key |
|  | Event Loop | Check-ins from the loop | check-in |
| Safety writes | Outbound buffer | Requests to the Gateway: `resubscribe(instruments)`, `reconnect`, `restart_broker_process`, executions, open orders, `poll_positions`; alerts | correlation id |
|  | Inbound buffer | Repaired Filled and Rejected order updates | order id, strategy key |
| Runner starts Safety | Runner | Start and stop the watcher and the freeze detector | safety config |

### Scope

Owns, from the System map:

> Detecting a lost broker connection, a stale feed or a frozen process, asking the Gateway to recover, un-sticking any strategy left waiting on an order whose fill happened during the outage, and the periodic stuck-order check.

| Not Safety's job | Owned by |
|----|----|
| Talking to the broker; the connection's own heartbeat, reconnect with backoff and resubscribe after reconnect | [Gateway](GATEWAY_MAP.md) |
| Deciding what to trade | [Strategy](STRATEGY_MAP.md), [StrategyExecution](STRATEGY_EXECUTION_MAP.md) |
| Normal event delivery; giving a repair event to its strategy | [Event Loop](EVENT_LOOP_MAP.md) |
| The runtime book's members | Not needed by Safety ([System map decision 69](SYSTEM_MAP.md)) |

### Place in the system

Drawn in the HTML page; the flows are the Public interface table above and the [SYSTEM_MAP.md](SYSTEM_MAP.md) Diagram.

Dashed arrows are taps: the watcher sees each event and command, and the buffer's normal consumer still gets it without waiting. Safety never sends an order and never holds a reference to the Gateway or the runtime book.

## Features

Each feature says which component needs it and why, what the upstream can give, and how Safety closes the gap. Every number is a parameter with its supported values; each default is a named value in the `alchemist/constants` package, and the config reads its default from there. Click a box in the map to jump to its feature.

### Algorithm map

```
  ( inbound buffer tap )   ( outbound buffer tap )
        │ feed, heartbeat,        │ subscribe, order
        │ orders, replies         │ commands
        ▼                         ▼
  ┌──────────────────────────────────────┐
  │ Watch state (f10, f12)               │
  │ feed times, heartbeat time, orders   │
  └──────────────────────────────────────┘
        │ every watcher check interval (system time)
        ├──▶ Heartbeat gap (f1) ── reconnect request, alert ──▶ ( outbound buffer )
        ├──▶ Feed gap, session open (f2)
        │          │ stale
        │          ▼
        │    Request ladder (f3): resubscribe → reconnect → restart ──▶ ( outbound buffer )
        ├──▶ Stuck-order timer (f11) ──┐
        ▼                              ▼
  connection_restored ──▶ Repair by order id (f6) ── executions?, open orders? ──▶ ( outbound buffer )
                               │ replies (same correlation id)
                               ├──▶ Filled update + strategy key ──▶ ( inbound buffer )
                               ▼ no match, old enough
                         Declare dead orders (f7) ── Rejected update ──▶ ( inbound buffer )

  ( Event Loop ) ── check-ins ──▶ Detect frozen loop (f8) ── stack dump ──▶ ( log )
  Poll positions (f9, off by default) ── poll_positions ──▶ ( outbound buffer )
```

Main path in colour: watch state → gap checks → repair. Every action is a request on the outbound buffer or a repair event on the inbound buffer. Features 4 and 5 moved to the Gateway; feature 10 now describes where the watch state comes from.

### Feature cards

### 1. A dead socket is caught within one interval *(changed 2026-11-11 · decision 1 · System map decision 65)*

#### Need

The engine must learn fast that the broker socket is dead, because every order and fill goes through that socket. The Gateway's own heartbeat loop can also stop, so Safety must not trust the Gateway alone.

#### Upstream gives

The Gateway runs its own heartbeat every heartbeat interval and emits a `heartbeat` event on success, or a `connection_lost` event ([Gateway feature 11](GATEWAY_MAP.md)). The watcher records the system time at which each one arrives.

#### Algorithm

1.  On `connection_lost`: log an alert. The Gateway is already reconnecting ([Gateway feature 1](GATEWAY_MAP.md)).
2.  Every watcher check interval: when the system-time gap since the last `heartbeat` event is over the heartbeat interval plus the heartbeat timeout, send a `reconnect` request and an alert. Send the request again only after the next gap of the same length.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Heartbeat interval | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any positive seconds below the stale threshold | 5 s |
| Heartbeat timeout | Configurable per process | any positive seconds below the heartbeat interval | 3 s |
| Watcher check interval | Configurable per process | any positive seconds below the stale threshold | 1 s |

#### Invariant

For every interval, timeout and check interval: a dead socket, or a stopped Gateway heartbeat loop, is noticed within the heartbeat interval plus the timeout plus one check interval, in system time.

### 2. A stale feed is caught only while a session is open *(changed 2026-11-11 · decision 1 · System map decision 63)*

#### Need

A live socket can still deliver no prices (a data farm down). The engine must learn that the feed is stale, without false alarms during breaks and weekends. A dead feed sends no next event, so the check must run on its own schedule.

#### Upstream gives

Feed events (`tick_update`, `bar_update`) on the inbound buffer, each with its event time; the subscribe and unsubscribe commands on the outbound buffer (feature 10); each exchange's continuous data session from its exchange profile.

#### Algorithm

1.  Per exchange with a subscribed instrument, keep the system time at which the last feed event arrived and that event's event time.
2.  The exchange is armed while the event time of its last feed event lies inside the exchange's data session (for CME, Sunday 18:00 to Friday 17:00 New York less the daily break and early closes; for Eurex, its own hours on the Frankfurt clock).
3.  Every watcher check interval: for each armed exchange, when the system-time gap since the last feed event is over the stale threshold, start the request ladder (feature 3).

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Stale threshold | Configurable per process ([System map decision 58](SYSTEM_MAP.md)), with an optional override per exchange | any positive seconds above the heartbeat interval | 45 s |
| Watcher check interval | Configurable per process | any positive seconds below the stale threshold | 1 s |

#### Invariant

The rule is the same in every mode; only the system-time gap matters. A day-session-only book is still watched overnight, and no alarm fires while every subscribed exchange is in its break or weekend. In replay, a stale alarm means that the engine stalled.

### 3. Recovery climbs from cheap to expensive *(changed 2026-11-11 · decision 3)*

#### Need

A stale feed often recovers with a resubscribe. A socket reconnect costs more, and a broker process restart costs most and is allowed only at night.

#### Upstream gives

The Gateway resubscribes, reconnects and restarts the broker process on request ([Gateway feature 15](GATEWAY_MAP.md)). The broker's "connectivity restored" signals (at IBKR, 1101 and 1102) reach Safety as `connection_restored` events and can repeat without any feed event arriving.

#### Algorithm

1.  Rung 1: send `resubscribe(instruments)` for the stale exchange's instruments, with exponential backoff, up to the resubscribe attempt limit. Cap consecutive resubscribes without a feed event, then fall through.
2.  Rung 2: send `reconnect`.
3.  Rung 3: send `restart_broker_process`, only inside a configured maintenance window.
4.  A feed event for the exchange ends the ladder.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Resubscribe attempts | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any count ≥ 1 | 4 |
| Resubscribes without a feed event before rung 2 | Configurable per process | any count ≥ 1 | 3 |
| Maintenance windows | Configurable (safety config) | list of start–end times | none |

#### Invariant

Cheaper rungs always come before an external process action. A restart request is sent only inside a configured maintenance window.

### 4. Reconnect never gives up *(moved 2026-11-11 · System map decision 65)*

#### Moved

The reconnect loop with backoff belongs to the Gateway ([Gateway feature 1](GATEWAY_MAP.md)). Safety only requests a reconnect (feature 1, feature 3).

### 5. Every stream comes back after a reconnect *(moved 2026-11-11 · System map decision 65)*

#### Moved

The Gateway knows its own subscriptions and restores every stream after a reconnect ([Gateway feature 1](GATEWAY_MAP.md)). The Gateway then emits `connection_restored`, which starts the repair (feature 6).

### 6. Missed fills are repaired by order id *(changed 2026-11-11 · decision 2 · System map decision 69)*

#### Need

A fill that happened while the engine was disconnected never reached its strategy. The strategy waits on that order, or holds an unbooked position.

#### Upstream gives

Safety's own order view (feature 10): every pending order id with its strategy key and the system time it was sent. The Gateway's reply to an executions request: the broker's execution rows, each with its order id ([Gateway feature 7](GATEWAY_MAP.md)).

#### Algorithm

1.  On `connection_restored`, send an executions request and an open-orders request, each with a correlation id.
2.  When both replies with those ids have arrived, match each pending order id, in every execution phase, against the execution rows. A resting stop that filled while in position is repaired too.
3.  On a match, push a Filled order update with the order's strategy key to the inbound buffer. The Event Loop gives the update to that strategy.
4.  The stuck-order timer (feature 11) runs the same steps for the orders it picks.

#### Parameters

None.

#### Invariant

Attribution is strictly by order id. A fill is never inferred from net position, so the assignment stays correct when several strategies trade one instrument.

### 7. Dead orders become Rejected *(changed 2026-11-11 · decision 4)*

#### Need

An order can vanish at the broker with no status (for example, a reject the broker never reported). The waiting strategy must be freed, but a slow order must not be killed.

#### Upstream gives

The executions and open-orders replies of a repair (feature 6), and each pending order's age in system time from Safety's order view.

#### Algorithm

1.  For a pending order older than the minimum age, check both replies.
2.  When the order is absent from both, push a terminal Rejected order update with the order's strategy key to the inbound buffer.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Minimum order age before Rejected | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any positive seconds | 120 s |

#### Invariant

An order is declared dead only when the order is absent from both the executed and the open sets after the minimum age.

### 8. A frozen loop is detected *(settled · System map decision 70)*

#### Need

When the Event Loop blocks, nothing trades and nothing logs. The operator needs evidence of where the process is stuck.

#### Upstream gives

Check-ins from the Event Loop while the loop is healthy.

#### Algorithm

1.  A separate thread counts missed check-ins in system time; a healthy check-in resets the counter.
2.  When the count reaches the freeze threshold, dump every thread's stack once.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Freeze threshold | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any count of missed check-ins ≥ 1, at a check-in interval of any positive seconds | 10 missed check-ins at 1 s (10 s) |

#### Invariant

A healthy loop never triggers a dump.

### 9. Positions poll, off by default *(changed 2026-11-11)*

#### Need

An operator can want broker positions in the logs for monitoring. Strategy positions must not change from this poll.

#### Upstream gives

The Gateway answers a `poll_positions` request with a `positions` event that carries the same correlation id ([Gateway feature 9](GATEWAY_MAP.md)), or with an error reply.

#### Algorithm

1.  When enabled, send `poll_positions` at most once per poll interval, in system time.
2.  The `positions` reply goes through the inbound buffer like any event; the Event Loop logs it.
3.  Log and throttle an error reply.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Poll interval | Configurable (safety config) | off, or any positive seconds | off |

#### Invariant

When disabled, Safety never sends `poll_positions`. The poll never changes a strategy's position.

### 10. The watch state comes from the buffers *(changed 2026-11-11 · was "Members come from the runtime book" · System map decision 69)*

#### Need

Safety must know which feeds to watch and which orders are pending, also for a strategy added while the process runs. Safety must not read the runtime book or ask Strategy.

#### Upstream gives

The outbound buffer carries every subscribe, unsubscribe, order and cancel command; each order command carries its order id and strategy key. The inbound buffer carries every order update.

#### Algorithm

1.  Subscribe and unsubscribe commands add and remove an instrument from the watched set (feature 2).
2.  An order command adds a pending order: order id, strategy key, system time sent.
3.  An order update with a terminal status removes the order. Any order update marks the order as acknowledged.

#### Parameters

None.

#### Invariant

Safety holds no reference to the runtime book, to Strategy or to the Gateway. A strategy added while the process runs is watched and repaired like a strategy present at start, because its commands pass through the same buffer.

### 11. Stuck orders are found on a timer *(changed 2026-11-11 · System map decision 50)*

#### Need

An order update can be lost without any outage. A strategy then waits forever on an order the strategy thinks is in flight.

#### Upstream gives

Safety's order view (feature 10): pending orders that were sent but never acknowledged.

#### Algorithm

1.  Every check interval, in system time, on the watcher thread: pick the pending orders that are not acknowledged.
2.  When at least one order is picked, run the repair (feature 6, feature 7) for the picked orders only.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Check interval | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any positive seconds | 60 s |

#### Invariant

The check never blocks or slows bar processing. When no order is picked, Safety sends no request.

### 12. The watcher never blocks the consumers *(new 2026-11-11 · System map decisions 61, 63)*

#### Need

Safety must see every event and command in time, also while the Event Loop is busy or blocked, without slowing the Event Loop or the Gateway.

#### Upstream gives

Each buffer gives every pushed item to a tap as well as to its normal consumer ([Event Loop feature 14](EVENT_LOOP_MAP.md)).

#### Algorithm

1.  The watcher runs on its own thread and drains its tap.
2.  For each item, the watcher updates its watch state (feature 10) and records the arrival system time.
3.  Every watcher check interval, the watcher runs the gap checks (feature 1, feature 2) and the timers (feature 9, feature 11).
4.  The watcher pushes its requests to the outbound buffer and its repairs to the inbound buffer; both pushes are thread-safe.

#### Parameters

Watcher check interval (feature 1).

#### Invariant

A push into a tap never waits. The watcher keeps running while the Event Loop is blocked.

## Tests

The goal and features as measurable cases. A test with parameter cases runs once per supported value. Every row has a test under `tests/safety/` (streaming core built 2026-11-11).

**End to end · the samples (2 tests)**

|  |  |
|----|----|
| Golden sample | Safety runs in replay like in live ([System map decision 46](SYSTEM_MAP.md)). In the [golden sample](SYSTEM_MAP.md), Safety sends no reconnect, resubscribe or restart request, pushes no repair event and logs no alert. |
| Test | `test_outage_fill_repaired_by_order_id` (component-level outage) |
| Input | Order command 42 for strategy A passes the outbound tap; a `connection_lost` event arrives; 10 s later a `connection_restored` event arrives; the executions reply holds order 42 filled at 5000.0. |
| Parameter cases | heartbeat interval ∈ {5 s} |
| Passes when | An alert is logged on `connection_lost`; after `connection_restored`, Safety sends one executions request and one open-orders request with correlation ids; on both replies, Safety pushes one Filled update for order 42 with strategy key A and average price 5000.0. |
| Fails when | No requests after `connection_restored`; no Filled update; the fill is attributed to another strategy. |

### End-to-end scenarios *(changed 2026-11-11)*

Each scenario drives Safety only through its taps and check-ins, from a known start state, and checks the commands and events Safety pushes. File: `tests/safety/test_safety_scenarios.py`. A recording outbound buffer and a recording inbound buffer capture Safety's output; a controlled system-time source steps time.

**S1. A stopped heartbeat requests a reconnect (features 1, 12)**

|  |  |
|----|----|
| Test | `test_scenario_heartbeat_gap` |
| Given | A `heartbeat` event every 5 s until t0, then none. |
| When | Step system time by 1 s steps to t0 + 20 s. |
| Parameter cases | heartbeat interval ∈ {5 s}; timeout ∈ {3 s}; check interval ∈ {1 s} |
| Passes when | The first `reconnect` request and alert are pushed by t0 + 9 s; the next one only after another 8 s gap. |
| Fails when | No request by t0 + 9 s; a request on every check. |

**S2. Missed fill during an outage (golden outage sample) (features 6, 10)**

|  |  |
|----|----|
| Test | `test_outage_fill_repaired_by_order_id` |
| Given | See the outage sample above; add strategy B in position whose resting stop (order 43) filled during the outage. |
| When | As the outage sample. |
| Parameter cases | — |
| Passes when | As the outage sample; order 43 is also repaired with one Filled update with strategy key B. |
| Fails when | As the outage sample; order 43 not repaired. |

**S3. A stale feed climbs the request ladder (features 2, 3)**

|  |  |
|----|----|
| Test | `test_scenario_stale_feed_ladder` |
| Given | MES subscribed; the last MES feed event has an event time inside the CME data session and arrived 46 s ago in system time. |
| When | Case a: a feed event arrives after the first resubscribe. Case b: no feed event after any resubscribe. Case c: as b, inside a maintenance window. |
| Parameter cases | stale threshold ∈ {45 s}; resubscribe attempts ∈ {4}; resubscribes without a feed event ∈ {3} |
| Passes when | a: one `resubscribe` request, then nothing. b: 3 `resubscribe` requests, then one `reconnect`; no restart. c: after the reconnect, one `restart_broker_process`. |
| Fails when | A reconnect before the resubscribes; a restart outside a maintenance window. |

**S4. A closed session is never stale (features 2)**

|  |  |
|----|----|
| Test | `test_scenario_closed_session_quiet` |
| Given | The last feed event's event time is Friday 17:00 New York (CME and Eurex closed). Second case: CME armed, the last FDAX event time outside the Eurex data session. |
| When | Step system time 10 minutes with no feed event. |
| Parameter cases | — |
| Passes when | No request for the closed exchange in either case. |
| Fails when | Any request for a closed exchange. |

**S5. Watch state follows the buffers (features 10, 12)**

|  |  |
|----|----|
| Test | `test_scenario_watch_state_from_buffers` |
| Given | Subscribe commands for MES and FDAX; an order command 70 for strategy C sent after start (a hot-added strategy); then an unsubscribe for FDAX. |
| When | No feed event for 60 s on either instrument; then a `connection_restored` with order 70 in the executions reply. |
| Parameter cases | — |
| Passes when | A ladder starts for CME only; order 70 is repaired with strategy key C. |
| Fails when | A ladder for Eurex after the unsubscribe; order 70 not repaired. |

**S6. Dead orders become Rejected (features 7)**

|  |  |
|----|----|
| Test | `test_scenario_dead_orders` |
| Given | Pending order 50 is 150 s old; order 51 is 100 s old; order 52 is 150 s old and in the open-orders reply. |
| When | Run the repair. |
| Parameter cases | minimum age ∈ {120 s} |
| Passes when | One Rejected update for order 50; nothing for 51 or 52. |
| Fails when | A Rejected for 51 or 52; no Rejected for 50. |

**S7. Stuck orders are found on the timer (features 6, 11)**

|  |  |
|----|----|
| Test | `test_scenario_stuck_order_timer` |
| Given | No outage. Order 60 was sent and never acknowledged; the broker has its fill. Order 61 is acknowledged. |
| When | Step system time 61 s; then a second case where every order is acknowledged. |
| Parameter cases | check interval ∈ {60 s} |
| Passes when | Within 60 s one Filled update for order 60; order 61 is not checked. Second case: no request. |
| Fails when | No repair within 60 s; a request when every order is acknowledged. |

**S8. A frozen loop is dumped once (features 8, 12)**

|  |  |
|----|----|
| Test | `test_scenario_frozen_loop` |
| Given | Check-ins every 1 s. |
| When | Stop check-ins for 12 s; resume; stop again for 10 s. |
| Parameter cases | freeze threshold ∈ {10 missed at 1 s} |
| Passes when | One stack dump in the first freeze, none while healthy, and one in the second freeze; the watcher keeps running its gap checks during both freezes. |
| Fails when | Two dumps in one freeze; no dump; a dump while healthy; the watcher stops during a freeze. |

**S9. Positions poll only when enabled (features 9)**

|  |  |
|----|----|
| Test | `test_scenario_positions_poll` |
| Given | The Gateway fake answers `poll_positions` with +1 MES, or with an error reply. |
| When | Run 90 s of system time. |
| Parameter cases | poll interval ∈ {off, 30 s} × reply ∈ {positions, error} |
| Passes when | off: no request. 30 s: at most 3 requests. Error: logged and throttled, no crash. |
| Fails when | A request when off; more than one request per interval; a crash. |

**1. A dead socket is caught within one interval (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_heartbeat_gap_requests_reconnect` | interval ∈ {5 s} × timeout ∈ {3 s} × check ∈ {1 s} | system time from the last heartbeat to the reconnect request | ≤ interval + timeout + check |
| `test_connection_lost_alerts` | — | alerts on `connection_lost` | 1 |

**2. A stale feed is caught only while a session is open (3 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_stale_feed_starts_ladder` | threshold ∈ {45 s} | system time from the last feed event to the first resubscribe request, exchange armed | ≤ threshold + one check interval |
| `test_no_stale_alarm_when_every_exchange_in_break` | exchanges ∈ {CME, CME + Eurex} | requests during breaks and weekend | 0 |
| `test_day_only_book_still_watched_overnight` | — | ladder starts for a stale overnight feed | 1 |

**3. Recovery climbs from cheap to expensive (4 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_ladder_order` | attempts ∈ {4} | requests sent, in order | resubscribes, then reconnect |
| `test_resubscribe_without_feed_event_capped` | cap ∈ {3} | resubscribe requests before the reconnect request when no feed event arrives | 3 |
| `test_restart_only_in_maintenance_window` | time ∈ {inside window, outside window} | `restart_broker_process` requests | 1; 0 |
| `test_connection_restored_without_feed_capped_then_reconnect` | — | `rig.kinds()` | `= ['resubscribe'] * cap + ['reconnect']` |

**6. Missed fills are repaired by order id (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_resting_stop_filled_while_in_position_repaired` | — | Filled updates pushed | 1, for the stop's order id |
| `test_two_strategies_same_instrument_attributed_by_id` | — | fills assigned to the wrong strategy key | 0 |

**7. Dead orders become Rejected (1 test · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_dead_order_rejected_only_after_grace_and_absent_both` | age ∈ {\< 120 s, ≥ 120 s} × found in ∈ {neither, open orders} | Rejected updates | only for ≥ 120 s and neither: 1; else 0 |

**8. A frozen loop is detected (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_freeze_dump_only_when_check_ins_missed` | loop ∈ {healthy, blocked past threshold (10 missed check-ins at 1 s)} | stack dumps | 0; 1 |

**9. Positions poll, off by default (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_positions_poll_respects_config` | poll ∈ {off, 60 s} | `poll_positions` requests in 10 minutes | 0; ≤ 10 |

**10. The watch state comes from the buffers (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_order_from_hot_added_strategy_is_repaired` | — | Filled updates for the late strategy's missed fill | 1 |
| `test_safety_holds_no_book_or_gateway_reference` | — | constructor arguments that are the Gateway, the runtime book or Strategy | 0 |

**11. Stuck orders are found on a timer (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_stuck_check_runs_every_interval` | interval ∈ {60 s} | checks in 10 minutes of system time | 10 |
| `test_stuck_check_does_not_block_bar_dispatch` | — | bar dispatch latency while a check waits 5 s for its replies | no bar waits for the check |

**12. The watcher never blocks the consumers (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_tap_push_never_waits` | watcher ∈ {running, blocked} | push time into a buffer with a tap | same as without a tap, within 10 % |
| `test_watcher_runs_while_loop_blocked` | — | gap checks while the Event Loop is blocked for 10 s | ≥ 9 |

## Open questions

None open. Decided on 2026-11-09 and 2026-11-11:

1.  **Watch numbers (features 1, 2, 12).** Heartbeat interval 5 s, heartbeat timeout 3 s, stale threshold 45 s (overridable per exchange), watcher check interval 1 s; defaults in the constants package, configurable per process ([System map decision 63](SYSTEM_MAP.md)).
2.  **Ladder numbers (feature 3).** 4 resubscribe attempts and a cap of 3 resubscribes without a feed event; configurable per process.
3.  **Dead-order age (feature 7).** 120 s; configurable per process.
4.  **Freeze threshold (feature 8).** The Event Loop checks in every 1 s; 10 missed check-ins (10 s) trigger the stack dump; both configurable per process.
5.  **Stuck-order check interval (feature 11).** 60 s, in the safety config.

## Reference

Implementation detail and history. Feature numbers equal the old behaviour numbers.

### Step → code

The watcher and the watch state are in `safety/monitor.py`; the repair is in `safety/fsm_repair.py`; the freeze detector is in `safety/freeze.py`; the defaults are in `constants/safety.py`.

| Step | Code |
|----|----|
| Watch state | `WatchState.observe_event()`, `observe_command()`; `SafetyMonitor.drain()` reads `inbound_tap` and `outbound_tap` |
| Heartbeat gap | `SafetyMonitor.check_heartbeat()`, `on_connection_events()`; constants `HEARTBEAT_INTERVAL_S`, `HEARTBEAT_TIMEOUT_S`, `WATCH_CHECK_INTERVAL_S` |
| Feed gap | `SafetyMonitor.armed()`, `check_feeds()`; constant `STALE_FEED_S` |
| Request ladder | `SafetyMonitor.climb_ladder()`, `on_restored()`, `in_maintenance_window()`; constants `RESUBSCRIBE_MAX_ATTEMPTS`, `RESUBSCRIBE_WITHOUT_FEED_CAP`, `RESUBSCRIBE_BACKOFF_BASE_S` |
| Repair by order id, Declare dead orders | `SafetyMonitor.start_repair()`, `finish_repairs()`; `repair_orders()`; constant `REJECT_SYNTHESIS_MIN_AGE_S` |
| Stuck-order timer | `SafetyMonitor.check_stuck_orders()`; constant `STUCK_CHECK_INTERVAL_S` |
| Poll positions | `SafetyMonitor.check_positions_poll()` |
| Watcher thread | `SafetyMonitor.start()`, `run()`, `step()`, `stop()` |
| Detect frozen loop | `FreezeDetector.check_in()`, `tick()`, `start()`, `stop()`; the Event Loop calls `SafetyMonitor.check_in()` |

### Skeleton code

| Piece | Role | Features |
|----|----|----|
| `SafetyMonitor` | The watcher thread: drains its tap, keeps the watch state, runs the gap checks and timers, pushes requests and repairs. | 1–3, 9–12 |
| `WatchState` | Last feed time per exchange, last heartbeat time, pending orders. | 10 |
| `repair_orders()` | Attribute broker executions to pending orders by id; build Filled or Rejected updates with the strategy key. | 6, 7 |
| `FreezeDetector` | Loop check-ins and the stack dump. | 8 |

**Full skeleton code (python)**

``` python
"""Skeleton contract for Safety (Alchemist v3)."""

from __future__ import annotations

from alchemist.constants.safety import (
    HEARTBEAT_INTERVAL_S, HEARTBEAT_TIMEOUT_S, STALE_FEED_S, WATCH_CHECK_INTERVAL_S,
    RESUBSCRIBE_MAX_ATTEMPTS, RESUBSCRIBE_WITHOUT_FEED_CAP, REJECT_SYNTHESIS_MIN_AGE_S,
)

class WatchState:
    def observe_event(self, event, arrived_at: float) -> None:
        """Feature 10: feed times per exchange, heartbeat time, order acks."""
    def observe_command(self, command, sent_at: float) -> None:
        """Feature 10: subscriptions and pending orders."""

class SafetyMonitor:
    def __init__(self, inbound_tap, outbound_tap, inbound, outbound,
                 exchange_profiles, config, system_time) -> None: ...

    def run(self) -> None:
        """Feature 12: watcher thread; drain taps, check every WATCH_CHECK_INTERVAL_S."""
    def check(self, now: float) -> None:
        """Features 1-3, 9, 11: gap checks, ladder, timers."""
    def on_replies(self, executions, open_orders, picked) -> None:
        """Features 6-7."""

def repair_orders(pending, executions, open_orders, now: float) -> list:
    """Features 6-7: by order id only; returns Filled / Rejected updates with strategy keys."""

class FreezeDetector:
    def check_in(self) -> None: ...
```

### Decisions already made

### 1. Why two layers of watching?

**Answer:** Layer 0 watches socket responsiveness (the heartbeat gap); Layer 1 watches feed freshness (the feed gap) — because the socket can stay alive while the feed is dead.

*changed 2026-11-11 · the Gateway runs the heartbeat; Safety watches the gap*

### 2. How are repaired fills attributed?

**Answer:** Strictly by order id, never by net position, so attribution stays correct even when several strategies trade the same instrument.

### 3. How is a resubscribe livelock avoided?

**Answer:** The broker's "connectivity restored" signals (at IBKR, 1101/1102) are capped at 3 consecutive resubscribes without a feed event, then Safety requests a socket reconnect.

### 4. When is a missing order safely declared dead?

**Answer:** Only when it is older than 120 seconds and confirmed absent from both the execution history and the open-order snapshot — preventing races with orders still in flight.

### 5. How does Safety see the system?

**Answer:** Through taps on the inbound and the outbound buffer, on its own thread. Safety holds no reference to the Gateway, the runtime book or Strategy ([System map decisions 61](SYSTEM_MAP.md), [69](SYSTEM_MAP.md)).

**Rejected:** Rejected: reading the runtime book and calling Gateway methods (each mode needed its own wiring, and replay needed a do-nothing stand-in).

*agreed 2026-11-11*
