# SAFETY_MAP — Safety · Alchemist v3

The watchdog. Safety keeps checking that the broker connection and the price feed are alive, climbs a ladder of fixes when a check fails, and afterwards repairs every strategy left waiting on an order whose outcome the engine missed.

Frozen contract for the Safety component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `safety_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-09.** **v3 final, 2026-11-09 — agreed.** The End-to-end scenarios in Tests were agreed on 2026-11-09. All features and decisions are agreed. Multi-asset: the feed watchdog arms on every configured exchange's data session. Change history: see `safety_map.html`.

## Goal and scope

**What it is.** Safety is the watchdog of the process. Safety runs its own heartbeat loop beside the Event Loop: Safety checks that the broker socket answers and that the price feed is fresh, climbs a ladder of fixes when a check fails, and after a reconnect repairs every strategy left waiting on an order whose outcome the engine missed.

**Why it exists.** A broker connection drops, a data farm goes quiet, and a fill can happen while the engine is disconnected. Without repair, a strategy waits forever on that order. This work has its own failure modes and its own recovery, independent of trading, so Safety is a separate component and not part of the Gateway ([System map decision 4](SYSTEM_MAP.md), [decision 8](SYSTEM_MAP.md)).

**Goal.** Keep watching the broker socket and the market-data feed; run escalating recovery steps and reconnects that never stop when a check fails; and repair strategies stranded during a disconnection by attributing the fills they missed, from the broker's executions, by order id.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Other components call Safety | Runner | Start and stop the heartbeat loop and the freeze detector | safety config: reconnect base and cap, maintenance windows, positions poll |
|  | Event Loop | Check in from the loop | check-in |
| Safety calls other components | Gateway | Probe the socket; read the last tick time; resubscribe; reconnect; fetch executions, open orders and positions; take the resubscribe request | timeout, since-timestamp |
|  | Strategy | Ask each strategy for its submitted, open and cancelling orders | pending order ids |
|  | Event Loop | Push repaired fills, Rejected updates and position updates into the event buffer; read the runtime book's members | order update, position update |
|  | Operating system | Restart the broker gateway process, only inside a maintenance window; write stack dumps | process restart, stack dump |

### Scope

Owns, from the System map:

> Detecting a lost broker connection or a frozen process, reconnecting, and un-sticking any strategy left waiting on an order whose fill happened during the outage.

| Not Safety's job | Owned by |
|----|----|
| Talking to the broker directly | [Gateway](GATEWAY_MAP.md) |
| Deciding what to trade | [Strategy](STRATEGY_MAP.md), [StrategyExecution](STRATEGY_EXECUTION_MAP.md) |
| Normal event delivery | [Event Loop](EVENT_LOOP_MAP.md) |

### Place in the system

Drawn in the HTML page; the flows are the Public interface table above and the [SYSTEM_MAP.md](SYSTEM_MAP.md) Diagram.

Safety never sends an order. Safety changes strategy state only through order updates in the event buffer, which the Event Loop delivers like any other update.

## Features

Each feature says which component needs it and why, what the upstream can give, and how Safety closes the gap. Every number is a parameter with its supported values.

### Algorithm map

```
  ( Gateway )
        │
        │ heartbeat answer: alive?
        ▼
  ┌─────────────────────────┐
  │ Probe socket            │
  │ rule: every heartbeat   │
  └─────────────────────────┘
        │
        ├── heartbeat (timeout) ──▶ ( Gateway )
        │
        │ socket dead
        ▼
  ┌─────────────────────────┐
  │ Climb recovery ladder   │
  │ rule: 3 rungs, in order │
  └─────────────────────────┘
        │
        ├── resubscribe ──▶ ( Gateway )
        │
        │ rung 2
        ▼
  ┌─────────────────────────┐
  │ Reconnect forever       │
  │ rule: capped backoff    │
  └─────────────────────────┘
        │
        ├── connect ──▶ ( Gateway )
        │
        │ connected
        ▼
  ┌─────────────────────────┐
  │ Restore subscriptions   │
  │ rule: each feed once    │
  └─────────────────────────┘
        │
        ├── reads ──▶ [ runtime book ]
        ├── resubscribe ──▶ ( Gateway )
        │
        │ streams back
        ▼
  ┌─────────────────────────┐
  │ Repair by order id      │
  │ rule: order id only     │
  └─────────────────────────┘
        │
        ├── pending ids? ──▶ ( Strategy )
        │   ◀── pending ids
        ├── executions, open orders? ──▶ ( Gateway )
        │   ◀── rows
        ├── Filled update ──▶ ( Event Loop )
        │
        │ no match
        ▼
  ┌─────────────────────────┐
  │ Declare dead orders     │
  │ rule: gone, old enough  │
  └─────────────────────────┘
        │
        │ Rejected update
        ▼
  ( Event Loop )

  ( Gateway )
        │
        │ last tick time
        ▼
  ┌─────────────────────────┐
  │ Check feed freshness    │
  │ rule: only in session   │
  └─────────────────────────┘
        │ stale
        └──▶ Climb recovery ladder

  ( Event Loop )
        │
        │ check-ins
        ▼
  ┌─────────────────────────┐
  │ Detect frozen loop      │
  │ rule: missed check-ins  │
  └─────────────────────────┘
        │
        │ stack dump
        ▼
  ( log )

  ( Gateway )
        │
        │ positions
        ▼
  ┌─────────────────────────┐
  │ Poll positions          │
  │ rule: off by default    │
  └─────────────────────────┘
        │
        │ position updates
        ▼
  ( Event Loop )
```

Main path in colour: probe and feed check on the heartbeat loop → reconnect → restore → repair. A dead socket skips the ladder and reconnects at once. The dashed box is the Event Loop's runtime book, read by restore, repair, the feed check and the poll.

### Feature cards

### 1. A dead socket is caught within one interval *(was behaviour 1 · decision 1)*

#### Need

The engine must learn fast that the broker socket is dead, because every order and fill goes through that socket.

#### Upstream gives

The Gateway's heartbeat returns True only when the broker answers within the timeout ([Gateway feature 11](GATEWAY_MAP.md)).

#### Algorithm

1.  Every heartbeat interval, call the Gateway heartbeat.
2.  On False, count the failure and start the reconnect loop at once (feature 4).

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Heartbeat interval | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 5 s | 5 s |
| Heartbeat timeout | Configurable per call ([Gateway feature 11](GATEWAY_MAP.md)) | any positive seconds | 3.0 s |

#### Invariant

For every interval and timeout: a socket failure is caught within one interval plus the timeout. The failure counter only goes up until the connection is restored.

### 2. A stale feed is caught only while a session is open *(was behaviour 2 · decision 1)*

#### Need

A live socket can still deliver no prices (a data farm down). The engine must learn that the feed is stale, without false alarms during breaks and weekends.

#### Upstream gives

The Gateway's last tick time, which counts only instruments whose exchange data session is open ([Gateway feature 2](GATEWAY_MAP.md)), and each configured exchange's continuous data session from its exchange profile.

#### Algorithm

1.  Check staleness only while the continuous data session of at least one configured exchange is open: for CME, Sunday 18:00 to Friday 17:00 New York less the daily break and early closes; for Eurex, its own hours on the Frankfurt clock.
2.  When now minus the last tick time is over the stale threshold, start the recovery ladder (feature 3).

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Stale-tick threshold | Configurable per process ([System map decision 58](SYSTEM_MAP.md)), with an optional override per exchange | any positive seconds | 45 s |

#### Invariant

A day-session-only book is still watched overnight, and no alarm fires while every configured exchange is in its break or weekend.

### 3. Recovery climbs from cheap to expensive *(was behaviour 3 · decision 3)*

#### Need

A stale feed often recovers with a resubscribe. A socket reconnect costs more, and a broker process restart costs most and is allowed only at night.

#### Upstream gives

The Gateway resubscribes and reconnects on request; the broker sends "connectivity restored" signals (at IBKR, 1101 and 1102) that can repeat without any tick arriving.

#### Algorithm

1.  Rung 1: resubscribe with exponential backoff, up to the resubscribe attempt limit. Cap consecutive resubscribes without a tick, then fall through.
2.  Rung 2: full socket reconnect (feature 4).
3.  Rung 3: restart the broker gateway process, only inside a configured maintenance window.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Resubscribe attempts | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 4 | 4 |
| Resubscribes without a tick before rung 2 | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 3 | 3 |
| Maintenance windows | Configurable (safety config) | list of start–end times | none |

#### Invariant

Cheaper in-process rungs always come before an external process action. A process restart happens only inside a configured maintenance window.

### 4. Reconnect never gives up *(was behaviour 4)*

#### Need

The engine must come back after any outage length without a human restart, and must not flood the broker with connects.

#### Upstream gives

The Gateway's reconnect, which can fail many times in a row ([Gateway feature 1](GATEWAY_MAP.md)).

#### Algorithm

1.  Loop: reconnect; on failure wait delay(n) = min(cap, base · 2^(n−1)) and try again.
2.  Log CRITICAL on persistent failure; never abort.

#### Parameters

| Parameter    | Configurable?                | Supported            | Default |
|--------------|------------------------------|----------------------|---------|
| Backoff base | Configurable (safety config) | any positive seconds | 15 s    |
| Backoff cap  | Configurable (safety config) | any positive seconds | 120 s   |

#### Invariant

For every base and cap: the loop ends only when the connection is back, and no wait is longer than the cap.

### 5. Every stream comes back after a reconnect *(was behaviour 5)*

#### Need

After a reconnect the engine needs ticks for every instrument, order updates and account updates again, before monitoring resumes.

#### Upstream gives

The Gateway's subscribe operations, and the runtime book's fed instruments (feature 10).

#### Algorithm

1.  Call the reconnect callbacks: resubscribe ticks once per fed instrument, order updates and account updates.
2.  Resume normal monitoring after the streams are confirmed.

#### Parameters

None.

#### Invariant

Every data and order stream the engine needs is restored before normal monitoring resumes.

### 6. Missed fills are repaired by order id *(was behaviour 6 · decision 2)*

#### Need

A fill that happened while the engine was disconnected never reached its strategy. The strategy waits on that order, or holds an unbooked position.

#### Upstream gives

Strategy reports each slot's submitted, open and cancelling orders (Order Manager's answer). The Gateway returns the broker's execution rows, each with its order id ([Gateway feature 7](GATEWAY_MAP.md)).

#### Algorithm

1.  After a reconnect, collect pending order ids from every strategy in the runtime book, in every execution phase, so a resting stop that filled while in position is repaired too.
2.  Match each pending order id against the execution rows; on a match, create a Filled order update and append the update to the event buffer.
3.  Repair each order on its own. The periodic stuck-order check (feature 11) calls the same repair with only the strategies processing an order.

#### Parameters

None.

#### Invariant

Attribution is strictly by order id. A fill is never inferred from net position, so the assignment stays correct when several strategies trade one instrument.

### 7. Dead orders become Rejected *(was behaviour 7 · decision 4)*

#### Need

An order can vanish at the broker with no status (for example, a reject the broker never reported). The waiting strategy must be freed, but a slow order must not be killed.

#### Upstream gives

The broker's executions and open orders, and each pending order's age.

#### Algorithm

1.  For a pending order older than the minimum age, check both the executions and the open orders.
2.  When the order is absent from both, append a terminal Rejected order update to the event buffer.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Minimum order age before Rejected | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 120 s | 120 s |

#### Invariant

An order is declared dead only when the order is absent from both the executed and the open sets after the minimum age.

### 8. A frozen loop is detected *(was behaviour 8)*

#### Need

When the Event Loop blocks, nothing trades and nothing logs. The operator needs evidence of where the process is stuck.

#### Upstream gives

Check-ins from the Event Loop while the loop is healthy.

#### Algorithm

1.  Count missed check-ins; a healthy check-in resets the counter.
2.  When the count reaches the freeze threshold, dump every thread's stack once.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Freeze threshold | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any count of missed check-ins ≥ 1, at a check-in interval of any positive seconds | 10 missed check-ins at 1 s (10 s) |

#### Invariant

A healthy loop never triggers a dump.

### 9. Positions poll, off by default *(was behaviour 9)*

#### Need

An operator can want broker positions in the logs for monitoring. Strategy positions must not change from this poll.

#### Upstream gives

The Gateway's position snapshot ([Gateway feature 9](GATEWAY_MAP.md)), which can fail.

#### Algorithm

1.  When enabled, on the heartbeat loop at most once per poll interval, fetch broker positions and push them as position updates.
2.  Log, swallow and throttle a failing fetch.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Poll interval | Configurable (safety config) | off, or any positive seconds | off |

#### Invariant

When disabled, the heartbeat loop never polls. The poll never changes a strategy's position.

### 10. Members come from the runtime book *(was behaviour 10 · System map decision 34)*

#### Need

A strategy added while the process runs must be repaired like a strategy present at start ([Event Loop feature 11](EVENT_LOOP_MAP.md)).

#### Upstream gives

The Event Loop's runtime book: strategies and fed instruments.

#### Algorithm

1.  Hold the runtime book, not a copy of the strategy list.
2.  Repair, the feed watchdog and the positions poll read the book's strategies when they run.
3.  A reconnect resubscribes the book's fed instruments, once per instrument.

#### Parameters

None.

#### Invariant

Safety keeps no copy of the strategy list.

### 11. Stuck orders are found on a timer *(was Event Loop behaviour 6 · System map decision 50)*

#### Need

An order update can be lost without any outage. A strategy then waits forever on an order the strategy thinks is in flight.

#### Upstream gives

The runtime book's strategies (feature 10), each slot's answer to "processing an order?" through Strategy, and the Gateway's executions and open orders ([Gateway feature 7](GATEWAY_MAP.md)).

#### Algorithm

1.  Every check interval, on the heartbeat loop and off the bar path: pick the slots that report they are processing an order.
2.  When at least one slot is picked, ask the Gateway for executions and open orders.
3.  Run the repair (feature 6, feature 7) for the picked slots only.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Check interval | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 60 s | 60 s |

#### Invariant

The check never blocks or slows bar processing. The Event Loop never calls the repair.

## Tests

The goal and features as measurable cases. A test with parameter cases runs once per supported value. No Safety test exists in v3 yet; every row is not written yet.

**End to end · the samples (1 test)**

|  |  |
|----|----|
| Golden sample | Replay uses the do-nothing stand-in ([System map decision 46](SYSTEM_MAP.md)), so the [golden sample](SYSTEM_MAP.md) has nothing to check for Safety. Safety is proved by the outage sample below and the feature tests. |
| Test | `test_outage_fill_repaired_by_order_id` (component-level outage) |
| Input | The socket drops for 10 s while a strategy waits on order id 42; the broker fills order 42 at 5000.0; the socket reconnects. |
| Parameter cases | reconnect base, cap ∈ {15 s, 120 s} |
| Passes when | The failure is caught within one heartbeat interval plus the timeout; the reconnect loop backs off without crashing; repair matches order 42 and pushes one Filled update with average price 5000.0; the strategy moves to IN_POSITION_ACTIVITY. |
| Fails when | The drop is not noticed within that time; the reconnect loop stops after repeated failures; the strategy stays waiting; or the fill is attributed to another strategy. |

### End-to-end scenarios *(agreed 2026-11-09)*

Each scenario drives the component only through its public interface, from a known start state, and checks one exact, observable output. A scenario tells one story across several features; the feature tables below keep the single rules. File: `tests/safety/test_safety_scenarios.py`. A Gateway fake has a controllable heartbeat, last tick time, executions, open orders and positions; a fake runtime book holds strategy stubs with pending order ids and phases; a recording event buffer stands in for the Event Loop; a controlled clock steps time.

**S1. A dead socket starts the reconnect loop (features 1, 4)**

|  |  |
|----|----|
| Test | `test_scenario_dead_socket` |
| Given | Connected; the heartbeat starts returning False at t0; the next three reconnects fail, the fourth succeeds. |
| When | Step the clock. |
| Parameter cases | heartbeat interval ∈ {5 s}; timeout ∈ {3.0 s}; backoff ∈ {base 15 s, cap 120 s} |
| Passes when | The reconnect loop starts by t0 + 8 s; waits between attempts are 15 s, 30 s and 60 s; CRITICAL is logged on the persistent failure; the loop never aborts; monitoring resumes after the fourth attempt. |
| Fails when | A start after t0 + 8 s; other waits; the loop gives up. |

**S2. Missed fill during an outage (golden outage sample) (features 5, 6)**

|  |  |
|----|----|
| Test | `test_outage_fill_repaired_by_order_id` |
| Given | See the end-to-end outage sample above; add a second strategy in position whose resting stop (order 43) filled during the outage. |
| When | As the outage sample. |
| Parameter cases | reconnect base, cap ∈ {15 s, 120 s} |
| Passes when | As the outage sample; order 43 is also repaired with one Filled update. |
| Fails when | As the outage sample; order 43 not repaired. |

**S3. A stale feed climbs the ladder (features 2, 3)**

|  |  |
|----|----|
| Test | `test_scenario_stale_feed_ladder` |
| Given | CME session open; the last MES tick is 46 s old. |
| When | Case a: ticks resume after the first resubscribe. Case b: no tick after any resubscribe. Case c: as b, inside a maintenance window. |
| Parameter cases | stale threshold ∈ {45 s}; resubscribe attempts ∈ {4}; resubscribes without a tick ∈ {3} |
| Passes when | a: one resubscribe, then normal monitoring. b: 3 resubscribes, then one reconnect; no restart. c: after the reconnect fails, a broker gateway restart. |
| Fails when | A reconnect before the resubscribes; a restart outside a maintenance window. |

**S4. A closed session is never stale (features 2)**

|  |  |
|----|----|
| Test | `test_scenario_closed_session_quiet` |
| Given | Saturday (CME and Eurex closed), last tick from Friday. Second case: CME open, Eurex closed, last FDAX tick old. |
| When | Step the clock 10 minutes. |
| Parameter cases | — |
| Passes when | No resubscribe, reconnect or restart in either case. |
| Fails when | Any recovery action. |

**S5. Every stream comes back once (features 5, 10)**

|  |  |
|----|----|
| Test | `test_scenario_streams_restored` |
| Given | Fed instruments MES (two strategies), ES and FDAX. |
| When | Reconnect. |
| Parameter cases | — |
| Passes when | MES, ES and FDAX are resubscribed once each; order and account updates are resubscribed; monitoring resumes after the streams are confirmed. |
| Fails when | An instrument resubscribed twice or not at all. |

**S6. Dead orders become Rejected (features 7)**

|  |  |
|----|----|
| Test | `test_scenario_dead_orders` |
| Given | Pending order 50 is 150 s old; order 51 is 100 s old; order 52 is 150 s old and in the open orders. |
| When | Run the repair. |
| Parameter cases | minimum age ∈ {120 s} |
| Passes when | One Rejected update for order 50; nothing for 51 or 52. |
| Fails when | A Rejected for 51 or 52; no Rejected for 50. |

**S7. Stuck orders are found on the timer (features 6, 11)**

|  |  |
|----|----|
| Test | `test_scenario_stuck_order_timer` |
| Given | No outage. Slot A is processing order 60, whose fill the broker has but the engine missed; slot B is idle. |
| When | Step the clock 61 s; then a second case where no slot is processing. |
| Parameter cases | check interval ∈ {60 s} |
| Passes when | Within 60 s one Filled update for order 60; only slot A is checked. No-slot case: no executions or open-orders query. |
| Fails when | No repair within 60 s; slot B queried; a query when no slot is processing. |

**S8. A frozen loop is dumped once (features 8)**

|  |  |
|----|----|
| Test | `test_scenario_frozen_loop` |
| Given | Check-ins every 1 s. |
| When | Stop check-ins for 12 s; resume; stop again for 10 s. |
| Parameter cases | freeze threshold ∈ {10 missed at 1 s} |
| Passes when | One stack dump in the first freeze, none while healthy, and one in the second freeze. |
| Fails when | Two dumps in one freeze; no dump; a dump while healthy. |

**S9. Positions poll only when enabled (features 9)**

|  |  |
|----|----|
| Test | `test_scenario_positions_poll` |
| Given | The broker holds +1 MES. |
| When | Run 90 s; in the failing case every fetch raises. |
| Parameter cases | poll interval ∈ {off, 30 s} × case ∈ {answers, fetch fails} |
| Passes when | off: no fetch. 30 s: at most 3 fetches, each pushing a position update of +1 MES. Fetch fails: errors logged and throttled, no crash. |
| Fails when | A fetch when off; more than one fetch per interval; a crash. |

**1. A dead socket is caught within one interval (1 test · 1 case)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_socket_failure_caught_within_interval` | interval ∈ {5 s} × timeout ∈ {3.0 s} | time from drop to reconnect start | ≤ interval + timeout |

**2. A stale feed is caught only while a session is open (3 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_stale_feed_starts_ladder` | threshold ∈ {45 s} | time from last tick to ladder start, session open | ≤ threshold + one interval |
| `test_no_stale_alarm_when_every_exchange_in_break` | exchanges ∈ {CME, CME + Eurex} | alarms during breaks and weekend | 0 |
| `test_day_only_book_still_watched_overnight` | — | alarm for a stale overnight feed | 1 |

**3. Recovery climbs from cheap to expensive (3 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_ladder_order` | attempts ∈ {4} | rungs tried, in order | 4 resubscribes, then reconnect |
| `test_resubscribe_without_tick_capped` | cap ∈ {3} | resubscribes before reconnect when no tick arrives | 3 |
| `test_process_restart_only_in_maintenance_window` | time ∈ {inside window, outside window} | process restarts | 1; 0 |

**4. Reconnect never gives up (2 tests · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_reconnect_never_gives_up` | failures before success ∈ {1, 50} | loop result | connected; no exit before |
| `test_backoff_delays` | base, cap ∈ {15 s, 120 s} | delay(n) for n = 1…8 | min(cap, base · 2^(n−1)), exact |

**5. Every stream comes back after a reconnect (1 test · 1 case)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_restore_subscribes_each_fed_instrument_once` | — | tick subscriptions per instrument; order and account streams | 1 each; both active |

**6. Missed fills are repaired by order id (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_resting_stop_filled_while_in_position_repaired` | — | Filled updates pushed | 1, for the stop's order id |
| `test_two_strategies_same_instrument_attributed_by_id` | — | fills assigned to the wrong strategy | 0 |

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
| `test_positions_poll_respects_config` | poll ∈ {off, 60 s} | polls in 10 minutes; strategy position changes | 0 and 0; ≤ 10 and 0 |

**10. Members come from the runtime book (1 test · 1 case)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_strategy_committed_after_start_is_repaired` | — | Filled updates for the late strategy's missed fill | 1 |

**11. Stuck orders are found on a timer (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_stuck_check_runs_every_interval` | interval ∈ {60 s} | checks in a 10-minute replay-clock run | 10 |
| `test_stuck_check_does_not_block_bar_dispatch` | — | bar dispatch latency while a check takes 5 s | no bar waits for the check |

## Open questions

All decided on 2026-11-09.

1.  **Watch numbers (features 1, 2).** Heartbeat interval 5 s and stale-tick threshold 45 s, in the safety config; the stale threshold may be overridden per exchange.
2.  **Ladder numbers (feature 3).** 4 resubscribe attempts and a cap of 3 resubscribes without a tick; configurable per process.
3.  **Dead-order age (feature 7).** 120 s; configurable per process.
4.  **Freeze threshold (feature 8).** The Event Loop checks in every 1 s; 10 missed check-ins (10 s) trigger the stack dump; both configurable per process.
5.  **Stuck-order check interval (feature 11).** 60 s, in the safety config.

## Reference

Implementation detail and history. Feature numbers equal the old behaviour numbers.

### Step → code

The heartbeat loop and freeze detector are in `safety/monitor.py`; the repair is in `safety/fsm_repair.py`.

| Step | Code |
|----|----|
| Probe socket, Check feed freshness, Climb recovery ladder, Poll positions | `SafetyMonitor.run()`, `feed_expected()`; constants `HEARTBEAT_INTERVAL_S`, `STALE_TICK_S`, `RESUBSCRIBE_MAX_ATTEMPTS`, `RESUBSCRIBE_WITHOUT_TICK_CAP` |
| Reconnect forever | `SafetyMonitor.reconnect_forever()`; `SafetyConfig.reconnect_base_s`, `reconnect_cap_s` |
| Restore subscriptions | `SafetyMonitor.restore_subscriptions()` |
| Repair by order id, Declare dead orders | `repair_stuck_strategies()`; `REJECT_SYNTHESIS_MIN_AGE_S` |
| Detect frozen loop | `FreezeDetector.check_in()`, `start()`, `stop()` |
| Replay | `NullSafety` (does nothing) |

### Skeleton code

| Piece | Role | Old behaviours |
|----|----|----|
| `SafetyMonitor` | The heartbeat loop: socket probe, feed liveness, recovery ladder, reconnect, resubscribe, positions poll. | 1–5, 9, 10 |
| `repair_stuck_strategies()` | Attribute broker executions to pending orders by id; synthesise Filled or Rejected updates. | 6, 7 |
| `FreezeDetector` | Loop check-ins and the stack dump. | 8 |

**Full skeleton code (python)**

``` python
"""Skeleton contract for Safety (Alchemist v3)."""

from __future__ import annotations

HEARTBEAT_INTERVAL_S = 5.0
STALE_TICK_S = 45.0
RESUBSCRIBE_MAX_ATTEMPTS = 4
RESUBSCRIBE_WITHOUT_TICK_CAP = 3        # decision 3
REJECT_SYNTHESIS_MIN_AGE_S = 120.0      # decision 4

class SafetyMonitor:
    def __init__(self, gateway, book, event_buffer, exchange_profiles,
                 maintenance_windows, positions_poll_s: float | None = None): ...

    async def run(self) -> None:
        """Behaviours 1-3, 9: heartbeat loop."""
    async def reconnect_forever(self) -> None:
        """Behaviour 4: min(cap, base * 2**(n-1)), never gives up."""
    async def restore_subscriptions(self) -> None:
        """Behaviour 5."""
    def feed_expected(self, now: float) -> bool:
        """Behaviour 2: any configured exchange's data session open."""

async def repair_stuck_strategies(strategies, gateway, event_buffer, now: float) -> int:
    """Behaviours 6-7: by order id only; returns the number of repaired orders."""

class FreezeDetector:
    def check_in(self) -> None: ...
```

### Decisions already made

### 1. Why two layers of watching?

**Answer:** Layer 0 watches socket responsiveness; Layer 1 watches tick-feed freshness — because the socket can stay alive while the feed is dead.

### 2. How are repaired fills attributed?

**Answer:** Strictly by order id, never by net position, so attribution stays correct even when several strategies trade the same instrument.

### 3. How is a resubscribe livelock avoided?

**Answer:** The broker's "connectivity restored" signals (at IBKR, 1101/1102) are capped at 3 consecutive resubscribes without a tick, then Safety falls through to a socket reconnect.

### 4. When is a missing order safely declared dead?

**Answer:** Only when it is older than 120 seconds and confirmed absent from both the execution history and the open-order snapshot — preventing races with orders still in flight.
