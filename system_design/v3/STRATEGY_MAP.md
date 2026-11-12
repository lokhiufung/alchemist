# STRATEGY_MAP — Strategy · Alchemist v3

The one object per slot that the Event Loop talks to. Strategy drives the slot's three parts (StrategySignal, StrategyExecution and Order Manager) with what the Event Loop delivers, and books the slot's own position from fills.

Frozen contract for the Strategy component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `strategy_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-09.** **v3 final, 2026-11-09 — agreed.** The End-to-end scenarios in Tests were agreed on 2026-11-09. Features and decisions are settled. The calls Strategy makes on StrategySignal use the v3 signal interface. Multi-asset: the position is booked in the execution instrument's currency ([System map decision 39](SYSTEM_MAP.md)). Change history: see `strategy_map.html`.

## Goal and scope

**What it is.** Strategy is the one object per slot that the Event Loop talks to. Strategy drives the slot's three parts (StrategySignal, StrategyExecution and Order Manager) with what the Event Loop delivers, and books the slot's own position from fills. A strategy author registers a strategy class and names its signal by registry name (decision 16).

**Why it exists.** The Event Loop must see one interface per slot, not three parts. The three parts must not reach into each other: the signal must not read the position, and StrategyExecution must not decide when a signal may be rebuilt. Strategy holds the position and the slot's timing rules (when to step the signal, when a rebuild is safe), so each part stays testable alone ([System map decisions 19](SYSTEM_MAP.md), [22](SYSTEM_MAP.md)).

**Goal.** Be the one per-slot object the Event Loop talks to, and drive StrategySignal, StrategyExecution and Order Manager with what the Event Loop delivers, booking the slot's own position from fills.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Other components call Strategy | Event Loop | Deliver a bar; an order update; a position update; a balance update; warmup history; a flatten request | bar, update, history, flatten reason and bar time |
|  | Aux Data | Tell the slot that auxiliary data is ready or not ready, with the frames | ready flag, frames |
|  | Event Loop, Safety | Ask the slot's state: processing an order? quarantined? flat and idle? trade closed? submitted, open and cancelling orders (decision 14) | flags, orders |
|  | Event Loop | Quarantine and release the slot | — |
| Strategy calls other components | StrategySignal | Build cold; step with bars and strategy data (entry price, seconds since entry); seed; snapshot and restore session state; read values | bars, strategy data, history |
|  | StrategyExecution | Pass a bar (with the step-signal callback); pass an order update; set warmup-ready and aux-ready; hand the booked position for a flatten | bar, update, flags, position |
|  | Order Manager | Forward an order update; ask for the slot's orders | update → change or nothing |

### Scope

Owns, from the System map:

> Being the one per-slot object the Event Loop talks to: booking its own position from fills, in the instrument's currency, and driving its StrategySignal, StrategyExecution and Order Manager with what the Event Loop delivers — stepping the signal on each bar, passing the bar on to StrategyExecution, and deciding when the signal may be rebuilt (only while flat with no open or submitted orders).

| Not Strategy's job | Owned by |
|----|----|
| The signal's params; the calculations and entry/exit logic | [StrategySignal](STRATEGY_SIGNAL_MAP.md) |
| The execution phase, session gating, the daily quota, asking the signal for the entry/exit answer, timing the exit-input freeze and release, and what an order event means | [StrategyExecution](STRATEGY_EXECUTION_MAP.md) |
| An order's life from submission to closed status, and which orders the slot owns | [Order Manager](ORDER_MANAGER_MAP.md) |
| When bars are complete | [Event Loop](EVENT_LOOP_MAP.md) |

### Place in the system

Drawn in the HTML page; the flows are the Public interface table above and the [SYSTEM_MAP.md](SYSTEM_MAP.md) Diagram.

StrategyExecution also talks to StrategySignal and Order Manager directly ([System map decisions 15](SYSTEM_MAP.md), [20](SYSTEM_MAP.md)); those links are on the StrategyExecution page.

## Features

Each feature says which component needs it and why, what the upstream can give, and how Strategy closes the gap. Every number is a parameter with its supported values.

### Algorithm map

```
  ( Event Loop )
        │
        │ bar
        ▼
  ┌─────────────────────────┐
  │ Hand bar to execution   │
  │ rule: + step callback   │
  └─────────────────────────┘
        │
        │ bar + step_signal
        ▼
  ( StrategyExecution )
        │
        │ call (step_signal)
        ▼
  ┌─────────────────────────┐
  │ Step signal             │
  │ rule: before decision   │
  └─────────────────────────┘
        │
        │ bars + strategy data
        ▼
  ( StrategySignal )

  ( Event Loop )
        │
        │ order update
        ▼
  ┌─────────────────────────┐
  │ Forward update          │
  │ rule: to Order Manager  │
  └─────────────────────────┘
        │
        │ order update
        ▼
  ( Order Manager )

  ( Event Loop )
        │
        │ position, balance update
        ▼
  ┌─────────────────────────┐
  │ Filter account update   │
  │ rule: own instr, acct   │
  └─────────────────────────┘
        │
        │ own update
        ▼
  ┌─────────────────────────┐
  │ Book position           │
  │ rule: returned changes  │
  └─────────────────────────┘
        │
        ├── writes ──▶ [ position, balance ]
        │
        │ change
        ▼
  ( StrategyExecution )

  ( Event Loop ): warmup     ( Aux Data ): frames
        │
        │ warmup history, history frames
        ▼
  ┌─────────────────────────┐
  │ Rebuild when flat idle  │
  │ rule: no pos, no orders │
  └─────────────────────────┘
        │
        ├── build cold, seed bars ──▶ ( StrategySignal )
        │
        │ warmup-ready, aux-ready
        ▼
  ( StrategyExecution )

  ( Event Loop )
        │
        │ flatten
        ▼
  ┌─────────────────────────┐
  │ Hand flatten            │
  │ rule: + booked position │
  └─────────────────────────┘
        │
        │ flatten + position
        ▼
  ( StrategyExecution )

  ( StrategySignal )
        │
        │ session-fixed values
        ▼
  ┌─────────────────────────┐
  │ Persist session values  │
  │ rule: warm; restore     │
  └─────────────────────────┘
        │
        ├── session-fixed values ──▶ ( state store )
        │   ◀── restored values

  ( StrategySignal )
        │
        │ signal values
        ▼
  ┌─────────────────────────┐
  │ Log snapshot            │
  │ rule: every N bars      │
  └─────────────────────────┘
        │
        │ signal values
        ▼
  ( log )
```

Main path in colour: bar → execution → step signal; update → Order Manager → book position → execution. The dashed box is the slot's position and balance: written by booking and by account updates, read when the signal is stepped (entry price, seconds since entry).

### Feature cards

### 1. The signal is stepped once per bar, before the decision *(was behaviour 1 · decisions 1, 3)*

#### Need

StrategyExecution asks the signal for an entry or exit answer on each bar, so the signal must already include that bar. The signal also needs position data (entry price, seconds since entry) without reading the position itself.

#### Upstream gives

The Event Loop delivers each completed bar once, in time order ([Event Loop feature 2](EVENT_LOOP_MAP.md)). StrategyExecution knows whether the bar is in session.

#### Algorithm

1.  Pass the bar to StrategyExecution together with a step-signal callback.
2.  When StrategyExecution calls the callback, build strategy data from the booked position: entry price = booked average price; seconds since entry = counted from the entry-order bar. Both are None while flat.
3.  Call the signal's step with the bar history and the strategy data.

#### Parameters

None.

#### Invariant

The signal is stepped exactly once per in-session bar, before StrategyExecution decides on that bar. The first in-session bar is the session's bar 0; no step happens while StrategyExecution waits for the session, or while a quarantined slot is out of session.

### 2. The position is booked only from Order Manager's changes *(was behaviour 3 · decisions 7, 8, 9)*

#### Need

The slot's position must count each fill exactly once, in the execution instrument's currency, even though every slot sees every broker update.

#### Upstream gives

The Event Loop delivers every order, position and balance update to every slot ([Event Loop feature 3](EVENT_LOOP_MAP.md)). Order Manager returns a change only for an owned order, once per fill ([Order Manager feature 3](ORDER_MANAGER_MAP.md)).

#### Algorithm

1.  Forward each order update to Order Manager. If Order Manager returns no change, stop.
2.  Book the position from the returned change: size, average price, currency, entry timestamp = the entry-order bar.
3.  Pass the update to StrategyExecution. StrategyExecution asks Order Manager itself for anything more it needs.
4.  Log a broker position update for this slot's execution instrument and account; never book the position from it, because several slots can share one account and instrument. Take a balance update only for this slot's account and the instrument's currency.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Position currency | Fixed per instrument (exchange profile, [System map decision 39](SYSTEM_MAP.md)) | USD (CME), EUR (Eurex) | — |

#### Invariant

The position changes only from changes Order Manager returns, each booked once. The signal sees position data only through strategy data.

### 3. A rebuild happens only while flat and idle *(was behaviour 4 · decisions 5, 10)*

#### Need

Late warmup history or late auxiliary data means the signal must be rebuilt cold. A rebuild in the middle of a trade would change the exit logic under an open position.

#### Upstream gives

Warmup history from the Event Loop ([Event Loop feature 7](EVENT_LOOP_MAP.md)); ready or not ready with frames from Aux Data ([Aux Data feature 4](AUX_DATA_MAP.md)), and frames again on request during a rebuild ([Aux Data feature 8](AUX_DATA_MAP.md)); the signal's warm report.

#### Algorithm

1.  Rebuild only while flat and idle: no position, and Order Manager has no open or submitted order.
2.  When late warmup history or late aux frames arrive while the slot is not flat and idle: keep them as a pending rebuild (a newer value replaces an older one) and clear the matching ready flag at once, so the slot makes no new entry. The open trade keeps its current signal and its exits. Run the pending rebuild at the first moment the slot is flat and idle. A pending rebuild still waiting at session end is logged at WARNING ([System map decision 53](SYSTEM_MAP.md)).
3.  Build the signal cold and replay the seed bars; then set StrategyExecution's warmup-ready, only when the Event Loop's seed state is READY and the signal reports warm ([System map decision 52](SYSTEM_MAP.md)). Set aux-ready when Aux Data reports ready; when the signal reads no auxiliary name, set aux-ready once, when the slot is built.
4.  During a mid-session replay, hold live bars back and step them afterwards, in order. Skip replayed bars outside the slot's session.

#### Parameters

None.

#### Invariant

No rebuild happens with a position or with an open or submitted order. StrategyExecution is never told "ready" while the signal is not warm. No live bar is stepped in the middle of a replay or dropped by the replay.

### 4. A flatten hands StrategyExecution the booked position *(was behaviour 5 · decision 6)*

#### Need

At session end, at shutdown, or after an exit failed in position, the slot must close exactly the position Strategy booked, and a flat slot must send nothing.

#### Upstream gives

A flatten request from the Event Loop ([Event Loop feature 8](EVENT_LOOP_MAP.md)), or StrategyExecution reporting that a flatten is wanted after a failed exit.

#### Algorithm

1.  On each flatten request, hand StrategyExecution the booked position. On each bar while StrategyExecution reports that a flatten is wanted, call the flatten with reason `exit_failed`.
2.  StrategyExecution cancels every order, waits until each is final, sends one market order opposite the booked position (none when flat) and ends the day.
3.  Flat and idle counts submitted, open and cancelling orders.

#### Parameters

None.

#### Invariant

A flat slot sends no close order.

### 5. Session-fixed signal values survive a restart *(was behaviour 6)*

#### Need

A process restart mid-session must not lose values fixed for the session (for example an opening range), because the history to rebuild those values may no longer be available.

#### Upstream gives

A state store, and the signal's list of values fixed for the session ([StrategySignal](STRATEGY_SIGNAL_MAP.md)).

#### Algorithm

1.  Accept only session-fixed values as cached.
2.  Once the signal is warm, persist their state once.
3.  On restart, restore the state and log loudly; treat a missing or corrupt record as nothing to restore.
4.  A quarantined slot never persists.

#### Parameters

None.

#### Invariant

Only values that cannot go stale while the process is down are restored.

### 6. Signal snapshots are logged without touching trading *(was behaviour 7 · System map decision 29)*

#### Need

A run must be auditable afterwards: the operator needs the signal's values next to each decision.

#### Upstream gives

The signal's values, and the slot's health (quarantined or not).

#### Algorithm

1.  Every N bars (never when N is 0), log a snapshot built from the signal's values, with instrument and event fields.
2.  Mark a quarantined slot's in-session snapshot row as stale; a healthy row carries no stale keys.
3.  Write the record on a logging thread, not on the Event Loop thread.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Snapshot interval N (bars) | Configurable per strategy (slot config) | 0 (off) or any positive count | 60 (once a minute at 1 s bars) |

#### Invariant

For every N: logging never changes what is traded.

## Tests

The goal and features as measurable cases. A test with parameter cases runs once per supported value. No Strategy test exists in v3 yet; every row is not written yet.

**End to end · the sample (1 test)**

|  |  |
|----|----|
| Test | `tests/strategy/test_strategy.py::test_golden_round_trip_books_position` |
| Input | The golden session's 23,400 bars for MES ([System map golden sample](SYSTEM_MAP.md)), the real `macd_signal_dag` signal with default params, a StrategyExecution stub that sends BUY 1 MKT on the phase B entry bar and SELL 1 MKT on the target bar, and an Order Manager mock that returns each fill (at the bar close) as an owned change. |
| Passes when | aux-ready is set at build; every bar reaches the signal once and StrategyExecution once, in that order; the position is +1 MES in USD between the two fills and 0 at the end; from the bar after the buy fill the signal gets entry price = fill price, and at the end entry price is None. |
| Fails when | A bar reaches StrategyExecution before the signal was stepped with the bar; the position moves on an update Order Manager did not return as a change; or the slot waits for aux data that the signal does not read. |

### End-to-end scenarios *(agreed 2026-11-09)*

Each scenario drives the component only through its public interface, from a known start state, and checks one exact, observable output. A scenario tells one story across several features; the feature tables below keep the single rules. File: `tests/strategy/test_strategy_scenarios.py`. A signal stub records each step and its strategy data; a StrategyExecution stub records bars, updates and flatten calls and plays scripted decisions; an Order Manager stub returns scripted changes; a state store stub keeps records in memory.

**S1. A bar goes to the signal, then to execution (features 1)**

|  |  |
|----|----|
| Test | `test_scenario_bar_order` |
| Given | A flat slot on MES; StrategyExecution steps the signal on every bar and sends BUY 1 MKT on bar 2, filled at 5001.00 on bar 2. |
| When | Send bars 1 to 5. |
| Parameter cases | — |
| Passes when | On every bar the signal is stepped once, before StrategyExecution decides. Strategy data is None on bars 1 and 2; from bar 3 on, entry price = 5001.00 and seconds since entry is counted from bar 2. |
| Fails when | StrategyExecution decides before the signal step; two steps on one bar; strategy data while flat. |

**S2. The position is booked from fills only (features 2)**

|  |  |
|----|----|
| Test | `test_scenario_position_from_fills` |
| Given | A flat slot on MES (or FDAX). |
| When | Send: BUY fill 1 @ 5001.00 (Order Manager returns a change); the same update again (no change); a broker position update for the same account and instrument; a balance update for another account; SELL fill 1 @ 5011.00. |
| Parameter cases | instrument ∈ {MES → USD, FDAX → EUR} |
| Passes when | Position +1 at 5001.00 in the instrument's currency after the BUY; no change on the repeat, the broker position update (logged only) and the other account's balance; flat after the SELL. |
| Fails when | The position moves on a repeat or on a broker position update; the wrong currency; the other account's balance is taken. |

**S3. Late data while in position waits for flat (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_late_data_pending_rebuild` |
| Given | A slot in position, ready to arm. |
| When | Send late warmup history, then a newer late warmup; send the exit fill; then a second case where the session ends before the slot is flat. |
| Parameter cases | case ∈ {flat later in session; session ends in position} |
| Passes when | Warmup-ready is cleared at the first late value, so no entry is made; exits stay live on the current signal; after the exit fill the signal is rebuilt once, from the newer value, and warmup-ready is set again. Session-end case: no rebuild runs and one WARNING is logged. |
| Fails when | An entry while the flag is cleared; a rebuild while in position; the older value used; two rebuilds. |

**S4. No rebuild while an order works (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_rebuild_waits_for_orders` |
| Given | A flat slot with a submitted order. |
| When | Send late warmup; then close the order. |
| Parameter cases | — |
| Passes when | No rebuild while the order is submitted; one rebuild after the order closes. |
| Fails when | A rebuild with a submitted, open or cancelling order. |

**S5. Aux readiness gates the slot (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_aux_ready_gate` |
| Given | Slot P's signal reads no auxiliary name; slot Q's signal reads X. |
| When | Build both slots; seed both; then Aux Data reports Q ready. |
| Parameter cases | — |
| Passes when | P has aux-ready at build and can arm after its seed; Q can arm only after Aux Data reports ready. |
| Fails when | P waits for Aux Data; Q arms before Aux Data is ready. |

**S6. A flatten carries the booked position (features 4)**

|  |  |
|----|----|
| Test | `test_scenario_flatten_hand_off` |
| Given | A slot long 1; in a second case StrategyExecution reports "flatten wanted" after a failed exit. |
| When | Send a flatten request (case request), or 3 bars (case flatten wanted). |
| Parameter cases | case ∈ {flatten request, flatten wanted} |
| Passes when | Request: StrategyExecution gets one flatten with position +1. Flatten wanted: one flatten with reason `exit_failed` on each of the 3 bars. |
| Fails when | A flatten without the booked position; no flatten while flatten is wanted. |

**S7. Session-fixed values survive a restart (features 5)**

|  |  |
|----|----|
| Test | `test_scenario_restart_restore` |
| Given | A slot whose signal has one session-fixed value. |
| When | Warm the signal; build a new slot on the same store. Repeat with a corrupt record, and with a quarantined slot. |
| Parameter cases | case ∈ {normal, corrupt record, quarantined} |
| Passes when | Normal: the state is persisted once and the new slot restores the value, with a loud log. Corrupt: nothing is restored and the slot starts normally. Quarantined: nothing is persisted. |
| Fails when | A second persist; a crash on a corrupt record; a quarantined slot persists. |

**S8. Snapshots every N bars (features 6)**

|  |  |
|----|----|
| Test | `test_scenario_snapshots` |
| Given | A slot stepping 180 bars. |
| When | Run the 180 bars; repeat with the slot quarantined in session. |
| Parameter cases | N ∈ {0, 60} |
| Passes when | N = 60: three snapshot rows, with instrument and event fields. N = 0: no row. Quarantined: rows are marked stale. |
| Fails when | A row count other than 180 / N; a row when N = 0; a healthy row with stale keys. |

**1. The signal is stepped once per bar, before the decision (3 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_signal_stepped_once_before_execution_each_bar` | — | steps per bar; order of calls | 1; step before decision |
| `test_strategy_data_carries_entry_price_after_fill` | position ∈ {flat, long} | entry price; seconds since entry | None, None; fill price, seconds from the entry-order bar |
| `test_no_step_outside_session_or_when_quarantined_out_of_session` | — | steps before bar 0 | 0 |

**2. The position is booked only from Order Manager's changes (4 tests · 9 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_position_booked_only_from_order_manager_changes` | update ∈ {owned change, not owned, duplicate} | position change | booked once; none; none |
| `test_position_booked_in_execution_instrument_currency` | instrument ∈ {NQ, FDAX} | position currency | USD; EUR |
| `test_broker_position_update_never_books_position` | update for ∈ {own instrument and account, other instrument} | position change | none; none |
| `test_aux_ready_set_at_build_when_no_aux_name` | signal reads ∈ {no aux name, one aux name} | aux-ready after build | set; not set until Aux Data reports ready |

**3. A rebuild happens only while flat and idle (4 tests · 7 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_late_data_in_position_kept_pending_then_rebuilt_when_flat` | late data ∈ {warmup history, aux frames} | new entries while pending; exits while pending; rebuilds; flag after the rebuild | 0; live; 1, when flat and idle; set |
| `test_rebuild_refused_unless_flat_idle` | state ∈ {position, open order, submitted order} | rebuilds | 0 |
| `test_rebuild_then_ready_when_flat_idle` | — | ready flags after a rebuild | set only after the signal is warm |
| `test_live_bars_held_during_replay_then_stepped_in_order` | — | live bars stepped during the replay; order afterwards | 0; time order, none dropped |

**4. A flatten hands StrategyExecution the booked position (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_flatten_closes_booked_position` | position ∈ {flat, long 1} | close orders sent | 0; 1 SELL 1 MKT |

**5. Session-fixed signal values survive a restart (2 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_restore_session_values` | record ∈ {valid, missing, corrupt} | values restored | all; none; none |
| `test_quarantined_slot_never_persists` | — | writes to the state store | 0 |

**6. Signal snapshots are logged without touching trading (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_snapshot_every_n_bars` | N ∈ {0, 10} | snapshots in 100 bars; stale keys on healthy rows | 0; 10; 0 |

## Open questions

All decided on 2026-11-09.

1.  **Snapshot interval (feature 6).** Default N = 60 bars, configurable per strategy.

## Reference

Implementation detail and history. The skeleton and decisions still cite the old behaviour numbers: old 1 → feature 1, old 3 → 2, old 4 → 3, old 5 → 4, old 6 → 5, old 7 → 6.

### Step → code

`Strategy` is in `strategy_builder/strategy.py`; the stores are in `stores.py`.

| Step | Code |
|----|----|
| Hand bar to execution, Step signal | `Strategy.on_bar()`, `strategy_data()` |
| Forward update, Book position | `Strategy.on_order_update()` |
| Filter account update | `Strategy.on_position_update()`, `on_balance_update()` |
| Rebuild when flat idle | `Strategy.warmup()`, `warmup_requirements()`, `rebuild_intraday_state()`, `deferring_bars()`, `reset_signal()`, `is_flat_idle()`, `on_aux_ready()` |
| Hand flatten | `Strategy.request_flatten()` |
| Persist session values | `Strategy.restore_signal_state()`; `DailyStore.save()`, `load()` (`calc_state`, `round_trips`) |
| Log snapshot | `Strategy.signal_snapshot()` |
| Slot-state answers | `Strategy.is_processing_order()`, `quarantine()`, `release_quarantine()`, `is_quarantined()`, `consume_trade_closed()`, `get_submitted_orders()`, `get_open_orders()`, `get_cancelling_orders()` |

### Skeleton code

### Major state

| State | What it holds |
|----|----|
| `config`, `name` | The slot's deployment config (instruments, account, execution block) and key. |
| `signal_instrument` / `execution_instrument` / `session` | The instrument it reads; the instrument orders go to; its session name from the exchange profile. |
| `position` | Signed size, average price, last price, currency, entry timestamp, side (LONG / SHORT / FLAT). Booked only from Order Manager's changes. |
| `balance` | Available funds for this slot's account in its instrument's currency. |
| `signal`, `om`, `execution` | The slot's three parts. The signal is stepped here and also held by StrategyExecution. |
| `deferred_bars` | Live bars held back while a rebuild replays recorded ones; `None` otherwise. |
| `state_store`, `roundtrip_store` | Persist session-fixed signal values across restarts; persist round trips. |

### Methods by behaviour

| Old behaviours | Methods |
|----|----|
| 1\. Bar drive | `on_bar()`, `strategy_data()` |
| 3\. Relay and booking | `on_order_update()`, `on_position_update()`, `on_balance_update()` |
| 4\. Readiness and rebuild | `warmup()`, `rebuild_intraday_state()`, `reset_signal()`, `is_flat_idle()`, `warmup_requirements()` (routes to the signal) |
| 5\. Teardown | `request_flatten(reason, bar_ts)` |
| 6\. Signal state | `restore_signal_state()`, `restore_round_trips()` |
| 7\. Observability | `signal_snapshot()` |
| Slot-state routing (decision 14) | `is_processing_order()`, `quarantine()`, `release_quarantine()`, `is_quarantined()`, `warmup_ready`, `aux_ready`, `consume_trade_closed()`, `get_submitted_orders()`, `get_open_orders()`, `get_cancelling_orders()`. Code outside the slot asks only these and never reaches into Order Manager or StrategyExecution. |

**Full skeleton code (python)**

``` python
"""Skeleton contract for Strategy, the slot driver (Alchemist v3)."""

from __future__ import annotations

from collections import deque
from typing import Any

from alchemist.instruments import Instrument
from alchemist.strategy_builder.strategy_signal import StrategySignal
from alchemist.strategy_builder.strategy_executions import make_execution
from alchemist.managers.order_manager import OrderManager

class Strategy:
    name: str
    config: dict
    signal_instrument: Instrument
    execution_instrument: Instrument
    session: str
    position: dict          # size, avg_price, last_price, currency, entry_ts, side
    balance: dict           # {'currency': str, 'available': float | None}
    signal: StrategySignal
    signal_name: str        # registry name of the signal, set by the strategy subclass
    om: OrderManager
    execution: Any          # StrategyExecution, built by make_execution(config)
    deferred_bars: list | None

    # Behaviour 1
    def on_bar(self, bars: deque, bar: dict) -> None:
        """execution.on_bar(bar, step_signal); step_signal calls
        signal.step(bars, self.strategy_data()) exactly once."""

    def strategy_data(self) -> dict:
        """{'entry_price': ..., 'secs_since_entry': ...}; both None while flat."""

    # Behaviour 3
    def on_order_update(self, update: dict) -> None:
        """change = om.on_order_update(update); book only from change;
        then execution.on_order_update(update)."""

    def on_position_update(self, update: dict) -> None: ...
    def on_balance_update(self, update: dict) -> None: ...

    # Behaviour 4
    def warmup_requirements(self) -> dict:
        """Route the signal's declared history needs to the Event Loop."""
    def warmup(self, history: dict) -> None: ...
    def rebuild_intraday_state(self, seed_bars: list) -> None: ...
    def reset_signal(self) -> None:
        """self.signal = get_signal_dag(self.signal_name).from_config(self.config)."""
    def is_flat_idle(self) -> bool:
        """No position; no submitted, open or cancelling order."""

    # Behaviour 5
    def request_flatten(self, reason: str, bar_ts: float) -> None: ...

    # Behaviour 6-7
    def restore_signal_state(self) -> None: ...
    def signal_snapshot(self) -> dict: ...

    # Slot-state routing (decision 14)
    def is_processing_order(self) -> bool: ...
    def quarantine(self, reason: str) -> None: ...
    def is_quarantined(self) -> bool: ...
    def get_submitted_orders(self) -> list: ...
    def get_open_orders(self) -> list: ...
    def get_cancelling_orders(self) -> list: ...
```

### Decisions already made

### 1. Who drives the signal?

**Answer:** Strategy steps and rebuilds it; StrategyExecution holds it and asks it for the entry/exit answers ([System map decision 20](SYSTEM_MAP.md)).

**Rejected:** Rejected: Strategy evaluating the answers and handing them over.

### 2. What does Strategy need from the signal's interface?

**Answer:** Step, warm state, rebuild, warmup requirements, and the entry/exit answers StrategyExecution asks for — including per-condition exit answers and each exit condition's level, so a protected slot can rest a broker order at the level the signal already computes — and the read-only `values()` for logs and snapshots.

**Rejected:** Rejected: skip-masks or a fill callback inside the signal.

### 3. How does the signal see position data?

**Answer:** Through strategy data (entry price, seconds since entry) passed into each step ([System map decision 21](SYSTEM_MAP.md)). It never reads the position itself.

### 4. Who freezes and releases the signal's exit inputs?

**Answer:** StrategyExecution: freeze on the entry-decision bar before the entry order is sent; release on the exit fill or when the entry dies.

**Rejected:** Rejected: moving them to Strategy.

### 5. Who owns the rebuild?

**Answer:** Split ([System map decision 22](SYSTEM_MAP.md)): StrategySignal owns how; Strategy owns when (flat, idle, no open or submitted order).

### 6. Where do session gating, entry/exit placement, quota and teardown live?

**Answer:** In StrategyExecution (its own map). StrategyExecution reads the params it needs through the signal it holds.

### 7. Fill attribution

**Answer:** Strategy books only what Order Manager returns as a change (Order Manager owns ownership).

### 8. What anchors the entry timestamp?

**Answer:** The bar that submitted the entry.

### 9. Does Strategy relay Order Manager's answers to StrategyExecution?

**Answer:** No. Strategy passes the order update on as is; StrategyExecution queries Order Manager itself.

**Rejected:** Rejected: an order-context relay — it duplicated the open and submitted orders Order Manager already stores.

### 10. How does the Event Loop's warmup seeding reach the signal?

**Answer:** Through Strategy only: `warmup_requirements()` and `warmup()` route to the signal; the Event Loop never reads the signal.

### 11. Who logs the entry/exit decision's inputs?

**Answer:** StrategyExecution, when it evaluates the signal.

### 14. How does code outside the slot ask about it?

**Answer:** Only through Strategy: `is_processing_order()` forwards to StrategyExecution; `get_submitted_orders()` / `get_open_orders()` / `get_cancelling_orders()` forward to Order Manager under its own status names; `is_flat_idle()` is answered here; `is_quarantined()` too. Safety's repair picks slots by their submitted and open orders.

**Rejected:** Rejected: names that are not order statuses ("pending", "working").

### 15. An unknown key in a slot's config?

**Answer:** Logged, never fatal. A bad execution block fails startup (StrategyExecution decision 21).

### 16. How is a strategy declared, and how is its signal built?

**Answer:** A strategy is a small declarative subclass in its folder's `strategy.py`, registered with `register_strategy(strat)`; the slot config names that strat. It names its signal by registry name (`signal_name`), not by import ([System map decision 43](SYSTEM_MAP.md)). Strategy builds the signal with `get_signal_dag(signal_name).from_config(config)` at start and on every rebuild, and reads it only through the public signal interface ([StrategySignal decision 8](STRATEGY_SIGNAL_MAP.md)).

**Rejected:** Rejected: reading or writing the signal's value cache directly for logs or restore.

*agreed 2026-11-09 · v3 · 2026-11-07*

Decisions 12 and 13 are not used; the numbers are kept so references stay valid.
