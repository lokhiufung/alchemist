# STRATEGY_EXECUTION_MAP — StrategyExecution · Alchemist v3

The decision state machine inside each slot. StrategyExecution knows which phase the slot is in, asks the signal only the question that phase needs, sends only the orders that phase allows, and decides what each order event means.

Frozen contract for the StrategyExecution component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `strategy_execution_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-09.** **v3 final, 2026-11-09 — agreed.** The End-to-end scenarios in Tests were agreed on 2026-11-09. Features 1–12 and decisions 1–22 are settled. The calls on the signal use the v3 signal interface. Change history: see `strategy_execution_map.html`.

## Goal and scope

**What it is.** StrategyExecution is the decision state machine inside each slot. StrategyExecution knows which phase the slot is in, asks the signal only the question that phase needs, sends only the orders that phase allows, and decides what each order event means. Three classes share one contract: the base class (bar-close market entries and exits), `ProtectedStrategyExecution` (resting broker-side exits in an OCA group) and `StopLossProtectedStrategyExecution` (one resting disaster stop). The slot's execution block in the config picks the class.

**Why it exists.** Resting broker-side exits make the order flow the hard part of a strategy: a stop can fill while the engine sends a market exit, a leg can be rejected, an exit can fail in position. Keeping that state machine apart from the signal (pure logic) and from Order Manager (order bookkeeping) lets each part be reasoned about and tested alone ([System map decision 14](SYSTEM_MAP.md)).

**Goal.** Each slot moves through its execution phases correctly and sends only the entry and exit orders those phases allow. A protected slot never sends a market exit outside the trade's OCA group while a protective order may still fill, and never leaves an open position without protection that the slot has not accounted for.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Other components call StrategyExecution | Strategy | Pass a bar (with the step-signal callback); pass an order update; set warmup-ready and aux-ready; request a flatten with the booked position; quarantine and release; ask "processing an order?", "quarantined?", "flatten wanted?", "trade closed?" | bar, update, flags, position, reason |
|  | Runner | Build the slot's execution class from its execution block | execution block → class, or a config error at start |
| StrategyExecution calls other components | Strategy (callback) | Step the signal on an in-session bar | — |
|  | StrategySignal | Ask for the entry answer, each exit condition's answer, a condition's level, a node's level; freeze and release the exit inputs | condition or node name, bar |
|  | Order Manager | Place an entry, exit or flatten order; place a protective group; cancel; ask for an order's status and fill, the group's orders, and whether the group is closed | order request, group request, order id, group name |

### Scope

Owns, from the System map:

> The slot's execution state machine: which phase it is in, when to enter and exit by asking the slot's signal, and what each order event means in the current phase.

| Not StrategyExecution's job | Owned by |
|----|----|
| The signal's logic (calculations, conditions, levels) and when the signal is stepped | [StrategySignal](STRATEGY_SIGNAL_MAP.md), [Strategy](STRATEGY_MAP.md) |
| The position. StrategyExecution reads the trade's filled quantities from Order Manager. | [Strategy](STRATEGY_MAP.md) |
| An order's life at the broker (ids, groups, fill accumulation, closed status) | [Order Manager](ORDER_MANAGER_MAP.md) |
| Talking to the Event Loop | [Strategy](STRATEGY_MAP.md) |

### Place in the system

Drawn in the HTML page; the flows are the Public interface table above and the [SYSTEM_MAP.md](SYSTEM_MAP.md) Diagram.

StrategyExecution asks Order Manager directly; Strategy does not relay Order Manager's answers ([System map decision 15](SYSTEM_MAP.md)).

## Features

Each feature says which component needs it and why, what the upstream can give, and how StrategyExecution closes the gap. Features marked "protected" apply to the protected classes only. Every number is a parameter with its supported values.

### Algorithm map

```
  ( Strategy )
        │
        │ bar + step call
        ▼
  ┌─────────────────────────┐
  │ Gate the bar            │
  │ rule: warm, in session  │
  └─────────────────────────┘
        │
        │ in-session bar
        ▼
  ┌─────────────────────────┐
  │ Seek entry              │
  │ rule: ask entry if flat │
  └─────────────────────────┘
        │
        ├── entry? ──▶ ( StrategySignal )
        │   ◀── answer
        │
        │ entry true
        ▼
  ┌─────────────────────────┐
  │ Send entry, freeze      │
  │ rule: freeze first      │
  └─────────────────────────┘
        │
        ├── freeze exit inputs ──▶ ( StrategySignal )
        ├── entry order ──▶ ( Order Manager )
        │
        │ order sent
        ▼
  ┌─────────────────────────┐
  │ Track order update      │
  │ rule: keyed by order    │
  └─────────────────────────┘
        │
        ├── status, fill, group? ──▶ ( Order Manager )
        │   ◀── answers
        │
        │ entry filled
        ▼
  ┌─────────────────────────┐
  │ Protect on entry        │
  │ rule: once; fill-sized  │
  └─────────────────────────┘
        │
        ├── levels? ──▶ ( StrategySignal )
        │   ◀── levels
        ├── protective group ──▶ ( Order Manager )
        │
        │ in position
        ▼
  ┌─────────────────────────┐
  │ Judge exits             │
  │ rule: mask working legs │
  └─────────────────────────┘
        │
        ├── exit conditions? ──▶ ( StrategySignal )
        │   ◀── per condition
        │
        │ exit true
        ▼
  ┌─────────────────────────┐
  │ Send exit               │
  │ rule: join group if leg │
  └─────────────────────────┘
        │
        ├── MKT exit (+ OCA group) ──▶ ( Order Manager )
        │
        │ exit or leg fill
        ▼
  ┌─────────────────────────┐
  │ Complete round trip     │
  │ rule: count; cap → DONE │
  └─────────────────────────┘
        │
        ├── writes ──▶ [ round-trip count (round-trip store) ]
        ├── release exit inputs ──▶ ( StrategySignal )
        │
        │ trade closed
        ▼
  ( Strategy )

  ( Strategy )
        │
        │ order update
        ▼
        └──▶ Track order update

  ( Strategy )
        │
        │ flatten + position
        ▼
  ┌─────────────────────────┐
  │ Flatten                 │
  │ rule: cancel, 1 MKT     │
  └─────────────────────────┘
        │
        │ cancels, one MKT
        ▼
  ( Order Manager )

  ( Strategy )
        │
        │ quarantine, release
        ▼
  ┌─────────────────────────┐
  │ Park in ERROR           │
  │ rule: no orders         │
  └─────────────────────────┘
```

Main path in colour: one trade from an in-session bar to a completed round trip. "Protect on entry" runs only in the protected classes. "Gate the bar" also resets the round-trip count on a new trading date. A failed exit leads to a flatten on the next bar (feature 12).

### Phases

Code outside the slot never reads phase names: it asks "processing an order?" through Strategy (feature 2).

| Phase | Class | Processing an order? |
|----|----|----|
| WARMUP | base | false |
| WAITING_FOR_WORKING_HOURS | base | false |
| ENTRY_LOOP_ACTIVITY | base | false |
| PROCESSING_ENTRY_ORDER | base | true |
| IN_POSITION_ACTIVITY | base | false |
| PROCESSING_EXIT_ORDER | base | true |
| SESSION_END_FLATTENING | base | true only while an order of the slot may still fill |
| DONE | base | false |
| ERROR | base | false |
| PROCESSING_PROTECTION_ORDERS (group sent, not every leg OPEN) | protected | true |
| IN_POSITION_PROTECTED (in position, every leg OPEN) | protected | false |

### Feature cards

### 1. One trade at a time, up to the daily cap *(both classes · was behaviour 1)*

#### Need

A slot must never have two orders in flight, never trade past its daily cap, and must keep its exit levels fixed at the entry bar for the whole trade.

#### Upstream gives

Bars from Strategy, the signal's entry and exit answers ([StrategySignal feature 3](STRATEGY_SIGNAL_MAP.md)), and order outcomes from Order Manager ([Order Manager feature 4](ORDER_MANAGER_MAP.md)).

#### Algorithm

1.  On every in-session bar, in every phase (including DONE, ERROR and SESSION_END_FLATTENING), call the step-signal callback exactly once, before the phase logic. The signal then holds that bar's values, and the rearm gate sees every bar.
2.  While flat, ask for the entry answer on each in-session bar; on true, freeze the signal's exit inputs, then send one entry order.
3.  On the entry fill, be in position. On a bar-close exit answer, send one exit order.
4.  On the exit fill, be flat, count the round trip, release the frozen inputs, and go to DONE at the daily cap.
5.  A dead entry returns to entry seeking and releases the inputs; a dead exit returns to in position.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Daily cap (round trips per day) | Configurable per signal (`max_trades_per_day`) | any positive count | 1 |
| Quantity per entry | Fixed by [System map decision 24](SYSTEM_MAP.md) | 1 unit | 1 unit |

#### Invariant

For every cap: at most one order in flight; no order after the cap; a trade's frozen inputs are never released while the trade is open.

### 2. One question answers "processing an order?" *(both classes · was behaviour 2)*

#### Need

Safety's repair, the Event Loop's stuck-order check and shutdown must know whether a slot waits on an order's outcome, without reading phase names.

#### Upstream gives

The current phase, and Order Manager's submitted, open and cancelling orders.

#### Algorithm

1.  Answer true in PROCESSING_ENTRY_ORDER and PROCESSING_EXIT_ORDER, and for a protected slot in PROCESSING_PROTECTION_ORDERS.
2.  In SESSION_END_FLATTENING, answer true only while an order of the slot may still fill.
3.  Answer false in every other phase, including IN_POSITION_PROTECTED.

#### Parameters

None.

#### Invariant

The only phase questions asked from outside are this one and "quarantined?" (feature 10). No caller reads phase names. Questions about orders are answered by Order Manager.

### 3. Protect on entry *(protected · was behaviour 3 · decisions 14, 22)*

#### Need

A protected slot needs broker-side exits that fill even when the engine is down, sized to what actually filled and placed at levels computed from the actual fill price.

#### Upstream gives

The entry order closed with its filled quantity and average price (Order Manager), and the selected conditions' levels from the signal (`exit_condition_level`, or `level(name)` for the stop-loss class).

#### Algorithm

1.  On the first bar in position after the entry closed with filled quantity \> 0, once per trade: take the selected levels for the actual average fill price. Placement is bar-driven, because the level needs the fill price and the order-update handler cannot await a group placement.
2.  If no level is available, or the latest close already crossed the stop, run this trade unprotected (stay in IN_POSITION_ACTIVITY).
3.  Otherwise request one group of the selected legs, sized to the filled quantity, under a fresh OCA group; enter PROCESSING_PROTECTION_ORDERS; when every leg is OPEN, enter IN_POSITION_PROTECTED.
4.  Stop-loss class: the level is the signal's node level moved by the offset against the trade (long: below, short: above); one STP leg.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Resting exits | Configurable per slot (execution block) | map of exit-condition names to STP or LMT, at most one of each | — (required) |
| Stop-loss level name | Configurable per slot (stop-loss class) | any node of the signal | — (required) |
| Stop-loss offset (points) | Configurable per slot (stop-loss class) | any number of points | — (required) |

#### Invariant

Protection is sized to the confirmed filled quantity; at most one group per trade; no group for a zero fill or a crossed stop.

### 4. Mask a condition only while a leg covers it *(protected only · was behaviour 4 · decisions 10, 15)*

#### Need

A selected condition that a resting leg covers must not also trigger a market exit, but no selected condition may ever go unwatched.

#### Upstream gives

The signal's per-condition exit answers on each bar in position ([StrategySignal feature 3](STRATEGY_SIGNAL_MAP.md)).

#### Algorithm

1.  In IN_POSITION_PROTECTED: send a bar-close market exit only when a non-selected condition is true and no selected condition is true. When a selected condition is true, send no market exit that bar, even if a non-selected condition is also true, and log the skip.
2.  In PROCESSING_PROTECTION_ORDERS and IN_POSITION_ACTIVITY: judge every condition at bar close.
3.  Log every raw condition answer.
4.  Stop-loss class: mask nothing; judge every condition at bar close in every phase.

#### Parameters

None.

#### Invariant

On every bar of an open position, each selected condition is covered by a working leg or by bar-close evaluation.

### 5. A market exit joins the group *(protected · was behaviour 5)*

#### Need

A market exit sent while a protective leg may still fill could fill twice. A cancel before the market exit would leave the position unprotected for a moment.

#### Upstream gives

The trade's group and its legs' states (Order Manager). The broker fills a market order in the OCA group and cancels the legs, or refuses it when a leg already filled (at IBKR: Inactive + error 201, normalised to REJECTED by the Gateway).

#### Algorithm

1.  While at least one leg is submitted or open: send one market order sized to the trade's position, carrying the group's OCA group and OCA type, with no cancel before it; enter PROCESSING_EXIT_ORDER.
2.  With every leg dead: send a plain market order.
3.  On the fill, complete the round trip. On a reject or cancel: if flat, a leg fill already closed the trade (feature 6); otherwise return to IN_POSITION_PROTECTED when every leg is still OPEN, else IN_POSITION_ACTIVITY, and the exit counts as failed (feature 12).

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| OCA type of a joined exit | Fixed (same as the group, [Order Manager feature 2](ORDER_MANAGER_MAP.md)) | 2 | 2 |

#### Invariant

No market exit outside the group while a leg may still fill; no cancel before a market exit; a market exit never exceeds the trade's position from Order Manager's filled quantities.

### 6. A resting fill completes the trade *(protected · was behaviour 6)*

#### Need

A leg can fill in any phase, before its acknowledgement or while a market exit is in flight. Every fill that makes the slot flat must count exactly one round trip.

#### Upstream gives

Order updates through Strategy, and each order's filled quantity from Order Manager.

#### Algorithm

1.  Interpret each update by the order it is for (entry, exit, or a leg of the trade's group), not by the current phase.
2.  When the position is flat, complete the round trip exactly like a bar-close exit (count, release the frozen inputs, daily cap → DONE), with no market order.
3.  A later update for an order of a closed trade (the sibling's Cancelled, a refused market exit) changes no phase.

#### Parameters

None.

#### Invariant

Every fill that makes the slot flat completes exactly one round trip.

### 7. Lost protection falls back to bar-close exits *(protected · was behaviour 7 · decision 18)*

#### Need

When a leg is rejected or cancelled while the position is open, the slot must still watch every exit condition, without placing a second group.

#### Upstream gives

A leg's Rejected or Cancelled update that was not caused by the trade's own market exit or a sibling fill.

#### Algorithm

1.  Enter IN_POSITION_ACTIVITY and judge every condition at bar close from then on; do not place the leg again.
2.  Later market exits still join the group while a leg survives, and go out plain once no leg survives.
3.  Log CRITICAL. Leave the surviving leg working.

#### Parameters

None.

#### Invariant

At most one group per trade.

### 8. Trade only while warm and in session; reset each day *(both classes · was behaviour 8)*

#### Need

A slot must not enter before the signal is warm or outside its session, and must get its daily quota back on each new trading date.

#### Upstream gives

Warmup-ready and aux-ready from Strategy ([Strategy feature 3](STRATEGY_MAP.md)), and the instrument's exchange profile with sessions, holidays and the overnight session roll.

#### Algorithm

1.  Seek entries only while `ready_to_arm` (warmup-ready AND aux-ready) holds and the bar is in session. Strategy is the only writer of both flags ([System map decision 52](SYSTEM_MAP.md)). A half-session holiday counts as closed; if the calendar is unavailable, fall back to open.
2.  On the first bar of a new trading date (overnight sessions: at the session roll, not midnight), reset the round-trip count and let a DONE slot trade again, without disturbing an open position.

#### Parameters

None.

#### Invariant

No entry order while not warm or out of session.

### 9. The round-trip count survives a restart *(both classes · was behaviour 9)*

#### Need

A process restart mid-day must not give the slot a fresh daily quota.

#### Upstream gives

A round-trip store, which can be missing or hold no record for today.

#### Algorithm

1.  Persist the count on each completed round trip and on the daily reset.
2.  On restart in session, restore the same-day count and park in DONE when the count is at or over the cap.
3.  With no store, no record, or outside session, start from zero.

#### Parameters

None.

#### Invariant

A restart never lets the slot exceed its daily cap.

### 10. A quarantined slot sends nothing *(both classes · was behaviour 10)*

#### Need

The Event Loop quarantines a slot that keeps raising ([Event Loop feature 6](EVENT_LOOP_MAP.md)). The slot must stay silent until release, and release must not reset the daily quota.

#### Upstream gives

Quarantine and release calls through Strategy.

#### Algorithm

1.  Stay in ERROR and place no order on every bar.
2.  Log a reminder at most once per reminder interval per quarantine, with how long the slot has been parked.
3.  On release, restart from WAITING_FOR_WORKING_HOURS, except a slot quarantined while DONE, which returns to DONE on the same trading date.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Reminder interval | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | any positive seconds | 300 s |

#### Invariant

An ERROR slot never sends an order; a quarantine and release never lets the slot exceed its daily cap.

### 11. Session-end flatten *(both classes · was behaviour 11)*

#### Need

At a session's cutoff, at shutdown, or after a failed exit, the slot must end flat with no market order sent next to an order that may still fill.

#### Upstream gives

A flatten request with the booked position from Strategy ([Strategy feature 4](STRATEGY_MAP.md)), on each call until done.

#### Algorithm

1.  Enter SESSION_END_FLATTENING. On each call: cancel every open order; while Order Manager has a submitted, open or cancelling order, wait; when flat, go to DONE; otherwise send one market order for the booked position, with no OCA group.
2.  Do not resend a rejected flatten order on the same trading date, even across a quarantine.
3.  While flattening, step the signal (feature 1) but take no other action on a bar, and let no order update change the phase; the flatten fill counts no round trip.

#### Parameters

None.

#### Invariant

No market order while an order of the slot may still fill; at most one flatten market order per trading date; the same rule for every class.

### 12. A failed exit is never resent *(both classes · was behaviour 12)*

#### Need

An exit that the broker rejected or cancelled in position must not be retried blindly; the slot must still end flat.

#### Upstream gives

A market exit ending REJECTED or CANCELLED (from the broker, or made up by Safety's repair), and the position on the next bar. Judging on the next bar lets a leg fill, for which the broker refused the joined exit, be booked first.

#### Algorithm

1.  If the slot is still in position on the next bar, log CRITICAL and send no further exit.
2.  Report "flatten wanted" until the flatten ends; Strategy then calls the flatten with reason `exit_failed` on each bar (feature 11).
3.  If that market order fails too, do not resend it; log CRITICAL.

#### Parameters

None.

#### Invariant

A slot never resends an exit that already failed in position; there is no retry counter.

## Tests

The goal and features as measurable cases. A test with parameter cases runs once per supported value. Inputs are scripted bars and order updates; the Order Manager mock's answers are scripted with them. No StrategyExecution test exists in v3 yet; every row is not written yet.

**End to end · Sample G, the system golden sample (base class) (1 test)**

|  |  |
|----|----|
| Test | `tests/execution/test_execution_sample.py::test_golden_macd_entry_then_target_exit` |
| Input | The golden session's 23,400 bars for MES ([System map golden sample](SYSTEM_MAP.md)), a scripted signal stub that answers entry true on one phase B bar and exit `target` true on the target bar, daily cap 1, warmup-ready and aux-ready set before the first bar, and a fill at the bar close for each order sent. |
| Passes when | The signal is stepped once on every bar, in every phase; exactly one BUY 1 MKT and one SELL 1 MKT reach Order Manager, on those bars; "processing an order?" is true only between each order's submission and its fill; the slot is DONE after the exit fill and sends nothing in phase C. |
| Fails when | An order is sent before the entry bar; a second buy is sent while the first is pending or after the daily cap; a bar is not stepped; or an order is sent after the exit fill. |

**End to end · Sample P, component-level protected sample (1 test · 6 cases)**

|  |  |
|----|----|
| Test | `test_protected_sample` |
| Setup | A protected slot (selected: stop STP, target LMT), long 1, entry Filled 1 @ P. Stop level S and target level T come from the signal's `exit_condition_level`. On the next bar the slot places the STP+LMT group G. |
| Parameter cases | case ∈ {A: STP fills, then LMT Cancelled → flat, one round trip, no market order; B: end-of-day cutoff bar, MKT fills, legs Cancelled → one MKT SELL 1 carrying G, no cancel, one round trip; B2: STP fills, then the joined MKT is Rejected → as B, round trip is the STP's, no second MKT; C: STP Rejected at placement, LMT OPEN, later a stop bar → fallback, no new group, one MKT SELL 1 into G; D: STP fills before the LMT is acknowledged → flat, one round trip, no market order; E: session-end flatten → both legs cancelled, once both are final one MKT outside G for the remaining position (none if a leg filled), DONE, no round trip} |
| Passes when | Every signal market exit of the trade carries G; no cancel comes before a signal market exit; at most one group per trade; the slot ends flat with one round trip per trade; no second exit fills. |
| Fails when | A market exit is sent without G while a leg may still fill; a cancel comes before a market exit; a second group is placed in one trade; a leg fill that makes the slot flat is not counted; or a selected condition is skipped while no working leg covers it. |

### End-to-end scenarios *(agreed 2026-11-09)*

Each scenario drives the component only through its public interface, from a known start state, and checks one exact, observable output. A scenario tells one story across several features; the feature tables below keep the single rules. File: `tests/strategy_execution/test_execution_scenarios.py`. A signal stub plays scripted entry and exit answers and levels; an Order Manager fake records orders and cancels and plays scripted order updates; a state store stub keeps the round-trip count in memory.

**S1. A bar-close round trip (features 1)**

|  |  |
|----|----|
| Test | `test_scenario_bar_close_round_trip` |
| Given | A base-class slot, flat, ready to arm, in session. |
| When | Entry is true on bar 3; BUY fills on bar 3; exit is true on bar 7; SELL fills on bar 7; entry is true again on bar 8. |
| Parameter cases | daily cap ∈ {1, 2} |
| Passes when | The signal is stepped once on every bar. Bar 3: exit inputs frozen, then one BUY 1 MKT. Bar 7: one SELL 1 MKT. After the SELL fill: flat, count 1, inputs released. Cap 1: DONE and no order on bar 8. Cap 2: a second BUY on bar 8. |
| Fails when | An order before the freeze; a second entry while in position; an order on bar 8 at cap 1; a skipped step. |

**S2. A dead entry returns to seeking (features 1)**

|  |  |
|----|----|
| Test | `test_scenario_dead_entry` |
| Given | As above. |
| When | Entry true on bar 3; the BUY is REJECTED; entry true on bar 5. |
| Parameter cases | — |
| Passes when | After the reject: entry seeking, inputs released, no other order on bar 3 or 4; one new BUY on bar 5. |
| Fails when | A resend on the reject; inputs still frozen; no BUY on bar 5. |

**S3. Entries wait for readiness and the session (features 8)**

|  |  |
|----|----|
| Test | `test_scenario_ready_and_session_gate` |
| Given | A flat slot. |
| When | Bars with `ready_to_arm` false, an out-of-session bar, then a new trading date for a DONE slot. |
| Parameter cases | session ∈ {NY_RTH, overnight (roll at the session roll)} |
| Passes when | No entry question while not ready or out of session; on the first bar of the new trading date the count resets and the DONE slot seeks entries again. |
| Fails when | An entry while not ready or out of session; a reset at midnight for an overnight session. |

**S4. "Processing an order?" in every phase (features 2)**

|  |  |
|----|----|
| Test | `test_scenario_processing_answer` |
| Given | A protected slot driven through every phase. |
| When | Ask the question in each phase. |
| Parameter cases | — |
| Passes when | True in PROCESSING_ENTRY_ORDER, PROCESSING_EXIT_ORDER and PROCESSING_PROTECTION_ORDERS; in SESSION_END_FLATTENING only while an order may still fill; false in every other phase, including IN_POSITION_PROTECTED. |
| Fails when | Any phase answers otherwise. |

**S5. Protected trades (Sample P) (features 3, 4, 5, 6, 7)**

|  |  |
|----|----|
| Test | `test_protected_sample` |
| Given | See the end-to-end Sample P above. |
| When | The six cases of Sample P, plus a stop-loss-class slot. |
| Parameter cases | class ∈ {protected, stop-loss}; stop-loss offset ∈ {2 points} |
| Passes when | As Sample P. Stop-loss class: one STP leg at the node level moved 2 points against the trade (long: below), and every condition judged at bar close in every phase. |
| Fails when | As Sample P; a stop-loss leg on the wrong side; a masked condition in the stop-loss class. |

**S6. No level means no protection (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_unprotected_fallback` |
| Given | A protected slot after an entry fill. |
| When | The signal gives no level (case a), or the latest close already crossed the stop (case b). |
| Parameter cases | case ∈ {no level, stop crossed} |
| Passes when | No group is placed; the slot stays IN_POSITION_ACTIVITY and judges every exit at bar close. |
| Fails when | A group is placed. |

**S7. A failed exit is never resent (features 12)**

|  |  |
|----|----|
| Test | `test_scenario_failed_exit` |
| Given | A slot in position. |
| When | Exit true; the SELL is REJECTED; next bar; the flatten MKT is REJECTED too; next bar. |
| Parameter cases | — |
| Passes when | After the first reject: CRITICAL, no new exit, "flatten wanted" reported. After the flatten reject: CRITICAL and no resend on any later bar. |
| Fails when | Any resend of an exit or of the flatten order. |

**S8. Session-end flatten (features 11)**

|  |  |
|----|----|
| Test | `test_scenario_session_end_flatten` |
| Given | A protected slot in position with two working legs. |
| When | Flatten calls on three bars: legs cancelled on the first, final on the second; the flatten MKT fills on the third. Second case: the flatten MKT is REJECTED, the slot is quarantined and released the same date. |
| Parameter cases | case ∈ {fills, rejected then quarantined} |
| Passes when | Every open order is cancelled; no market order while an order is submitted, open or cancelling; then one MKT for the booked position with no OCA group; DONE after the fill; the fill counts no round trip. Rejected case: no second flatten order on that date, even after the release. |
| Fails when | A MKT while a leg works; an OCA group on the flatten; a round trip counted; a resend on the same date. |

**S9. A quarantined slot sends nothing (features 10)**

|  |  |
|----|----|
| Test | `test_scenario_quarantine` |
| Given | A slot quarantined while seeking (case a) or while DONE (case b). |
| When | Run 700 s of bars with entry true; release. |
| Parameter cases | reminder interval ∈ {300 s} |
| Passes when | No order on any bar; reminders at most once per 300 s (3 reminders in 700 s at most); on release case a restarts from WAITING_FOR_WORKING_HOURS and case b returns to DONE on the same date. |
| Fails when | An order while in ERROR; more reminders than the interval allows; a DONE slot trades again on the same date. |

**S10. The round-trip count survives a restart (features 9)**

|  |  |
|----|----|
| Test | `test_scenario_count_restart` |
| Given | Count 1 persisted today; cap 1. |
| When | Restart in session today; restart on the next trading date; restart with no store. |
| Parameter cases | case ∈ {same day, next day, no store} |
| Passes when | Same day: parked in DONE. Next day: count 0, seeking. No store: count 0. |
| Fails when | A trade on the same day; a count carried to the next day. |

**1. One trade at a time, up to the daily cap (3 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_at_most_one_order_in_flight` | — | orders in flight at any time | ≤ 1 |
| `test_daily_cap` | cap ∈ {1, 2} | round trips in a day with 5 entry signals | = cap; no order after |
| `test_freeze_before_entry_release_after` | trade ends by ∈ {exit fill, entry dies} | freeze before the entry order; release time | yes; at the exit fill or the entry's death |

**2. One question answers "processing an order?" (2 tests · 12 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_processing_order_answer_per_phase` | phase ∈ the 11 phases | answer | as in the Phases table |
| `test_no_caller_reads_state_names` | — | reads of phase names outside StrategyExecution | 0 |

**3. Protect on entry (3 tests · 6 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_no_group_for_crossed_stop_missing_level_or_zero_fill` | case ∈ {crossed stop, missing level, zero fill} | groups placed | 0 |
| `test_group_sized_to_filled_quantity_once_per_trade` | — | group size; groups per trade | filled quantity; 1 |
| `test_stop_loss_level_moved_by_offset_against_trade` | side ∈ {long, short} | STP price | level − offset; level + offset |

**4. Mask a condition only while a leg covers it (2 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_selected_condition_masked_only_when_every_leg_open` | phase ∈ {IN_POSITION_PROTECTED, PROCESSING_PROTECTION_ORDERS, IN_POSITION_ACTIVITY} | market exit on a selected-condition bar | none; sent; sent |
| `test_stop_loss_class_masks_nothing` | — | market exit on any true condition | sent |

**5. A market exit joins the group (1 test · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_market_exit_joins_group_while_leg_alive` | legs alive ∈ {2, 1, 0} | OCA group on the exit; cancels before the exit | G; G; none (plain) — 0 cancels in every case |

**6. A resting fill completes the trade (1 test · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_leg_fill_completes_one_round_trip` | phase ∈ {PROCESSING_PROTECTION_ORDERS, IN_POSITION_PROTECTED, PROCESSING_EXIT_ORDER} | round trips counted; market orders sent | 1; 0 |

**7. Lost protection falls back to bar-close exits (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_lost_leg_falls_back_without_replacement` | leg ends ∈ {Rejected, Cancelled} | phase; new groups; CRITICAL logs | IN_POSITION_ACTIVITY; 0; 1 |

**8. Trade only while warm and in session; reset each day (3 tests · 12 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_no_entry_when_not_warm_or_out_of_session` | case ∈ {not warm, aux not ready, out of session, half-session holiday} | entry orders | 0 |
| `test_signal_stepped_once_per_in_session_bar_in_every_phase` | phase ∈ {ENTRY_LOOP_ACTIVITY, PROCESSING_ENTRY_ORDER, IN_POSITION_ACTIVITY, PROCESSING_EXIT_ORDER, SESSION_END_FLATTENING, DONE, ERROR} | step-signal calls per in-session bar; order vs. phase logic | 1; before |
| `test_daily_reset_at_overnight_session_roll` | — | time of the count reset | at the session roll, not midnight |

**9. The round-trip count survives a restart (2 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_restart_restores_cap` | count at restart ∈ {0, cap − 1, cap} | phase after restart | trading; trading; DONE |
| `test_no_store_starts_from_zero` | — | count after restart | 0 |

**10. A quarantined slot sends nothing (2 tests · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_error_slot_sends_no_order` | — | orders sent while in ERROR | 0 |
| `test_release_from_quarantine` | quarantined while ∈ {trading, DONE} | phase after release on the same date | WAITING_FOR_WORKING_HOURS; DONE |

**11. Session-end flatten (2 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_flatten_waits_then_one_market` | position ∈ {flat, long 1} × working orders ∈ {none, open} | market orders sent while an order may fill; flatten orders in total | 0; 0 when flat, 1 when long |
| `test_rejected_flatten_not_resent` | — | flatten orders on the same date | 1 |

**12. A failed exit is never resent (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_failed_exit_never_resent_flattens_next_bar` | exit ends ∈ {REJECTED, CANCELLED} | further exits; flatten requested on the next bar | 0; yes |

## Open questions

All decided on 2026-11-09.

1.  **Error reminder interval (feature 10).** 300 s; configurable per process.

## Reference

Implementation detail and history. Feature numbers equal the old behaviour numbers.

### Step → code

All steps are in `strategy_builder/strategy_executions/strategy_execution.py`.

| Step | Code |
|----|----|
| Gate the bar, Seek entry, Send entry, Judge exits, Send exit, Complete round trip | `StrategyExecution.on_bar()`, `set_warmup_ready()`, `set_aux_ready()`, `ready_to_arm()`; phases in `Phase` |
| Track order update | `StrategyExecution.on_order_update()`, `consume_trade_closed()` |
| Protect on entry, masking, joined exit | `ProtectedStrategyExecution`, `StopLossProtectedStrategyExecution` |
| Processing-order answer | `StrategyExecution.is_processing_order()` |
| Flatten | `StrategyExecution.request_flatten()`, `flatten_wanted()` |
| Park in ERROR | `StrategyExecution.quarantine()`, `release_quarantine()`, `is_quarantined()` |
| \[ round-trip count \] | `StrategyExecution` state; `stores.py` · `DailyStore` (`round_trips`) |
| Build from the execution block | `make_execution()`, `EXECUTION_CLASSES`, `options_from_config()`, `ExecutionConfigError` |

### Skeleton code

| Piece | Role |
|----|----|
| `EXECUTION_CLASSES` | Every execution class by name (subclasses register themselves). |
| `make_execution(...)` | The one plug point: builds the class the slot's execution block names, with the options that class accepts; no block builds the base class. Raises `ExecutionConfigError` on anything invalid. |
| `ExecutionConfigError` | The slot's execution block is invalid; raised while the slot is built, so startup fails before trading. |
| `StrategyExecution` | The base state machine. `TRANSITIONS` lists every legal state change (a subclass extends it). |
| `ProtectedStrategyExecution` | Requires `resting_exits`: a non-empty map of exit-condition names to STP or LMT, at most one of each. Each name must be in the signal's `exit_condition_names()`. |
| `StopLossProtectedStrategyExecution` | Requires a level name and an offset in points. The level must be a node of the signal (`has_level()`), not a condition (`condition_names()`). |

### Major state of the base class

| State | What it holds |
|----|----|
| `signal`, `om` | The slot's signal (entry/exit answers, params, freeze/release) and Order Manager (sends orders, answers queries). |
| `state`, handlers | The current phase and `{phase: on_<phase>(bar) → next phase}`. |
| `round_trips`, `trading_day`, `roundtrip_store` | Round trips completed today (DONE at the daily cap); the session date that count belongs to; the store that persists it. |
| `trade_closed` | Set on an exit fill; Strategy reads it through `consume_trade_closed()`. |
| `entry_order_bar_ts` | The bar that submitted the entry order; Strategy anchors the entry timestamp to it. |

**Full skeleton code (python)**

``` python
"""Skeleton contract for StrategyExecution (Alchemist v3)."""

from __future__ import annotations

from typing import Callable

EXECUTION_CLASSES: dict[str, type["StrategyExecution"]] = {}

class ExecutionConfigError(ValueError): ...

def make_execution(execution_config: dict | None, signal, om, **kw) -> "StrategyExecution":
    """The one plug point (decision 21)."""

class StrategyExecution:
    TRANSITIONS: dict[str, set[str]]

    def __init_subclass__(cls, **kw):
        EXECUTION_CLASSES[cls.__name__] = cls

    @classmethod
    def options_from_config(cls, options: dict) -> dict: ...

    def on_bar(self, bar: dict, step_signal: Callable[[], None]) -> None:
        """Behaviours 1, 3-5, 8, 10, 12."""
    def on_order_update(self, update: dict) -> None:
        """Behaviours 1, 5-7, 11."""
    def is_processing_order(self) -> bool: ...          # Behaviour 2
    def is_quarantined(self) -> bool: ...               # Behaviour 10
    def quarantine(self, reason: str) -> None: ...
    def release_quarantine(self) -> None: ...
    def request_flatten(self, position: dict, reason: str) -> None: ...  # Behaviour 11
    def flatten_wanted(self) -> bool: ...               # Behaviour 12
    def consume_trade_closed(self) -> bool: ...
    def set_warmup_ready(self) -> None: ...
    def set_aux_ready(self) -> None: ...

class ProtectedStrategyExecution(StrategyExecution):
    """Behaviours 3-7; adds PROCESSING_PROTECTION_ORDERS, IN_POSITION_PROTECTED."""
    resting_exits: dict[str, str]     # exit condition name -> 'STP' | 'LMT'

class StopLossProtectedStrategyExecution(ProtectedStrategyExecution):
    """Decision 22: one STP at level(name) +/- offset_points; masks nothing."""
    level_name: str
    offset_points: float
```

### Decisions already made

### 2. One class or several?

**Answer:** Subclasses. `ProtectedStrategyExecution`, built only for slots whose execution block names it (decision 21), owns the broker group and the mask. The base class takes no resting exits and masks nothing: masking without a broker order is never safe, so it lives only where the order is placed.

### 3. How does outside code learn the phase?

**Answer:** One question, `is_processing_order()`, reached only through Strategy; plus `is_quarantined()`. Safety's repair picks slots by their submitted or open orders, asking `is_processing_order()` only to decide whether a slot with no orders deserves a warning.

**Rejected:** Rejected: reading state names, which a subclass's new phases would silently escape; order questions on this component — they belong to Order Manager.

### 4. How does it learn that a group is closed?

**Answer:** It asks Order Manager directly.

**Rejected:** Rejected: Strategy relaying an order context, and tracking orders itself.

### 5. Which sample?

**Answer:** The system golden sample for the base class plus a component-level protected sample. The join-vs-fill race lives here, not in Order Manager.

### 6. A protective leg rejected or cancelled by the broker without our request?

**Answer:** No retry. The trade falls back to bar-close market exits for every condition; its market exits still join the group while a leg survives, and go out plain once every leg is dead.

**Rejected:** Rejected: a live retry (cancel the rest, wait, place a fresh group) — more states for a rare event.

### 7. Is a new group ever placed in a trade?

**Answer:** No. A market exit joins the working group instead (decision 16). Moving an existing working order into a group is refused by IBKR (errors 10326/10327) and is not used.

### 8. Stop already crossed when protection would be placed?

**Answer:** Place no group; the trade runs unprotected and exits through the normal bar-close market exit.

**Rejected:** Rejected: an immediate market exit.

### 9. Entry partially filled?

**Answer:** Place protection only once the entry order is closed, sized to its total filled quantity; nothing for a zero fill.

### 10. Which conditions are masked?

**Answer:** Selected conditions, and only in IN_POSITION_PROTECTED (every leg OPEN). While placing and after a fallback, every condition is judged at bar close; a market exit there is safe because it joins the group.

### 11. Who enforces "an exit never exceeds the position"?

**Answer:** StrategyExecution, from Order Manager's filled quantities. Order Manager sends what it is given.

### 13. Does StrategyExecution hold the signal?

**Answer:** Yes ([System map decision 20](SYSTEM_MAP.md)). It asks for entry or exit in the phase that needs the answer, reads its params through the signal, freezes and releases exit inputs, and logs the inputs with each decision.

### 14. Where do per-condition exit answers and levels come from?

**Answer:** From the signal: `evaluate_exit_conditions()` returns each named condition's answer; `exit_condition_level(name)` returns the price the condition compares the close with. The entry price behind a level is the broker's average fill price. The protected class reads each selected level on the trade's first bar in position and stores none; a missing level places no group. The condition's direction and the slot's side give the leg's side; the Gateway rounds to tick.

**Rejected:** Rejected: forcing selected conditions false inside the signal (it loses the fact that the condition really fired).

### 15. A selected and a non-selected condition both fire on the same bar?

**Answer:** No market exit that bar: the selected condition means price crossed its level, so the broker order has exited or is exiting. A market exit on top would be refused rather than exit twice (decision 16), so this is a preference, not the safety rule. If the broker order did not fill, the non-selected condition is judged again next bar.

### 16. How does a market exit avoid exiting twice while legs are working?

**Answer:** The MKT carries the trade's OCA group (type 2). Verified on an IBKR paper account: into a working group it filled and the broker stopped both legs first; into a group whose leg had filled, the broker refused it (Inactive + error 201), also when sent 0.2 ms after a marketable sibling — 5 of 5. This replaces cancel → wait for final → market and needs no closing state.

*Not tested: the MKT reaching the broker before a sibling fills at the exchange; accepted as very unlikely.*

### 17. Which phases does the protected class add?

**Answer:** Two: PROCESSING_PROTECTION_ORDERS and IN_POSITION_PROTECTED. The base states keep their meaning; no closing state.

### 18. After a fallback, is the surviving leg left working or cancelled?

**Answer:** Left working. It needs no cancel, no cancel outcome and no cancel-vs-fill race, and keeps a broker-side order in place. Later market exits join the group, so nothing exits twice; with no surviving leg they go out plain.

### 19. How does a leg in the broker's pending-cancel state count?

**Answer:** It never reaches this component: the Gateway drops pending-cancel, and Order Manager enters CANCELLING only for a cancel the slot sent. An unrequested cancel is seen on its final Cancelled, which triggers the fallback. A CANCELLING leg is not open.

### 20. Is the session-end flatten a signal exit?

**Answer:** No. Signal exits come from the signal (the protected class joins the OCA group); the flatten comes from outside the strategy (cancel all → wait for final → MKT → DONE, the same for every class). A rejected flatten order is not retried; the shutdown flat check is the backstop.

### 21. How does a slot pick its execution class?

**Answer:** Explicitly, by name, in the slot's execution block: `execution: {class: ProtectedStrategyExecution, resting_exits: {...}}`. The other keys are that class's options; no block builds the base class. Anything invalid fails startup naming the slot.

**Rejected:** Rejected: inferring the class from an option; dropping an invalid block with a log line — a slot believed protected at the broker but not is exactly the risk the feature exists to remove.

### 22. A disaster stop without masking?

**Answer:** `StopLossProtectedStrategyExecution`: one STP at a named level ± an offset in points. Every exit stays a bar-close market order that joins the group; the stop is there for when the server is down.

### 23. What does StrategyExecution read from the signal besides its answers?

**Answer:** Only public methods: `entry_conditions()` for the entry log; `exit_condition_names()`, `condition_names()` and `has_level()` to check the execution block at startup; `params` for session, direction and the daily quota. It never reads the signal's private state ([StrategySignal decision 8](STRATEGY_SIGNAL_MAP.md)).

**Rejected:** Rejected: reading the signal's logic structure or value cache directly.

*agreed 2026-11-09 · v3 · 2026-11-07*

Decisions 1 and 12 are not used; the numbers are kept so references stay valid.
