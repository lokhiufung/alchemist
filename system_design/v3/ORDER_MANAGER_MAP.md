# ORDER_MANAGER_MAP — Order Manager · Alchemist v3

The slot's order clerk. Order Manager builds every order, sends each order to the Gateway exactly once, and follows each order from submission to a closed status, so the slot can always ask which orders are the slot's own, which orders still work, and how much each order filled.

Frozen contract for the Order Manager component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `order_manager_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-09.** **v3 final, 2026-11-09 — agreed.** The End-to-end scenarios in Tests were agreed on 2026-11-09. Features 1–5 and decisions 2–16 are settled. Multi-asset: every order carries its instrument (System map decision 37). Change history: see `order_manager_map.html`.

## Goal and scope

**What it is.** Order Manager is the order clerk of one strategy slot. Each slot has one Order Manager. Order Manager builds every order the slot sends, sends each order to the Gateway exactly once, and tracks each order from submission to a closed status.

**Why it exists.** Every slot sees every broker order update, and several slots can trade the same instrument. StrategyExecution must decide on facts ("my stop filled", "my group is closed"), not on raw broker messages. Order Manager owns the order bookkeeping, so that StrategyExecution and Strategy ask one place which orders the slot owns, which orders still work, and how much each order filled ([System map decision 14](SYSTEM_MAP.md)).

**Goal.** Every order a slot sends is built correctly, reaches the Gateway exactly once, and is tracked from submission to a closed status, so the slot can always ask which orders the slot owns, which orders still work, and how much each order filled.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Other components call Order Manager | StrategyExecution | Place an order; place a group of protective orders; cancel one order, a group, or every owned order | order request (type, side, quantity, instrument, price); group request; order id or group name |
|  | Strategy | Forward a broker order update; ask whether an order is owned | order update → changed order with fill delta, or nothing |
|  | Strategy, StrategyExecution | Ask for an order; list submitted, open and cancelling orders (of the slot or of one group); list a group's orders; ask whether a group is closed | order id, group name → orders, flag |
| Order Manager calls other components | Gateway | Place an order; cancel an order | order with id, account, instrument, slot tag, group; order id |

### Scope

Owns, from the System map:

> The life of every order the slot sends: building it, tracking it from submission to a closed status, knowing which orders the slot owns, and sending cancels.

| Not Order Manager's job | Owned by |
|----|----|
| Deciding to enter or exit; what an order event means, including the policy when the broker rejects or cancels an order | [StrategyExecution](STRATEGY_EXECUTION_MAP.md) |
| The position, and the rule that exit quantity never exceeds the position. Order Manager sends the quantity Order Manager is given. | [Strategy](STRATEGY_MAP.md), [StrategyExecution](STRATEGY_EXECUTION_MAP.md) |
| Broker communication | [Gateway](GATEWAY_MAP.md) |

### Place in the system

Drawn in the HTML page; the flows are the Public interface table above and the [SYSTEM_MAP.md](SYSTEM_MAP.md) Diagram.

Order updates reach Order Manager through Strategy, not straight from the Gateway: every slot sees every update, and Order Manager returns nothing for an order the slot does not own.

## Features

Each feature says which component needs it and why, what the upstream can give, and how Order Manager closes the gap. Every number is a parameter with its supported values.

### Algorithm map

```
  ( StrategyExecution )
        │
        │ order request
        ▼
  ┌─────────────────────────┐
  │ Stamp order             │
  │ rule: id, account, slot │
  └─────────────────────────┘
        │
        │ order
        ▼
  ┌─────────────────────────┐
  │ Name group              │
  │ rule: fresh group name  │
  └─────────────────────────┘
        │
        │ order
        ▼
  ┌─────────────────────────┐
  │ Send once               │
  │ rule: one send per id   │
  └─────────────────────────┘
        │
        ├── writes ──▶ [ stores by order id: submitted · open · cancelling · closed ]
        │
        │ order
        ▼
  ( Gateway )

  ( StrategyExecution )
        │
        │ cancel
        ▼
  ┌─────────────────────────┐
  │ Pick open orders        │
  │ rule: open only         │
  └─────────────────────────┘
        │
        ├── reads ──▶ [ stores by order id ]
        │
        │ open order ids
        ▼
  ┌─────────────────────────┐
  │ Send cancel             │
  │ rule: open → CANCELLING │
  └─────────────────────────┘
        │
        │ cancel
        ▼
  ( Gateway )

  ( Strategy )
        │
        │ order update
        ▼
  ┌─────────────────────────┐
  │ Match owned order       │
  │ rule: unowned → nothing │
  └─────────────────────────┘
        │
        │ owned update
        ▼
  ┌─────────────────────────┐
  │ Apply update            │
  │ rule: closed = terminal │
  └─────────────────────────┘
        │
        ├── writes ──▶ [ stores by order id ]
        │
        │ changed order + fill delta
        ▼
  ( Strategy )

  ( Strategy, StrategyExecution )
        │
        │ query
        ▼
  ┌─────────────────────────┐
  │ Answer query            │
  │ rule: group: all closed │
  └─────────────────────────┘
        │
        ├── reads ──▶ [ stores by order id ]
        │
        │ answer
        ▼
  ( Strategy, StrategyExecution )
```

Main path in colour: an order is stamped and sent once; an update is matched and applied. Dashed accent lines write to and read from the four stores. Order state: `SUBMITTED` ─open─▶ `OPEN` ─cancel sent─▶ `CANCELLING` ─CANCEL_REJECTED─▶ `OPEN`; `SUBMITTED`, `OPEN` or `CANCELLING` ─Filled, Cancelled, Rejected─▶ closed.

### Feature cards

### 1. Every order is sent once and owned *(was behaviour 1)*

#### Need

StrategyExecution sends entry and exit orders and must never send one order twice. Every slot sees every update, so the slot must know which orders are the slot's own.

#### Upstream gives

StrategyExecution gives an order request: type, side, quantity, instrument, and a stop or limit price for STP and LMT. The Gateway gives the next order id ([Gateway feature 4](GATEWAY_MAP.md)).

#### Algorithm

1.  Take the next order id from the Gateway (`next_order_id()`); stamp the id, the slot's account, the instrument and the slot tag.
2.  Record the order in the submitted store as owned by the slot, before the send, so an update that arrives at once finds its owner.
3.  Send the order with that id to the Gateway exactly once. The Gateway does not number the order again.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Order types | Fixed for v3 ([System map decision 56](SYSTEM_MAP.md)) | MKT, STP, LMT | — |
| Quantity per entry | Fixed by [System map decision 24](SYSTEM_MAP.md) | 1 unit | 1 unit |

#### Invariant

Every order the Gateway receives from this slot is owned by this slot, and no order id is sent twice.

### 2. A group shares one fresh name *(was behaviour 2 · decision 9)*

#### Need

StrategyExecution places protective exits (a stop and a limit) that must cancel each other at the broker. When one leg fills, the broker must cancel the other leg.

#### Upstream gives

StrategyExecution gives a group request of one or more orders. The broker links orders that carry the same OCA group name and OCA type, and refuses a group name longer than 48 characters (IBKR error 401).

#### Algorithm

1.  Make a group name that no earlier group of this slot used, at most 48 characters for every slot name length.
2.  Give every order of the group that name, the OCA type and the time in force.
3.  Submit each order as in feature 1.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| OCA type | Fixed by decision 9 | 2 | 2 |
| Group time in force | Fixed by decision 9 | DAY | DAY |
| Group name length limit | Fixed (IBKR refuses longer names) | 48 characters | 48 |

#### Invariant

Orders of one group always share one name. A group name is never reused, even after the group is closed.

### 3. Each execution is applied once *(was behaviour 3 · decisions 6, 8, 15)*

#### Need

Strategy books the position from Order Manager's changes and must book each fill exactly once. StrategyExecution must see a cancelling order as cancelling, not open.

#### Upstream gives

Strategy forwards every broker order update, including updates for other slots' orders, duplicates, and updates that arrive after the order closed. The Gateway drops the broker's pending-cancel status ([Gateway feature 5](GATEWAY_MAP.md)).

#### Algorithm

1.  For an owned order that is not closed: apply each partial or full execution to the cumulative filled quantity and average price, and return the updated order with this update's fill delta.
2.  On Filled, Cancelled or Rejected, close the order from submitted, open or cancelling, keeping any quantity filled before.
3.  On CANCEL_REJECTED, move a cancelling order back to open.
4.  A Cancelled that the slot did not request (a broker or OCA cancel) closes an open order directly.
5.  Return nothing for an update that is not owned, arrives after the order closed, or changes nothing. Apply no policy to rejections or system cancels.
6.  Log a cancel or reject only in the slot that owns the order, and tag every log line with the slot's name.

#### Parameters

None.

#### Invariant

A closed status is terminal. An order's filled quantity never decreases and is never counted twice. A cancelling order is never reported open.

### 4. Ownership and status on request *(was behaviour 4 · decisions 3, 7, 11, 12, 16)*

#### Need

Strategy must filter updates by ownership. StrategyExecution must know its submitted, open and cancelling orders, each order's fill, and when a group is closed, to drive its state machine.

#### Upstream gives

The four stores, kept by features 1, 3 and 5.

#### Algorithm

1.  Answer ownership: an update for an order the slot does not own returns nothing; a request for an order the slot never sent returns nothing.
2.  Report an owned order's status, filled quantity and average price; closed orders stay queryable for the session.
3.  List the slot's or one group's submitted, open and cancelling orders, with their filled quantity.
4.  List every order of a group in any store, and say whether the group is closed.

#### Parameters

None.

#### Invariant

A group's orders are exactly the owned orders that carry its group name, however the orders were sent. A group is closed only when every one of its orders is closed.

### 5. Cancels go only to open orders *(was behaviour 5 · decisions 13, 14)*

#### Need

StrategyExecution cancels one order, a group, or every owned order, and must not cause a second cancel or a cancel for an order the broker has not acknowledged.

#### Upstream gives

A cancel request by order id, by group name, or for every owned order.

#### Algorithm

1.  For each order in scope that is open: send one cancel to the Gateway and move the order to cancelling.
2.  For one order by id that is not open (unknown, submitted, cancelling or closed): send nothing, change nothing, log a warning, and return CANCEL_REJECTED_INTERNALLY.
3.  A group or every-order cancel skips orders that are not open.

#### Parameters

None.

#### Invariant

A cancel request never closes an order; only the broker's closed status closes an order (feature 3). A submitted order is never cancelled.

## Tests

The goal and features as measurable cases. A test with parameter cases runs once per supported value. No Order Manager test exists in v3 yet; every row is not written yet.

**End to end · Sample G, the system golden sample (1 test)**

|  |  |
|----|----|
| Test | `test_golden_two_market_orders` |
| Input | The two orders of the golden run for MES: a BUY 1 MES MKT request, then broker updates Submitted and Filled 1 at the bar close; later a SELL 1 MES MKT request, then Submitted and Filled 1 ([System map golden sample](SYSTEM_MAP.md)). |
| Passes when | Each order id came from `next_order_id()` and was recorded before the send; the Gateway received exactly two orders, each once; both are reported closed, Filled with filled 1; the working-order answer is empty. |
| Fails when | An order is sent to the Gateway twice; an order is still reported working after its Filled; or a second Filled update for an already filled order is returned as a change. |

**End to end · Sample C, component-level group and cancel (1 test)**

|  |  |
|----|----|
| Test | `test_group_cancel_race_fill_wins` |
| Input | \(a\) BUY 1 MKT, Submitted, Filled 1 @ P. (b) A group of STP SELL 1 and LMT SELL 1, OPEN for both. (c) Cancel the group; LMT Cancelled (0 filled), later STP Filled 1. (d) Cancel the filled entry. |
| Passes when | Three orders were sent; the two group orders share one fresh name with OCA type 2 and DAY. Right after (c) both group orders are cancelling and the group is not closed; after LMT Cancelled the group is still not closed; after STP Filled the group is closed, STP filled 1, LMT filled 0, nothing open or cancelling. (d) returns CANCEL_REJECTED_INTERNALLY and sends nothing. |
| Fails when | The group or an order is reported closed because of the cancel request alone, or after only one order's closed status; a cancelling order is reported open; a cancel is sent for an order that is not open; the two orders carry different names; or the name equals an earlier name. |

### End-to-end scenarios *(agreed 2026-11-09)*

Each scenario drives the component only through its public interface, from a known start state, and checks one exact, observable output. A scenario tells one story across several features; the feature tables below keep the single rules. File: `tests/order_manager/test_order_manager_scenarios.py`. A Gateway fake gives order ids and records every order and cancel; the test plays Strategy (order updates, ownership checks) and StrategyExecution (orders, cancels, queries).

**S1. Market order life (features 1, 3, 4)**

|  |  |
|----|----|
| Test | `test_scenario_market_order_life` |
| Given | A slot with account SIM and slot tag T. |
| When | Submit BUY 1 MKT; send Submitted, then Filled 1 @ P, then Filled again. |
| Parameter cases | — |
| Passes when | One send, with the Gateway's id, account SIM, the instrument and tag T; the order is in the submitted store before the send. Filled: the order is closed with filled quantity 1 at P, and the change carries a fill delta of 1. The repeated Filled returns nothing. The status query answers FILLED, 1, P. |
| Fails when | Two sends; the order not owned when the first update arrives; a change returned for the repeat. |

**S2. Limit order cancelled (features 3, 5)**

|  |  |
|----|----|
| Test | `test_scenario_limit_order_cancel` |
| Given | A LMT order, OPEN. |
| When | Cancel it; send Cancelled (case cancelled) or CANCEL_REJECTED (case refused). |
| Parameter cases | case ∈ {cancelled, refused} |
| Passes when | One cancel sent; the order is cancelling. Cancelled: closed with filled 0. Refused: open again. |
| Fails when | Two cancels; a cancelling order reported open; a refused cancel closes the order. |

**S3. Cancel of an order that is not open (features 5)**

|  |  |
|----|----|
| Test | `test_scenario_cancel_not_open` |
| Given | Orders in each state of the case. |
| When | Cancel each by id. |
| Parameter cases | state ∈ {unknown id, submitted, cancelling, closed} |
| Passes when | Each call returns CANCEL_REJECTED_INTERNALLY, sends nothing, changes nothing and logs one warning. |
| Fails when | A cancel sent; a state change. |

**S4. Group with a fill race (Sample C) (features 2, 3, 4, 5)**

|  |  |
|----|----|
| Test | `test_group_cancel_race_fill_wins` |
| Given | See the end-to-end Sample C above. |
| When | As Sample C, plus three groups in a row. |
| Parameter cases | slot name length ∈ {1, 40, 100} characters |
| Passes when | As Sample C; the three group names differ and each has at most 48 characters. |
| Fails when | As Sample C; a repeated name; a name over 48 characters. |

**S5. A cancel the slot did not ask for (features 3)**

|                 |                                                 |
|-----------------|-------------------------------------------------|
| Test            | `test_scenario_unrequested_cancel`              |
| Given           | An OPEN leg of a group.                         |
| When            | Send Cancelled for it (the OCA sibling filled). |
| Parameter cases | —                                               |
| Passes when     | The leg is closed directly, with filled 0.      |
| Fails when      | The leg stays open or moves to cancelling.      |

**S6. Updates for other slots are ignored (features 3, 4)**

|  |  |
|----|----|
| Test | `test_scenario_foreign_update` |
| Given | Two slots, each with one open order. |
| When | Send slot 2's Filled to slot 1. |
| Parameter cases | — |
| Passes when | Slot 1 returns nothing, logs nothing and changes nothing; an ownership check for that id answers not owned. |
| Fails when | Slot 1 changes or logs anything. |

**S7. A rejected order closes (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_rejected_order` |
| Given | A submitted BUY 1 MKT. |
| When | Send Rejected. |
| Parameter cases | — |
| Passes when | The order is closed with filled 0; one log line in the owning slot only; no retry is sent. |
| Fails when | A retry; a log line in another slot. |

**1. Every order is sent once and owned (1 test · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_order_sent_once_and_owned` | type ∈ {MKT, STP, LMT} | sends per order id; ownership answer | 1; owned |

**2. A group shares one fresh name (2 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_group_name_fresh_and_at_most_48_chars` | slot name length ∈ {8, 100} characters | name length; reuse across 1000 groups | ≤ 48; 0 reuses |
| `test_group_orders_share_name_oca_type_and_day` | group size ∈ {1, 2} | distinct names; OCA type; time in force | 1; 2; DAY |

**3. Each execution is applied once (5 tests · 13 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_partial_then_full_fill_accumulates_and_duplicate_ignored` | — | filled quantity; average price; change returned for the duplicate | sum of fills; size-weighted mean; none |
| `test_closed_status_closes_from_any_store` | status ∈ {Filled, Cancelled, Rejected} × from ∈ {submitted, open, cancelling} | store after the update | closed; earlier fill kept |
| `test_cancel_rejected_returns_order_to_open` | — | store after CANCEL_REJECTED | open |
| `test_unrequested_cancel_closes_open_order` | — | store after a broker Cancelled | closed |
| `test_cancel_or_reject_logged_only_by_owning_slot` | — | log lines in a slot that does not own the order | 0 |

**4. Ownership and status on request (3 tests · 5 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_update_for_unowned_order_returns_none` | — | return value | None |
| `test_group_closed_only_when_all_closed` | closed orders ∈ {0, 1, 2} of 2 | group closed answer | False; False; True |
| `test_closed_order_stays_queryable` | — | order returned after close | status, filled, average price |

**5. Cancels go only to open orders (3 tests · 6 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_cancel_of_non_open_order_rejected_internally` | state ∈ {unknown, submitted, cancelling, closed} | return value; cancels sent | CANCEL_REJECTED_INTERNALLY; 0 |
| `test_group_cancel_skips_non_open_orders` | — | cancels sent | one per open order |
| `test_cancel_request_never_closes_order` | — | store right after the request | cancelling |

## Open questions

All decided on 2026-11-09.

1.  **Order types (feature 1).** Same answer as the Gateway: MKT, STP and LMT, fixed for v3 ([System map decision 56](SYSTEM_MAP.md)).

## Reference

Implementation detail and history. Feature numbers equal the old behaviour numbers.

### Step → code

All steps are in `managers/order_manager.py`.

| Step | Code |
|----|----|
| Stamp order, Send once | `OrderManager.place_order()` |
| Name group | `OrderManager.place_group()` |
| Pick open orders, Send cancel | `OrderManager.cancel_order()`, `cancel_group()`, `cancel_all()` |
| Match owned order, Apply update | `OrderManager.on_order_update()` |
| Answer query | `OrderManager.get_order()`, `get_submitted_orders()`, `get_open_orders()`, `get_cancelling_orders()`, `get_order_group()`, `is_group_closed()` |
| \[ four stores \] | `OrderManager` state; statuses in `OrderState` |

### Skeleton code

### Statuses

| Status | Meaning |
|----|----|
| SUBMITTED, OPEN, PARTIALLY_FILLED | Broker order statuses as Order Manager sees them. |
| CANCELLING | A cancel was sent for an open order; no answer yet. |
| FILLED, CANCELLED, REJECTED | The closed statuses. |
| CANCEL_REJECTED | The broker refused our cancel; the order is open again. |
| CANCEL_REJECTED_INTERNALLY | `cancel_order`'s answer for an id that is not open — never an order's status. |

### Major state

| State | What it holds |
|----|----|
| `gateway` | Where orders and cancels are sent. |
| `account_id` | The sub-account stamped on every order (long/short routing). |
| `slot` | The slot tag stamped on every order and log line. |
| the four stores | Submitted, open, cancelling, closed — by order id. |

**Full skeleton code (python)**

``` python
"""Skeleton contract for Order Manager (Alchemist v3)."""

from __future__ import annotations

from enum import Enum

from alchemist.instruments import Instrument

OCA_GROUP_MAX_LEN = 48

class OrderState(str, Enum):
    SUBMITTED = "SUBMITTED"; OPEN = "OPEN"; PARTIALLY_FILLED = "PARTIALLY_FILLED"
    CANCELLING = "CANCELLING"; CANCEL_REJECTED = "CANCEL_REJECTED"
    FILLED = "FILLED"; CANCELLED = "CANCELLED"; REJECTED = "REJECTED"

CLOSED = {OrderState.FILLED, OrderState.CANCELLED, OrderState.REJECTED}
CANCEL_REJECTED_INTERNALLY = "CANCEL_REJECTED_INTERNALLY"

class OrderManager:
    def __init__(self, gateway, account_id: str, slot: str): ...

    # Behaviours 1-2
    async def place_order(self, instrument: Instrument, side: str, qty: int,
                          order_type: str, stop_price: float | None = None,
                          limit_price: float | None = None,
                          oca_group: str | None = None) -> dict: ...
    async def place_group(self, orders: list[dict]) -> str:
        """One fresh OCA type 2 DAY group; returns its name."""

    # Behaviour 3
    def on_order_update(self, update: dict) -> dict | None:
        """The changed order with its fill delta, or None."""

    # Behaviour 4
    def get_order(self, order_id: int) -> dict | None: ...
    def get_submitted_orders(self, group: str | None = None) -> list[dict]: ...
    def get_open_orders(self, group: str | None = None) -> list[dict]: ...
    def get_cancelling_orders(self, group: str | None = None) -> list[dict]: ...
    def get_order_group(self, group: str) -> list[dict]: ...
    def is_group_closed(self, group: str) -> bool: ...

    # Behaviour 5
    async def cancel_order(self, order_id: int) -> str | None: ...
    async def cancel_group(self, group: str) -> None: ...
    async def cancel_all(self) -> None: ...
```

### Decisions already made

### 2. Which sample?

**Answer:** The system golden sample for the market-order path, plus a component-level group/cancel sample.

**Rejected:** Rejected: a cancel-vs-fill race sample. The race is about state changes at the StrategyExecution level, not a behaviour that a test of Order Manager alone can check.

### 3. How does StrategyExecution learn that a group is closed?

**Answer:** It asks Order Manager directly (it already holds it to send orders).

**Rejected:** Rejected: StrategyExecution tracking orders itself (a second lifecycle tracker that can disagree with this one).

### 4. Who enforces "exit quantity never exceeds the position"?

**Answer:** Strategy / StrategyExecution.

**Rejected:** Rejected: an Order Manager guard. Order Manager sends what it is given.

### 5. How much of the lifecycle is tracked?

**Answer:** Per execution: cumulative filled quantity and average price. Closed only on Filled / Cancelled / Rejected; a closed status keeps the quantity filled before it.

**Rejected:** Rejected: closed-status-only tracking (partial fills go unbooked until closed).

### 6. What does Order Manager do when the broker rejects or cancels an order in a group?

**Answer:** Nothing beyond reporting the closed status; the policy belongs to StrategyExecution.

### 7. Do closed orders stay queryable?

**Answer:** Yes, for the session — needed to report a group closed with each order's filled quantity.

### 8. How does Strategy avoid double-booking?

**Answer:** `on_order_update` returns the changed order with its fill delta, or nothing when the update is ignored (not owned, after closed, no change). Strategy books only what is returned.

### 9. Group order attributes?

**Answer:** One fresh group name per group, never reused; OCA type 2; DAY time in force. The name is the slot name cut to 24 characters, `-oca-`, and 12 hex characters of a hash of slot, clock and sequence — at most 48 characters for any slot name.

### 11. How do callers see the slot's orders?

**Answer:** Through public getters over the stores — `get_submitted_orders()`, `get_open_orders()`, `get_cancelling_orders()`, each with an optional group.

**Rejected:** Rejected: a merged "working orders" answer; callers reading the stores directly.

### 12. What is a group's membership?

**Answer:** Each order's own group name across the stores, so an order that joins a group through `place_order` (a market exit) counts. A group whose orders are all closed is closed.

**Rejected:** Rejected: a separate per-group id list kept by `place_group` (it misses such orders).

### 13. What does a cancel do to an order?

**Answer:** An open order moves to cancelling (listed by `get_cancelling_orders()`, not by `get_open_orders()`), and leaves it on Cancelled, Filled (the fill won the race) or Rejected, or on CANCEL_REJECTED (open again).

**Rejected:** Rejected: a cancel-rejected order keeping status CANCEL_REJECTED — it is alive or about to fill, so it is open.

### 14. Which orders can be cancelled?

**Answer:** Open orders only. Anything else is refused with CANCEL_REJECTED_INTERNALLY. Group and every-order cancels skip non-open orders. The shutdown coordinator defers a flatten while an order is in flight; the final flat check stays the backstop.

### 15. The broker's pending-cancel status?

**Answer:** Not a status Order Manager sees: the Gateway drops it, and a cancel is confirmed only by the final Cancelled. A cancel the slot did not request leaves the order open until its Cancelled closes it.

### 16. Do outside callers read the cancelling store?

**Answer:** No. Safety's repair selects slots by submitted or open orders.

*Accepted cost: if the socket drops while an order is cancelling and its final status is lost, the order stays cancelling for the session; under the join design (StrategyExecution decision 16) no exit waits on it.*

Decisions 1 and 10 are not used; the numbers are kept so references stay valid.
