# AUX_DATA_MAP — Aux Data · Alchemist v3

The engine's second input. Aux Data reads daily auxiliary series from outside data sources, decides when a value is settled for the next session, and tells each strategy that reads a series whether the strategy may trade.

Frozen contract for the Aux Data component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `aux_data_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-09.** **v3 final, 2026-11-09 — agreed ([System map decision 44](SYSTEM_MAP.md)).** The End-to-end scenarios in Tests were agreed on 2026-11-09. Features 1–6 carry over the auxiliary and arming rules that the Event Loop map held before; features 7–9 are new interface rules. Change history: see `aux_data_map.html`.

## Goal and scope

**What it is.** Aux Data is the engine's second input, beside the Gateway. Aux Data reads daily auxiliary series (values that are not broker prices) from outside data sources, decides when a value is settled for the session a strategy will trade, and tells each strategy that reads a series whether that strategy may trade. One resident task steps every arm group; the Event Loop calls Aux Data once per bar; Strategy calls Aux Data when Strategy rebuilds a signal.

**Why it exists.** Some signals read a daily series that an outside source publishes, sometimes late, sometimes provisional, on the clock of a market other than the broker's. The Gateway talks to the broker only ([System map decision 7](SYSTEM_MAP.md)), and the Event Loop must stay narrow (bars, dispatch, broker history). Aux Data owns the outside sources, their failure modes and their arming clock, so that no strategy trades on an auxiliary value that was not settled ([System map decision 44](SYSTEM_MAP.md)).

**Goal.** Give every strategy that reads an auxiliary series a value the strategy can trust for the session it will trade, before that session opens, and keep that value fresh across day boundaries.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Other components call Aux Data | Event Loop | Annotate a completed bar before dispatch | bar → bar + auxiliary values |
|  | Strategy | Ask for the history frames of an instrument during a cold rebuild | instrument → frames |
|  | Runner | Start and stop the resident task | mode, replay date |
| Aux Data calls other components | Outside data sources | Fetch a series | name, target date → rows |
|  | Strategy | Give a member its frames to seed; tell the member it is ready or not ready | frames, ready flag |
|  | Event Loop (runtime book) | Read the current members on each step | strategies and the names they read |

### Scope

Owns, from the System map:

> Reading auxiliary daily series from outside data sources, each through one data-source interface; deciding when a value is settled for its target session; arming and disarming each group of strategies that read them; adding the values to bars; and serving the history frames a cold rebuild needs ([System map decision 44](SYSTEM_MAP.md)).

| Not Aux Data's job | Owned by |
|----|----|
| Broker history (daily and prior-session bars) | [Gateway](GATEWAY_MAP.md), requested by the [Event Loop](EVENT_LOOP_MAP.md) |
| Delivering bars to strategies | [Event Loop](EVENT_LOOP_MAP.md) |
| When a signal may be rebuilt; telling StrategyExecution that the slot is ready | [Strategy](STRATEGY_MAP.md) |
| How a calculation is seeded from the frames | [StrategySignal](STRATEGY_SIGNAL_MAP.md) |

### Place in the system

Drawn in the HTML page; the flows are the Public interface table above and the [SYSTEM_MAP.md](SYSTEM_MAP.md) Diagram.

Aux Data reads the runtime book's members on each step; the Event Loop owns the runtime book ([Event Loop feature 11](EVENT_LOOP_MAP.md)).

## Features

Each feature says which component needs it and why, what the outside sources can give, and how Aux Data closes the gap. Every number is a parameter with its supported values.

### Algorithm map

```
  ( Event Loop )
        │
        │ members (runtime book)
        ▼
  ┌─────────────────────────┐
  │ Group by arm session    │
  │ rule: profile session   │
  └─────────────────────────┘
        │
        ├── writes ──▶ [ arm groups ]
        │
        │ groups
        ▼
  ┌─────────────────────────┐
  │ Fetch value             │
  │ rule: backup: confirmed │
  └─────────────────────────┘
        │
        ├── reads ──▶ [ clock: every step ]
        ├── fetch (name, date) ──▶ ( outside data sources )
        │   ◀── rows
        │
        │ target date, columns, rows
        ▼
  ┌─────────────────────────┐
  │ Judge settled           │
  │ rule: target-date row   │
  └─────────────────────────┘
        │
        │ settled value
        ▼
  ┌─────────────────────────┐
  │ Arm group               │
  │ rule: seed, then ready  │
  └─────────────────────────┘
        │
        ├── frames, ready ──▶ ( Strategy )
        │   ◀── seed error
        │
        │ armed group
        ▼
  ┌─────────────────────────┐
  │ Recheck until open      │
  │ rule: hourly; adopt new │
  └─────────────────────────┘
        │
        ├── frames (re-seed) ──▶ ( Strategy )
        │
        │ open passed
        ▼
  ┌─────────────────────────┐
  │ Roll target date        │
  │ rule: new date: disarm  │
  └─────────────────────────┘
        │
        ├── disarm ──▶ [ arm groups ]

  ( Event Loop )
        │
        │ completed bar
        ▼
  ┌─────────────────────────┐
  │ Annotate bar            │
  │ rule: bound names only  │
  └─────────────────────────┘
        │
        │ bar + aux values
        ▼
  ( Event Loop )

  ( Strategy )
        │
        │ frame request (cold rebuild)
        ▼
  ┌─────────────────────────┐
  │ Serve frames            │
  │ rule: current frames    │
  └─────────────────────────┘
        │
        │ history frames
        ▼
  ( Strategy )
```

Main path in colour: members → groups → fetch → judge → arm. In replay, every group resolves at start in one pass (feature 6). Dashed boxes are shared data structures; the dashed accent arrow is the step clock.

### Feature cards

### 1. One interface per outside source *(was behaviour 1 · System map decision 32)*

#### Need

A strategy reads an auxiliary series by name. The strategy must not know which source serves that name, and a new feed or backup must not change any other code.

#### Upstream gives

Replay has files. Live has an outside source that can be down or late, and a backup source that can be wrong when used alone.

#### Algorithm

1.  Pick the source registered for the name: files in replay, the outside source live.
2.  Fill a value missing from the primary source with a backup value only when a second source confirms the backup value.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Source per name | Configurable (registry) | file source; confirmed backup source | — |
| Confirmations a backup value needs | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 1 second source | 1 |

#### Invariant

A new feed or backup is one new source class. An unconfirmed backup value is never used.

### 2. A value counts only when settled for its target *(was behaviour 2)*

#### Need

A strategy that trades a session needs that session's value, not yesterday's value carried forward and not a provisional value.

#### Upstream gives

A source serves rows by date. The row for a target date appears at some time after that market's close, on that market's own clock, and can be missing for a while.

#### Algorithm

1.  Take the group's target date: the next or current trading date of its arm session.
2.  Accept a value only when the row is the one the calendar names for that target, settled on that market's own clock (New York for CME, Frankfurt for Eurex).
3.  While no settled row can exist yet, fetch at most once per target when no frame is loaded, and warn once per change in what the source serves.

#### Parameters

None.

#### Invariant

A carried-forward value never arms a group.

### 3. Arm groups are independent *(was behaviour 3 · decision 3)*

#### Need

Strategies on CME and on Eurex arm for different sessions on different clocks. A Eurex outage must not stop a CME strategy from trading.

#### Upstream gives

The members from the runtime book, and each instrument's exchange profile with its sessions.

#### Algorithm

1.  Group the members by the session they arm for, taken from the exchange profile.
2.  Give each group its own target date, its deadline (that session's open) and its column scope (pairs of instrument and name).
3.  Step every group concurrently, at the step interval.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Step interval | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 60 s | 60 s |

#### Invariant

One group's slow fetch or outage never changes another group's verdict.

### 4. Arm before the open, or sit the session out *(was behaviour 4 · decision 2)*

#### Need

Strategy must not mark a slot ready on a value that was not settled, or on frames that did not seed. A value that arrives after the open must not change a session already running.

#### Upstream gives

A settled value (feature 2) and Strategy's seeding result.

#### Algorithm

1.  Give each member its frames through Strategy. If seeding raises, log CRITICAL and leave the group unarmed.
2.  Then mark the group armed and tell each member the member is ready.
3.  A group not armed by its open sits that session out: no fetch and no late arm for that target. A process started after the open still arms, because the value existed before the open.
4.  When the target date rolls, disarm the group and wait for the next value.

#### Parameters

None.

#### Invariant

No strategy is ready on a value that was not settled, or on frames that did not seed.

### 5. Recheck until the open *(was behaviour 5)*

#### Need

A source can correct a value after the group armed. The strategy must trade the corrected value if the correction arrives before the open.

#### Upstream gives

The same source, which can serve a changed row, fail, or stop serving the target.

#### Algorithm

1.  Refetch at most once per recheck interval while the group is armed and before its open.
2.  Adopt a changed value, re-seed the members and warn.
3.  A recheck that fails, or no longer serves the target, keeps the armed value. A re-seed that raises disarms the group.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Recheck interval | Configurable per process ([System map decision 58](SYSTEM_MAP.md)) | 1 h | 1 h |

#### Invariant

A member never trades on frames built from a value that was replaced.

### 6. Replay resolves at once *(was behaviour 6)*

#### Need

A replay must start with every auxiliary value it needs, and must fail loudly when one is missing.

#### Upstream gives

Files that hold the whole history, and a replay date.

#### Algorithm

1.  Load the files at start.
2.  Resolve every bound name for the replay date, seed and arm the members.
3.  Raise when a value is missing.

#### Parameters

None.

#### Invariant

A replay never runs a day with an auxiliary value silently missing.

### 7. Values on the bar *(was behaviour 7)*

#### Need

A signal reads auxiliary values together with the bar, and every strategy on one instrument must see the same values.

#### Upstream gives

The Event Loop passes each completed bar before dispatch ([Event Loop feature 2](EVENT_LOOP_MAP.md)).

#### Algorithm

1.  Add each auxiliary name bound on the bar's instrument, with its value for the bar's trading date.
2.  Return an instrument with no bound name unchanged.

#### Parameters

None.

#### Invariant

Strategies on one instrument all see the same bar object with the same values.

### 8. Frames for a cold rebuild *(was behaviour 8)*

#### Need

Strategy rebuilds a signal cold while the group is already armed, and the new signal needs the auxiliary frames to seed.

#### Upstream gives

The frames Aux Data already holds for the armed group.

#### Algorithm

1.  Answer a frame request for an instrument with the current history frames.

#### Parameters

None.

#### Invariant

A rebuild never leaves an armed slot on an unseeded calculation.

### 9. Members come from the runtime book *(was behaviour 9 · System map decision 34)*

#### Need

A strategy added while the process runs must get its auxiliary values like any other strategy ([Event Loop feature 11](EVENT_LOOP_MAP.md)).

#### Upstream gives

The runtime book, which the Event Loop changes only between events.

#### Algorithm

1.  Read the members from the runtime book again on each step.
2.  A strategy committed later joins its arm session's group only if that group already covers what the strategy reads; otherwise the strategy gets its own group.
3.  A crash of the resident task logs CRITICAL.

#### Parameters

None.

#### Invariant

Aux Data keeps no copy of the strategy list.

## Tests

The goal and features as measurable cases. A test with parameter cases runs once per supported value. No Aux Data test exists in v3 yet; every row is not written yet.

**End to end · the sample (1 test)**

|  |  |
|----|----|
| Test | `tests/aux_data/test_aux_data_sample.py::test_golden_no_aux_names_is_inert` |
| Input | The golden replay session ([System map golden sample](SYSTEM_MAP.md)). The `macd_signal_dag` signal reads no auxiliary name, so Strategy sets aux-ready at build ([Strategy feature 3](STRATEGY_MAP.md)). |
| Parameter cases | — |
| Passes when | No source is called; every bar reaches the strategy with only its OHLCV fields; the strategy's aux state stays as built. |
| Fails when | A source is read, a bar gets an extra field, or the strategy is gated on auxiliary data that the strategy does not read. |

### End-to-end scenarios *(agreed 2026-11-09)*

Each scenario drives the component only through its public interface, from a known start state, and checks one exact, observable output. A scenario tells one story across several features; the feature tables below keep the single rules. File: `tests/aux_data/test_aux_data_scenarios.py`. A fake outside source serves scripted rows; a fake runtime book holds the members; recording Strategy stubs record frames, ready calls and seed errors; a controlled clock steps time. Times are New York for CME and Frankfurt for Eurex.

**S1. Arm before the open (features 2, 3, 4, 7)**

|  |  |
|----|----|
| Test | `test_scenario_arm_before_open` |
| Given | One member on MES (NY_RTH) reads the auxiliary name X. The source serves a settled row for the target date from 08:00. |
| When | Step the clock from 07:00 to 09:31. |
| Parameter cases | step interval ∈ {60 s} |
| Passes when | The group arms by 08:01; the member gets its frames, then ready, in that order, once; every bar on the target date carries X with that value. |
| Fails when | Ready before frames; an arm after 08:01 or after the open; a bar without X. |

**S2. A value that is not settled is not used (features 2, 4)**

|  |  |
|----|----|
| Test | `test_scenario_unsettled_value_waits` |
| Given | As above, but until 08:30 the source serves only the row of an earlier date. |
| When | Step the clock from 07:00 to 09:31. |
| Parameter cases | step interval ∈ {60 s} |
| Passes when | Until 08:30 the group is not armed and the member gets no ready; while no frame is loaded the source is fetched at most once per target; one warning per change in what the source serves. The group arms by 08:31. |
| Fails when | The earlier row is accepted; repeated fetches for one target while no frame is loaded; a warning on every step. |

**S3. Missed open sits the session out (features 4)**

|  |  |
|----|----|
| Test | `test_scenario_missed_open` |
| Given | The settled row never appears before 09:30; it appears at 10:00. |
| When | Step the clock to 16:00, then into the next target date with a row present before its open. Second case: start the process at 10:00 with the row present since 08:00. |
| Parameter cases | case ∈ {row after the open; process started after the open} |
| Passes when | Row after the open: the group stays unarmed for the session, no fetch is made for that target after 09:30, the member gets no ready; the next target date arms before its open. Late process start: the group arms at start. |
| Fails when | A late arm in the missed session; a fetch after the open for that target; the next date fails to arm; the late-start process does not arm. |

**S4. Recheck adopts a changed value (features 5)**

|  |  |
|----|----|
| Test | `test_scenario_recheck_adopts_change` |
| Given | The group armed at 08:00 with value v1. |
| When | At 09:00 the source serves v2 (case change), fails (case fetch fails), or serves v2 and the re-seed raises (case re-seed raises). |
| Parameter cases | recheck interval ∈ {1 h} × case ∈ {change, fetch fails, re-seed raises} |
| Passes when | Change: the member is re-seeded with v2, one warning is logged, bars carry v2. Fetch fails: the group keeps v1. Re-seed raises: the group is disarmed. |
| Fails when | A second recheck within the hour; v1 kept after a successful change; the group stays armed after a re-seed error. |

**S5. Groups arm on their own clocks (features 3, 4)**

|  |  |
|----|----|
| Test | `test_scenario_independent_groups` |
| Given | One member on MES (NY_RTH) and one on FDAX, each reading its own name; the Eurex source is down. |
| When | Step the clock across both opens. |
| Parameter cases | — |
| Passes when | The MES group arms before the NY open; the FDAX group stays unarmed and sits its session out; the MES group is not affected. |
| Fails when | One group's failure delays or blocks the other; a group arms on the other exchange's clock. |

**S6. A backup value needs a confirmation (features 1)**

|  |  |
|----|----|
| Test | `test_scenario_backup_needs_confirmation` |
| Given | The primary source has no row for the target; a backup source has value v. |
| When | Step the clock to the open. |
| Parameter cases | case ∈ {second source confirms v; second source differs; second source absent}; confirmations ∈ {1} |
| Passes when | Confirmed: the group arms with v. Differs or absent: v is not used and the group does not arm. |
| Fails when | An unconfirmed backup value is used. |

**S7. Replay resolves at once (features 6)**

|  |  |
|----|----|
| Test | `test_scenario_replay_resolves_at_start` |
| Given | Replay of 2026-04-15 with file sources. |
| When | Start the replay. |
| Parameter cases | case ∈ {every value present; one value missing} |
| Passes when | Present: every member is seeded and armed before the first bar. Missing: start raises and names the missing value. |
| Fails when | A member armed after the first bar; start passes with a missing value. |

**S8. A strategy added later joins a group (features 9)**

|  |  |
|----|----|
| Test | `test_scenario_late_member` |
| Given | The MES group is armed for name X. |
| When | Commit strategy C reading X, and strategy D reading a new name Y, to the runtime book. |
| Parameter cases | — |
| Passes when | C joins the armed group on the next step; D gets its own group, which arms on its own when Y settles. |
| Fails when | C waits for a new arm; D joins the X group. |

**S9. Frames for a cold rebuild (features 8)**

|  |  |
|----|----|
| Test | `test_scenario_frames_for_rebuild` |
| Given | The MES group is armed. |
| When | Strategy asks for the frames of MES. |
| Parameter cases | — |
| Passes when | The answer equals the frames the member got at the last arm or re-seed. |
| Fails when | Stale or empty frames for an armed group. |

**1. One interface per outside source (1 test · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_backup_value_used_only_when_confirmed` | second source ∈ {confirms, disagrees, missing} | value used | backup; none; none |

**2. A value counts only when settled for its target (2 tests · 3 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_carried_value_never_arms` | market ∈ {CME, Eurex} | arm with only a carried-forward row | not armed |
| `test_fetch_at_most_once_per_target_before_settle_time` | — | fetches before a settled row can exist | ≤ 1 per target |

**3. Arm groups are independent (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_arm_groups_per_exchange_are_independent` | — | CME group state during a Eurex source outage | unchanged |
| `test_groups_stepped_every_interval` | interval ∈ {60 s} | steps in a 10-minute replay clock run | 10 per group |

**4. Arm before the open, or sit the session out (4 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_not_armed_by_open_sits_session_out` | — | fetches and arms after the open for that target | 0 |
| `test_seed_error_leaves_group_unarmed` | — | ready flags set; CRITICAL logs | 0; 1 |
| `test_start_after_open_still_arms` | — | group state | armed |
| `test_target_roll_disarms_group` | — | group state after the roll | unarmed |

**5. Recheck until the open (2 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_changed_value_after_arm_is_adopted_and_reseeded` | interval ∈ {1 h} | value and seeds after the recheck | new value; members re-seeded once |
| `test_recheck_outcomes` | outcome ∈ {fetch fails, target gone, re-seed raises} | group state | armed; armed; unarmed |

**6. Replay resolves at once (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_replay_resolves_or_raises` | value ∈ {present, missing} | start result | armed before the first bar; raises |

**7. Values on the bar (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_annotate_adds_bound_values_before_dispatch` | instrument ∈ {with bound name, without} | fields added to the bar | bound values only; none |

**8. Frames for a cold rebuild (1 test · 1 case)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_rebuild_while_armed_gets_current_frames` | — | frames returned vs. armed frames | equal |

**9. Members come from the runtime book (1 test · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_late_joiner_gets_own_group_unless_covered` | joiner reads ∈ {covered names, a new name} | group joined | existing group; own group |

## Open questions

All decided on 2026-11-09.

1.  **Backup confirmations (feature 1).** One second source; configurable per process, not per name.
2.  **Step and recheck intervals (features 3, 5).** 60 s and 1 h; configurable per process.
3.  **Whole component.** Decided: Aux Data is the eighth component ([System map decision 44](SYSTEM_MAP.md)).

## Reference

Implementation detail and history. Feature numbers equal the old behaviour numbers.

### Step → code

Sources are in `external_data/sources.py`; the provider and groups are in `external_data/data_providers..py` (the file name has a double dot).

| Step | Code |
|----|----|
| Group by arm session | `AuxProvider` · Planned grouping; `AuxArmGroup.covers()` |
| \[ arm groups \] | `AuxArmGroup` |
| Fetch value | `AuxSource.fetch()`, `FileAuxSource`, `ConfirmedBackupAuxSource`, `register_aux_source()` |
| Judge settled, Arm group, Recheck until open, Roll target date | `AuxArmGroup.step()` |
| Replay resolve | `AuxProvider.start()` |
| Annotate bar | `AuxProvider.annotate()` |
| Serve frames | `AuxProvider.frames_for()` |
| Resident task | `AuxProvider.start()`, `stop()` |

### Skeleton code

One service object, one source interface and one group class.

| Piece | Role | Old behaviours |
|----|----|----|
| `AuxSource`, `AUX_SOURCES` | One outside source; picked by name from a registry. | 1 |
| `AuxArmGroup` | Target date, deadline, columns, armed state and warnings for one arm session. | 2, 3, 4, 5 |
| `AuxProvider.start()`, `stop()` | Replay: resolve or raise. Live: start and cancel the resident task. | 3, 6, 9 |
| `AuxProvider.annotate()` | Values on the bar. | 7 |
| `AuxProvider.frames_for()` | Frames for a cold rebuild. | 8 |

**Full skeleton code (python)**

``` python
"""Skeleton contract for the Aux Data component (Alchemist v3)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import date
from typing import Any

from alchemist.clock import Clock
from alchemist.instruments import Instrument

AUX_STEP_INTERVAL_S = 60.0        # Behaviour 3
AUX_RECHECK_INTERVAL_S = 3600.0   # Behaviour 5

class AuxSource(ABC):
    """Behaviour 1: one outside source of auxiliary daily series."""
    @abstractmethod
    async def fetch(self, name: str, start: date, end: date) -> Any: ...

AUX_SOURCES: dict[str, type[AuxSource]] = {}

class AuxArmGroup:
    """Behaviours 2-5: arming state for one arm session."""
    arm_session: str
    columns: tuple[tuple[Instrument, str], ...]
    ready: bool
    async def step(self, aux: "AuxProvider") -> None: ...

class AuxProvider:
    """Behaviours 1-9."""
    def __init__(self, sources: list[AuxSource], book, clock: Clock) -> None: ...
    async def start(self) -> None: ...
    async def stop(self) -> None: ...
    def annotate(self, instrument: Instrument, bar: dict) -> None: ...
    def frames_for(self, instrument: Instrument) -> dict: ...
```

### Decisions already made

### 1. Why is Aux Data its own component?

**Answer:** It has its own outside input (not the broker), its own failure modes (source down, value not settled, provisional value changed, backup not confirmed) and its own clock (arming by each session's open). It touches the rest of the engine at three points only: values on a bar, frames for a rebuild, and ready / not ready. Taking it out narrows the Event Loop to bars, dispatch and broker history.

**Rejected:** Rejected: keeping it inside the Event Loop (the widest boundary in the system); moving it behind the Gateway (the Gateway is the broker only, System map decision 7).

*decided 2026-11-09 · proposed 2026-11-07*

### 2. Who tells a strategy it is ready?

**Answer:** Aux Data tells Strategy, with the frames. Strategy seeds the signal and then tells StrategyExecution ([Strategy feature 3](STRATEGY_MAP.md)). Aux Data never reaches into the signal or the execution state machine.

*decided 2026-11-09 · proposed 2026-11-07*

### 3. Where does a group's arm session come from?

**Answer:** From the exchange profile of the strategy's instrument (System map decision 31). No code branches on a session or exchange name.

*decided 2026-11-09 · proposed 2026-11-07*
