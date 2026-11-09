# STRATEGY_SIGNAL_MAP — StrategySignal · Alchemist v3

The slot's signal: a DAG of calculations built in Python by a `SignalDag` subclass, and the entry and exit logic over them. The signal is pure (bars and position data in, answers out), so the signal can be tested alone, one bar at a time.

Frozen contract for the StrategySignal component. Agents implement against this file and must not edit it: it changes only by re-running `/component-design` with the human, compiled from the HTML design page `strategy_signal_map.html` in this folder (the human-readable source). System boundary: [SYSTEM_MAP.md](SYSTEM_MAP.md).

**Last agreed: 2026-11-09.** **v3 final, 2026-11-09 — agreed.** The End-to-end scenarios in Tests were agreed on 2026-11-09. A signal is a `SignalDag` subclass: typed params the class holds, and a DAG of registered calculations it builds in code, behind an abstract interface. All features and decisions are agreed. Change history: see `strategy_signal_map.html`.

## Goal and scope

**What it is.** StrategySignal is the slot's signal: an abstract interface, and one shipped implementation, `SignalDag`. A strategy author writes a `SignalDag` subclass that holds typed params and builds a DAG of registered calculations, with entry and exit logic over the DAG's nodes. The signal is pure: bars and position data go in, answers come out, with no I/O, no clock and no knowledge of orders.

**Why it exists.** Most warmup and signal bugs live in the calculations and the entry and exit logic. A pure signal with its own boundary can be tested alone, one bar at a time, from bars in to answers out ([System map decision 19](SYSTEM_MAP.md)). Strategy decides when the signal steps and rebuilds; StrategyExecution decides when to ask for an answer and when to freeze the exit inputs; the signal only computes.

**Goal.** Turn a stream of bars, plus the position data Strategy passes in, into deterministic entry and exit answers: the same signal class and the same bars always give the same answers. Report when the signal has enough history to be trusted, and rebuild from scratch on request.

### Public interface

| Direction | Who | Operation | Carries |
|----|----|----|----|
| Other components call StrategySignal | Strategy | Build cold from the slot config; ask for warmup requirements; seed with history; ask "warm?"; step with bars and strategy data; snapshot and restore session-fixed values; read all node values | config, history frames, bars, strategy data (entry price, seconds since entry) |
|  | StrategyExecution | Ask for the entry answer, the exit answer, and each exit condition's answer; read a condition's level or any node's level; list condition names; ask "rearm required?" | condition or node name → answer, level |
|  | StrategyExecution | Freeze the exit inputs at the entry-decision bar; release them when flat | bar |
| StrategySignal calls | No other component. A signal reads only the calculation registry and the instrument's exchange profile, at build. |  | — |

### Scope

Owns, from the System map:

> The slot's signal: its calculation DAG and params, stepping the calculations on each bar in dependency order, evaluating the entry and exit logic, reporting whether it is warm, and how to rebuild itself cold by replaying seed bars.

| Not StrategySignal's job | Owned by |
|----|----|
| When the signal is stepped or rebuilt; the position | [Strategy](STRATEGY_MAP.md) |
| When the exit inputs are frozen and released; acting on the answers | [StrategyExecution](STRATEGY_EXECUTION_MAP.md) |
| Orders | [Order Manager](ORDER_MANAGER_MAP.md) |
| Fetching history | [Event Loop](EVENT_LOOP_MAP.md) |
| Writing a strategy's `SignalDag` subclass and its params | The strategy author (code under `local/strategies/`) |

### Place in the system

Drawn in the HTML page; the flows are the Public interface table above and the [SYSTEM_MAP.md](SYSTEM_MAP.md) Diagram.

Neither the Event Loop nor Order Manager talks to the signal ([System map decision 20](SYSTEM_MAP.md)).

### How a signal is written (interface for strategy authors)

A strategy author writes one `SignalDag` subclass in Python, in the strategy's folder under `local/strategies/`, and registers it by name with `register_signal_dag`. The subclass has two parts:

| Part | What it holds |
|----|----|
| `Params` | A frozen dataclass that extends `SignalParams`. Each field has a type hint and a default, and the defaults are the values: there is no params config (decision 9). `SignalParams` holds the fields every signal has: `session`, `direction`, `max_trades_per_day`, `warm_up_months`, `rearm_required`. |
| `build_graph(g, p)` | Adds the nodes to the graph `g`, reading values from `p`. |

The graph calls:

| Call | Adds |
|----|----|
| `g.bar(field)` | A market-data leaf: a bar field, or a registered auxiliary name. |
| `g.strategy_data(field)` | A strategy-data leaf (`entry_price`, `secs_since_entry`); null while flat. A node that reads it is exit-side. |
| `g.calc(name, calc_key, *inputs, **params)` | A calculation from the registry, wired to its inputs. |
| `g.expr(name, fn, *inputs)` | An inline pure Python function of its inputs; unavailable on a bar where an input is null. |
| `g.warmup(node, history)` | Marks a calculation as seeded from `daily`, `intraday` or `aux` history and held at its seeded value. |
| `g.freeze_at_entry(node)` | Marks a node as an exit input (decision 6). |
| `g.condition(name, left, op, right)` | A named comparison. |
| `g.any_of(name, *conditions)` | A named OR of conditions. |
| `g.entry(*conditions)` | The entry logic: the AND of the conditions. |
| `g.exit(name, left, op, right)` | A named exit condition. The exit answer is the OR of the exits, in the order they were added. |

Every node has a name: `level()`, `has_level()`, the condition methods and `values()` use it. A variant with other values is a subclass that overrides `Params`. A new calculation is one class registered with `register_calc` in the strategy folder's `calculations.py` (System map R2). It is bound in that folder's scope, so another folder may use the same name; `g.calc()` looks a name up in the folder's scope first, then in the built-in library (System map decision 43). The `SignalDag` base class implements the whole interface, so a signal holds no stepping, evaluation, freeze, warmup or rebuild code. A different implementation can still plug in through the interface (decision 7).

## Features

Each feature says which component needs it and why, what the upstream can give, and how the signal closes the gap. Every number is a parameter with its supported values.

### Algorithm map

```
  ( Strategy )
        │
        │ class + config, rebuild
        ▼
  ┌─────────────────────────┐
  │ Build DAG               │
  │ rule: refuse bad signal │
  └─────────────────────────┘
        │
        ├── nodes ──▶ [ DAG: nodes in order ]

  ( Strategy )
        │
        │ history frames
        ▼
  ┌─────────────────────────┐
  │ Seed warmup nodes       │
  │ rule: warm = all valued │
  └─────────────────────────┘
        │
        ├── seeded values ──▶ [ DAG: nodes in order ]
        │
        │ warm?
        ▼
  ( Strategy )

  ( Strategy )
        │
        │ bars + strategy data
        ▼
  ┌─────────────────────────┐
  │ Step in DAG order       │
  │ rule: failed → null     │
  └─────────────────────────┘
        │
        ├── reads ──▶ [ DAG: nodes in order ]
        ├── reads ──▶ [ frozen exit inputs ]
        │
        │ this bar's values
        ▼
  ┌─────────────────────────┐
  │ Evaluate entry          │
  │ rule: rising edge       │
  └─────────────────────────┘
        │
        ├── entry answer ──▶ ( Strategy, StrategyExecution )
        │
        │ same bar's values
        ▼
  ┌─────────────────────────┐
  │ Evaluate exits          │
  │ rule: OR of conditions  │
  └─────────────────────────┘
        │
        ├── answers, levels ──▶ ( StrategyExecution )
        │
        ▼
  ┌─────────────────────────┐
  │ Read values             │
  │ rule: copy, no effects  │
  └─────────────────────────┘
        │
        │ values
        ▼
  ( Strategy )

  ( StrategyExecution )
        │
        │ freeze / release
        ▼
  [ frozen exit inputs ]
```

Main path in colour: build → seed → step → evaluate. A rebuild is a fresh build from the same class (feature 6). The frozen exit inputs are captured at the entry-decision bar and released when flat (feature 5).

### Feature cards

### 1. Build cold, or refuse a bad signal *(was behaviour 1 · decisions 5, 7, 9 · System map decision 43)*

#### Need

Strategy builds the slot's signal before trading. A broken signal must fail at start, never during a session, and two slots must never share state.

#### Upstream gives

A `SignalDag` subclass with typed params and a `build_graph()`, the slot's deployment config (signal instrument), the calculation registry, and the instrument's exchange profile.

#### Algorithm

1.  Type-check the params against their type hints. None is refused, and a bool is refused where an int or float is expected. `direction` must be `long` or `short`; `warm_up_months` must be an int ≥ 0.
2.  Run `build_graph(g, params)`: take each calculation from the registry by name (the strategy folder's scope first, then the built-in library) and wire its inputs. A node reads only nodes added before it, so the graph has no cycle.
3.  Refuse a bad signal: a param value that does not match its type hint; an unregistered calculation name; a duplicate node name; no entry condition; no exit; an unknown session; an unregistered market-data name, or one the session cannot supply; a non-bool `rearm_required`; a node named `entry` while rearm is on; an operator outside the supported set (`+ - * / < <= > >= == AND OR`) in a condition, an exit or an expression; a calculation whose `setup()` raises.
4.  Set up every calculation node with its params: `Calculation.setup(**params)` raises on a value that is not well defined, and the build reports the node name and the reason. A calculation that declares a `session` param receives the slot's session. Example: a session-bar calculation refuses a session that the exchange profile does not have. Session-window rules (open skip, last entry, end-of-day exit) are signal nodes, not StrategyExecution rules.
5.  Resolve the params' session label (for example `regular`) to the instrument's session name from its exchange profile.
6.  Tell each calculation that reads auxiliary history which market-data names its inputs read.
7.  Start cold: no stepped values, no rising edge, no frozen inputs.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Session label | Configurable per signal class | the session names of the instrument's exchange profile | — (required) |
| Direction | Configurable per signal class | long, short | — (required) |

#### Invariant

A refused signal leaves no state behind for the next build, and two builds of the same class share nothing. Within one scope, a registry name is bound to exactly one class. Every calculation node of a built signal was set up without an error.

### 2. Step every node once per bar, in dependency order *(was behaviour 2)*

#### Need

Each answer must read values from the same bar, and a value that could not be computed must never pass as current.

#### Upstream gives

Strategy passes the bar history up to this bar and strategy data ([Strategy feature 1](STRATEGY_MAP.md)).

#### Algorithm

1.  Step every calculation and inline expression once, in dependency order; warmup calculations stay at their seeded value.
2.  Skip an inline step that cannot be evaluated yet (a missing input, or the entry price while flat); the step is unavailable for this bar.
3.  A step that fails is null for this bar; its previous value is dropped, never carried, and the step is marked stale, so `level()` and `exit_condition_level()` answer None for that step.

#### Parameters

None.

#### Invariant

The values for a bar depend only on the signal class, the bars, the seed and the strategy data given so far.

### 3. Evaluate entry and exit *(was behaviour 3 · decisions 3, 8)*

#### Need

StrategyExecution asks for the entry answer while flat and the exit answers in a trade. A protected execution masks exit conditions that a resting broker order already covers, so StrategyExecution needs each exit condition by name and its level.

#### Upstream gives

The values of the last stepped bar (feature 2), which can contain nulls.

#### Algorithm

1.  **Rearm.** With rearm on, entry is true only on the per-day rising edge of the entry logic. Track the edge on every bar, in position too.
2.  **Nulls.** Use three-valued logic: a null leg does not block an OR whose other leg is true; an AND with a false leg is false; a result still null reads as false. With rearm on, warn once per trading date about a null entry, naming the missing refs. Raise any evaluation error other than a missing ref.
3.  **Exits.** Answer each exit condition by name, in the order the exits were added; the exit answer is their OR. Report every condition as evaluated; the signal knows nothing of resting broker exits.
4.  **Levels.** A condition of the shape `close <op> step` reports that step's value as its level; any other shape reports None. Any node's level is its last value as a price, or None.
5.  **Names and values.** List entry conditions, exit condition names and condition names without evaluating, so the lists are correct before the first bar. Return a copy of every node's value on the last stepped bar.

#### Parameters

| Parameter      | Configurable?                 | Supported   | Default |
|----------------|-------------------------------|-------------|---------|
| Rearm required | Configurable per signal class | true, false | false   |

#### Invariant

For both rearm values: asking twice on the same bar gives the same answers, with no side effect. With rearm on, a sustained run of true entry logic gives one entry, and the gate stays closed until the signal is warm.

### 4. Report history needs and warmth *(was behaviour 4)*

#### Need

The Event Loop and Aux Data must fetch exactly the history the signal needs. StrategyExecution must not trade before every warmup calculation has a value. Strategy persists session-fixed values across a restart ([Strategy feature 5](STRATEGY_MAP.md)).

#### Upstream gives

History frames from Strategy: daily bars, prior-session intraday bars, auxiliary series. A frame can be empty or too short.

#### Algorithm

1.  Report which calculations need intraday, daily or auxiliary history, and how many months of daily bars.
2.  Seed the warmup calculations; report warm only when every warmup calculation has a value.
3.  Let session-fixed calculations snapshot and restore their value; a restore puts each value back so the logic reads it before the next bar.

#### Parameters

| Parameter | Configurable? | Supported | Default |
|----|----|----|----|
| Warmup months of daily bars | Configurable per signal class | 0 or any positive count | 0 |
| History kinds | Chosen per warmup node | daily, intraday, aux | — |

#### Invariant

A calculation whose history was empty or too short is never reported warm. A restored session-fixed value equals the snapshotted value.

### 5. Freeze exit inputs at the entry bar *(was behaviour 5 · decision 6)*

#### Need

A stop placed at entry must stay at its entry-bar level for the whole trade, even when the calculation it was derived from keeps moving.

#### Upstream gives

StrategyExecution freezes at the entry-decision bar and releases when flat again ([StrategyExecution](STRATEGY_EXECUTION_MAP.md)).

#### Algorithm

1.  On freeze, capture the calculation and market inputs of every exit-side step at that bar. An exit-side step reads strategy data, or is wrapped in `g.freeze_at_entry()`.
2.  While frozen, exit-side steps read the captured inputs; the close, strategy data and every other step stay live.
3.  On release, exit-side steps read live inputs again. Each freeze replaces the previous one.

#### Parameters

None.

#### Invariant

An input missing at the entry bar stays unavailable for the whole trade. A signal with no exit-side nodes is unaffected by freeze and release.

### 6. Rebuild cold *(was behaviour 6 · System map decision 22)*

#### Need

When late history arrives, Strategy rebuilds the signal while flat and idle ([Strategy feature 3](STRATEGY_MAP.md)), and the rebuilt signal must equal a fresh process.

#### Upstream gives

A rebuild request from Strategy, then the seed bars replayed through `step()`.

#### Algorithm

1.  Build afresh from the same class (feature 1), discarding every value, the rearm state and any freeze.
2.  Strategy replays the seed bars through the new signal.

#### Parameters

None.

#### Invariant

After a rebuild and the same replay, the signal's values equal those of a fresh process given the same bars.

## Tests

Contract tests for the interface: any implementation must pass them. Each calculation in the library also gets its own step and warmup tests. A test with parameter cases runs once per supported value. Every row is not written yet.

**End to end · the component sample (1 test)**

|  |  |
|----|----|
| Test | `tests/signal/test_signal_sample.py::test_level_cross_then_stop_on_frozen_entry_level` |
| Source | A component-level sample, separate from the system golden sample. The golden sample runs the real `macd_signal_dag` and checks daily warmup (ATR = 20.00), one entry and the `target` exit; it never makes the frozen stop fire. This sample checks the intraday seed, the rearm gate and the freeze on a small signal, one bar at a time. |
| Input | A signal class with one warmup calculation `sma5` (5-bar SMA of the close, seeded from 5 prior bars); entry close \> L with rearm on; exit-side step `stop_px = sma5 − 2 ticks`; exit `stop`: close \< `stop_px`; exit `target`: close ≥ T. Five seed bars, then 10 bars: the close crosses L on bar 3 and rises on bars 4–6; on bar 8 the close falls below the bar-3 `stop_px` without reaching T. StrategyExecution freezes on bar 3. The full class is under Skeleton code. |
| Parameter cases | rearm ∈ {true} |
| Passes when | Warm after the seed; over bars 1–10 entry is true exactly once, on bar 3; exit is true exactly once, on bar 8, from `stop`; `target` is never true; `stop_px` on bars 4–8 equals its bar-3 value, and so does `exit_condition_level("stop")`. |
| Fails when | Entry is also true on bars 4–6 (the rising-edge gate is broken); `stop_px` on bar 5 differs from bar 3 (the freeze is broken); or the signal reports warm before the seed. |

### End-to-end scenarios *(agreed 2026-11-09)*

Each scenario drives the component only through its public interface, from a known start state, and checks one exact, observable output. A scenario tells one story across several features; the feature tables below keep the single rules. File: `tests/signal/test_signal_scenarios.py`. Small test signal classes built with the real `SignalDag` base and scripted bars; no other component is needed.

**S1. A bad signal is refused at build (features 1)**

|  |  |
|----|----|
| Test | `test_scenario_bad_signal_refused` |
| Given | One test signal class per case. |
| When | Build each class. |
| Parameter cases | case ∈ {unregistered calculation; duplicate node name; no entry; no exit; unknown session; bool where an int is expected; None param; direction not long or short; calculation setup raises} |
| Passes when | Each build raises, and the message names the node or param and the reason. |
| Fails when | A build passes; a message without the node or param name. |

**S2. Warm only when every warmup node is seeded (features 4)**

|  |  |
|----|----|
| Test | `test_scenario_warmup` |
| Given | A signal with one warmup calculation `sma5` seeded from 5 prior bars. |
| When | Seed with 4 bars, then with 5 bars. |
| Parameter cases | — |
| Passes when | History needs report `sma5` as intraday. With 4 bars the signal is not warm; with 5 bars it is warm. |
| Fails when | Warm with 4 bars; not warm with 5. |

**S3. Entry fires on the rising edge (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_rearm_edge` |
| Given | Entry: close \> L. |
| When | Send closes below, above, above, below, above L on one trading date. |
| Parameter cases | rearm ∈ {true, false} |
| Passes when | rearm false: entry is true on bars 2, 3 and 5. rearm true: entry is true on bar 2, false on bar 3. |
| Fails when | With rearm true, entry is true on bar 3; with rearm false, entry is false on a bar where close \> L. |

**S4. Nulls follow three-valued logic (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_three_valued_logic` |
| Given | Entry A OR B, and a second signal with A AND C, where A is null on bar 1. |
| When | Step bar 1 with B true and C false. |
| Parameter cases | rearm ∈ {true} |
| Passes when | A OR B is true; A AND C is false; a result that stays null reads as false; one warning for the trading date names the missing ref. |
| Fails when | A null leg blocks a true OR; a null result reads as true; more than one warning per date. |

**S5. A failed step is null for that bar (features 2)**

|  |  |
|----|----|
| Test | `test_scenario_failed_step` |
| Given | A calculation that raises on bar 3 only. |
| When | Step bars 1 to 4. |
| Parameter cases | — |
| Passes when | On bar 3 the step's value is null, `level()` answers None and the bar-2 value is not carried; on bar 4 the step has a value again. |
| Fails when | The bar-2 value is carried into bar 3; the signal stops stepping. |

**S6. Exits answer per condition, with levels (features 3)**

|  |  |
|----|----|
| Test | `test_scenario_exits_and_levels` |
| Given | Exits `stop` (close \< `stop_px`) and `target` (close ≥ T), added in that order. |
| When | Step bars where neither, only `stop`, and both are true. |
| Parameter cases | — |
| Passes when | Answers come per name in the order added; the exit answer is their OR; `exit_condition_level("stop")` equals `stop_px`; a condition of another shape reports None. |
| Fails when | The exit answer is not the OR; the order of names changes; a level for a condition of another shape. |

**S7. Frozen exit inputs are released (features 5)**

|  |  |
|----|----|
| Test | `test_scenario_freeze_release` |
| Given | Exit step `stop_px = sma5 − 2 ticks`. |
| When | Freeze at bar 3; step bars 4 to 6; release; step bars 7 and 8. |
| Parameter cases | — |
| Passes when | On bars 4 to 6 `stop_px` equals its bar-3 value while the close stays live; from bar 7 `stop_px` follows the live `sma5` again. |
| Fails when | `stop_px` moves while frozen, or stays frozen after release. |

**S8. A cold rebuild starts clean (features 6)**

|  |  |
|----|----|
| Test | `test_scenario_cold_rebuild` |
| Given | A signal after 10 bars, with a freeze held and the rising edge spent. |
| When | Rebuild; replay the same seed and the same 10 bars. |
| Parameter cases | — |
| Passes when | Right after the rebuild: no values, no freeze, rearm reset. After the replay every node value equals the value of the first run. |
| Fails when | A value, freeze or edge survives the rebuild; the replay gives other values. |

**1. Build cold, or refuse a bad signal (5 tests · 21 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_calculation_setup_error_refuses_build_and_names_node` | case ∈ {setup raises on a param, session calculation given an unknown session, params that fit} | build result; error names the node | refused; refused; built — node named in each refusal |
| `test_bad_signal_refused_and_leaves_no_state` | reason ∈ {param type mismatch, unregistered calculation, duplicate node, no entry, no exit, unknown session, unregistered market-data name, market data the session cannot supply, non-bool rearm, node named entry with rearm, unsupported operator, None param, bool for a numeric param, direction not long or short, negative warm_up_months, calculation setup raises} | build result; state left for the next build | refused; none |
| `test_rebinding_a_registry_name_to_another_class_raises` | — | second registration | raises |
| `test_folder_calc_shadows_builtin_and_folders_do_not_collide` | — | class resolved per folder | folder class first; two folders keep their own |
| `test_session_label_resolved_from_exchange_profile` | exchange ∈ {CME} | session name | the profile's session for the label |

**2. Step every node once per bar, in dependency order (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_unavailable_value_not_carried_from_previous_bar` | — | value of a step whose input is missing | unavailable, not the previous value |
| `test_failed_step_is_null_and_level_is_none` | — | value; `level()` | null; None |

**3. Evaluate entry and exit (6 tests · 9 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_sustained_entry_run_entries` | rearm ∈ {true, false} | entries over a 5-bar true run | 1; 5 |
| `test_null_leg_follows_kleene_rules` | case ∈ {true OR null, false AND null, null alone} | answer | true; false; false |
| `test_evaluate_twice_same_answers` | — | answers on a second ask | equal |
| `test_exit_is_or_of_named_conditions` | — | exit answer vs. per-condition answers | OR, in the order added |
| `test_condition_names_listed_before_first_bar` | — | names before any step | complete |
| `test_values_returns_a_copy` | — | signal state after the caller changes the copy | unchanged |

**4. Report history needs and warmth (2 tests · 4 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_warm_only_with_enough_history` | history ∈ {empty, too short, enough} | warm report | false; false; true |
| `test_restore_session_state_puts_value_back` | — | value read before the next bar | equal to the snapshot |

**5. Freeze exit inputs at the entry bar (2 tests · 2 cases)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_release_returns_exit_inputs_to_live` | — | exit-side step after release | follows live inputs |
| `test_input_missing_at_entry_stays_unavailable` | — | exit-side step during the trade | unavailable until release |

**6. Rebuild cold (1 test · 1 case)**

| Test | Parameter cases | Measure | Pass threshold |
|----|----|----|----|
| `test_rebuild_and_replay_equals_fresh_process` | — | every node value after rebuild and replay | equal to a fresh process |

## Open questions

None open. Direction was closed on 2026-11-09: `long` or `short`.

## Reference

Implementation detail and history. Feature numbers equal the old behaviour numbers.

### Step → code

| Step | Code |
|----|----|
| Build DAG | `strategy_builder/signal_dag.py` · `SignalDag.from_config()`, `build_graph()`, `Graph`, `SignalParams`; `registry.py` · `register_calc()`, `get_calc()`, `register_signal_dag()`, `get_signal_dag()` |
| \[ DAG \] | `Graph`, `Node` |
| Seed warmup nodes | `SignalDag.warmup_requirements()`, `seed()`, `is_warm()`, `snapshot_session_state()`, `restore_session_state()`; `calculations/calculation.py` · `Calculation.warmup()`, `SessionFixedCalculation` |
| Step in DAG order (dependency order) | `SignalDag.step()`; `Calculation.setup()`, `step()`, `value()`, `bind_aux()` |
| \[ frozen exit inputs \] | `SignalDag.freeze_exit_inputs()`, `release_exit_inputs()`; `Graph.freeze_at_entry()` |
| Evaluate entry | `SignalDag.evaluate_entry()`, `rearm_required`, `entry_conditions()` |
| Evaluate exits | `SignalDag.evaluate_exit_conditions()`, `exit_condition_level()`, `level()`, `has_level()`, `exit_condition_names()`, `condition_names()` |
| Read values | `SignalDag.values()` |
| Interface | `strategy_builder/strategy_signal.py` · `StrategySignal`, `WarmupRequirements` |

### Skeleton code

The interface, the `SignalDag` implementation and its graph, the calculation base classes and registries, and the Sample's signal class.

### The pieces

| Piece | Role |
|----|----|
| `StrategySignal` (abstract) | The interface Strategy and StrategyExecution call. |
| `SignalDag` | The shipped implementation: type-checks `Params`, runs `build_graph()`, and implements every interface method. |
| `SignalParams` | Base dataclass of the fields every signal has. |
| `Graph` | The builder passed to `build_graph()`. |
| `Calculation`, `SessionFixedCalculation` | Base classes of every calculation. |
| `register_calc`, `get_calc` | Calculation name → one class within a scope: a strategy folder, or the built-in library. `get_calc` tries the folder's scope, then the built-ins. Re-binding a name in the same scope raises. |
| `register_signal_dag`, `get_signal_dag` | Signal name → one `SignalDag` subclass, global. Re-binding raises. |
| Calculation library | The registered calculations (for example `sma`, `ema`, `atr`, `vwap`). Session facts come from the exchange profile, never from session names inside a calculation (decision 4). |

### Methods by behaviour

| Old behaviours | Methods |
|----|----|
| 1\. Build cold | `from_config(strategy_config)` (class method) |
| 2\. Step | `step(bars, strategy_data)` |
| 3\. Evaluate | `evaluate_entry()`, `evaluate_exit()`, `evaluate_exit_conditions()`, `exit_condition_level(name)`, `level(name)`, `has_level(name)`, `rearm_required`, `entry_conditions()`, `exit_condition_names()`, `condition_names()`, `values()` |
| 4\. Warm | `warmup_requirements()`, `seed(history)`, `is_warm()`, `snapshot_session_state()`, `restore_session_state()` |
| 5\. Freeze | `freeze_exit_inputs(bar)`, `release_exit_inputs()` |
| 6\. Rebuild | a fresh `from_config()`, then `step()` over the seed bars |

**Interface, SignalDag and calculation base (python)**

``` python
"""StrategySignal interface (Alchemist v3)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass
from typing import Any, Callable

@dataclass(frozen=True)
class WarmupRequirements:
    daily_months: int = 0                  # months of daily bars
    intraday: tuple[str, ...] = ()         # calculations seeded from prior-session bars
    daily: tuple[str, ...] = ()            # calculations seeded from daily history
    aux: tuple[str, ...] = ()              # calculations seeded from auxiliary series
    session_fixed: tuple[str, ...] = ()    # values safe to restore after a restart

class StrategySignal(ABC):

    @classmethod
    @abstractmethod
    def from_config(cls, strategy_config: dict) -> "StrategySignal":
        """Behaviours 1, 6: build cold from the slot config; refuse a bad signal."""

    @abstractmethod
    def warmup_requirements(self) -> WarmupRequirements: ...
    @abstractmethod
    def seed(self, history: dict) -> None: ...
    @abstractmethod
    def is_warm(self) -> bool: ...

    @abstractmethod
    def step(self, bars: deque, strategy_data: dict) -> None:
        """Behaviour 2 (and 3: drives the rearm gate every bar)."""

    @abstractmethod
    def evaluate_entry(self) -> bool: ...
    @abstractmethod
    def evaluate_exit_conditions(self) -> dict[str, bool]:
        """Behaviour 3: every named exit condition, in the order the exits were added."""

    def evaluate_exit(self) -> bool:
        return any(self.evaluate_exit_conditions().values())

    @property
    @abstractmethod
    def rearm_required(self) -> bool: ...
    @abstractmethod
    def exit_condition_level(self, name: str) -> float | None: ...
    @abstractmethod
    def level(self, name: str) -> float | None: ...
    @abstractmethod
    def has_level(self, name: str) -> bool: ...

    @abstractmethod
    def entry_conditions(self) -> dict[str, bool | None]:
        """Behaviour 3, decision 8: each entry condition's value on the last bar."""
    @abstractmethod
    def exit_condition_names(self) -> list[str]: ...
    @abstractmethod
    def condition_names(self) -> set[str]: ...
    @abstractmethod
    def values(self) -> dict[str, Any]:
        """Behaviour 3, decision 8: a copy of every node's value on the last bar."""

    @abstractmethod
    def freeze_exit_inputs(self, bar: dict) -> None: ...
    @abstractmethod
    def release_exit_inputs(self) -> None: ...

    def snapshot_session_state(self) -> dict: return {}
    def restore_session_state(self, state: dict) -> None: ...

# alchemist/strategy_builder/signal_dag.py
@dataclass(frozen=True)
class SignalParams:
    session: str
    direction: str
    max_trades_per_day: int = 1
    warm_up_months: int = 0
    rearm_required: bool = False

class Graph:
    def bar(self, field: str) -> Node: ...
    def strategy_data(self, field: str) -> Node: ...
    def calc(self, name: str, calc_key: str, *inputs: Node, **params) -> Node: ...
    def expr(self, name: str, fn: Callable[..., Any], *inputs: Node) -> Node: ...
    def warmup(self, node: Node, history: str) -> Node: ...
    def freeze_at_entry(self, node: Node) -> Node: ...
    def condition(self, name: str, left: Node, op: str, right: Node | float) -> Node: ...
    def any_of(self, name: str, *conditions: Node) -> Node: ...
    def entry(self, *conditions: Node) -> None: ...
    def exit(self, name: str, left: Node, op: str, right: Node | float) -> None: ...

class SignalDag(StrategySignal):
    """The shipped implementation. A subclass gives only Params and build_graph()."""
    Params: type[SignalParams]

    def build_graph(self, g: Graph, p: SignalParams) -> None:
        raise NotImplementedError
    # Major state:
    #   params                  the type-checked Params instance
    #   _nodes                  nodes in the order build_graph() added them (a DAG order)
    #   _values                 node name -> value for the last stepped bar
    #   _rearm                  rising-edge gate: day, previous raw entry, armed
    #   _exit_inputs, _frozen   exit-side nodes; the entry-bar capture
    #   _stale                  nodes that failed on the last stepped bar

# alchemist/calculations/calculation.py
class Calculation(ABC):
    def setup(self, **params) -> None:
        """Behaviour 1: take params (and session, if declared); raise if not
        well defined; start cold."""
    def step(self, *inputs) -> Any:
        """Behaviour 2: value for this bar; None = unavailable; raise = failed.
        Default: value()."""
    def warmup(self, data) -> None:
        """Behaviour 4: seed from history (default: nothing)."""
    def value(self) -> Any:
        """Behaviour 4: the seeded value, or None if not seeded."""
    def bind_aux(self, names: frozenset[str]) -> None:
        """Behaviour 1: optional, for calculations that read auxiliary history."""

class SessionFixedCalculation(Calculation):
    @abstractmethod
    def snapshot_state(self) -> dict | None: ...
    @abstractmethod
    def restore_state(self, state: dict) -> None: ...

# alchemist/registry.py
def register_calc(name: str):
    """Behaviour 1: class decorator; binds name in the calling folder's scope
    (the built-in library is its own scope); re-binding in a scope raises."""
def get_calc(name: str, scope: str) -> type[Calculation]:
    """Behaviour 1: the folder scope first, then the built-ins."""
def register_signal_dag(name: str):
    """Behaviour 1: class decorator; binds a global name to one SignalDag subclass."""
def get_signal_dag(name: str) -> type[SignalDag]: ...
```

**The Sample's signal class (python · one SignalDag subclass)**

``` python
# local/strategies/level_cross/strategy_signal.py
@register_signal_dag("level_cross")
class LevelCross(SignalDag):

    @dataclass(frozen=True)
    class Params(SignalParams):
        session: str = "regular"
        direction: str = "long"
        rearm_required: bool = True
        level: float = 100.0         # L
        target: float = 110.0        # T
        sma_period: int = 5
        stop_offset: float = 0.5     # 2 ticks of 0.25

    def build_graph(self, g: Graph, p: Params) -> None:
        close = g.bar("close")
        sma5 = g.warmup(g.calc("sma5", "sma", close, period=p.sma_period), history="intraday")
        stop_px = g.freeze_at_entry(
            g.expr("stop_px", lambda sma: sma - p.stop_offset, sma5))

        g.entry(g.condition("above_level", close, ">", p.level))
        g.exit("stop", close, "<", stop_px)
        g.exit("target", close, ">=", p.target)
```

### Decisions already made

### 1. Is the signal pure?

**Answer:** Yes: no I/O, no clock, no orders. Position data arrives only as strategy data ([System map decision 21](SYSTEM_MAP.md)).

### 2. Who decides when it steps, rebuilds, freezes?

**Answer:** Strategy steps and rebuilds it; StrategyExecution asks it questions and times the freeze ([System map decisions 20](SYSTEM_MAP.md), [22](SYSTEM_MAP.md)).

### 3. Does the signal know about resting broker exits?

**Answer:** No. It reports every exit condition and each one's level; StrategyExecution decides which conditions a broker order already covers.

### 4. Are the calculations part of StrategySignal?

**Answer:** Yes. The calculations are the DAG's nodes, so they and their tests belong to this component. They read session facts (first-bar anchor, which sessions count as regular trading hours) from the exchange profile, not from session names (System map decision 31).

### Agreed at finalize · 2026-11-09

### 5. How is a signal written inside the boundary?

**Answer:** As a Python `SignalDag` subclass registered by name: typed params it holds itself, and a `build_graph()` that adds registered calculations, inline Python expressions, named conditions and named exits to the DAG. The `SignalDag` base class implements the whole interface. The instrument comes from deployment config and the session from the exchange profile.

**Rejected:** Rejected: a declarative definition file (a second format to parse and check; a Python class is checked at import and build); class-attribute nodes built with operator overloading (an exit level must stay readable as `close <op> node`, and every node needs a name); interface only, with no signal shipped (every user would have to bring a signal layer).

*agreed 2026-11-09 · v3 · changed 2026-11-07 · closes the System map open question*

### 6. How is a node marked as an exit input?

**Answer:** A node that reads strategy data is an exit input. A signal may also wrap any node in `g.freeze_at_entry()`, so a stop built from live calculations alone (like the Sample's `stop_px`) can be frozen at entry without reading the entry price.

**Rejected:** Rejected: strategy-data nodes only (a stop that does not read the entry price could not be frozen).

*agreed 2026-11-09 · v3*

### 7. Why put an interface in front of one implementation?

**Answer:** So Strategy and StrategyExecution can be tested against a small fake signal, and so a different signal layer can plug in without engine changes (System map R2). `SignalDag` is still the only one the engine ships.

*agreed 2026-11-09 · v3*

### 8. What may a caller read from the signal?

**Answer:** Only its public interface. For logs and startup checks the interface has read-only methods: `entry_conditions()`, `exit_condition_names()`, `condition_names()` and `values()`. A restore after a restart goes through `restore_session_state()`, which also puts the restored values back. No caller reads a signal's private state.

**Rejected:** Rejected: callers reading the signal's value cache or its logic structure directly (they would depend on one implementation's private layout, so another implementation could not plug in).

*agreed 2026-11-09 · v3 · 2026-11-07*

### 9. Where do a signal's params live?

**Answer:** In its `Params` dataclass; the defaults are the values. The build type-checks every field against its type hint. A variant with other values is a subclass that overrides `Params`. Version control of a strategy's code and params is outside the system.

**Rejected:** Rejected: params in outside config (a second place that must agree with the code).

*agreed 2026-11-09 · v3 · 2026-11-07*
