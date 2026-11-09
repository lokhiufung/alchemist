# Alchemist v3 execution engine — build spec

Date: 2026-11-09 · Branch: `v3` · Version: `v3` (from `README.md`)

## 1. Intent

Build the v3 execution engine that `system_design/v3/` designs. The design is frozen and complete: one system map, eight component maps, one golden sample, 59 system decisions, and per-component decisions. This spec does not repeat the design. It says what "built" means, the order of work, the rules every builder follows, and the few how-choices the maps leave open.

**Why v3:** one engine trades a multi-asset, multi-currency book (futures on CME and Eurex, stocks on NASDAQ) with one code path for replay, paper and live (SYSTEM_MAP R1–R5).

## 2. Source of truth

Read in this order. A higher source wins over a lower one.

1. `system_design/v3/SYSTEM_MAP.md` — boundary, requirements R1–R5, golden sample, decisions 1–59.
2. `system_design/v3/<COMPONENT>_MAP.md` — the frozen contract of each component: public interface, algorithm map, feature cards with parameter tables, tests (sample, end-to-end scenarios, feature tests), Step → code table, skeleton code, decisions.
3. This spec.
4. The existing skeleton under `alchemist/` (interfaces with empty bodies, from earlier commits). Where it differs from the map's *Skeleton code* section, the map wins.

Builders never edit the maps. A gap or contradiction in a map is reported to the human, not fixed in the map.

## 3. Goals — what "built" means

v3 is built when all of these are true:

1. **Golden sample passes** — `tests/golden/test_golden.py` runs `python run_strategies.py --config tests/golden/config.yaml --mode replay` as a child process, and all 8 checks of SYSTEM_MAP *Golden sample → End to end* pass. The frozen files `tests/golden/expected/broker_journal.jsonl` and `tests/golden/expected/tape.sha256` are committed with a commit message that says why.
2. **Every test row in every component map is written and passes**: the component sample(s), every end-to-end scenario, and every feature-test table row, at the file path and test name the map gives.
3. **IBKR paper tests** (Gateway scenarios marked `ibkr_paper`) are written. They run only by hand, outside the default run, while the instrument's session is open. They are not required to pass in CI.
4. **R3 load test passes** — Event Loop `test_scenario_load_target_latency`: 3 tick streams, 10–15 strategies, processing latency p99 ≤ 5 ms.
5. **R5 works** — `deploy_strategy.py --config FILE --key KEY` adds one strategy to a running paper process (Event Loop feature 12, scenario `test_scenario_deploy_strategy`).
6. Default test run (`uv run pytest`) is green, with no network access.

## 4. Non-goals

From SYSTEM_MAP *Goal*: strategy research, comparing trades with research, a strategy/params registry, version control of strategies, base-currency reporting. Also out of v3 (decisions 54, 56): options trading, `quote_update`, bar resolutions other than 1 s, order sizes other than 1 unit, stop-limit orders, process shards.

## 5. Anti-goals — fails even if it runs

- A component checks the run mode, a mode string, or a collaborator for `None` (decision 46). Mode differences come only from what the runner passes in.
- A component reaches another component's internals instead of its public interface, or a special path exists for one mode.
- A file branches on an exchange name, outside the exchange profiles (decision 31).
- A hard-coded operational number that the maps call configurable (decision 58).
- The golden expected files refreshed to make a failing run pass, without a reason in the commit.
- Long comments. Code is read by agents; names carry the meaning.

## 6. Constraints

- Python ≥ 3.12, `asyncio`, `uv`, `pytest` + `pytest-asyncio` (already in `pyproject.toml`).
- One process, one asyncio loop, the broker reader thread and one send thread (decision 28). Logging through one queue handler and a listener thread (decision 29).
- Determinism in replay: no wall clock, no random numbers, no dict-order or float-format drift in journal output.
- Every operational number lives in its component's typed config section with the map's default (decision 58).

## 7. Build order

Dependencies between components set the order. Within a phase, components are independent and can be built in parallel by separate agents. Each component is built feature card by feature card with TDD against the map's tests.

| Phase | Work | Depends on |
|----|----|----|
| 0 · Shared | Instruments (decision 37), exchange profiles CME, Eurex, NASDAQ (decision 31, 54), `Clock` (`WallClock`, `ReplayClock`), `RunMode`, registries (`register_calc`, `register_signal_dag`, `register_strategy`, `register_gateway`, synthetic tape and aux source registries), config loading with `${VAR}`, queue logging, strategy folder loader (decision 43), message types (`tick_update`, `bar_update`, order/position/balance updates, `end_of_data`). | — |
| 1 · Leaf components | StrategySignal (+ calculation library used by the example strategy); Order Manager; Gateway base class + simulated broker + synthetic `golden_session` tape (Gateway features 2, 4–8, 12, 14); Aux Data. | 0 |
| 2 · Slot | StrategyExecution (base and protected classes); Strategy. | 1 |
| 3 · Loop | Event Loop: event buffer with control and data lanes, Resampler, `emit_bar`, dispatch, Seeder, runtime book, recorders, shutdown and session-end flatten, latency metrics. | 1, 2 |
| 4 · Runner + golden | `Runtime.build`, `run_strategies.py`, `NullSafety` and other stand-ins, golden config, frozen strategy copy, golden test, frozen expected files. | 0–3 |
| 5 · Live parts | Safety (heartbeat, reconnect ladder, repair, freeze detector, stuck-order timer); IBKR gateway (Gateway features 1, 3, 9, 10, 11, 13 and IBKR halves of the rest); deploy socket server + `deploy_strategy.py` + Event Loop feature 12; R3 load test. | 4 |

Phase 4 is the first point where the whole engine runs. Phase 5 parts are independent of each other.

## 8. Rules for every builder

- Before a component, read its whole map. Build to the *Skeleton code* section and the *Step → code* file locations.
- TDD: write the map's test, see it fail, write code, see it pass. Test names and files are the ones in the map.
- A component test drives only the public interface and uses stubs for neighbours (each map names its stubs).
- Commit per feature card or per scenario, on branch `v3`.

## 9. Calls made by the builder (not by the human)

These are left open by the maps. Each is my call; the human may change it.

1. **IBKR client library**: the official IBKR TWS API Python client (`ibapi`), pinned, imported only inside the IBKR gateway subclass. Reason: v1 used it; the map's error codes and callbacks are TWS API terms.
2. **Parquet tapes**: `pyarrow` added as a dependency, used only by `ParquetTape`. The golden run uses the synthetic tape and needs no file.
3. **Message types** stay plain dicts, as the map skeletons use them, with their keys pinned once as `TypedDict`s in `alchemist/messages.py`. The maps name no keys, so that module is the single place for them. Event types are `tick_update`, `bar_update`, `order_update`, `position_update`, `balance_update`, `repaired_fill`, `book_op`, `end_of_data` (decision 49 wins over the older `tick` in the Gateway's state table).
6. **Module paths**: where a map skeleton and its own Step → code table differ, Step → code wins (e.g. the Event Loop stays the `EventLoop` class in `event_loop/loop.py`). `Instrument` moves to `alchemist/instruments.py` and the exchange profiles go in `alchemist/exchanges.py`, the import paths the skeletons use.
4. **Golden test file**: `tests/golden/test_golden.py`; the generator `golden_session` in `alchemist/gateways/synthetic_tapes.py` (decision 57).
5. **IBKR paper tests** read the account from `tests/.env` and instruments from `tests/ibkr_paper_instruments.yaml`, under the `ibkr_paper` pytest marker, deselected by default.

## 10. Left to the builder

Everything inside a component that its map does not fix: private helpers, module splits below the Step → code files, test fixtures, and the exact phase-B bar count of the golden tape (the map says the builder confirms it with the frozen strategy copy).
