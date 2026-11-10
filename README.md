# Alchemist trading engine

## Objective

Deploy your trading idea

## version

v3


## Getting started

### 1. Install

Requires Python 3.12+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

The IBKR gateway needs the TWS API client. The PyPI `ibapi` package is outdated: install the Python client from IBKR's TWS API distribution (`pip install <TWS API>/source/pythonclient`), or use the pinned `ibkr` extra (`uv sync --extra ibkr`) for development and tests. Only `alchemist/gateways/ib_gateway/client.py` imports `ibapi`.

### 2. Write a deployment config

A config is one YAML file. `${VAR}` values resolve from the environment (`.env` is loaded at start). Each slot under `strategies:` is one running strategy; the slot name is its strategy key, `strat` is the registered strategy name. A slot names one `signal_instrument` (market data it reads) and one `execution_instrument` (where its orders and position live).

```yaml
system:
  bot_id: my_bot
  log_path: logs/my_bot            # logs and the deploy socket live here
  strategies_root: local/strategies

gateway:
  # gateway name and connection settings

safety: {}
aux_sources: {}

strategies:
  macd_rsi_rth_long:
    strat: macd_rsi_rth_long
    account_id: ${ACCOUNT_ID}
    signal_instrument: NQ
    execution_instrument: NQ

replay:                            # replay mode only: tape, date, session
```

See [configs/replay_config.yaml](configs/replay_config.yaml) for an example.

### 3. Start the system

```bash
uv run python run_strategies.py --config configs/replay_config.yaml --mode replay
```

`--mode` is one of:

| Mode     | Broker                                   | Deploy socket |
|----------|------------------------------------------|---------------|
| `replay` | simulated broker that replays a tape     | no            |
| `paper`  | real gateway, paper account              | yes           |
| `live`   | real gateway, live account               | yes           |

At start the runner imports every folder under `local/strategies/` and builds one strategy per config slot.

## Add a strategy without a restart

In `paper` and `live` mode you can add one new strategy to a running process. The strategies that already trade keep their connection, positions, orders, indicators and partly built bars.

### 1. Write the strategy

Make a new folder under `local/strategies/<folder>/` with these files:

- `calculations.py`: custom calculations, `@register_calc("name")`. Optional.
- `strategy_signal.py`: a `SignalDag` subclass, `@register_signal_dag("name")`. Its `Params` dataclass holds the strategy params.
- `strategy.py`: a `Strategy` subclass, `@register_strategy("name")`, with `signal_dag = "<signal name>"`.

The files do not import each other. They connect by registry names. Use [local/strategies/macd_rsi_rth_long/](local/strategies/macd_rsi_rth_long/) as a template.

### 2. Add a slot to the same config file

Add the new slot under `strategies:` in the config file that the running process uses. Do not change anything else in the file. The next cold start then also runs this strategy.

```yaml
strategies:
  macd_rsi_rth_long:
    ...
  my_new_strategy:                 # new strategy key
    strat: my_new_strategy
    account_id: ${ACCOUNT_ID}      # must be an account a running strategy already uses
    signal_instrument: ES
    execution_instrument: ES
```

### 3. Send a deploy request

```bash
uv run python deploy_strategy.py --config configs/live_config.yaml --key my_new_strategy
```

The client sends the key and the sha256 of the config file to the socket in `system.log_path`. It prints one status line per step and exits with the terminal status:

| Status       | Meaning                                                    |
|--------------|------------------------------------------------------------|
| `preparing`  | in progress                                                |
| `registered` | strategy added; it enters first at its next session open   |
| `noop`       | nothing to do                                              |
| `rejected`   | refused before anything changed (reason is printed)        |
| `failed`     | failed after staging; only the candidate is cleaned up     |

### Rules

A deploy request only adds one new strategy key. It is rejected when:

- the config differs anywhere but the new slot (`config_drift`, `stale_config`)
- the key is not in the config (`unknown_key`) or already runs (`conflict`)
- the strategy code does not import or its params are invalid (`import_error`, `params_invalid`)
- the strategy's session is open now (`session_open`), or its next session closes after the process exits (`after_process_exit`)
- no running strategy uses its account (`unknown_account_route`)
- a position or order on its execution instrument belongs to no running strategy (`unmanaged_exposure`)

To change, remove or reload a running strategy, restart the process.
