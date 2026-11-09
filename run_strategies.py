from __future__ import annotations

import asyncio
import sys
from argparse import ArgumentParser
from pathlib import Path

from dotenv import load_dotenv

import alchemist.gateways  # noqa: F401  registers the gateways and the golden tape
from alchemist.config import ConfigError, Configuration, load_config
from alchemist.constants.mode import MODES, Mode
from alchemist.loader import StrategyLoadError
from alchemist.logger import setup_logging
from alchemist.registry import get_strategy
from alchemist.runtime import Runtime, log_deploy_fingerprint

STARTUP_FAILURE_EXIT_CODE = 1


def parse_args(argv=None):
    parser = ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--mode", required=True, choices=MODES)
    return parser.parse_args(argv)


async def run(config: Configuration, mode: Mode) -> None:
    async with Runtime.build(config, mode) as runtime:
        await runtime.run()


def main(argv=None) -> None:
    args = parse_args(argv)
    load_dotenv()
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        sys.exit(STARTUP_FAILURE_EXIT_CODE)
    listener = setup_logging(config.system.log_path)
    try:
        log_deploy_fingerprint(config)
        asyncio.run(run(config, Mode(args.mode)))
    except (StrategyLoadError, KeyError, ValueError) as exc:
        print(f"startup failed: {exc}", file=sys.stderr)
        sys.exit(STARTUP_FAILURE_EXIT_CODE)
    finally:
        listener.stop()


if __name__ == "__main__":
    main()
