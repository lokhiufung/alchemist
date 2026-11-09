from __future__ import annotations

import asyncio
from argparse import ArgumentParser
from pathlib import Path

from dotenv import load_dotenv

from alchemist.config import Configuration, load_config
from alchemist.logger import setup_logging
from alchemist.constants.mode import Mode, MODES
from alchemist.runtime import Runtime


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
    config = load_config(args.config)
    listener = setup_logging(config.system.log_path)
    try:
        asyncio.run(run(config, Mode(args.mode)))
    finally:
        listener.stop()


if __name__ == "__main__":
    main()
