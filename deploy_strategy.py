from __future__ import annotations

import asyncio
from argparse import ArgumentParser
from pathlib import Path

from dotenv import load_dotenv

from alchemist.config import load_config
from alchemist.event_loop.deploy import send_deploy_request


def parse_args(argv=None):
    parser = ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--key", required=True)
    return parser.parse_args(argv)


def main(argv=None) -> int:
    args = parse_args(argv)
    load_dotenv()
    config = load_config(args.config)
    return asyncio.run(send_deploy_request(
        config.system.log_path / "deploy.sock", args.key, config.sha256))


if __name__ == "__main__":
    raise SystemExit(main())
