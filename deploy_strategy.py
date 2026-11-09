from __future__ import annotations

import asyncio
import sys
from argparse import ArgumentParser
from pathlib import Path

from dotenv import load_dotenv

from alchemist.config import ConfigError, load_config
from alchemist.event_loop.deploy import send_deploy_request

SOCKET_FILE = "deploy.sock"


def main(argv=None) -> None:
    parser = ArgumentParser()
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--key", required=True)
    args = parser.parse_args(argv)
    load_dotenv()
    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"config error: {exc}", file=sys.stderr)
        sys.exit(1)
    socket_path = config.system.log_path / SOCKET_FILE
    sys.exit(asyncio.run(send_deploy_request(socket_path, args.key, config.sha256)))


if __name__ == "__main__":
    main()
