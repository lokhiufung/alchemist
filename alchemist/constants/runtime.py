from __future__ import annotations

SERVICE_LOGGERS = {"gateway": "gateway", "event_loop": "event_loop", "aux_data": "aux_data", "safety": "safety",
                   "deploy_socket": "deploy"}
NOT_FLAT_EXIT_CODE = 2
SOCKET_FILE = "deploy.sock"
STRATEGY_FILES = ("calculations.py", "strategy_signal.py", "strategy.py")
LOGS_DIR = "logs"
BUILTIN_SCOPE = "builtin"
