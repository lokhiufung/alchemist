from __future__ import annotations

from enum import StrEnum


class DeployStatus(StrEnum):
    PREPARING = "preparing"
    REGISTERED = "registered"
    NOOP = "noop"
    BUSY = "busy"
    REJECTED = "rejected"
    FAILED = "failed"
    SHUTTING_DOWN = "shutting_down"

    @property
    def is_success(self) -> bool:
        return self in (DeployStatus.REGISTERED, DeployStatus.NOOP)


class RefusalReason(StrEnum):
    STALE_CONFIG = "stale_config"
    CONFIG_DRIFT = "config_drift"
    UNKNOWN_KEY = "unknown_key"
    CONFLICT = "conflict"
    IMPORT_ERROR = "import_error"
    PARAMS_INVALID = "params_invalid"
    SESSION_OPEN = "session_open"
    AFTER_PROCESS_EXIT = "after_process_exit"
    UNKNOWN_ACCOUNT_ROUTE = "unknown_account_route"
    UNMANAGED_EXPOSURE = "unmanaged_exposure"
