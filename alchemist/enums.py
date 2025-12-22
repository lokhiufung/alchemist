import enum


class BaseEnum(enum.Enum):
    @classmethod
    def validate(cls, value: str) -> str:
        if value in cls.__members__:
            return value
        raise ValueError(f"Invalid value: {value}, Possible values are {cls.__members__}")


class OrderTypeEnum(BaseEnum):
    MARKET = "market"
    LIMIT = "limit"
    STOP = "stop"
    STOP_LIMIT = "stop_limit"


class OrderStatusEnum(BaseEnum):
    CREATED = "created"
    OPENED = "opened"                  # When the broker acknowledges the order
    SUBMITTED = "submitted"            # When an order is initially placed
    CANCELED = "canceled"              # When the order is canceled
    FILLED = "filled"                  # When the order is fully filled
    PARTIALLY_FILLED = "partial_filled"  # When the order is partially filled
    REJECTED = "rejected"              # When the broker rejects the order
    INTERNALLY_REJECTED = "internal_rejected"  # When the system rejects the order internally
    CLOSED = "CLOSED"
    AMENDED = "amended"                # When the order is modified (e.g., price or size)
    AMEND_SUBMITTED = "amend_submitted"


class TimeInForceEnum(BaseEnum):
    GTC = "good_till_cancel"
    IOC = "immediate_or_cancel"
    FOK = "fill_or_kill"
    DAY = "day"


class SideEnum(BaseEnum):
    LONG = 1
    SHORT = -1


class DataSubscriptionEnum(BaseEnum):
    TICK = 'tick'
    QUOTE = 'quote'
    BAR = 'bar'
    ACCOUNT_SUMMARY = 'account_summary'
    ACCOUNT_UPDATE = 'account_update'


class ResolutionEnum(BaseEnum):
    TICK = 'tick'
    QUOTE = 'quote'
    SECOND = 'second'
    MINUTE = 'minute'
    HOUR = 'hour'
    DAY = 'day'


class OHLCVEnum(BaseEnum):
    TS = 'ts'
    OPEN = 'open'
    HIGH = 'high'
    LOW = 'low'
    CLOSE = 'close'
    VOLUME = 'volume'