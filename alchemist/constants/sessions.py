from datetime import time
from dataclasses import dataclass


@dataclass(frozen=True)
class Session:
    name: str
    start: time
    end: time
    timezone: str


NY_GLOBEX = Session("NY_GLOBEX", start=time(18, 0), end=time(17, 0), timezone="America/New_York")
NY_RTH = Session("NY_RTH", start=time(9, 30), end=time(16, 0), timezone="America/New_York")
