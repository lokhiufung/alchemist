from datetime import datetime, timezone

from alchemist.event_loop.watch_time import WatchTime


def epoch(moment: datetime | float) -> float:
    return moment.timestamp() if isinstance(moment, datetime) else float(moment)


class ManualWatchTime(WatchTime):
    def __init__(self, start: datetime | float | None = None) -> None:
        super().__init__(system_time=lambda: 0.0)
        if start is not None:
            self.set(start)

    def set(self, moment: datetime | float) -> None:
        self.event_time = epoch(moment)
        self.arrived_at = 0.0
        self.first_event.set()

    def advance(self, moment: datetime | float) -> None:
        self.set(moment)

    def step(self, seconds: float) -> None:
        self.set(self.now() + seconds)
