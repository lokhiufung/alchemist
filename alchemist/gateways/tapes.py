from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Callable, Iterator

from alchemist.exchanges import exchange_profile
from alchemist.instruments import Instrument
from alchemist.registry import get_tape

BAR_SECONDS = 1.0
SYNTHETIC_PREFIX = "synthetic:"


class ReplayDataError(Exception):
    pass


class TapeSource(ABC):
    @abstractmethod
    def bars(self, instrument: Instrument, start: datetime, end: datetime) -> Iterator[dict]: ...


class SyntheticTape(TapeSource):
    def __init__(self, generator: Callable[[Instrument, datetime, datetime], Iterator[dict]]) -> None:
        self.generator = generator

    def bars(self, instrument: Instrument, start: datetime, end: datetime) -> Iterator[dict]:
        return self.generator(instrument, start, end)


class ParquetTape(TapeSource):
    def __init__(self, path: str) -> None:
        self.path = path

    def bars(self, instrument: Instrument, start: datetime, end: datetime) -> Iterator[dict]:
        import pyarrow.dataset as ds

        window = ((ds.field("symbol") == instrument.symbol)
                  & (ds.field("start_ts") >= start.timestamp()) & (ds.field("start_ts") < end.timestamp()))
        for batch in ds.dataset(self.path, format="parquet").to_batches(filter=window):
            columns = batch.to_pydict()
            for index in range(batch.num_rows):
                begin = columns["start_ts"][index]
                yield {"instrument": instrument, "start_ts": begin, "end_ts": begin + BAR_SECONDS,
                       "open": columns["open"][index], "high": columns["high"][index],
                       "low": columns["low"][index], "close": columns["close"][index],
                       "volume": columns["volume"][index], "resolution_s": BAR_SECONDS,
                       "broker_ts": begin + BAR_SECONDS}


def tape_from_spec(spec: str) -> TapeSource:
    if spec.startswith(SYNTHETIC_PREFIX):
        return SyntheticTape(get_tape(spec[len(SYNTHETIC_PREFIX):]))
    return ParquetTape(spec)


def check_tape_in_session(tape: TapeSource, instrument: Instrument, session: str,
                          start: datetime, end: datetime) -> int:
    profile = exchange_profile(instrument)
    overnight = profile.session(session).crosses_midnight
    window: tuple[float, float] | None = None
    count = 0
    only_regular = True
    for bar in tape.bars(instrument, start, end):
        count += 1
        at = bar["start_ts"]
        if window is None or not window[0] <= at < window[1]:
            found = profile.session_window_at(session, datetime.fromtimestamp(at, timezone.utc))
            if found is None:
                raise ReplayDataError(
                    f"tape row for {instrument.symbol} at {datetime.fromtimestamp(at, timezone.utc).isoformat()} "
                    f"is outside session {session} of {profile.name}")
            window = (found[0].timestamp(), found[1].timestamp())
        if overnight and only_regular:
            only_regular = profile.session_window_at("regular", datetime.fromtimestamp(at, timezone.utc)) is not None
    if overnight and count and only_regular:
        raise ReplayDataError(
            f"tape for {instrument.symbol} holds the regular session only, but session {session} is overnight")
    return count
