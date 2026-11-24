"""How each tier is polled.

The time window starts a bit in the past so that trains that just left are seen again
with their actual (``timeType = REAL``) times, and reaches into the future so that we
record DB's forecasts with a lead time.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta


@dataclass(frozen=True)
class TierPolicy:
    tier: int
    interval: timedelta  # how often the whole tier is polled
    lookback: timedelta  # window start = now - lookback
    horizon: timedelta  # window end = now + horizon (API max: start + 8h)
    group_size: int  # stations per request (API max: 15 EVA numbers)


POLICIES: dict[int, TierPolicy] = {
    1: TierPolicy(1, timedelta(minutes=2), timedelta(minutes=15), timedelta(minutes=120), 1),
    2: TierPolicy(2, timedelta(minutes=10), timedelta(minutes=20), timedelta(minutes=120), 3),
    3: TierPolicy(3, timedelta(minutes=60), timedelta(minutes=60), timedelta(minutes=60), 10),
}

BOARDS = ("departures", "arrivals")
MAX_ENTRIES_PER_TRANSPORT_TYPE = 50  # API maximum
MAX_EVA_NUMBERS_PER_REQUEST = 15  # API maximum
