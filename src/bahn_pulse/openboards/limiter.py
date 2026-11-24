"""Async token-bucket rate limiter shared by all polling tiers."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Callable


class TokenBucket:
    """Allows ``rate`` acquisitions per second on average, with bursts up to ``capacity``.

    One instance is shared by every coroutine that calls the API, so the sum of all
    tiers never exceeds the OpenBoards limit.
    """

    def __init__(
        self,
        rate: float,
        capacity: float | None = None,
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        self.rate = rate
        self.capacity = capacity if capacity is not None else rate
        self._tokens = self.capacity
        self._clock = clock
        self._updated = clock()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        now = self._clock()
        self._tokens = min(self.capacity, self._tokens + (now - self._updated) * self.rate)
        self._updated = now

    async def acquire(self) -> None:
        async with self._lock:  # FIFO-ish: waiters queue on the lock
            while True:
                self._refill()
                if self._tokens >= 1:
                    self._tokens -= 1
                    return
                await asyncio.sleep((1 - self._tokens) / self.rate)
