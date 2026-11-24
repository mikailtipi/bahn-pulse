"""Async OpenBoards client: auth, shared rate limit, retries on 429/5xx."""

from __future__ import annotations

import asyncio
import logging
import random
from dataclasses import dataclass
from typing import Any

import httpx

from bahn_pulse.clients.db_api import BASE_URL, RETRY_STATUS
from bahn_pulse.openboards.limiter import TokenBucket
from bahn_pulse.settings import DbApiCredentials

OPENBOARDS_PATH = "/open-boards/v1"

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class BoardResponse:
    status: int
    body: bytes | None
    error: str | None
    attempts: int
    duration_ms: int


class OpenBoardsClient:
    def __init__(
        self,
        credentials: DbApiCredentials,
        limiter: TokenBucket,
        *,
        base_url: str = BASE_URL + OPENBOARDS_PATH,
        timeout_s: float = 60.0,
        max_retries: int = 4,
        backoff_base_s: float = 1.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout_s,
            transport=transport,
            headers={
                "DB-Client-Id": credentials.client_id,
                "DB-Api-Key": credentials.api_key,
                "Accept": "application/json",
                "User-Agent": "bahn-pulse (open-source data project)",
            },
        )
        self._limiter = limiter
        self._max_retries = max_retries
        self._backoff_base_s = backoff_base_s

    async def __aenter__(self) -> OpenBoardsClient:
        return self

    async def __aexit__(self, *exc: object) -> None:
        await self._client.aclose()

    async def board(self, board: str, payload: dict[str, Any]) -> BoardResponse:
        """POST /departures or /arrivals.

        Never raises on HTTP errors: the returned BoardResponse says what happened.
        """
        if board not in ("departures", "arrivals"):
            raise ValueError(f"unknown board {board!r}")
        loop = asyncio.get_running_loop()
        started = loop.time()
        attempt = 0
        while True:
            attempt += 1
            await self._limiter.acquire()
            error: str | None = None
            try:
                response = await self._client.post(f"/{board}", json=payload)
            except httpx.TransportError as exc:
                status, body, error = 0, None, f"{type(exc).__name__}: {exc}"
                retry_after = None
            else:
                status, body = response.status_code, response.content
                retry_after = _retry_after_seconds(response)
                if status not in RETRY_STATUS:
                    if status >= 400:
                        error = response.text[:500]
                    return BoardResponse(
                        status, body if status < 400 else None, error, attempt, _ms(loop, started)
                    )
                error = f"HTTP {status}"
            if attempt > self._max_retries:
                return BoardResponse(status, None, error, attempt, _ms(loop, started))
            log.warning("%s on %s (attempt %d), retrying", error, board, attempt)
            await asyncio.sleep(retry_after if retry_after is not None else self._backoff(attempt))

    def _backoff(self, attempt: int) -> float:
        return self._backoff_base_s * (2 ** (attempt - 1)) + random.uniform(  # noqa: S311
            0, self._backoff_base_s
        )


def _ms(loop: asyncio.AbstractEventLoop, started: float) -> int:
    return int((loop.time() - started) * 1000)


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
