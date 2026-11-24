"""HTTP client for the DB API Marketplace (OpenBoards, StaDa, FaSta)."""

from __future__ import annotations

import logging
import random
import time
from collections.abc import Callable
from typing import Any

import httpx

from bahn_pulse.settings import DbApiCredentials

BASE_URL = "https://apis.deutschebahn.com/db-api-marketplace/apis"
RETRY_STATUS = frozenset({429, 500, 502, 503, 504})

log = logging.getLogger(__name__)


class DbApiClient:
    """Thin wrapper around httpx with auth headers and retries on 429/5xx.

    Retries use exponential backoff with jitter and respect a Retry-After header.
    """

    def __init__(
        self,
        credentials: DbApiCredentials,
        *,
        base_url: str = BASE_URL,
        timeout_s: float = 60.0,
        max_retries: int = 4,
        backoff_base_s: float = 1.0,
        transport: httpx.BaseTransport | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client = httpx.Client(
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
        self._max_retries = max_retries
        self._backoff_base_s = backoff_base_s
        self._sleep = sleep

    def __enter__(self) -> DbApiClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    def request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        attempt = 0
        while True:
            try:
                response = self._client.request(method, path, **kwargs)
            except httpx.TransportError as exc:
                if attempt >= self._max_retries:
                    raise
                log.warning("Transport error on %s %s: %s", method, path, exc)
            else:
                if response.status_code not in RETRY_STATUS or attempt >= self._max_retries:
                    response.raise_for_status()
                    return response
                log.warning("HTTP %s on %s %s", response.status_code, method, path)
                retry_after = _retry_after_seconds(response)
                if retry_after is not None:
                    self._sleep(retry_after)
                    attempt += 1
                    continue
            self._sleep(self._backoff(attempt))
            attempt += 1

    def get(self, path: str, **kwargs: Any) -> httpx.Response:
        return self.request("GET", path, **kwargs)

    def _backoff(self, attempt: int) -> float:
        return self._backoff_base_s * (2**attempt) + random.uniform(0, self._backoff_base_s)  # noqa: S311


def _retry_after_seconds(response: httpx.Response) -> float | None:
    value = response.headers.get("Retry-After")
    if value is None:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        return None
