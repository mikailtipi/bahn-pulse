"""Batch poll records into gzipped JSONL objects, so S3 sees few large PUTs, not one per call."""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from bahn_pulse.ingestion.bronze import S3Client, bronze_key, write_bronze
from bahn_pulse.openboards.poller import Record

log = logging.getLogger(__name__)


def to_jsonl(records: list[Record]) -> bytes:
    return b"".join(
        json.dumps(r, ensure_ascii=False, separators=(",", ":")).encode() + b"\n" for r in records
    )


class BatchWriter:
    """Collects records of one run and flushes them every ``max_records`` and on close.

    ``put`` receives (key, jsonl_bytes) and does the actual write (S3 or local file).
    """

    def __init__(
        self,
        run_id: str,
        put: Callable[[str, bytes], None],
        *,
        max_records: int = 500,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._run_id = run_id
        self._put = put
        self._max_records = max_records
        self._clock = clock
        self._buffer: list[Record] = []
        self._part = 0
        self._lock = asyncio.Lock()
        self.keys: list[str] = []

    async def add(self, records: list[Record]) -> None:
        async with self._lock:
            self._buffer.extend(records)
            if len(self._buffer) >= self._max_records:
                await self._flush()

    async def close(self) -> None:
        async with self._lock:
            if self._buffer:
                await self._flush()

    async def _flush(self) -> None:
        records, self._buffer = self._buffer, []
        self._part += 1
        key = bronze_key(
            "openboards",
            "boards",
            self._clock(),
            ext="jsonl.gz",
            part=f"{self._run_id}_p{self._part:03d}",
        )
        await asyncio.to_thread(self._put, key, to_jsonl(records))
        self.keys.append(key)
        log.info("Flushed %d records to %s", len(records), key)


def s3_put(s3: S3Client, bucket: str) -> Callable[[str, bytes], None]:
    def put(key: str, body: bytes) -> None:
        write_bronze(
            s3,
            bucket,
            key,
            body,
            metadata={"source": "openboards"},
            content_type="application/x-ndjson",
        )

    return put


def local_put(root: Path) -> Callable[[str, bytes], None]:
    def put(key: str, body: bytes) -> None:
        path = root / key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(gzip.compress(body))

    return put
