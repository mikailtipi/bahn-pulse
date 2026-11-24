"""Bronze layer: raw source payloads on S3, gzipped, immutable, hourly partitions."""

from __future__ import annotations

import gzip
from datetime import UTC, datetime
from typing import Any, Protocol


class S3Client(Protocol):
    def put_object(self, **kwargs: Any) -> Any: ...


def bronze_key(
    source: str,
    dataset: str,
    fetched_at: datetime,
    ext: str = "json.gz",
    part: str | None = None,
) -> str:
    """Object key, partitioned by UTC date and hour.

    Example: stada/stations/dt=2026-11-24/hour=16/stada_stations_20261124T163005Z.json.gz
    """
    if fetched_at.tzinfo is None:
        raise ValueError("fetched_at must be timezone-aware")
    ts = fetched_at.astimezone(UTC)
    suffix = f"_{part}" if part else ""
    return (
        f"{source}/{dataset}/dt={ts:%Y-%m-%d}/hour={ts:%H}/"
        f"{source}_{dataset}_{ts:%Y%m%dT%H%M%SZ}{suffix}.{ext}"
    )


def write_bronze(
    s3: S3Client,
    bucket: str,
    key: str,
    body: bytes,
    metadata: dict[str, str],
    content_type: str = "application/json",
) -> None:
    """Write one raw payload. Never overwrites: fails if the key already exists."""
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=gzip.compress(body),
        ContentType=content_type,
        ContentEncoding="gzip",
        Metadata=metadata,
        IfNoneMatch="*",
    )
