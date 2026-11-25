"""Load the German rail GTFS feeds (planned schedule) into bronze.

Source: gtfs.de, built from DELFI e.V. data, licensed CC BY 4.0
("Data source: DELFI e.V., via gtfs.de"). Shapes contain OpenStreetMap data (ODbL).
Feeds: ``fv_free`` (long-distance: ICE/IC/EC) and ``rv_free`` (regional rail: RE/RB/S-Bahn).
The local transport feed (buses, trams) is not loaded: OpenBoards covers rail only.

gtfs.de publishes a new version about once a week. Each version is stored once, as the
original zip, under a key derived from its ``Last-Modified`` time, so running this often
is cheap and idempotent: a version already in bronze is not downloaded again.

Usage:
    uv run python -m bahn_pulse.ingestion.gtfs [--feed rv_free] [--local DIR]
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any

import httpx
from botocore.exceptions import ClientError

from bahn_pulse.ingestion.bronze import bronze_key, write_bronze

FEEDS: dict[str, str] = {
    "fv_free": "https://download.gtfs.de/germany/fv_free/latest.zip",
    "rv_free": "https://download.gtfs.de/germany/rv_free/latest.zip",
}
REQUIRED_FILES = ("agency.txt", "stops.txt", "routes.txt", "trips.txt", "stop_times.txt")
USER_AGENT = "bahn-pulse/0.1 (open-data portfolio project)"

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FeedVersion:
    feed: str
    url: str
    last_modified: datetime
    etag: str | None

    @property
    def key(self) -> str:
        # Partitioned by the feed's publish time: one immutable object per feed version.
        return bronze_key("gtfs", self.feed, self.last_modified, ext="zip")


@dataclass(frozen=True)
class FeedSummary:
    stops: int
    routes: int
    trips: int
    service_start: str | None
    service_end: str | None


def head_feed(client: httpx.Client, feed: str) -> FeedVersion:
    url = FEEDS[feed]
    response = client.head(url)
    response.raise_for_status()
    last_modified = response.headers.get("last-modified")
    if not last_modified:
        raise RuntimeError(f"{url} sent no Last-Modified header; cannot version the feed")
    return FeedVersion(
        feed=feed,
        url=url,
        last_modified=parsedate_to_datetime(last_modified).astimezone(UTC),
        etag=response.headers.get("etag"),
    )


def _count_rows(z: zipfile.ZipFile, name: str) -> int:
    with z.open(name) as f:
        return max(sum(1 for _ in f) - 1, 0)  # minus header


def summarize_feed(body: bytes) -> FeedSummary:
    """Check the GTFS contract (required files present) and count the main entities."""
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        missing = [n for n in REQUIRED_FILES if n not in z.namelist()]
        if missing:
            raise ValueError(f"GTFS feed lacks required files: {missing}")
        start = end = None
        if "calendar.txt" in z.namelist():
            with z.open("calendar.txt") as f:
                rows = list(csv.DictReader(io.TextIOWrapper(f, "utf-8-sig")))
            if rows:
                start = min(r["start_date"] for r in rows)
                end = max(r["end_date"] for r in rows)
        return FeedSummary(
            stops=_count_rows(z, "stops.txt"),
            routes=_count_rows(z, "routes.txt"),
            trips=_count_rows(z, "trips.txt"),
            service_start=start,
            service_end=end,
        )


def _exists(s3: Any, bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
            return False
        raise
    return True


def load_feed(
    client: httpx.Client,
    feed: str,
    *,
    s3: Any = None,
    bucket: str | None = None,
    local: Path | None = None,
) -> str:
    """Store the current version of one feed. Returns ``"loaded"`` or ``"unchanged"``."""
    version = head_feed(client, feed)
    if local is None and _exists(s3, bucket, version.key):
        log.info("%s version %s already in bronze", feed, version.last_modified)
        return "unchanged"
    if local is not None and (local / version.key).exists():
        return "unchanged"

    response = client.get(version.url)
    response.raise_for_status()
    body = response.content
    summary = summarize_feed(body)  # validate before anything is written
    metadata = {
        "source-url": version.url,
        "last-modified": version.last_modified.isoformat(),
        "etag": response.headers.get("etag", version.etag or ""),
        "fetched-at": datetime.now(UTC).isoformat(),
        "stops": str(summary.stops),
        "trips": str(summary.trips),
        "service-start": summary.service_start or "",
        "service-end": summary.service_end or "",
    }
    if local is not None:
        path = local / version.key
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(body)
    else:
        write_bronze(
            s3,
            bucket,
            version.key,
            body,
            metadata,
            content_type="application/zip",
            compress=False,
        )
    log.info("Loaded %s (%d bytes, %s) to %s", feed, len(body), summary, version.key)
    return "loaded"


def load_all(
    *,
    s3: Any = None,
    bucket: str | None = None,
    local: Path | None = None,
    feeds: list[str] | None = None,
) -> dict[str, str]:
    with httpx.Client(
        headers={"User-Agent": USER_AGENT},
        timeout=httpx.Timeout(30, read=300),
        follow_redirects=True,
    ) as client:
        return {
            feed: load_feed(client, feed, s3=s3, bucket=bucket, local=local)
            for feed in (feeds or list(FEEDS))
        }


def handler(event: dict[str, Any], context: Any) -> dict[str, str]:
    """AWS Lambda entry point (daily schedule)."""
    import boto3

    from bahn_pulse.settings import bronze_bucket

    logging.getLogger().setLevel(logging.INFO)
    result = load_all(s3=boto3.client("s3"), bucket=bronze_bucket())
    log.info("gtfs result=%s", result)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--feed", choices=sorted(FEEDS), action="append")
    parser.add_argument("--local", type=Path, help="write to this folder instead of S3")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    from bahn_pulse.settings import load_dotenv

    load_dotenv()
    if args.local:
        print(load_all(local=args.local, feeds=args.feed))
        return
    import boto3

    from bahn_pulse.settings import bronze_bucket

    print(load_all(s3=boto3.client("s3"), bucket=bronze_bucket(), feeds=args.feed))


if __name__ == "__main__":
    main()
