"""Copy DWD weather warnings (CAP) from DWD open data into bronze.

Source: https://opendata.dwd.de/weather/alerts/cap/ (Deutscher Wetterdienst, open data,
attribution "Quelle: Deutscher Wetterdienst"; warn area geometry © GeoBasis-DE / BKG).
We use the municipality-level warnings (``COMMUNEUNION``) in German (``_DE``). Each zip holds
one CAP 1.2 XML per alert, with warn cell ids (``WARNCELLID``) and area polygons.

DWD publishes a package about every 10 minutes and keeps them for about 48 hours:
- ``COMMUNEUNION_DWD_DIFF``: what changed since the previous package (new, updated, cancelled)
- ``COMMUNEUNION_DWD_STAT``: all warnings active at that moment

We copy every DIFF package (complete history, ~1 MB per day) and the first STAT package of
each UTC day (a full state to start replaying from). Files are stored unchanged under their
DWD name; a file already in bronze is skipped, so a run can repeat or follow an outage of up
to ~2 days without gaps or duplicates.

Usage:
    uv run python -m bahn_pulse.ingestion.dwd_warnings
"""

from __future__ import annotations

import io
import logging
import re
import zipfile
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, date, datetime
from typing import Any

import httpx

from bahn_pulse.ingestion.bronze import write_bronze

BASE_URL = "https://opendata.dwd.de/weather/alerts/cap/"
SOURCES = {  # bronze dataset -> DWD directory
    "warnings_diff": "COMMUNEUNION_DWD_DIFF/",
    "warnings_status": "COMMUNEUNION_DWD_STAT/",
}
FILE_RE = re.compile(
    r"Z_CAP_C_EDZW_(?P<ts>\d{14})_PVW_(?:DIFFERENCE|STATUS)_PREMIUMDWD_COMMUNEUNION_DE\.zip"
)
USER_AGENT = "bahn-pulse/0.1 (open-data portfolio project)"

log = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class DwdFile:
    published_at: datetime  # UTC, from the file name
    name: str
    dataset: str

    @property
    def url(self) -> str:
        return BASE_URL + SOURCES[self.dataset] + self.name

    @property
    def key(self) -> str:
        ts = self.published_at
        return f"dwd/{self.dataset}/dt={ts:%Y-%m-%d}/hour={ts:%H}/{self.name}"


def parse_listing(html: str, dataset: str) -> list[DwdFile]:
    """Timestamped files in a directory listing (the ``LATEST`` aliases are ignored)."""
    names = sorted({m.group(0) for m in FILE_RE.finditer(html)})
    return [
        DwdFile(
            datetime.strptime(FILE_RE.fullmatch(n).group("ts"), "%Y%m%d%H%M%S").replace(tzinfo=UTC),
            n,
            dataset,
        )
        for n in names
    ]


def first_per_day(files: Iterable[DwdFile]) -> list[DwdFile]:
    by_day: dict[date, DwdFile] = {}
    for f in sorted(files):
        by_day.setdefault(f.published_at.date(), f)
    return list(by_day.values())


def count_alerts(body: bytes) -> int:
    """Validate a package: a zip of CAP XML alerts (an empty zip is valid). Returns the count."""
    with zipfile.ZipFile(io.BytesIO(body)) as z:
        names = z.namelist()
        for name in names:
            head = z.read(name)[:500]
            if not name.endswith(".xml") or b"<alert" not in head:
                raise ValueError(f"{name} is not a CAP alert")
        return len(names)


def stored_keys(s3: Any, bucket: str, dataset: str, days: Iterable[date]) -> set[str]:
    keys: set[str] = set()
    for day in sorted(set(days)):
        prefix = f"dwd/{dataset}/dt={day:%Y-%m-%d}/"
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            keys.update(obj["Key"] for obj in page.get("Contents", []))
    return keys


def sync(client: httpx.Client, *, s3: Any, bucket: str) -> dict[str, int]:
    """Copy all DIFF packages and one STAT package per day that are not in bronze yet."""
    stats = {"copied": 0, "skipped": 0}
    for dataset, directory in SOURCES.items():
        response = client.get(BASE_URL + directory)
        response.raise_for_status()
        files = parse_listing(response.text, dataset)
        if dataset == "warnings_status":
            files = first_per_day(files)
        existing = stored_keys(s3, bucket, dataset, (f.published_at.date() for f in files))
        if dataset == "warnings_status":
            # One per day: skip a day that already has a snapshot, even if it is another file.
            done_days = {k.split("/dt=")[1][:10] for k in existing}
            todo = [f for f in files if f"{f.published_at:%Y-%m-%d}" not in done_days]
        else:
            todo = [f for f in files if f.key not in existing]
        stats["skipped"] += len(files) - len(todo)
        for f in todo:
            body = client.get(f.url).raise_for_status().content
            alerts = count_alerts(body)
            write_bronze(
                s3,
                bucket,
                f.key,
                body,
                metadata={
                    "source-url": f.url,
                    "published-at": f.published_at.isoformat(),
                    "fetched-at": datetime.now(UTC).isoformat(),
                    "alerts": str(alerts),
                },
                content_type="application/zip",
                compress=False,
            )
            stats["copied"] += 1
        log.info("%s: %d files listed, %d copied", dataset, len(files), len(todo))
    return stats


def _client() -> httpx.Client:
    return httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=30, follow_redirects=True)


def handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """AWS Lambda entry point (hourly schedule)."""
    import boto3

    from bahn_pulse.settings import bronze_bucket

    logging.getLogger().setLevel(logging.INFO)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    with _client() as client:
        stats = sync(client, s3=boto3.client("s3"), bucket=bronze_bucket())
    log.info("dwd warnings stats=%s", stats)
    return stats


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    logging.getLogger("httpx").setLevel(logging.WARNING)
    import boto3

    from bahn_pulse.settings import bronze_bucket, load_dotenv

    load_dotenv()
    with _client() as client:
        print(sync(client, s3=boto3.client("s3"), bucket=bronze_bucket()))


if __name__ == "__main__":
    main()
