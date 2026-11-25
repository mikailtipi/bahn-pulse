"""Load one day of hourly weather per station area from Open-Meteo into bronze.

Source: Open-Meteo Historical Forecast API (archived high-resolution model runs, no lag).
Data licence CC BY 4.0 ("Weather data by Open-Meteo.com"). Free API, non-commercial use:
< 10,000 calls/day, 5,000/hour, 600/minute; one location-day with <= 10 variables costs 1.

Stations are grouped into a 0.05 degree grid (~5 x 3.5 km): 3,807 cells instead of 5,408
stations, close to the weather model resolution. A station's cell is ``cell_of(lat, lon)``;
the same rule maps stations to weather rows later in the vault.

Once a day the previous UTC day is loaded as one bronze object
(``openmeteo/hourly/dt=<weather date>/...jsonl.gz``, one line per API call). The object is
written only if every call succeeded, and a day that is already in bronze is skipped.

Usage:
    uv run python -m bahn_pulse.ingestion.openmeteo [--date 2026-11-24 | --start D --end D]
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx

from bahn_pulse.ingestion.bronze import write_bronze

URL = "https://historical-forecast-api.open-meteo.com/v1/forecast"
GRID_DEG = 0.05
HOURLY_VARIABLES = (  # <= 10 variables keeps the cost at 1 call per location-day
    "temperature_2m",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
)
LOCATIONS_PER_REQUEST = 50  # 100 sometimes hit a server-side timeout
COST_PER_MINUTE = 480  # below the API limit of 600 per minute
MAX_ATTEMPTS = 5
USER_AGENT = "bahn-pulse/0.1 (open-data portfolio project)"
STATIONS_CSV = Path("config/station_tiers.csv")

log = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class Cell:
    lat: float
    lon: float

    @property
    def cell_id(self) -> str:
        return f"{self.lat:.2f}_{self.lon:.2f}"


def cell_of(lat: float, lon: float, grid: float = GRID_DEG) -> Cell:
    """Centre of the grid cell that contains the point."""
    return Cell(round(round(lat / grid) * grid, 4), round(round(lon / grid) * grid, 4))


def load_cells(path: Path = STATIONS_CSV) -> list[Cell]:
    with path.open(encoding="utf-8", newline="") as f:
        rows = [r for r in csv.DictReader(f) if r["lat"] and r["lon"]]
    return sorted({cell_of(float(r["lat"]), float(r["lon"])) for r in rows})


def chunks(cells: Sequence[Cell], size: int) -> list[Sequence[Cell]]:
    return [cells[i : i + size] for i in range(0, len(cells), size)]


def build_params(cells: Sequence[Cell], day: date) -> dict[str, str]:
    return {
        "latitude": ",".join(f"{c.lat:.2f}" for c in cells),
        "longitude": ",".join(f"{c.lon:.2f}" for c in cells),
        "hourly": ",".join(HOURLY_VARIABLES),
        "start_date": day.isoformat(),
        "end_date": day.isoformat(),
        "timezone": "GMT",
    }


def _parse_locations(content: bytes, expected: int) -> list[Any] | None:
    """The response as a list with one entry per requested location, or None if unusable."""
    try:
        body = json.loads(content)
    except ValueError:
        return None
    if isinstance(body, dict) and expected == 1 and "hourly" in body:
        body = [body]  # a single location comes back as an object, not a list
    if not isinstance(body, list) or len(body) != expected:
        return None
    return body


def fetch_batch(
    client: httpx.Client,
    cells: Sequence[Cell],
    day: date,
    sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """One API call for a batch of cells. Retries 429/5xx; raises if it still fails.

    Open-Meteo can answer HTTP 200 with a non-JSON error text (e.g. a server-side timeout);
    that counts as a failed attempt too.
    """
    params = build_params(cells, day)
    for attempt in range(1, MAX_ATTEMPTS + 1):
        response = client.get(URL, params=params)
        problem = f"HTTP {response.status_code}"
        if response.status_code == 200:
            body = _parse_locations(response.content, len(cells))
            if body is not None:
                return {
                    "weather_date": day.isoformat(),
                    "fetched_at": datetime.now(UTC).isoformat(timespec="seconds"),
                    "cell_ids": [c.cell_id for c in cells],
                    "request": params,
                    "attempts": attempt,
                    "cost": float(response.headers.get("x-ratelimit-cost", len(cells))),
                    "response": body,
                }
            problem = f"unusable 200 body: {response.text[:80]!r}"
        elif response.status_code != 429 and response.status_code < 500:
            response.raise_for_status()
        wait = min(60.0, 2.0**attempt)
        log.warning("Open-Meteo %s, retry %d in %.0fs", problem, attempt, wait)
        sleep(wait)
    raise RuntimeError(f"Open-Meteo failed {MAX_ATTEMPTS} times for {day} ({len(cells)} cells)")


def fetch_day(
    client: httpx.Client,
    cells: Sequence[Cell],
    day: date,
    *,
    sleep: Callable[[float], None] = time.sleep,
) -> list[dict[str, Any]]:
    """All cells for one day, paced to stay under the per-minute cost limit."""
    pause = LOCATIONS_PER_REQUEST * 60 / COST_PER_MINUTE  # seconds between full batches
    records = []
    for i, batch in enumerate(chunks(cells, LOCATIONS_PER_REQUEST)):
        if i:
            sleep(pause)
        records.append(fetch_batch(client, batch, day, sleep))
    return records


def to_jsonl(records: Iterable[dict[str, Any]]) -> bytes:
    return b"".join(
        json.dumps(r, ensure_ascii=False, separators=(",", ":")).encode() + b"\n" for r in records
    )


def day_prefix(day: date) -> str:
    return f"openmeteo/hourly/dt={day.isoformat()}/"


def day_key(day: date, fetched_at: datetime) -> str:
    return f"{day_prefix(day)}openmeteo_hourly_{day:%Y%m%d}_{fetched_at:%Y%m%dT%H%M%SZ}.jsonl.gz"


def day_exists(s3: Any, bucket: str, day: date) -> bool:
    response = s3.list_objects_v2(Bucket=bucket, Prefix=day_prefix(day), MaxKeys=1)
    return response.get("KeyCount", 0) > 0


def load_day(
    client: httpx.Client,
    cells: Sequence[Cell],
    day: date,
    *,
    s3: Any,
    bucket: str,
    sleep: Callable[[float], None] = time.sleep,
) -> str:
    """Store one day. Returns ``"loaded"`` or ``"exists"``."""
    if day >= datetime.now(UTC).date():
        raise ValueError(f"{day} is not complete yet (UTC)")
    if day_exists(s3, bucket, day):
        log.info("Weather for %s already in bronze", day)
        return "exists"
    records = fetch_day(client, cells, day, sleep=sleep)
    fetched_at = datetime.now(UTC)
    key = day_key(day, fetched_at)
    write_bronze(
        s3,
        bucket,
        key,
        to_jsonl(records),
        metadata={
            "source-url": URL,
            "weather-date": day.isoformat(),
            "fetched-at": fetched_at.isoformat(),
            "cells": str(len(cells)),
            "grid-deg": str(GRID_DEG),
            "cost": str(sum(r["cost"] for r in records)),
        },
        content_type="application/x-ndjson",
    )
    log.info("Loaded weather for %s: %d cells, %d calls to %s", day, len(cells), len(records), key)
    return "loaded"


def _client() -> httpx.Client:
    return httpx.Client(headers={"User-Agent": USER_AGENT}, timeout=httpx.Timeout(30, read=120))


def handler(event: dict[str, Any], context: Any) -> dict[str, str]:
    """AWS Lambda entry point. Loads ``event["date"]`` or, by default, yesterday (UTC)."""
    import boto3

    from bahn_pulse.settings import bronze_bucket

    logging.getLogger().setLevel(logging.INFO)
    day = date.fromisoformat(event["date"]) if event.get("date") else None
    day = day or datetime.now(UTC).date() - timedelta(days=1)
    stations_csv = Path(os.environ.get("STATIONS_CSV", str(STATIONS_CSV)))
    with _client() as client:
        result = load_day(
            client, load_cells(stations_csv), day, s3=boto3.client("s3"), bucket=bronze_bucket()
        )
    return {"date": day.isoformat(), "result": result}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", type=date.fromisoformat)
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    import boto3

    from bahn_pulse.settings import bronze_bucket, load_dotenv

    load_dotenv()
    yesterday = datetime.now(UTC).date() - timedelta(days=1)
    start = args.date or args.start or yesterday
    end = args.date or args.end or start
    cells = load_cells()
    s3, bucket = boto3.client("s3"), bronze_bucket()
    with _client() as client:
        day = start
        while day <= end:
            print(day, load_day(client, cells, day, s3=s3, bucket=bucket))
            day += timedelta(days=1)


if __name__ == "__main__":
    main()
