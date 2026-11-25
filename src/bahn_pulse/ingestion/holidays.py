"""Load German school holidays and public holidays into bronze.

Source: OpenHolidays API (https://openholidaysapi.org), open data under ODbL
("© OpenHolidays API contributors"). No key. Holidays are per federal state
(``subdivisions[].code`` like ``DE-BY``, the same codes as StaDa ``federal_state_code``);
some public holidays are local (e.g. ``DE-BY-AU``, Augsburg).

Each run stores a full snapshot of both lists for the current and the next calendar year
(the API allows up to three years per request). The data rarely changes, so a weekly run
is enough; later snapshots pick up corrections and newly published years.

Usage:
    uv run python -m bahn_pulse.ingestion.holidays
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, date, datetime
from typing import Any

import httpx

from bahn_pulse.ingestion.bronze import bronze_key, write_bronze

BASE_URL = "https://openholidaysapi.org/"
DATASETS = {  # bronze dataset -> API endpoint
    "school_holidays": "SchoolHolidays",
    "public_holidays": "PublicHolidays",
}
REQUIRED_FIELDS = ("id", "startDate", "endDate", "type", "name")
USER_AGENT = "bahn-pulse/0.1 (open-data portfolio project)"

log = logging.getLogger(__name__)


def window(today: date) -> tuple[date, date]:
    """Current and next calendar year."""
    return date(today.year, 1, 1), date(today.year + 1, 12, 31)


def build_params(valid_from: date, valid_to: date) -> dict[str, str]:
    return {
        "countryIsoCode": "DE",
        "languageIsoCode": "DE",
        "validFrom": valid_from.isoformat(),
        "validTo": valid_to.isoformat(),
    }


def validate(body: bytes) -> int:
    """Contract check: a non-empty JSON list of holidays with the fields we use."""
    rows = json.loads(body)
    if not isinstance(rows, list) or not rows:
        raise ValueError("Expected a non-empty list of holidays")
    for row in rows:
        missing = [f for f in REQUIRED_FIELDS if f not in row]
        if missing:
            raise ValueError(f"Holiday {row.get('id')} lacks {missing}")
    return len(rows)


def load_all(client: httpx.Client, *, s3: Any, bucket: str) -> dict[str, int]:
    fetched_at = datetime.now(UTC)
    valid_from, valid_to = window(fetched_at.date())
    params = build_params(valid_from, valid_to)
    bodies = {}
    for dataset, endpoint in DATASETS.items():
        response = client.get(BASE_URL + endpoint, params=params)
        response.raise_for_status()
        bodies[dataset] = (str(response.url), response.content, validate(response.content))
    # Write only after both lists passed validation.
    counts = {}
    for dataset, (url, body, count) in bodies.items():
        key = bronze_key("openholidays", dataset, fetched_at)
        write_bronze(
            s3,
            bucket,
            key,
            body,
            metadata={
                "source-url": url,
                "fetched-at": fetched_at.isoformat(),
                "valid-from": valid_from.isoformat(),
                "valid-to": valid_to.isoformat(),
                "record-count": str(count),
            },
        )
        log.info("Wrote %d %s to %s", count, dataset, key)
        counts[dataset] = count
    return counts


def _client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"}, timeout=30
    )


def handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    """AWS Lambda entry point (weekly schedule)."""
    import boto3

    from bahn_pulse.settings import bronze_bucket

    logging.getLogger().setLevel(logging.INFO)
    with _client() as client:
        return load_all(client, s3=boto3.client("s3"), bucket=bronze_bucket())


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    import boto3

    from bahn_pulse.settings import bronze_bucket, load_dotenv

    load_dotenv()
    with _client() as client:
        print(load_all(client, s3=boto3.client("s3"), bucket=bronze_bucket()))


if __name__ == "__main__":
    main()
