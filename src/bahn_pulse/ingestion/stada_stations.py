"""Load all StaDa stations into bronze and regenerate the station tier list.

Usage:
    uv run python -m bahn_pulse.ingestion.stada_stations [--no-bronze]
"""

from __future__ import annotations

import argparse
import collections
import logging
from datetime import UTC, datetime
from pathlib import Path

import boto3

from bahn_pulse.clients.db_api import BASE_URL, DbApiClient
from bahn_pulse.ingestion.bronze import bronze_key, write_bronze
from bahn_pulse.settings import DbApiCredentials, bronze_bucket, load_dotenv
from bahn_pulse.stations.stada import STATIONS_PATH, fetch_stations, parse_stations
from bahn_pulse.stations.tiers import assign_tiers, read_overrides, write_tiers_csv

CONFIG_DIR = Path("config")
TIERS_CSV = CONFIG_DIR / "station_tiers.csv"
OVERRIDES_CSV = CONFIG_DIR / "station_tier_overrides.csv"

log = logging.getLogger(__name__)


def run(*, write_to_bronze: bool) -> None:
    fetched_at = datetime.now(UTC)
    with DbApiClient(DbApiCredentials.from_env()) as client:
        body = fetch_stations(client)

    stations = parse_stations(body)  # validate the contract before anything is written
    log.info("Fetched %d stations from StaDa", len(stations))

    if write_to_bronze:
        bucket = bronze_bucket()
        key = bronze_key("stada", "stations", fetched_at)
        write_bronze(
            boto3.client("s3"),
            bucket,
            key,
            body,
            metadata={
                "source-url": BASE_URL + STATIONS_PATH,
                "fetched-at": fetched_at.isoformat(),
                "record-count": str(len(stations)),
            },
        )
        log.info("Wrote s3://%s/%s", bucket, key)

    rows = assign_tiers(stations, read_overrides(OVERRIDES_CSV))
    write_tiers_csv(rows, TIERS_CSV)
    counts = collections.Counter(r.tier for r in rows)
    log.info(
        "Wrote %s: tier1=%d tier2=%d tier3=%d no_tier=%d",
        TIERS_CSV,
        counts[1],
        counts[2],
        counts[3],
        counts[None],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--no-bronze", action="store_true", help="skip the S3 write")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    load_dotenv()
    run(write_to_bronze=not args.no_bronze)


if __name__ == "__main__":
    main()
