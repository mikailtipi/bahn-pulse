"""Run the OpenBoards poller.

Examples:
    # one run of tier 1, first 5 stations, written to ./data instead of S3
    uv run python -m bahn_pulse.openboards.run --tier 1 --once --limit 5 --local data

    # all tiers on their schedules, forever, into the bronze bucket
    uv run python -m bahn_pulse.openboards.run --forever
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import os
from datetime import UTC, datetime
from pathlib import Path

from bahn_pulse.openboards.client import OpenBoardsClient
from bahn_pulse.openboards.limiter import TokenBucket
from bahn_pulse.openboards.policy import POLICIES, TierPolicy
from bahn_pulse.openboards.poller import Station, load_stations, poll_tier
from bahn_pulse.openboards.writer import BatchWriter, local_put, s3_put
from bahn_pulse.settings import DbApiCredentials, bronze_bucket, load_dotenv

TIERS_CSV = Path("config/station_tiers.csv")
DEFAULT_RATE = 8.0  # requests per second, below the API limit of 10

log = logging.getLogger("bahn_pulse.openboards")


def _put_factory(local: Path | None):
    if local is not None:
        return local_put(local)
    import boto3

    return s3_put(boto3.client("s3"), bronze_bucket())


async def run_tier_once(
    client: OpenBoardsClient, stations: list[Station], policy: TierPolicy, local: Path | None
) -> dict[str, int]:
    run_id = f"tier{policy.tier}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}"
    writer = BatchWriter(run_id, _put_factory(local))
    try:
        return await poll_tier(client, stations, policy, writer.add)
    finally:
        await writer.close()


async def run_forever(
    client: OpenBoardsClient, stations: list[Station], tiers: list[int], local: Path | None
) -> None:
    """Start each tier on its interval.

    A tier slot is skipped (and logged) if the previous run of that tier is still busy.
    """
    running: dict[int, asyncio.Task[dict[str, int]]] = {}
    next_due = {t: 0.0 for t in tiers}
    loop = asyncio.get_running_loop()
    while True:
        now = loop.time()
        for tier in tiers:
            if now < next_due[tier]:
                continue
            next_due[tier] = now + POLICIES[tier].interval.total_seconds()
            task = running.get(tier)
            if task is not None and not task.done():
                log.warning("Tier %d still running, skipping this slot", tier)
                continue
            running[tier] = asyncio.create_task(
                run_tier_once(client, stations, POLICIES[tier], local)
            )
        await asyncio.sleep(1)


async def main_async(args: argparse.Namespace) -> None:
    stations = load_stations(TIERS_CSV)
    if args.limit:
        by_tier: dict[int, list[Station]] = {}
        for s in stations:
            by_tier.setdefault(s.tier, []).append(s)
        stations = [s for group in by_tier.values() for s in group[: args.limit]]
    tiers = [args.tier] if args.tier else sorted(POLICIES)
    limiter = TokenBucket(rate=args.rate)
    async with OpenBoardsClient(DbApiCredentials.from_env(), limiter) as client:
        if args.forever:
            await run_forever(client, stations, tiers, args.local)
        else:
            for tier in tiers:
                await run_tier_once(client, stations, POLICIES[tier], args.local)


def main() -> None:
    parser = argparse.ArgumentParser(description="OpenBoards poller")
    parser.add_argument("--tier", type=int, choices=sorted(POLICIES), help="only this tier")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="one run per selected tier")
    mode.add_argument("--forever", action="store_true", help="run all selected tiers on schedule")
    parser.add_argument("--limit", type=int, help="max stations per tier (for testing)")
    parser.add_argument("--local", type=Path, help="write to this folder instead of S3")
    parser.add_argument(
        "--rate", type=float, default=float(os.environ.get("OPENBOARDS_RATE", DEFAULT_RATE))
    )
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    load_dotenv()
    asyncio.run(main_async(args))


if __name__ == "__main__":
    main()
