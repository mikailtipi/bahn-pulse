"""AWS Lambda entry point: one invocation = one poll run of one tier.

EventBridge Scheduler invokes the function with ``{"tier": <n>}``, or
``{"tier": <n>, "shard": <k>}`` for tiers split into shards (see ``TierPolicy.shards``).
Environment:
    BRONZE_BUCKET           target bucket
    OPENBOARDS_RATE         requests per second for this tier (tiers share the API limit)
    DB_CLIENT_ID_PARAM      SSM parameter name (SecureString) with the DB client id
    DB_API_KEY_PARAM        SSM parameter name (SecureString) with the DB api key
    STATIONS_CSV            path of the packaged station tier list
"""

from __future__ import annotations

import asyncio
import logging
import os
from functools import cache
from typing import Any

import boto3

from bahn_pulse.openboards.client import OpenBoardsClient
from bahn_pulse.openboards.limiter import TokenBucket
from bahn_pulse.openboards.policy import POLICIES
from bahn_pulse.openboards.poller import load_stations
from bahn_pulse.openboards.run import TIERS_CSV, run_tier_once
from bahn_pulse.settings import DbApiCredentials

logging.getLogger().setLevel(logging.INFO)
logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per request is too much in Lambda
log = logging.getLogger(__name__)

MAX_ERROR_SHARE = 0.5


@cache
def _credentials() -> DbApiCredentials:
    """Read once per container (warm invocations reuse it)."""
    ssm = boto3.client("ssm")
    names = [os.environ["DB_CLIENT_ID_PARAM"], os.environ["DB_API_KEY_PARAM"]]
    response = ssm.get_parameters(Names=names, WithDecryption=True)
    if response["InvalidParameters"]:
        raise RuntimeError(f"Missing SSM parameters: {response['InvalidParameters']}")
    values = {p["Name"]: p["Value"] for p in response["Parameters"]}
    return DbApiCredentials(client_id=values[names[0]], api_key=values[names[1]])


async def _run(tier: int, shard: int | None) -> dict[str, int]:
    limiter = TokenBucket(rate=float(os.environ.get("OPENBOARDS_RATE", "2")))
    stations = load_stations(TIERS_CSV)
    async with OpenBoardsClient(_credentials(), limiter) as client:
        return await run_tier_once(client, stations, POLICIES[tier], local=None, shard=shard)


def handler(event: dict[str, Any], context: Any) -> dict[str, int]:
    tier = int(event["tier"])
    shard = int(event["shard"]) if event.get("shard") is not None else None
    stats = asyncio.run(_run(tier, shard))
    log.info("tier=%d shard=%s stats=%s", tier, shard, stats)
    if stats["requests"] and stats["errors"] / stats["requests"] > MAX_ERROR_SHARE:
        # Fail the invocation so the Lambda Errors metric (and later an alarm) shows it.
        raise RuntimeError(f"Too many failed API calls in tier {tier}: {stats}")
    return stats
