"""Poll OpenBoards for one tier and turn every API call into one bronze record.

Stations are grouped (several EVA numbers per request) to save API calls. If a grouped
response is truncated (``meta.truncatedTransportTypes``), the group is split in half and
polled again, down to single stations. A truncated single-station response is kept and
flagged: at very large hubs the window then ends a bit earlier for that transport type.
"""

from __future__ import annotations

import asyncio
import csv
import json
import logging
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bahn_pulse.openboards.client import OpenBoardsClient
from bahn_pulse.openboards.policy import (
    BOARDS,
    MAX_ENTRIES_PER_TRANSPORT_TYPE,
    MAX_EVA_NUMBERS_PER_REQUEST,
    TierPolicy,
)

log = logging.getLogger(__name__)

Record = dict[str, Any]


@dataclass(frozen=True)
class Station:
    eva_number: int
    eva_numbers: tuple[str, ...]
    tier: int


def load_stations(path: Path) -> list[Station]:
    with path.open(encoding="utf-8", newline="") as f:
        return [
            Station(int(r["eva_number"]), tuple(r["eva_numbers"].split("|")), int(r["tier"]))
            for r in csv.DictReader(f)
            if r["tier"].strip()
        ]


def group_stations(stations: Sequence[Station], group_size: int) -> list[list[Station]]:
    groups: list[list[Station]] = []
    current: list[Station] = []
    for station in stations:
        n_eva = sum(len(s.eva_numbers) for s in current) + len(station.eva_numbers)
        if current and (len(current) >= group_size or n_eva > MAX_EVA_NUMBERS_PER_REQUEST):
            groups.append(current)
            current = []
        current.append(station)
    if current:
        groups.append(current)
    return groups


def make_run_id(tier: int, now: datetime, shard: int | None = None) -> str:
    part = f"s{shard}" if shard is not None else ""
    return f"tier{tier}{part}-{now.astimezone(UTC):%Y%m%dT%H%M%SZ}"


def select_stations(
    stations: Iterable[Station], policy: TierPolicy, shard: int | None = None
) -> list[Station]:
    """Stations of the policy's tier; only one shard of them if ``shard`` is given."""
    selected = [s for s in stations if s.tier == policy.tier]
    if shard is None:
        return selected
    if not 0 <= shard < policy.shards:
        raise ValueError(f"Shard {shard} out of range for tier {policy.tier} ({policy.shards})")
    return [s for s in selected if s.eva_number % policy.shards == shard]


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def build_payload(group: Sequence[Station], policy: TierPolicy, now: datetime) -> dict[str, Any]:
    return {
        "stopPlaceOrStation": {
            "evaNumbers": [eva for s in group for eva in s.eva_numbers],
            "boardConfigurations": ["NONE"],  # exactly these stations, no transfer-area expansion
        },
        "timeWindow": {
            "timeStart": _iso(now - policy.lookback),
            "timeEnd": _iso(now + policy.horizon),
        },
        "maxEntriesPerTransportType": MAX_ENTRIES_PER_TRANSPORT_TYPE,
        # Every station is polled itself, so the stop chain comes from our own data.
        "maxViaStops": 0,
        "languages": ["deu"],
    }


def _truncated_types(body: bytes | None) -> list[str]:
    if not body:
        return []
    try:
        meta = json.loads(body).get("meta") or {}
    except ValueError:
        return []
    return list(meta.get("truncatedTransportTypes") or [])


async def poll_group(
    client: OpenBoardsClient,
    board: str,
    group: list[Station],
    policy: TierPolicy,
    now: datetime,
    run_id: str,
) -> list[Record]:
    payload = build_payload(group, policy, now)
    polled_at = datetime.now(UTC)
    result = await client.board(board, payload)
    truncated = _truncated_types(result.body)
    if truncated and len(group) > 1:
        mid = len(group) // 2
        left, right = await asyncio.gather(
            poll_group(client, board, group[:mid], policy, now, run_id),
            poll_group(client, board, group[mid:], policy, now, run_id),
        )
        return left + right
    record: Record = {
        "run_id": run_id,
        "tier": policy.tier,
        "board": board,
        "polled_at": _iso(polled_at),
        "eva_numbers": payload["stopPlaceOrStation"]["evaNumbers"],
        "request": payload,
        "http_status": result.status,
        "attempts": result.attempts,
        "duration_ms": result.duration_ms,
        "truncated_transport_types": truncated,
        "error": result.error,
        # Raw response bytes, embedded verbatim by the writer (keeps memory low, stays truly raw).
        "response": result.body,
    }
    return [record]


async def poll_tier(
    client: OpenBoardsClient,
    stations: Iterable[Station],
    policy: TierPolicy,
    sink: Callable[[list[Record]], Awaitable[None]],
    *,
    now: datetime | None = None,
    run_id: str | None = None,
    shard: int | None = None,
    max_in_flight: int = 64,
) -> dict[str, int]:
    """Poll every station of one tier, or one shard of it (both boards).

    Records go to ``sink`` as they arrive. ``max_in_flight`` must be high enough that slow
    responses (grouped requests take ~10 s) do not keep the run below the limiter's rate.
    """
    now = now or datetime.now(UTC)
    run_id = run_id or make_run_id(policy.tier, now, shard)
    groups = group_stations(select_stations(stations, policy, shard), policy.group_size)
    semaphore = asyncio.Semaphore(max_in_flight)
    stats = {"requests": 0, "errors": 0, "truncated": 0}

    async def one(board: str, group: list[Station]) -> None:
        async with semaphore:
            records = await poll_group(client, board, group, policy, now, run_id)
        stats["requests"] += len(records)
        stats["errors"] += sum(1 for r in records if r["error"])
        stats["truncated"] += sum(1 for r in records if r["truncated_transport_types"])
        await sink(records)

    await asyncio.gather(*(one(board, g) for g in groups for board in BOARDS))
    log.info("Run %s done: %d groups, %s", run_id, len(groups), stats)
    return stats
