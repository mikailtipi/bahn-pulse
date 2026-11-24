import asyncio
import gzip
import json
from datetime import UTC, datetime

import httpx
import pytest

from bahn_pulse.ingestion.bronze import bronze_key
from bahn_pulse.openboards.client import OpenBoardsClient
from bahn_pulse.openboards.limiter import TokenBucket
from bahn_pulse.openboards.policy import POLICIES
from bahn_pulse.openboards.poller import (
    Station,
    build_payload,
    group_stations,
    load_stations,
    make_run_id,
    poll_tier,
    select_stations,
)
from bahn_pulse.openboards.writer import BatchWriter, to_jsonl
from bahn_pulse.settings import DbApiCredentials

CREDS = DbApiCredentials(client_id="id-123", api_key="secret-456")
NOW = datetime(2026, 11, 24, 16, 0, tzinfo=UTC)


def st(eva: int, tier: int = 3, extra: tuple[str, ...] = ()) -> Station:
    return Station(eva, (str(eva), *extra), tier)


# --- grouping and payload -------------------------------------------------


def test_group_stations_respects_group_size_and_eva_limit() -> None:
    stations = [st(i) for i in range(1, 8)]
    assert [len(g) for g in group_stations(stations, 3)] == [3, 3, 1]

    many_evas = [st(1, extra=tuple(str(x) for x in range(10, 23))), st(2), st(3)]  # 14 + 1 + 1
    groups = group_stations(many_evas, 10)
    assert all(sum(len(s.eva_numbers) for s in g) <= 15 for g in groups)
    assert [len(g) for g in groups] == [2, 1]


def test_build_payload_uses_all_evas_no_expansion_and_utc_window() -> None:
    payload = build_payload([st(8000105, 1, ("8098105",))], POLICIES[1], NOW)
    assert payload["stopPlaceOrStation"] == {
        "evaNumbers": ["8000105", "8098105"],
        "boardConfigurations": ["NONE"],
    }
    assert payload["timeWindow"] == {
        "timeStart": "2026-11-24T15:45:00Z",
        "timeEnd": "2026-11-24T18:00:00Z",
    }
    assert payload["maxEntriesPerTransportType"] == 50


def test_policy_windows_stay_within_api_limits() -> None:
    for policy in POLICIES.values():
        assert policy.lookback + policy.horizon <= __import__("datetime").timedelta(hours=8)
        assert 1 <= policy.group_size <= 15


def test_load_stations_skips_rows_without_tier(tmp_path) -> None:
    csv_path = tmp_path / "tiers.csv"
    csv_path.write_text(
        "eva_number,eva_numbers,tier\n8000105,8000105|8098105,1\n8011060,8011060,\n"
    )
    assert load_stations(csv_path) == [Station(8000105, ("8000105", "8098105"), 1)]


# --- limiter --------------------------------------------------------------


def test_token_bucket_limits_rate() -> None:
    async def scenario() -> float:
        bucket = TokenBucket(rate=50, capacity=1)
        loop = asyncio.get_running_loop()
        start = loop.time()
        await asyncio.gather(*(bucket.acquire() for _ in range(11)))
        return loop.time() - start

    elapsed = asyncio.run(scenario())
    assert elapsed >= 10 / 50 * 0.9  # first token is free, then 10 more at 50/s


def test_token_bucket_rejects_bad_rate() -> None:
    with pytest.raises(ValueError, match="positive"):
        TokenBucket(rate=0)


# --- client + poller ------------------------------------------------------


def board_body(evas: list[str], truncated: list[str] | None = None) -> dict:
    return {
        "departures": [{"evaNumber": e, "timeType": "PREVIEW"} for e in evas],
        "meta": {"consideredStopPlaces": evas, "truncatedTransportTypes": truncated or []},
    }


def make_client(handler) -> OpenBoardsClient:
    return OpenBoardsClient(
        CREDS,
        TokenBucket(rate=1000, capacity=1000),
        base_url="https://example.test",
        transport=httpx.MockTransport(handler),
        backoff_base_s=0.001,
    )


def test_poll_tier_splits_truncated_groups_and_keeps_raw_response() -> None:
    calls: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        evas = json.loads(request.content)["stopPlaceOrStation"]["evaNumbers"]
        calls.append(evas)
        truncated = ["REGIONAL_TRAIN"] if len(evas) > 2 else None
        return httpx.Response(200, json=board_body(evas, truncated))

    records: list[dict] = []

    async def sink(batch: list[dict]) -> None:
        records.extend(batch)

    async def scenario() -> dict:
        async with make_client(handler) as client:
            return await poll_tier(client, [st(i) for i in range(1, 5)], POLICIES[3], sink)

    stats = asyncio.run(scenario())

    # one group of 4 per board -> truncated -> split into 2 + 2, for both boards
    assert stats == {"requests": 4, "errors": 0, "truncated": 0}
    assert {r["board"] for r in records} == {"departures", "arrivals"}
    assert sorted(tuple(r["eva_numbers"]) for r in records) == [
        ("1", "2"),
        ("1", "2"),
        ("3", "4"),
        ("3", "4"),
    ]
    assert (
        json.loads(records[0]["response"])["meta"]["consideredStopPlaces"]
        == (records[0]["eva_numbers"])
    )
    assert records[0]["polled_at"].endswith("Z")
    assert len(calls) == 6  # 2 truncated + 4 split


def test_client_retries_then_records_error_without_raising() -> None:
    attempts = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        attempts["n"] += 1
        return httpx.Response(503)

    async def scenario():
        async with make_client(handler) as client:
            return await client.board("departures", {"x": 1})

    result = asyncio.run(scenario())
    assert result.status == 503
    assert result.body is None
    assert result.error == "HTTP 503"
    assert attempts["n"] == 5  # 1 + 4 retries


def test_client_retries_429_then_succeeds() -> None:
    responses = iter([httpx.Response(429, headers={"Retry-After": "0"}), httpx.Response(200)])

    async def scenario():
        async with make_client(lambda r: next(responses)) as client:
            return await client.board("arrivals", {})

    result = asyncio.run(scenario())
    assert (result.status, result.attempts, result.error) == (200, 2, None)


def test_client_does_not_retry_4xx() -> None:
    async def scenario():
        async with make_client(lambda r: httpx.Response(400, text="bad request")) as client:
            return await client.board("departures", {})

    result = asyncio.run(scenario())
    assert (result.status, result.attempts, result.error) == (400, 1, "bad request")


# --- writer ---------------------------------------------------------------


def test_batch_writer_flushes_by_size_and_on_close() -> None:
    written: dict[str, bytes] = {}

    async def scenario() -> BatchWriter:
        writer = BatchWriter(
            "tier1-x", lambda k, b: written.__setitem__(k, b), max_records=3, clock=lambda: NOW
        )
        await writer.add([{"i": 1}, {"i": 2}])
        await writer.add([{"i": 3}, {"i": 4}])  # -> flush of 4
        await writer.add([{"i": 5}])
        await writer.close()  # -> flush of 1
        return writer

    writer = asyncio.run(scenario())
    assert writer.keys == [
        bronze_key("openboards", "boards", NOW, ext="jsonl.gz", part="tier1-x_p001"),
        bronze_key("openboards", "boards", NOW, ext="jsonl.gz", part="tier1-x_p002"),
    ]
    first = written[writer.keys[0]].decode().splitlines()
    assert [json.loads(line)["i"] for line in first] == [1, 2, 3, 4]


def test_to_jsonl_is_compact_utf8() -> None:
    assert to_jsonl([{"name": "Köln Hbf"}]) == '{"name":"Köln Hbf"}\n'.encode()
    assert gzip.decompress(gzip.compress(to_jsonl([{}]))) == b"{}\n"


def test_to_jsonl_embeds_raw_response_bytes_verbatim() -> None:
    raw = b'{"departures":[],"meta":{"x":"\xc3\xa4"}}'
    line = to_jsonl([{"tier": 1, "response": raw}])
    assert line == b'{"tier":1,"response":' + raw + b"}\n"
    assert json.loads(line)["response"]["meta"]["x"] == "\u00e4"


def test_to_jsonl_reserializes_multiline_response() -> None:
    line = to_jsonl([{"response": b'{\n  "a": 1\n}\n'}])
    assert line == b'{"response":{"a":1}}\n'


def test_select_stations_shards_cover_tier_exactly_once():
    policy = POLICIES[3]
    stations = [Station(8000000 + i, (str(8000000 + i),), 3) for i in range(41)]
    stations.append(Station(8100000, ("8100000",), 1))  # other tier, never selected
    shards = [select_stations(stations, policy, k) for k in range(policy.shards)]
    seen = [s.eva_number for shard in shards for s in shard]
    assert sorted(seen) == [s.eva_number for s in stations if s.tier == 3]
    assert select_stations(stations, policy) == [s for s in stations if s.tier == 3]
    with pytest.raises(ValueError, match="out of range"):
        select_stations(stations, policy, policy.shards)


def test_make_run_id_includes_shard():
    now = datetime(2026, 11, 24, 17, 5, tzinfo=UTC)
    assert make_run_id(3, now, 2) == "tier3s2-20261124T170500Z"
    assert make_run_id(1, now) == "tier1-20261124T170500Z"
