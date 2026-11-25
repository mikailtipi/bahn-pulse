import gzip
import json
from datetime import UTC, date, datetime

import httpx
import pytest

from bahn_pulse.ingestion import openmeteo
from bahn_pulse.ingestion.openmeteo import Cell, cell_of, fetch_day, load_cells, load_day

DAY = date(2026, 11, 24)


def test_cell_of_snaps_to_grid_centre():
    assert cell_of(50.1071, 8.6633) == Cell(50.1, 8.65)
    assert cell_of(52.5251, 13.3694).cell_id == "52.55_13.35"


def test_load_cells_dedupes_stations_in_same_cell(tmp_path):
    csv_path = tmp_path / "stations.csv"
    csv_path.write_text(
        "eva_number,lat,lon,tier\n1,50.107,8.663,1\n2,50.109,8.661,3\n3,52.5251,13.369,1\n4,,,\n"
    )
    assert load_cells(csv_path) == [Cell(50.1, 8.65), Cell(52.55, 13.35)]


def _response(n_locations: int) -> list[dict]:
    hourly = {"time": [f"2026-11-24T{h:02d}:00" for h in range(24)], "temperature_2m": [1.0] * 24}
    return [{"latitude": 50.0, "longitude": 8.0, "hourly": hourly}] * n_locations


def mock_client(statuses: list[int | str], seen: list[httpx.Request]) -> httpx.Client:
    """``statuses``: HTTP codes in order; "text" means HTTP 200 with a non-JSON error body."""

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        status = statuses.pop(0) if statuses else 200
        n = len(request.url.params["latitude"].split(","))
        headers = {"x-ratelimit-cost": str(float(n))}
        if status == "text":
            return httpx.Response(200, headers=headers, text="Unexpected error: timeoutReached")
        body = _response(n) if status == 200 else {"error": True}
        return httpx.Response(status, headers=headers, json=body)

    return httpx.Client(transport=httpx.MockTransport(handle))


def test_fetch_day_batches_paces_and_retries(monkeypatch):
    monkeypatch.setattr(openmeteo, "LOCATIONS_PER_REQUEST", 2)
    cells = [Cell(50.0 + i / 10, 8.0) for i in range(5)]
    seen: list[httpx.Request] = []
    sleeps: list[float] = []
    with mock_client([200, 429, "text", 200], seen) as client:
        records = fetch_day(client, cells, DAY, sleep=sleeps.append)
    assert [len(r["cell_ids"]) for r in records] == [2, 2, 1]
    assert [r["attempts"] for r in records] == [1, 3, 1]
    assert len(seen) == 5
    assert seen[0].url.params["start_date"] == seen[0].url.params["end_date"] == "2026-11-24"
    assert seen[0].url.params["timezone"] == "GMT"
    pause = 2 * 60 / openmeteo.COST_PER_MINUTE
    assert sleeps == [pause, 2.0, 4.0, pause]  # pacing, 429 and error-text backoff, pacing


class FakeS3:
    def __init__(self, keys: tuple[str, ...] = ()) -> None:
        self.keys = list(keys)
        self.puts: list[dict] = []

    def list_objects_v2(self, Bucket: str, Prefix: str, MaxKeys: int) -> dict:
        return {"KeyCount": sum(1 for k in self.keys if k.startswith(Prefix))}

    def put_object(self, **kwargs) -> dict:
        self.puts.append(kwargs)
        return {}


def test_load_day_writes_one_object_per_day():
    s3 = FakeS3()
    cells = [Cell(50.0, 8.0), Cell(52.5, 13.35)]
    with mock_client([], []) as client:
        assert load_day(client, cells, DAY, s3=s3, bucket="b", sleep=lambda _: None) == "loaded"
    (put,) = s3.puts
    assert put["Key"].startswith("openmeteo/hourly/dt=2026-11-24/openmeteo_hourly_20261124_")
    lines = gzip.decompress(put["Body"]).splitlines()
    record = json.loads(lines[0])
    assert record["cell_ids"] == ["50.00_8.00", "52.50_13.35"]
    assert len(record["response"]) == 2
    assert put["Metadata"]["cost"] == "2.0"


def test_load_day_skips_existing_day_and_rejects_today():
    s3 = FakeS3(keys=("openmeteo/hourly/dt=2026-11-24/x.jsonl.gz",))
    seen: list[httpx.Request] = []
    with mock_client([], seen) as client:
        assert load_day(client, [Cell(50.0, 8.0)], DAY, s3=s3, bucket="b") == "exists"
        with pytest.raises(ValueError, match="not complete"):
            load_day(client, [Cell(50.0, 8.0)], datetime.now(UTC).date(), s3=s3, bucket="b")
    assert seen == []
    assert s3.puts == []


def test_load_day_writes_nothing_when_a_batch_keeps_failing(monkeypatch):
    monkeypatch.setattr(openmeteo, "LOCATIONS_PER_REQUEST", 1)
    s3 = FakeS3()
    with mock_client([200] + [503] * 5, []) as client, pytest.raises(RuntimeError, match="failed"):
        load_day(
            client, [Cell(50.0, 8.0), Cell(51.0, 9.0)], DAY, s3=s3, bucket="b", sleep=lambda _: None
        )
    assert s3.puts == []
