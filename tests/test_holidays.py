import gzip
import json
from datetime import date

import httpx
import pytest

from bahn_pulse.ingestion.holidays import load_all, validate, window

HOLIDAY = {
    "id": "b5045dcf",
    "startDate": "2026-11-02",
    "endDate": "2026-11-06",
    "type": "School",
    "name": [{"language": "DE", "text": "Herbstferien"}],
    "subdivisions": [{"code": "DE-BY", "shortName": "BY"}],
}


def test_window_covers_current_and_next_year():
    assert window(date(2026, 11, 25)) == (date(2026, 1, 1), date(2027, 12, 31))


def test_validate_counts_and_rejects_bad_payloads():
    assert validate(json.dumps([HOLIDAY, HOLIDAY]).encode()) == 2
    with pytest.raises(ValueError, match="non-empty list"):
        validate(b"[]")
    with pytest.raises(ValueError, match="endDate"):
        validate(json.dumps([{k: v for k, v in HOLIDAY.items() if k != "endDate"}]).encode())


class FakeS3:
    def __init__(self) -> None:
        self.puts: list[dict] = []

    def put_object(self, **kwargs) -> dict:
        self.puts.append(kwargs)
        return {}


def test_load_all_writes_both_lists_raw():
    seen: list[httpx.Request] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json=[HOLIDAY])

    s3 = FakeS3()
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        assert load_all(client, s3=s3, bucket="b") == {"school_holidays": 1, "public_holidays": 1}
    assert [r.url.path for r in seen] == ["/SchoolHolidays", "/PublicHolidays"]
    assert seen[0].url.params["countryIsoCode"] == "DE"
    keys = [p["Key"] for p in s3.puts]
    assert keys[0].startswith("openholidays/school_holidays/dt=")
    assert keys[1].startswith("openholidays/public_holidays/dt=")
    assert json.loads(gzip.decompress(s3.puts[0]["Body"])) == [HOLIDAY]


def test_load_all_writes_nothing_if_one_list_is_invalid():
    def handle(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[] if request.url.path == "/PublicHolidays" else [HOLIDAY])

    s3 = FakeS3()
    with (
        httpx.Client(transport=httpx.MockTransport(handle)) as client,
        pytest.raises(ValueError, match="non-empty"),
    ):
        load_all(client, s3=s3, bucket="b")
    assert s3.puts == []
