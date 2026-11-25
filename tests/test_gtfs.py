import io
import zipfile
from datetime import UTC, datetime

import httpx
import pytest
from botocore.exceptions import ClientError

from bahn_pulse.ingestion.gtfs import FEEDS, load_feed, summarize_feed

LAST_MODIFIED = "Sat, 21 Nov 2026 09:42:35 GMT"


def make_feed(*, drop: str | None = None) -> bytes:
    files = {
        "agency.txt": "agency_id,agency_name\n1,DB Regio\n",
        "stops.txt": "stop_id,stop_name\na,Aachen Hbf\nb,Köln Hbf\n",
        "routes.txt": "route_id,route_type\nr1,2\n",
        "trips.txt": "route_id,service_id,trip_id\nr1,s1,t1\nr1,s2,t2\nr1,s1,t3\n",
        "stop_times.txt": "trip_id,stop_id,stop_sequence\nt1,a,1\nt1,b,2\n",
        "calendar.txt": (
            "service_id,start_date,end_date\ns1,20261121,20261221\ns2,20261123,20261130\n"
        ),
    }
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for name, text in files.items():
            if name != drop:
                z.writestr(name, text)
    return buf.getvalue()


class FakeS3:
    def __init__(self, existing: set[str] = frozenset()) -> None:
        self.existing = set(existing)
        self.puts: list[dict] = []

    def head_object(self, Bucket: str, Key: str) -> dict:
        if Key not in self.existing:
            raise ClientError({"Error": {"Code": "404"}}, "HeadObject")
        return {}

    def put_object(self, **kwargs) -> dict:
        self.puts.append(kwargs)
        return {}


def client_for(body: bytes, calls: list[str]) -> httpx.Client:
    def handle(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        headers = {"last-modified": LAST_MODIFIED, "etag": '"abc"'}
        return httpx.Response(
            200, headers=headers, content=b"" if request.method == "HEAD" else body
        )

    return httpx.Client(transport=httpx.MockTransport(handle))


def test_summarize_feed_counts_rows_and_service_range():
    summary = summarize_feed(make_feed())
    assert (summary.stops, summary.routes, summary.trips) == (2, 1, 3)
    assert (summary.service_start, summary.service_end) == ("20261121", "20261221")


def test_summarize_feed_rejects_missing_required_file():
    with pytest.raises(ValueError, match=r"stop_times\.txt"):
        summarize_feed(make_feed(drop="stop_times.txt"))


def test_load_feed_stores_new_version_as_raw_zip():
    calls: list[str] = []
    s3 = FakeS3()
    body = make_feed()
    with client_for(body, calls) as client:
        assert load_feed(client, "rv_free", s3=s3, bucket="b") == "loaded"
    assert calls == ["HEAD", "GET"]
    (put,) = s3.puts
    assert put["Key"] == "gtfs/rv_free/dt=2026-11-21/hour=09/gtfs_rv_free_20261121T094235Z.zip"
    assert put["Body"] == body  # stored as is, not gzipped again
    assert "ContentEncoding" not in put
    assert put["IfNoneMatch"] == "*"
    assert put["Metadata"]["source-url"] == FEEDS["rv_free"]
    assert (
        put["Metadata"]["last-modified"]
        == datetime(2026, 11, 21, 9, 42, 35, tzinfo=UTC).isoformat()
    )


def test_load_feed_skips_version_already_in_bronze():
    calls: list[str] = []
    key = "gtfs/fv_free/dt=2026-11-21/hour=09/gtfs_fv_free_20261121T094235Z.zip"
    s3 = FakeS3(existing={key})
    with client_for(make_feed(), calls) as client:
        assert load_feed(client, "fv_free", s3=s3, bucket="b") == "unchanged"
    assert calls == ["HEAD"]  # no download
    assert s3.puts == []


def test_load_feed_writes_nothing_for_invalid_feed():
    s3 = FakeS3()
    with (
        client_for(make_feed(drop="trips.txt"), []) as client,
        pytest.raises(ValueError, match=r"trips\.txt"),
    ):
        load_feed(client, "rv_free", s3=s3, bucket="b")
    assert s3.puts == []
