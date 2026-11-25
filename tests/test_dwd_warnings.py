import io
import zipfile
from datetime import UTC, datetime

import httpx
import pytest

from bahn_pulse.ingestion.dwd_warnings import count_alerts, first_per_day, parse_listing, sync

DIFF = "Z_CAP_C_EDZW_{ts}_PVW_DIFFERENCE_PREMIUMDWD_COMMUNEUNION_DE.zip"
STAT = "Z_CAP_C_EDZW_{ts}_PVW_STATUS_PREMIUMDWD_COMMUNEUNION_DE.zip"


def listing(*names: str) -> str:
    rows = [f'<a href="{n}">{n}</a> 25-Nov-2026 16:03 22' for n in names]
    rows.append('<a href="Z_CAP_C_EDZW_LATEST_PVW_STATUS_PREMIUMDWD_COMMUNEUNION_DE.zip">x</a>')
    rows.append(f'<a href="{DIFF.format(ts="20261125160317").replace("_DE", "_EN")}">en</a>')
    return "\n".join(rows)


def package(n_alerts: int, *, bad: bool = False) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        for i in range(n_alerts):
            z.writestr(f"a{i}.xml", b"<?xml version='1.0'?><alert>...</alert>")
        if bad:
            z.writestr("readme.txt", b"hello")
    return buf.getvalue()


def test_parse_listing_keeps_timestamped_german_files_only():
    files = parse_listing(listing(DIFF.format(ts="20261125160317")), "warnings_diff")
    assert [f.name for f in files] == [DIFF.format(ts="20261125160317")]
    (f,) = files
    assert f.published_at == datetime(2026, 11, 25, 16, 3, 17, tzinfo=UTC)
    assert f.key == f"dwd/warnings_diff/dt=2026-11-25/hour=16/{f.name}"
    assert f.url.endswith(f"/COMMUNEUNION_DWD_DIFF/{f.name}")


def test_first_per_day():
    names = [STAT.format(ts=t) for t in ("20261124235000", "20261125000500", "20261125001500")]
    files = first_per_day(parse_listing(listing(*names), "warnings_status"))
    assert [f.name for f in files] == [names[0], names[1]]


def test_count_alerts_accepts_empty_and_rejects_non_cap():
    assert count_alerts(package(0)) == 0
    assert count_alerts(package(3)) == 3
    with pytest.raises(ValueError, match="not a CAP alert"):
        count_alerts(package(1, bad=True))


class FakeS3:
    def __init__(self, keys: tuple[str, ...] = ()) -> None:
        self.keys = list(keys)
        self.puts: list[dict] = []

    def get_paginator(self, name: str):
        s3 = self

        class Paginator:
            def paginate(self, Bucket: str, Prefix: str):
                yield {"Contents": [{"Key": k} for k in s3.keys if k.startswith(Prefix)]}

        return Paginator()

    def put_object(self, **kwargs) -> dict:
        self.puts.append(kwargs)
        self.keys.append(kwargs["Key"])
        return {}


def test_sync_copies_new_diffs_and_one_status_per_day():
    diffs = [DIFF.format(ts=t) for t in ("20261125150000", "20261125151000")]
    stats = [STAT.format(ts=t) for t in ("20261125000500", "20261125001500", "20261126000400")]
    seen: list[str] = []

    def handle(request: httpx.Request) -> httpx.Response:
        seen.append(request.url.path)
        if request.url.path.endswith("COMMUNEUNION_DWD_DIFF/"):
            return httpx.Response(200, text=listing(*diffs))
        if request.url.path.endswith("COMMUNEUNION_DWD_STAT/"):
            return httpx.Response(200, text=listing(*stats))
        return httpx.Response(200, content=package(2))

    s3 = FakeS3(
        keys=(
            f"dwd/warnings_diff/dt=2026-11-25/hour=15/{diffs[0]}",  # already copied
            f"dwd/warnings_status/dt=2026-11-25/hour=00/{stats[1]}",  # day 25 already anchored
        )
    )
    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        assert sync(client, s3=s3, bucket="b") == {"copied": 2, "skipped": 2}
    assert [p["Key"].rsplit("/", 1)[1] for p in s3.puts] == [diffs[1], stats[2]]
    put = s3.puts[0]
    assert put["ContentType"] == "application/zip"
    assert "ContentEncoding" not in put
    assert put["Metadata"]["alerts"] == "2"

    with httpx.Client(transport=httpx.MockTransport(handle)) as client:
        assert sync(client, s3=s3, bucket="b") == {"copied": 0, "skipped": 4}  # idempotent
