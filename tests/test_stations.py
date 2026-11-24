from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path

import pytest

from bahn_pulse.stations.stada import parse_stations
from bahn_pulse.stations.tiers import (
    TierOverride,
    assign_tiers,
    read_overrides,
    tier_for_segment,
    write_tiers_csv,
)

FIXTURE = Path(__file__).parent / "fixtures" / "stada" / "stations_sample.json"


@pytest.fixture
def stations():
    return parse_stations(FIXTURE.read_bytes())


def test_parse_stations_reads_contract_fields(stations) -> None:
    assert len(stations) == 8
    ffm = next(s for s in stations if s.name == "Frankfurt (Main) Hbf")
    assert ffm.ifopt == "de:06412:10"
    assert ffm.price_category == 1
    assert ffm.product_line is not None
    assert ffm.product_line.segment == "Metropolbahnhof"
    assert ffm.main_eva is not None
    assert ffm.main_eva.number == 8000105


def test_parse_stations_fails_when_result_is_truncated() -> None:
    body = b'{"offset": 0, "limit": 1, "total": 2, "result": [{"number": 1, "name": "A"}]}'
    with pytest.raises(ValueError, match="paging needed"):
        parse_stations(body)


@pytest.mark.parametrize(
    ("segment", "tier"),
    [
        ("Metropolbahnhof", 1),
        ("Großstadtknoten", 1),
        ("Regionaler Knoten", 2),
        ("Zentraler S-Bahnhof", 3),
        ("Ländlicher Zubringer", 3),  # StaDa uses both spellings
        ("ländlicher Zubringer", 3),
        ("nicht anwendbar", None),
        (None, None),
    ],
)
def test_tier_for_segment(segment, tier) -> None:
    assert tier_for_segment(segment) == tier


def test_assign_tiers_uses_main_eva_and_sorts_by_tier(stations) -> None:
    rows = assign_tiers(stations)
    assert [r.tier for r in rows] == sorted(
        [r.tier for r in rows], key=lambda t: (t is None, t or 0)
    )
    ffm = next(r for r in rows if r.name == "Frankfurt (Main) Hbf")
    assert ffm.eva_number == 8000105
    assert ffm.tier == 1
    assert ffm.tier_source == "rule"
    assert ffm.lon == pytest.approx(8.66282825)
    not_applicable = next(r for r in rows if r.product_line_segment == "nicht anwendbar")
    assert not_applicable.tier is None
    assert not_applicable.tier_source == "none"


def test_override_wins_over_rule(stations) -> None:
    overrides = {
        8000105: TierOverride(8000105, None, "test: remove"),
        8000410: TierOverride(8000410, 1, "test: promote"),
    }
    rows = {r.eva_number: r for r in assign_tiers(stations, overrides)}
    assert rows[8000105].tier is None
    assert rows[8000105].tier_source == "override"
    assert rows[8000410].tier == 1


def test_overrides_and_tiers_csv_round_trip(tmp_path, stations) -> None:
    overrides_csv = tmp_path / "overrides.csv"
    overrides_csv.write_text("eva_number,tier,reason\n8000410,2,busy\n8000105,,skip\n")
    overrides = read_overrides(overrides_csv)
    assert overrides[8000410].tier == 2
    assert overrides[8000105].tier is None

    out = tmp_path / "tiers.csv"
    write_tiers_csv(assign_tiers(stations, overrides), out)
    lines = out.read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("eva_number,stada_number,name")
    assert len(lines) == 1 + 8


def test_bronze_key_is_utc_hour_partitioned() -> None:
    from bahn_pulse.ingestion.bronze import bronze_key

    cet = timezone(timedelta(hours=1))
    key = bronze_key("stada", "stations", datetime(2026, 11, 24, 17, 30, 5, tzinfo=cet))
    assert key == ("stada/stations/dt=2026-11-24/hour=16/stada_stations_20261124T163005Z.json.gz")
    with pytest.raises(ValueError, match="timezone-aware"):
        bronze_key("stada", "stations", datetime(2026, 11, 24, 17, 30))  # noqa: DTZ001


def test_write_bronze_gzips_and_never_overwrites() -> None:
    import gzip

    from bahn_pulse.ingestion.bronze import write_bronze

    calls = []

    class FakeS3:
        def put_object(self, **kwargs):
            calls.append(kwargs)

    write_bronze(FakeS3(), "bucket", "k", b'{"a":1}', {"fetched-at": "x"})
    assert gzip.decompress(calls[0]["Body"]) == b'{"a":1}'
    assert calls[0]["IfNoneMatch"] == "*"
    assert datetime.now(UTC)  # keep UTC import used for clarity of intent
