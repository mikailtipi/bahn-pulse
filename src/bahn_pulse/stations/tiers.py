"""Polling tiers per station.

Rule: DB's own station segmentation (StaDa ``productLine.segment``) decides the tier.
A manual override file can change or remove the tier of single stations.

Tier 1: major hubs, polled most often.
Tier 2: regional hubs.
Tier 3: all other passenger stations.
"""

from __future__ import annotations

import csv
from collections.abc import Iterable
from dataclasses import asdict, dataclass, fields
from pathlib import Path

from bahn_pulse.stations.stada import StadaStation

TIER_BY_SEGMENT: dict[str, int] = {
    "Metropolbahnhof": 1,
    "Großstadtknoten": 1,
    "Regionaler Knoten": 2,
    "Zentraler S-Bahnhof": 3,
    "Vorstädtischer S-Bahnhof": 3,
    "Regionaler Zubringer": 3,
    "Ländlicher Zubringer": 3,
}
# StaDa spells some segments with different case; casefold() also maps "ß" to "ss".
_TIER_BY_FOLDED_SEGMENT = {k.casefold(): v for k, v in TIER_BY_SEGMENT.items()}


@dataclass(frozen=True)
class StationTier:
    eva_number: int
    stada_number: int
    name: str
    ifopt: str | None
    federal_state_code: str | None
    product_line_segment: str | None
    price_category: int | None
    lon: float | None
    lat: float | None
    tier: int | None
    tier_source: str  # "rule", "override" or "none"


@dataclass(frozen=True)
class TierOverride:
    eva_number: int
    tier: int | None  # None removes the station from polling
    reason: str


def tier_for_segment(segment: str | None) -> int | None:
    if not segment:
        return None
    return _TIER_BY_FOLDED_SEGMENT.get(segment.strip().casefold())


def read_overrides(path: Path) -> dict[int, TierOverride]:
    if not path.is_file():
        return {}
    with path.open(encoding="utf-8", newline="") as f:
        rows = [r for r in csv.DictReader(f) if r.get("eva_number", "").strip()]
    overrides: dict[int, TierOverride] = {}
    for row in rows:
        tier = row["tier"].strip()
        override = TierOverride(
            eva_number=int(row["eva_number"]),
            tier=int(tier) if tier else None,
            reason=row.get("reason", "").strip(),
        )
        overrides[override.eva_number] = override
    return overrides


def assign_tiers(
    stations: Iterable[StadaStation],
    overrides: dict[int, TierOverride] | None = None,
) -> list[StationTier]:
    overrides = overrides or {}
    rows: list[StationTier] = []
    for station in stations:
        eva = station.main_eva
        if eva is None:
            continue
        segment = station.product_line.segment if station.product_line else None
        tier, source = tier_for_segment(segment), "rule"
        if eva.number in overrides:
            tier, source = overrides[eva.number].tier, "override"
        elif tier is None:
            source = "none"
        coords = eva.geographic_coordinates.coordinates if eva.geographic_coordinates else None
        rows.append(
            StationTier(
                eva_number=eva.number,
                stada_number=station.number,
                name=station.name,
                ifopt=station.ifopt,
                federal_state_code=station.federal_state_code,
                product_line_segment=segment,
                price_category=station.price_category,
                lon=coords[0] if coords else None,
                lat=coords[1] if coords else None,
                tier=tier,
                tier_source=source,
            )
        )
    rows.sort(key=lambda r: (r.tier is None, r.tier or 0, r.name))
    return rows


def write_tiers_csv(rows: Iterable[StationTier], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[fld.name for fld in fields(StationTier)])
        writer.writeheader()
        for row in rows:
            writer.writerow({k: "" if v is None else v for k, v in asdict(row).items()})
