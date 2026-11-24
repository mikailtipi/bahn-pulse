"""StaDa (Station Data API v2): fetch and data contract."""

from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field

from bahn_pulse.clients.db_api import DbApiClient

STATIONS_PATH = "/station-data/v2/stations"


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore", frozen=True)


class Point(_Model):
    type: str
    coordinates: tuple[float, float]  # (lon, lat)


class EvaNumber(_Model):
    number: int
    is_main: bool = Field(alias="isMain")
    geographic_coordinates: Point | None = Field(default=None, alias="geographicCoordinates")


class ProductLine(_Model):
    product_line: str | None = Field(default=None, alias="productLine")
    segment: str | None = None


class StadaStation(_Model):
    """The fields we rely on. Everything else stays in bronze untouched."""

    number: int
    name: str
    ifopt: str | None = None
    category: int | None = None
    price_category: int | None = Field(default=None, alias="priceCategory")
    federal_state_code: str | None = Field(default=None, alias="federalStateCode")
    product_line: ProductLine | None = Field(default=None, alias="productLine")
    eva_numbers: list[EvaNumber] = Field(default_factory=list, alias="evaNumbers")

    @property
    def main_eva(self) -> EvaNumber | None:
        if not self.eva_numbers:
            return None
        return next((e for e in self.eva_numbers if e.is_main), self.eva_numbers[0])


class StadaStationsResponse(_Model):
    offset: int
    limit: int
    total: int
    result: list[StadaStation]


def fetch_stations(client: DbApiClient) -> bytes:
    """Return the raw JSON body with all stations (the API returns up to 10,000 per page)."""
    return client.get(STATIONS_PATH).content


def parse_stations(body: bytes) -> list[StadaStation]:
    response = StadaStationsResponse.model_validate_json(body)
    if response.total > len(response.result):
        raise ValueError(
            f"StaDa returned {len(response.result)} of {response.total} stations; paging needed"
        )
    return response.result
