import strawberry

from datetime import datetime
from typing import List

from .fetch import fetch_events
from .inputs import HailFilter, TornadoFilter
from .models import Hail as HailModel, Tornado as TornadoModel


@strawberry.type
class Tornado:
    state: str
    fatalities: int
    injuries: int
    datetime: datetime
    loss: float
    closs: float
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float
    magnitude: float
    length: float
    width: float

    @classmethod
    def fetch(cls, filter: TornadoFilter = None):
        if filter is not None:
            queried = fetch_events(TornadoModel, filter.to_query())
            return [cls._from_model(event) for event in queried]
        return []

    @classmethod
    def _from_model(cls, model: TornadoModel):
        return cls(
            state=model.state,
            fatalities=model.fatalities,
            injuries=model.injuries,
            datetime=model.datetime,
            loss=model.loss,
            closs=model.closs,
            start_lat=model.start_lat,
            start_lon=model.start_lon,
            end_lat=model.end_lat,
            end_lon=model.end_lon,
            magnitude=model.magnitude,
            length=model.length,
            width=model.width
        )


@strawberry.type
class Hail:
    state: str
    fatalities: int
    injuries: int
    datetime: datetime
    loss: float
    closs: float
    lat: float
    lon: float
    magnitude: float
    county: int

    @classmethod
    def fetch(cls, filter: HailFilter = None):
        if filter is not None:
            queried = fetch_events(HailModel, filter.to_query())
            return [cls._from_model(event) for event in queried]
        return []

    @classmethod
    def _from_model(cls, model: HailModel):
        return cls(
            state=model.state,
            fatalities=model.fatalities,
            injuries=model.injuries,
            datetime=model.datetime,
            loss=model.loss,
            closs=model.closs,
            lat=model.lat,
            lon=model.lon,
            magnitude=model.magnitude,
            county=model.county_id,
        )


@strawberry.type
class Query:
    @strawberry.field
    def tornado(self, filter: TornadoFilter = None) -> List[Tornado]:
        return Tornado.fetch(filter)

    @strawberry.field
    def hail(self, filter: HailFilter = None) -> List[Hail]:
        return Hail.fetch(filter)
