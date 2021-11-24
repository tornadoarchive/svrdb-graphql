import strawberry

from datetime import datetime
from typing import List

from .fetch import fetch_events
from .inputs import HailFilter
from .models import Hail as HailModel


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
    def hail(self, filter: HailFilter = None) -> List[Hail]:
        return Hail.fetch(filter)
