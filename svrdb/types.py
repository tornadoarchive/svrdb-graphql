import strawberry

from datetime import datetime
from typing import List

from .fetch import fetch_events
from .inputs import HailFilter, TornadoFilter
from .models import (
    Hail as HailModel,
    Tornado as TornadoModel,
)


@strawberry.interface
class _Event:
    id: int
    datetime: datetime
    state: str
    fatalities: int
    injuries: int
    loss: float
    closs: float

    @classmethod
    def _to_dict(cls, model):
        return dict(
            id=model.id,
            datetime=model.datetime,
            state=model.state,
            fatalities=model.fatalities,
            injuries=model.injuries,
            loss=model.loss,
            closs=model.closs
        )

    @classmethod
    def marshal(cls, model):
        return cls(**cls._to_dict(model))


@strawberry.interface
class _PointEvent(_Event):
    lat: float
    lon: float

    @classmethod
    def _to_dict(cls, model):
        return super(_PointEvent, cls)._to_dict(model) | dict(
            lat=model.lat,
            lon=model.lon
        )


@strawberry.interface
class _PathEvent(_Event):
    length: float
    width: float
    start_lat: float
    start_lon: float
    end_lat: float
    end_lon: float

    @classmethod
    def _to_dict(cls, model):
        return super(_PathEvent, cls)._to_dict(model) | dict(
            length=model.length,
            width=model.width,
            start_lat=model.start_lat,
            start_lon=model.start_lon,
            end_lat=model.end_lat,
            end_lon=model.end_lon
        )


@strawberry.type
class County:
    order: int
    state: str
    name: str
    state_fips: int
    county_fips: int

    @classmethod
    def marshal(cls, model, county_order):
        return cls(
            state=model.state,
            name=model.county,
            state_fips=model.state_fips,
            county_fips=model.county_fips,
            order=county_order
        )


@strawberry.type
class TornadoSegment(_PathEvent):
    magnitude: float
    counties: List[County]

    @staticmethod
    def _extract_counties(model):
        county_relationships = model.counties
        return [County.marshal(c.county, c.county_order) for c in county_relationships]

    @classmethod
    def _to_dict(cls, model):
        print(model.counties)
        return super(TornadoSegment, cls)._to_dict(model) | dict(
            magnitude=model.magnitude,
            counties=TornadoSegment._extract_counties(model)
        )


@strawberry.type
class Tornado(_PathEvent):
    magnitude: float
    segments: List[TornadoSegment]

    @classmethod
    def _to_dict(cls, model):
        return super(Tornado, cls)._to_dict(model) | dict(
            magnitude=model.magnitude,
            segments=[TornadoSegment.marshal(ts) for ts in model.segments]
        )

    @classmethod
    def fetch(cls, filter: TornadoFilter = None):
        if filter is not None:
            queried = fetch_events(TornadoModel, filter.to_query())
            return [cls.marshal(event) for event in queried]
        return []


@strawberry.type
class Hail(_PointEvent):
    magnitude: float
    county: County

    @classmethod
    def _to_dict(cls, model):
        return super(Hail, cls)._to_dict(model) | dict(
            magnitude=model.magnitude,
            county=County.marshal(model.county, 1)
        )

    @classmethod
    def fetch(cls, filter: HailFilter = None):
        if filter is not None:
            queried = fetch_events(HailModel, filter.to_query())
            return [cls.marshal(event) for event in queried]
        return []


@strawberry.type
class Query:
    @strawberry.field
    def tornado(self, filter: TornadoFilter = None) -> List[Tornado]:
        return Tornado.fetch(filter)

    @strawberry.field
    def hail(self, filter: HailFilter = None) -> List[Hail]:
        return Hail.fetch(filter)
