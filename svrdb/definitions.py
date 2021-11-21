import strawberry
from datetime import datetime

from svrdb.loader.tornado import Tornado as TornadoModel
from svrdb.loader.tornado import TornadoSegment as TornadoSegModel
from svrdb.models import Hail as HailModel, Wind as WindModel


@strawberry.interface
class Event:
    state: str
    fatalities: int
    injuries: int
    datetime: datetime
    loss: float
    closs: float

    @classmethod
    def from_model(cls, model_obj):
        return cls(
            state=model_obj.state,
            fatalities=model_obj.fatalities,
            injuries=model_obj.injuries,
            datetime=model_obj.datetime,
            loss=model_obj.loss,
            closs=model_obj.closs
        )


@strawberry.type 
class Tornado(Event):
    length: float
    width: float
    start_lat: float 
    start_lon: float 
    end_lat: float
    end_lon: float
    magnitude: int
    counties: list[int]

    @classmethod
    def from_model(cls, model: TornadoModel):
        kw = {k: _extract_item(model, k) for k in _get_attrs(TornadoSegModel)}
        return cls(**kw)


@strawberry.type 
class Hail(Event):
    lat: float 
    lon: float
    magnitude: float
    county: str

    @classmethod
    def from_model(cls, model: HailModel):
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
            county=model.county_id
        )
        # ret.lat = model.lat
        # ret.lon = model.lon
        # ret.magnitude = model.magnitude
        # ret.county = model.county.county if hasattr(model.county, 'county') else None


@strawberry.type 
class Wind(Event):
    lat: float 
    lon: float
    magnitude: int
    county: str

    @classmethod
    def from_model(cls, model: WindModel):
        ret = super(Wind, cls).from_model(model)
        ret.lat = model.lat
        ret.lon = model.lon
        ret.magnitude = model.magnitude
        ret.county = model.county.county


def _extract_item(model, k):
    item = model[k]
    if k.lower() == 'state' and isinstance(item, list):
        return ', '.join(item)
    return item


def _get_attrs(model_cls):
    return [attr for attr in model_cls.aliases] + ['datetime', 'loss', 'closs']