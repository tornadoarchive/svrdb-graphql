import strawberry
from datetime import datetime

from svrdb.loader.tornado import Tornado as TornadoModel
from svrdb.loader.tornado import TornadoSegment as TornadoSegModel
from svrdb.loader.hail import Hail as HailModel
from svrdb.loader.wind import Wind as WindModel


@strawberry.interface
class Event:
    state: str
    counties: list[str]
    magnitude: float
    fatalities: int
    injuries: int
    datetime: datetime
    loss: float
    closs: float 


@strawberry.type 
class Tornado(Event):
    length: float
    width: float
    start_lat: float 
    start_lon: float 
    end_lat: float
    end_lon: float

    @classmethod
    def from_model(cls, model: TornadoModel):
        kw = {k: _extract_item(model, k) for k in _get_attrs(TornadoSegModel)}
        return cls(**kw)


@strawberry.type 
class Hail(Event):
    lat: float 
    lon: float 

    @classmethod
    def from_model(cls, model: HailModel):
        kw = {k: _extract_item(model, k) for k in _get_attrs(HailModel)}
        return cls(**kw)


@strawberry.type 
class Wind(Event):
    lat: float 
    lon: float 

    @classmethod
    def from_model(cls, model: WindModel):
        kw = {k: _extract_item(model, k) for k in _get_attrs(WindModel)}
        return cls(**kw)


def _extract_item(model, k):
    item = model[k]
    if k.lower() == 'state' and isinstance(item, list):
        return ', '.join(item)
    return item


def _get_attrs(model_cls):
    return [attr for attr in model_cls.aliases] + ['datetime', 'loss', 'closs']