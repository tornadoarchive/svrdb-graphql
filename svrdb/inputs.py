from datetime import datetime
from typing import List, Optional

import strawberry


@strawberry.interface
class SpatialFilter:
    states: List[str] = None


@strawberry.interface
class TemporalFilter:
    years: List[int] = None
    months: List[int] = None
    days: List[int] = None
    hours: List[int] = None
    datetimeRange: List[Optional[datetime]] = None


@strawberry.input
class HailFilter(SpatialFilter, TemporalFilter):
    sizeRange: List[Optional[float]] = None


@strawberry.input
class WindFilter(SpatialFilter, TemporalFilter):
    windSpeedRange: List[Optional[int]] = None


@strawberry.input
class TornadoFilter(SpatialFilter, TemporalFilter):
    efs: List[int] = None
    pathLengthRange: List[Optional[float]] = None


@strawberry.input
class Pagination:
    offset: Optional[int]
    limit: Optional[int]
