from datetime import datetime
from typing import List, Optional

import strawberry

from sqlalchemy import column, extract
from sqlalchemy.sql.elements import ColumnClause


@strawberry.interface
class _GeographicalFilter:
    states: List[str] = None

    def to_query(self) -> List[ColumnClause]:
        ret = []
        if self.states is not None:
            ret.append(column('state').in_(self.states))
        return ret


@strawberry.interface
class _TemporalFilter:
    years: List[int] = None
    months: List[int] = None
    days: List[int] = None
    hours: List[int] = None
    datetimeRange: List[Optional[datetime]] = None

    def to_query(self) -> List[ColumnClause]:
        ret = []
        if self.datetimeRange is not None:
            ret += parse_range('datetime', self.datetimeRange)
        if self.years is not None:
            ret.append(extract('year', column('datetime')).in_(self.years))
        if self.months is not None:
            ret.append(extract('month', column('datetime')).in_(self.months))
        if self.days is not None:
            ret.append(extract('day', column('datetime')).in_(self.days))
        if self.hours is not None:
            ret.append(extract('hour', column('datetime')).in_(self.hours))
        return ret


@strawberry.input
class HailFilter(_GeographicalFilter, _TemporalFilter):
    sizeRange: List[Optional[float]] = None

    def to_query(self) -> List[ColumnClause]:
        ret = _GeographicalFilter.to_query(self) + _TemporalFilter.to_query(self)
        if self.sizeRange is not None:
            ret += parse_range('magnitude', self.sizeRange)
        return ret


@strawberry.input
class WindFilter(_GeographicalFilter, _TemporalFilter):
    windSpeedRange: List[Optional[int]] = None

    def to_query(self) -> List[ColumnClause]:
        ret = _GeographicalFilter.to_query(self) + _TemporalFilter.to_query(self)
        if self.windSpeedRange is not None:
            ret += parse_range('magnitude', self.windSpeedRange)
        return ret


@strawberry.input
class TornadoFilter(_GeographicalFilter, _TemporalFilter):
    efs: List[int] = None
    pathLengthRange: List[Optional[float]] = None

    def to_query(self) -> List[ColumnClause]:
        # TODO: we must query geographically by only segment in the future
        ret = _TemporalFilter.to_query(self) + _GeographicalFilter.to_query(self)

        if self.efs is not None:
            ret.append(column('magnitude').in_(self.efs))
        if self.pathLengthRange is not None:
            ret += parse_range('length', self.pathLengthRange)
        return ret


def parse_range(col, lst):
    if not lst:
        return []
    if len(lst) == 1:
        return [column(col) == lst[0]]
    else:
        ret = []
        rng_start, rng_end = tuple(lst[:2])
        if rng_start is not None:
            ret.append(column(col) >= rng_start)
        if rng_end is not None:
            ret.append(
                column(col) <= rng_end if rng_start is None else column(col) < rng_end
            )
        return ret
