from typing import Any

from sqlalchemy import column, extract
from sqlalchemy.orm import Session, selectinload

from .inputs import SpatialFilter, TemporalFilter, TornadoFilter, HailFilter, WindFilter
from .models import Base, Tornado, TornadoSegment, Hail, Wind, TornadoSegmentCounty


class _ModelFetch:
    def __init__(self, model: Base, session: Session):
        self._model = model
        self._session = session

    def _where_args(self, _filter: Any):
        return []

    def fetch(self, filter: Any, order_by: str):
        if filter is None:
            return self._session.query(self._model).order_by(order_by)
        return self._session.query(self._model).where(*self._where_args(filter)).order_by(order_by)


class _SpatialFetch(_ModelFetch):
    def _where_args(self, filter: SpatialFilter):
        ret = []
        if filter.states is not None:
            ret.append(column('state').in_(filter.states))
        return ret


class _TemporalFetch(_ModelFetch):
    def _where_args(self, filter: TemporalFilter):
        ret = []
        if filter.datetimeRange is not None:
            ret += parse_range('datetime', filter.datetimeRange)
        if filter.years is not None:
            ret.append(extract('year', column('datetime')).in_(filter.years))
        if filter.months is not None:
            ret.append(extract('month', column('datetime')).in_(filter.months))
        if filter.days is not None:
            ret.append(extract('day', column('datetime')).in_(filter.days))
        if filter.hours is not None:
            ret.append(extract('hour', column('datetime')).in_(filter.hours))
        return ret


class TornadoFetch(_SpatialFetch, _TemporalFetch):
    def __init__(self, session: Session):
        super().__init__(model=Tornado, session=session)

    def _where_args(self, filter: TornadoFilter):
        temporal_wheres = _TemporalFetch._where_args(self, filter)

        # TODO: we have to modify this when spatial filters is more than just states
        spatial_wheres = []
        if filter.states is None:
            spatial_wheres += _SpatialFetch._where_args(self, filter)
        else:
            # override state `where` behavior we want to query for segment states not
            # parent tornado states (which are just the touchdown states)
            # unfortunately, due to column conflicts between Tornado (parent)
            # and TornadoSegment (child), explict join gets screwy...
            # workaround is to subquery for tornado_id list
            # and select tornado by those ids
            subquery = self._session.query(TornadoSegment.tornado_id).where(
                TornadoSegment.state.in_(filter.states)
            )
            spatial_wheres.append(column('id').in_(subquery))

        others = []
        if filter.efs is not None:
            others.append(column('magnitude').in_(filter.efs))
        if filter.pathLengthRange is not None:
            others += parse_range('length', filter.pathLengthRange)

        return temporal_wheres + spatial_wheres + others

    def fetch(self, filter: TornadoFilter, order_by: str):
        if filter is None:
            raise ValueError('TornadoFilter must not not be null!')

        return self._session.query(self._model)\
            .options(selectinload(Tornado.segments)
                     .joinedload(TornadoSegment.counties)
                     .joinedload(TornadoSegmentCounty.county))\
            .where(*self._where_args(filter))\
            .order_by(order_by).all()


class HailFetch(_SpatialFetch, _TemporalFetch):
    def __init__(self, session: Session):
        super().__init__(model=Hail, session=session)

    def _where_args(self, filter: HailFilter):
        others = []
        if filter.sizeRange is not None:
            others += parse_range('magnitude', filter.sizeRange)

        return others + _TemporalFetch._where_args(self, filter) + _SpatialFetch._where_args(self, filter)


class WindFetch(_SpatialFetch, _TemporalFetch):
    def __init__(self, session: Session):
        super().__init__(model=Wind, session=session)

    def _where_args(self, filter: WindFilter):
        others = []
        if filter.windSpeedRange is not None:
            others += parse_range('magnitude', filter.windSpeedRange)

        return others + _TemporalFetch._where_args(self, filter) + _SpatialFetch._where_args(self, filter)


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

