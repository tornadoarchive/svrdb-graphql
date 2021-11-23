from typing import List

import strawberry


@strawberry.interface
class _GeographicalFilter:
    states: List[str] = None

    def to_dict(self):
        if self.states is not None:
            return {'state': self.states}
        return {}


@strawberry.interface
class _TemporalFilter:
    # years: List[int] = None
    # months: List[int] = None
    # days: List[int] = None
    # hours: List[int] = None
    datetimeRange: List[str] = None

    def to_dict(self):
        if self.datetimeRange is None:
            return {}

        if len(self.datetimeRange) != 2:
            raise ValueError("DatetimeRange must be an array of two")
        return {
            'datetime': {
                'gte': self.datetimeRange[0],
                'lt': self.datetimeRange[1]
            }
        }


@strawberry.input
class HailFilter(_GeographicalFilter, _TemporalFilter):
    sizeRange: List[float] = None

    def to_dict(self):
        ret = _GeographicalFilter.to_dict(self) | _TemporalFilter.to_dict(self)
        if self.sizeRange is not None:
            ret['magnitude'] = {
                'gte': self.sizeRange[0],
                'lt': self.sizeRange[1]
            }
        elif self.sizeRange is not None and len(self.sizeRange) != 2:
            raise ValueError("SizeRnge must be an array of two")
        return ret