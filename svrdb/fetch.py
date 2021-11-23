from typing import List

from sqlalchemy import column

from .models import get_session, Base

from operator import eq, gt, ge, lt, le, ne

_OPS = {
    'eq': eq,
    'gt': gt,
    'gte': ge,
    'lt': lt,
    'lte': le,
    'ne': ne
}


def fetch_events(model: Base,
                 query: dict = None,
                 order_by: str = 'datetime') -> List[Base]:
    with get_session() as session:
        if query is None:
            return session.query(model).order_by(order_by)
        return session.query(model).where(*_generate_select(query)).order_by(order_by)


def _generate_select(query):
    ret = []
    for k, v in query.items():
        col = column(k)
        if isinstance(v, (list, set, tuple)):
            ret.append(col.in_(v))
        elif isinstance(v, dict):
            for op in _OPS:
                if op in v:
                    ret.append(_OPS[op](col, v[op]))
        else:
            ret.append(col == v)
    return ret
