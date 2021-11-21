from sqlalchemy import column

from .models import get_session, Base


def query_events(model: Base, query: dict, order_by: str = 'datetime'):
    with get_session() as session:
        return session.query(model).where(*_generate_select(query)).order_by(order_by)


def _generate_select(query: dict):
    ret = []
    for k, v in query.items():
        col = column(k)
        if isinstance(v, (list, set, tuple)):
            ret.append(col.in_(v))
        else:
            ret.append(col == v)
    return ret
