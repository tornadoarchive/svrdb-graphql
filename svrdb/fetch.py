from typing import List

from .models import get_session, Base


def fetch_events(model: Base,
                 where_args: list = None,
                 order_by: str = 'datetime') -> List[Base]:
    with get_session() as session:
        if where_args is None:
            return session.query(model).order_by(order_by)
        return session.query(model).where(*where_args).order_by(order_by)
