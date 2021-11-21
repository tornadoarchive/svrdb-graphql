import strawberry

from typing import List

from svrdb.definitions import Tornado, Hail
from svrdb.loader import TornadoList
from svrdb.models import Hail as HailModel
from svrdb.query import query_events


@strawberry.type
class Query:
    _tordb = None

    @strawberry.field
    def tornado(self, 
                state: str = None,
                magnitude: int = -1) -> List[Tornado]:

        if Query._tordb is None:
            Query._tordb = TornadoList.load_db()

        search_kw = {}

        if state is not None:
            search_kw['state'] = state
        
        if magnitude >= 0:
            search_kw['magnitude'] = magnitude

        query_result = Query._tordb.search(**search_kw)
        return[Tornado.from_model(tor) for tor in query_result]

    @strawberry.field
    def hail(self,
             state: str = None,
             magnitude: float = None) -> List[Hail]:

        results = query_events(
            model=HailModel,
            query=dict(state=state, magnitude=magnitude)
        )

        return [Hail.from_model(mdl) for mdl in results]


schema = strawberry.Schema(query=Query)
