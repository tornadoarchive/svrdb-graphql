import os

import strawberry

from seed import seed
from svrdb.definitions import Tornado
from svrdb.loader import TornadoList

# initialize DB
from svrdb import models


@strawberry.type
class Query:
    _tordb = None

    @strawberry.field
    def tornado(self, 
                state: str = None,
                magnitude: int = -1) -> list[Tornado]:

        if Query._tordb is None:
            Query._tordb = TornadoList.load_db()

        search_kw = {}

        if state is not None:
            search_kw['state'] = state
        
        if magnitude >= 0:
            search_kw['magnitude'] = magnitude

        query_result = Query._tordb.search(**search_kw)
        return[Tornado.from_model(tor) for tor in query_result]


schema = strawberry.Schema(query=Query)
