import strawberry

from svrdb.types import Query

schema = strawberry.Schema(query=Query)
