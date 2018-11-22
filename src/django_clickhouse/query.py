from infi.clickhouse_orm.query import QuerySet as InfiQuerySet, AggregateQuerySet as InfiAggregateQuerySet


class QuerySet(InfiQuerySet):
    pass


class AggregateQuerySet(InfiAggregateQuerySet):
    pass
