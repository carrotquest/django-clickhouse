from typing import Optional, Iterable, List, Type

from copy import copy
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model as InfiModel
from infi.clickhouse_orm.query import QuerySet as InfiQuerySet, AggregateQuerySet as InfiAggregateQuerySet

from .database import connections


class QuerySet(InfiQuerySet):
    """
    Basic QuerySet to use
    """

    def __init__(self, model_cls: Type[InfiModel], database: Optional[Database] = None) -> None:
        super(QuerySet, self).__init__(model_cls, database)
        self._db_alias = None

    @property
    def _database(self) -> Database:
        # HACK for correct work of all infi.clickhouse-orm methods
        # There are no write QuerySet methods now, so I use for_write=False by default
        return self.get_database(for_write=False)

    @_database.setter
    def _database(self, database: Database) -> None:
        # HACK for correct work of all infi.clickhouse-orm methods
        self._db = database

    def get_database(self, for_write: bool = False) -> Database:
        """
        Gets database to execute query on. Looks for constructor or using() method.
        If nothing was set tries to get database from model class using router.
        :param for_write: Return QuerySet for read or for write.
        :return: Database instance
        """
        if not self._db:
            if self._db_alias:
                self._db = connections[self._db_alias]
            else:
                self._db = self._model_cls.get_database(for_write=for_write)

        return self._db

    def using(self, db_alias: str) -> 'QuerySet':
        """
        Sets database alias to use for this query
        :param db_alias: Database alias name from CLICKHOUSE_DATABASES config option
        :return: None
        """
        qs = copy(self)
        qs._db_alias = db_alias
        qs._db = None  # Previous database should be forgotten
        return qs

    def all(self) -> 'QuerySet':
        """
        Returns all items of queryset
        :return: QuerySet
        """
        return copy(self)

    def create(self, **kwargs):
        """
        Create single item in database
        :return: Created instance
        """
        instance = self._model_cls(**kwargs)
        self.get_database(for_write=True).insert([instance])
        return instance

    def bulk_create(self, model_instances: Iterable[InfiModel], batch_size: int = 1000) -> List[InfiModel]:
        self.get_database(for_write=True).insert(model_instances=model_instances, batch_size=batch_size)
        return list(model_instances)


class AggregateQuerySet(QuerySet, InfiAggregateQuerySet):
    pass
