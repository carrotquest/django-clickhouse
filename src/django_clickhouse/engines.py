"""
This file contains wrappers for infi.clckhouse_orm engines to use in django-clickhouse
"""
import datetime
import logging
from typing import List, Type, Union, Iterable, Optional, Tuple, NamedTuple

from django.db.models import Model as DjangoModel
from infi.clickhouse_orm import engines as infi_engines
from statsd.defaults.django import statsd

from .clickhouse_models import ClickHouseModel
from .configuration import config
from .database import connections
from .utils import format_datetime


logger = logging.getLogger('django-clickhouse')


class InsertOnlyEngineMixin:
    def get_insert_batch(self, model_cls: Type[ClickHouseModel], objects: List[DjangoModel]) -> Iterable[tuple]:
        """
        Gets a list of model_cls instances to insert into database
        :param model_cls: ClickHouseModel subclass to import
        :param objects: A list of django Model instances to sync
        :return: A generator of model_cls named tuples
        """
        serializer = model_cls.get_django_model_serializer(writable=True)
        return (serializer.serialize(obj) for obj in objects)


class MergeTree(InsertOnlyEngineMixin, infi_engines.MergeTree):
    pass


class ReplacingMergeTree(InsertOnlyEngineMixin, infi_engines.ReplacingMergeTree):
    pass


class SummingMergeTree(InsertOnlyEngineMixin, infi_engines.SummingMergeTree):
    pass


class CollapsingMergeTree(InsertOnlyEngineMixin, infi_engines.CollapsingMergeTree):
    pk_column = 'id'

    def __init__(self, *args, **kwargs):
        self.version_col = kwargs.pop('version_col', None)
        super(CollapsingMergeTree, self).__init__(*args, **kwargs)

    def _get_final_versions_by_version(self, db_alias: str, model_cls: Type[ClickHouseModel], object_pks: Iterable[str],
                                       columns: str, date_range_filter: str = '') -> List[NamedTuple]:
        """
        Performs request to ClickHouse in order to fetch latest version for each object pk
        :param db_alias: ClickHouse database alias used
        :param model_cls: Model class for which data is fetched
        :param object_pks: Objects primary keys to filter by
        :param columns: Columns to fetch
        :param date_range_filter: Optional date_range_filter which speeds up query if date_col is set
        :return: List of named tuples with requested columns
        """
        if date_range_filter:
            date_range_filter = 'PREWHERE {}'.format(date_range_filter)

        query = """
            SELECT {columns}
            FROM $table
            {date_range_filter}
            WHERE `{pk_column}` IN ({object_pks})
            ORDER BY `{pk_column}`, `{version_col}` DESC
            LIMIT 1 BY `{pk_column}`
        """.format(columns=','.join(columns), version_col=self.version_col, pk_column=self.pk_column,
                   date_range_filter=date_range_filter, object_pks=','.join(object_pks), sign_col=self.sign_col)

        return connections[db_alias].select_tuples(query, model_cls)

    def _get_final_versions_by_final(self, db_alias: str, model_cls: Type[ClickHouseModel], object_pks: Iterable[str],
                                     columns: str, date_range_filter: str = '') -> List[NamedTuple]:
        """
        Performs request to ClickHouse in order to fetch latest version for each object pk
        :param db_alias: ClickHouse database alias used
        :param model_cls: Model class for which data is fetched
        :param object_pks: Objects primary keys to filter by
        :param columns: Columns to fetch
        :param date_range_filter: Optional date_range_filter which speeds up query if date_col is set
        :return: List of named tuples with requested columns
        """
        if date_range_filter:
            date_range_filter += ' AND'

        query = """
            SELECT {columns} FROM $table FINAL
            WHERE {date_range_filter} `{pk_column}` IN ({object_pks})
        """
        query = query.format(columns=','.join(columns), pk_column=self.pk_column, date_range_filter=date_range_filter,
                             object_pks=','.join(object_pks))
        return connections[db_alias].select_tuples(query, model_cls)

    def _get_date_rate_filter(self, objects, model_cls: Type[ClickHouseModel], db_alias: str,
                              date_col: Optional[str]) -> str:
        """
        Generates datetime filter to speed up final queries, if date_col is present
        :param objects: Objects, which are inserted
        :param model_cls: Model class for which data is fetched
        :param db_alias: ClickHouse database alias used
        :param date_col: Optional column name, where partition date is hold. Defaults to self.date_col
        :return: String to add to WHERE or PREWHERE query section
        """
        def _dt_to_str(dt: Union[datetime.date, datetime.datetime]) -> str:
            if isinstance(dt, datetime.datetime):
                return format_datetime(dt, 0, db_alias=db_alias)
            elif isinstance(dt, datetime.date):
                return dt.isoformat()
            else:
                raise Exception('Invalid date or datetime object: `%s`' % dt)

        date_col = date_col or self.date_col

        if not date_col:
            logger.warning('django-clickhouse: date_col is not provided for model %s.'
                           ' This can cause significant performance problems while fetching data.'
                           ' It is worth inheriting CollapsingMergeTree engine with custom get_final_versions() method,'
                           ' based on your partition_key' % model_cls)
            return ''

        min_date, max_date = None, None
        for obj in objects:
            obj_date = getattr(obj, date_col)

            if min_date is None or min_date > obj_date:
                min_date = obj_date

            if max_date is None or max_date < obj_date:
                max_date = obj_date

        min_date = _dt_to_str(min_date)
        max_date = _dt_to_str(max_date)

        return "`{date_col}` >= '{min_date}' AND `{date_col}` <= '{max_date}'".\
            format(min_date=min_date, max_date=max_date, date_col=date_col)

    def get_final_versions(self, model_cls: Type[ClickHouseModel], objects: Iterable[DjangoModel],
                           date_col: Optional[str] = None) -> Iterable[tuple]:
        """
        Get objects, that are currently stored in ClickHouse.
        Depending on the partition key this can be different for different models.
        In common case, this method is optimized for date field that doesn't change.
        It also supposes primary key to by self.pk_column
        :param model_cls: ClickHouseModel subclass to import
        :param objects: Objects for which final versions are searched
        :param date_col: Optional column name, where partition date is hold. Defaults to self.date_col
        :return: A generator of named tuples, representing previous state
        """
        if not objects:
            raise StopIteration()

        object_pks = [str(getattr(obj, self.pk_column)) for obj in objects]

        db_alias = model_cls.get_database_alias()

        date_range_filter = self._get_date_rate_filter(objects, model_cls, db_alias, date_col)

        # Get fields. Sign is replaced to negative for further processing
        columns = list(model_cls.fields(writable=True).keys())
        columns.remove(self.sign_col)
        columns.append('-1 AS sign')

        params = (db_alias, model_cls, object_pks, columns, date_range_filter)

        if self.version_col:
            return self._get_final_versions_by_version(*params)
        else:
            return self._get_final_versions_by_final(*params)

    def get_insert_batch(self, model_cls: Type[ClickHouseModel], objects: List[DjangoModel]) -> Iterable[tuple]:
        """
        Gets a list of model_cls instances to insert into database
        :param model_cls: ClickHouseModel subclass to import
        :param objects: A list of django Model instances to sync
        :return: A list of model_cls objects
        """
        defaults = {self.sign_col: 1}
        if self.version_col:
            defaults[self.version_col] = 1
        serializer = model_cls.get_django_model_serializer(writable=True, defaults=defaults)
        new_objs = [serializer.serialize(obj) for obj in objects]

        statsd_key = "%s.sync.%s.steps.get_final_versions" % (config.STATSD_PREFIX, model_cls.__name__)
        with statsd.timer(statsd_key):
            # NOTE I don't use generator pattern here, as it move all time into insert.
            # That makes hard to understand where real problem is in monitoring
            old_objs = tuple(self.get_final_versions(model_cls, new_objs))

        # -1 sign has been set get_final_versions()
        old_objs_versions = {}
        for obj in old_objs:
            pk = getattr(obj, self.pk_column)
            if self.version_col:
                old_objs_versions[pk] = getattr(obj, self.version_col)
            yield obj

        # 1 sign is set by default in serializer
        for obj in new_objs:
            pk = getattr(obj, self.pk_column)
            if self.version_col:
                obj = obj._replace(**{self.version_col: old_objs_versions.get(pk, 0) + 1})

            yield obj
