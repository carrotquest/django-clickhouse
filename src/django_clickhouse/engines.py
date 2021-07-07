"""
This file contains wrappers for infi.clckhouse_orm engines to use in django-clickhouse
"""
import datetime
from typing import List, Type, Union, Iterable, Optional

from django.db.models import Model as DjangoModel
from infi.clickhouse_orm import engines as infi_engines
from statsd.defaults.django import statsd

from .clickhouse_models import ClickHouseModel
from .configuration import config
from .database import connections
from .utils import format_datetime


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

    def _get_final_versions_by_version(self, db_alias, model_cls, min_date, max_date, object_pks, date_col, columns):
        query = """
            SELECT {columns} FROM $table WHERE (`{pk_column}`, `{version_col}`) IN (
                SELECT `{pk_column}`, MAX(`{version_col}`)
                FROM $table
                PREWHERE `{date_col}` >= '{min_date}' AND `{date_col}` <= '{max_date}'
                    AND `{pk_column}` IN ({object_pks})
                GROUP BY `{pk_column}`
           )
        """.format(columns=','.join(columns), version_col=self.version_col, date_col=date_col, pk_column=self.pk_column,
                   min_date=min_date, max_date=max_date, object_pks=','.join(object_pks))

        return connections[db_alias].select_tuples(query, model_cls)

    def _get_final_versions_by_final(self, db_alias, model_cls, min_date, max_date, object_pks, date_col, columns):
        query = """
            SELECT {columns} FROM $table FINAL
            WHERE `{date_col}` >= '{min_date}' AND `{date_col}` <= '{max_date}'
                AND `{pk_column}` IN ({object_pks})
        """
        query = query.format(columns=','.join(columns), date_col=date_col, pk_column=self.pk_column, min_date=min_date,
                             max_date=max_date, object_pks=','.join(object_pks))
        return connections[db_alias].select_tuples(query, model_cls)

    def get_final_versions(self, model_cls: Type[ClickHouseModel], objects: Iterable[DjangoModel],
                           date_col: Optional[str] = None) -> Iterable[tuple]:
        """
        Get objects, that are currently stored in ClickHouse.
        Depending on the partition key this can be different for different models.
        In common case, this method is optimized for date field that doesn't change.
        It also supposes primary key to by self.pk_column
        :param model_cls: ClickHouseModel subclass to import
        :param objects: Objects for which final versions are searched
        :param date_col: Optional column name, where partiion date is hold. Defaults to self.date_col
        :return: A generator of named tuples, representing previous state
        """

        def _dt_to_str(dt: Union[datetime.date, datetime.datetime]) -> str:
            if isinstance(dt, datetime.datetime):
                return format_datetime(dt, 0, db_alias=db_alias)
            elif isinstance(dt, datetime.date):
                return dt.isoformat()
            else:
                raise Exception('Invalid date or datetime object: `%s`' % dt)

        if not objects:
            raise StopIteration()

        date_col = date_col or self.date_col
        min_date, max_date = None, None
        for obj in objects:
            obj_date = getattr(obj, date_col)

            if min_date is None or min_date > obj_date:
                min_date = obj_date

            if max_date is None or max_date < obj_date:
                max_date = obj_date

        object_pks = [str(getattr(obj, self.pk_column)) for obj in objects]

        db_alias = model_cls.get_database_alias()

        min_date = _dt_to_str(min_date)
        max_date = _dt_to_str(max_date)

        # Get fields. Sign is replaced to negative for further processing
        columns = list(model_cls.fields(writable=True).keys())
        columns.remove(self.sign_col)
        columns.append('-1 AS sign')

        params = (db_alias, model_cls, min_date, max_date, object_pks, date_col, columns)

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
