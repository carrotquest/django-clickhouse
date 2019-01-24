"""
This file contains wrappers for infi.clckhouse_orm engines to use in django-clickhouse
"""
import datetime
from typing import List, TypeVar, Type, Union, Iterable

from django.db.models import Model as DjangoModel
from infi.clickhouse_orm import engines as infi_engines
from infi.clickhouse_orm.models import Model as InfiModel
from statsd.defaults.django import statsd

from django_clickhouse.database import connections
from .configuration import config
from .utils import format_datetime

T = TypeVar('T')


class InsertOnlyEngineMixin:
    def get_insert_batch(self, model_cls, objects):
        # type: (Type[T], List[DjangoModel]) -> List[T]
        """
        Gets a list of model_cls instances to insert into database
        :param model_cls: ClickHouseModel subclass to import
        :param objects: A list of django Model instances to sync
        :return: An iterator of model_cls instances
        """
        serializer = model_cls.get_django_model_serializer(writable=True)
        return list(serializer.serialize_many(objects))


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

    def _get_final_versions_by_version(self, db_alias, model_cls, min_date, max_date, object_pks, date_col):
        query = """
            SELECT * FROM $table WHERE (`{pk_column}`, `{version_col}`) IN (
                SELECT `{pk_column}`, MAX(`{version_col}`) 
                FROM $table 
                PREWHERE `{date_col}` >= '{min_date}' AND `{date_col}` <= '{max_date}' 
                    AND `{pk_column}` IN ({object_pks})
                GROUP BY `{pk_column}`
           )
        """.format(version_col=self.version_col, date_col=date_col, pk_column=self.pk_column,
                   min_date=min_date, max_date=max_date, object_pks=','.join(object_pks))

        qs = connections[db_alias].select_init_many(query, model_cls)
        return list(qs)

    def _get_final_versions_by_final(self, db_alias, model_cls, min_date, max_date, object_pks, date_col):
        query = """
            SELECT * FROM $table FINAL
            WHERE `{date_col}` >= '{min_date}' AND `{date_col}` <= '{max_date}'
                AND `{pk_column}` IN ({object_pks})
        """
        query = query.format(date_col=date_col, pk_column=self.pk_column, min_date=min_date,
                             max_date=max_date, object_pks=','.join(object_pks))
        qs = connections[db_alias].select_init_many(query, model_cls)
        return list(qs)

    def get_final_versions(self, model_cls, objects, date_col=None):
        """
        Get objects, that are currently stored in ClickHouse.
        Depending on the partition key this can be different for different models.
        In common case, this method is optimized for date field that doesn't change.
        It also supposes primary key to by self.pk_column
        :param model_cls: ClickHouseModel subclass to import
        :param objects: Objects for which final versions are searched
        :return: A list of model objects
        """

        def _dt_to_str(dt):  # type: (Union[datetime.date, datetime.datetime]) -> str
            if isinstance(dt, datetime.datetime):
                return format_datetime(dt, 0, db_alias=db_alias)
            elif isinstance(dt, datetime.date):
                return dt.isoformat()
            else:
                raise Exception('Invalid date or datetime object: `%s`' % dt)

        if not objects:
            return []

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

        if self.version_col:
            return self._get_final_versions_by_version(db_alias, model_cls, min_date, max_date, object_pks, date_col)
        else:
            return self._get_final_versions_by_final(db_alias, model_cls, min_date, max_date, object_pks, date_col)

    def get_insert_batch(self, model_cls, objects):
        # type: (Type[T], List[DjangoModel]) -> List[T]
        """
        Gets a list of model_cls instances to insert into database
        :param model_cls: ClickHouseModel subclass to import
        :param objects: A list of django Model instances to sync
        :return: A list of model_cls objects
        """
        new_objs = super(CollapsingMergeTree, self).get_insert_batch(model_cls, objects)

        statsd_key = "%s.sync.%s.steps.get_final_versions" % (config.STATSD_PREFIX, model_cls.__name__)
        with statsd.timer(statsd_key):
            old_objs = self.get_final_versions(model_cls, new_objs)

        old_objs_versions = {}
        for obj in old_objs:
            self.set_obj_sign(obj, -1)
            old_objs_versions[obj.id] = obj.version

        for obj in new_objs:
            self.set_obj_sign(obj, 1)

            if self.version_col:
                setattr(obj, self.version_col, old_objs_versions.get(obj.id, 0) + 1)

        return old_objs + new_objs

    def set_obj_sign(self, obj, sign):  # type: (InfiModel, int) -> None
        """
        Sets objects sign. By default gets attribute name from sign_col
        :return: None
        """
        setattr(obj, self.sign_col, sign)
