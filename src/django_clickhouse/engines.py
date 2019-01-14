"""
This file contains wrappers for infi.clckhouse_orm engines to use in django-clickhouse
"""
from typing import List, TypeVar, Type

from django.db.models import Model as DjangoModel
from infi.clickhouse_orm import engines as infi_engines
from infi.clickhouse_orm.models import Model as InfiModel
from statsd.defaults.django import statsd

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
        :return: A list of model_cls objects
        """
        serializer = model_cls.get_django_model_serializer(writable=True)
        return [serializer.serialize(obj) for obj in objects]


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

    def _get_final_versions_by_version(self, model_cls, min_date, max_date, object_pks):
        db = model_cls.get_database()
        min_date = format_datetime(min_date, 0, db_alias=db.db_alias)
        max_date = format_datetime(min_date, 0, day_end=True, db_alias=db.db_alias)

        query = """
            SELECT * FROM $table WHERE (`{pk_column}`, `{version_col}`) IN (
                SELECT `{pk_column}`, MAX(`{version_col}`) 
                FROM $table 
                PREWHERE `{date_col}` >= '{min_date}' AND `{date_col}` <= '{max_date}' 
                    AND `{pk_column}` IN ({object_pks})
                GROUP BY `{pk_column}`
           )
        """.format(version_col=self.version_col, date_col=self.date_col, pk_column=self.pk_column,
                   min_date=min_date.isoformat(), max_date=max_date.isoformat(), object_pks=','.join(object_pks))

        qs = db.select(query, model_class=model_cls)
        return list(qs)

    def _get_final_versions_by_final(self, model_cls, min_date, max_date, object_pks):
        db = model_cls.get_database()
        min_date = format_datetime(min_date, 0, db_alias=db.db_alias)
        max_date = format_datetime(min_date, 0, day_end=True, db_alias=db.db_alias)

        query = """
            SELECT * FROM $table FINAL
            WHERE `{date_col}` >= '{min_date}' AND `{date_col}` <= '{max_date}'
                AND `{pk_column}` IN ({object_pks})
        """
        query = query.format(date_col=self.date_col, pk_column=self.pk_column, min_date=min_date.isoformat(),
                             max_date=max_date.isoformat(), object_pks=','.join(object_pks))
        qs = db.select(query, model_class=model_cls)
        return list(qs)

    def get_final_versions(self, model_cls, objects):
        """
        Get objects, that are currently stored in ClickHouse.
        Depending on the partition key this can be different for different models.
        In common case, this method is optimized for date field that doesn't change.
        It also supposes primary key to by self.pk_column
        :param model_cls: ClickHouseModel subclass to import
        :param objects: Objects for which final versions are searched
        :return: A list of model objects
        """
        if not objects:
            return []

        min_date, max_date = None, None
        for obj in objects:
            obj_date = getattr(obj, self.date_col)

            if min_date is None or min_date > obj_date:
                min_date = obj_date

            if max_date is None or max_date < obj_date:
                max_date = obj_date

        object_pks = [str(getattr(obj, self.pk_column)) for obj in objects]

        if self.version_col:
            return self._get_final_versions_by_version(model_cls, min_date, max_date, object_pks)
        else:
            return self._get_final_versions_by_final(model_cls, min_date, max_date, object_pks)

    def get_insert_batch(self, model_cls, objects):
        # type: (Type[T], List[DjangoModel]) -> List[T]
        """
        Gets a list of model_cls instances to insert into database
        :param model_cls: ClickHouseModel subclass to import
        :param objects: A list of django Model instances to sync
        :return: A list of model_cls objects
        """
        new_objs = super(CollapsingMergeTree, self).get_insert_batch(model_cls, objects)

        statsd_key = "%s.sync.%s.get_final_versions" % (config.STATSD_PREFIX, model_cls.__name__)
        with statsd.timer(statsd_key):
            old_objs = self.get_final_versions(model_cls, new_objs)

        for obj in old_objs:
            self.set_obj_sign(obj, -1)
            self.inc_obj_version(obj)

        for obj in new_objs:
            self.set_obj_sign(obj, 1)

        return old_objs + new_objs

    def set_obj_sign(self, obj, sign):  # type: (InfiModel, int) -> None
        """
        Sets objects sign. By default gets attribute name from sign_col
        :return: None
        """
        setattr(obj, self.sign_col, sign)

    def inc_obj_version(self, obj):  # type: (InfiModel, int) -> None
        """
        Increments object version, if version column is set. By default gets attribute name from sign_col
        :return: None
        """
        if self.version_col:
            prev_version = getattr(obj, self.version_col) or 0
            setattr(obj, self.version_col, prev_version + 1)
