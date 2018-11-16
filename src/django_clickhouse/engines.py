"""
This file contains wrappers for infi.clckhouse_orm engines to use in django-clickhouse
"""
from typing import List, TypeVar, Type

from django.db.models import Model as DjangoModel
from infi.clickhouse_orm import engines as infi_engines
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model as InfiModel
from statsd.defaults.django import statsd

from django_clickhouse.database import connections
from .configuration import config
from .utils import lazy_class_import

T = TypeVar('T')


class InsertOnlyEngineMixin:
    def get_insert_batch(self, model_cls, database, objects):
        # type: (Type[T], Database, List[DjangoModel]) -> List[T]
        """
        Gets a list of model_cls instances to insert into database
        :param model_cls: ClickHouseModel subclass to import
        :param database: infi.clickhouse_orm Database instance to sync data with
        :param objects: A list of django Model instances to sync
        :return: A list of model_cls objects
        """
        serializer = model_cls.get_django_model_serializer()
        return [serializer.serialize(obj, model_cls) for obj in objects]


class MergeTree(InsertOnlyEngineMixin, infi_engines.MergeTree):
    pass


class CollapsingMergeTree(InsertOnlyEngineMixin, infi_engines.CollapsingMergeTree):
    def get_final_versions(self, model_cls, objects):
        """
        Get objects, that are currently stored in ClickHouse.
        Depending on the partition key this can be different for different models.
        In common case, this method is optimized for date field that doesn't change.
        It also supposes primary key to by id
        :param model_cls: ClickHouseModel subclass to import
        :param objects: Objects for which final versions are searched
        :return: A list of
        """
        min_date, max_date = None, None
        for obj in objects:
            obj_date = getattr(obj, self.date_col)

            if min_date is None or min_date > obj_date:
                min_date = obj_date

            if max_date is None or max_date < obj_date:
                max_date = obj_date

        obj_ids = [str(obj.id) for obj in objects]
        query = "SELECT * FROM $table FINAL WHERE `%s` >= '%s' AND `%s` <= '%s' AND id IN (%s)" \
                % (self.date_col, min_date.isoformat(), self.date_col, max_date.isoformat(), ', '.join(obj_ids))

        db_router = lazy_class_import(config.DATABASE_ROUTER)()
        db = db_router.db_for_read(model_cls)
        qs = connections[db].select(query, model_class=model_cls)
        return list(qs)

    def get_insert_batch(self, model_cls, database, objects):
        # type: (Type[T], Database, List[DjangoModel]) -> List[T]
        """
        Gets a list of model_cls instances to insert into database
        :param model_cls: ClickHouseModel subclass to import
        :param database: infi.clickhouse_orm Database instance to sync data with
        :param objects: A list of django Model instances to sync
        :return: A list of model_cls objects
        """
        new_objs = super(CollapsingMergeTree, self).get_insert_batch(model_cls, database, objects)

        statsd_key = "%s.sync.%s.get_final_versions" % (config.STATSD_PREFIX, model_cls.__name__)
        with statsd.timer(statsd_key):
            old_objs = self.get_final_versions(model_cls, new_objs)

        for obj in old_objs:
            self.set_obj_sign(obj, -1)

        for obj in new_objs:
            self.set_obj_sign(obj, 1)

        return old_objs + new_objs

    def set_obj_sign(self, obj, sign):  # type: (InfiModel, int) -> None
        """
        Sets objects sign. By default gets attribute nmae from sign_col
        :return: None
        """
        setattr(obj, self.sign_col, sign)
