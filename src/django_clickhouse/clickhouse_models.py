"""
This file defines base abstract models to inherit from
"""
import datetime
from collections import defaultdict
from itertools import chain
from typing import List, Tuple

from django.db.models import Model as DjangoModel
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.models import Model as InfiModel, ModelBase as InfiModelBase
from six import with_metaclass
from statsd.defaults.django import statsd

from .configuration import config
from .database import connections, DEFAULT_DB_ALIAS
from .models import ClickHouseSyncModel
from .serializers import Django2ClickHouseModelSerializer
from .utils import lazy_class_import


class ClickHouseModelMeta(InfiModelBase):
    def __new__(cls, *args, **kwargs):
        res = super().__new__(cls, *args, **kwargs)  # type: ClickHouseModel

        if res.django_model and not issubclass(res.django_model, ClickHouseSyncModel):
            raise TypeError('django_model must be ClickHouseSyncModel subclass')

        if res.django_model and res.get_sync_delay():
            res.django_model.register_clickhouse_sync_model(res)

        return res


class ClickHouseModel(with_metaclass(ClickHouseModelMeta, InfiModel)):
    """
    Base model for all other models
    """
    django_model = None
    django_model_serializer = Django2ClickHouseModelSerializer

    read_db_aliases = (DEFAULT_DB_ALIAS,)
    write_db_aliases = (DEFAULT_DB_ALIAS,)

    sync_enabled = False
    sync_batch_size = None
    sync_storage = None
    sync_delay = None
    sync_database_alias = None

    def get_database(self, for_write=False):
        # type: (bool) -> Database
        """
        Gets database for read or write purposes
        :param for_write: Boolean flag if database is neede for read or for write
        :return: infi.clickhouse_orm.Database instance
        """
        db_router = lazy_class_import(config.DATABASE_ROUTER)()
        if for_write:
            return db_router.db_for_write(self.__class__, instance=self)
        else:
            return db_router.db_for_read(self.__class__, instance=self)

    @classmethod
    def get_django_model_serializer(cls):
        serializer_cls = lazy_class_import(cls.django_model_serializer)
        return serializer_cls()

    @classmethod
    def get_sync_batch_size(cls):
        return cls.sync_batch_size or config.SYNC_BATCH_SIZE

    @classmethod
    def get_storage(cls):
        return lazy_class_import(cls.sync_storage or config.SYNC_STORAGE)()

    @classmethod
    def get_sync_delay(cls):
        return cls.sync_delay or config.SYNC_DELAY

    @classmethod
    def get_import_key(cls):
        return cls.__name__

    def _prepare_val_for_eq(self, field_name, field, val):
        if isinstance(val, datetime.datetime):
            return val.replace(microsecond=0)
        elif field_name == '_version':
            return True  # Независимо от версии должно сравнение быть истиной
        return val

    def __eq__(self, other):
        if other.__class__ != self.__class__:
            return False

        for name, f in self.fields().items():
            val_1 = getattr(self, name, None)
            val_2 = getattr(other, name, None)
            if self._prepare_val_for_eq(name, f, val_1) != self._prepare_val_for_eq(name, f, val_2):
                return False

        return True

    @classmethod
    def get_sync_objects(cls, operations):  # type: (List[Tuple[str, str]]) -> List[DjangoModel]
        """
        Returns objects from main database to sync
        :param operations: A list of operations to perform
        :return: A list of django_model instances
        """
        pk_by_db = defaultdict(set)
        for op, pk_str in operations:
            using, pk = pk_str.split('.')
            pk_by_db[using].add(pk)

        objs = chain(*(
            cls.django_model.objects.filter(pk__in=pk_set).using(using)
            for using, pk_set in pk_by_db.items()
        ))
        return list(objs)

    @classmethod
    def sync_batch_from_storage(cls):
        """
        Gets one batch from storage and syncs it.
        :return:
        """
        statsd_key = "%s.sync.%s.{0}" % (config.STATSD_PREFIX, cls.__name__)
        with statsd.timer(statsd_key.format('total')):

            storage = cls.get_storage()
            import_key = cls.get_import_key()
            conn = connections[cls.sync_database_alias]

            with statsd.timer(statsd_key.format('pre_sync')):
                storage.pre_sync(import_key)

            with statsd.timer(statsd_key.format('get_import_batch')):
                batch = storage.get_import_batch(import_key)

            if batch is None:
                with statsd.timer(statsd_key.format('get_operations')):
                    operations = storage.get_operations(import_key, cls.get_sync_batch_size())

                with statsd.timer(statsd_key.format('get_sync_objects')):
                    import_objects = cls.get_sync_objects(operations)

                with statsd.timer(statsd_key.format('get_insert_batch')):
                    batch = cls.engine.get_insert_batch(cls, conn, import_objects)

                if batch:
                    with statsd.timer(statsd_key.format('write_import_batch')):
                        storage.write_import_batch(import_key, [obj.to_tsv() for obj in batch])
            else:
                # Previous import error, retry
                statsd.incr(statsd_key.format('restore_existing_batch'))

            if batch:
                with statsd.timer(statsd_key.format('insert')):
                    conn.insert(batch)

            with statsd.timer(statsd_key.format('post_sync')):
                storage.post_sync(import_key)


# class ClickHouseModelConverter:
#     """
#     Абстрактный класс, описывающий процесс конвертации модели django в модель ClickHouse и обратно.
#
#     @classmethod
#     def start_sync(cls):
#         """
#         Проверяет, нужна ли модели синхронизация.
#         Если синхронизация нужна, отмечает, что синхронизация началась.
#         :return: Boolean, надо ли начинать синхронизацию
#         """
#         if cls.auto_sync is None:
#             return False
#
#         assert type(cls.auto_sync) is int and cls.auto_sync > 0, \
#             "auto_sync attribute must be positive integer if given"
#
#         # Получаем результаты предыдущей синхронизации
#         redis_dict_key = "{0}:{1}".format(cls.__module__, cls.__name__)
#
#         now_ts = int(now().timestamp())
#
#         # Сразу же делаем вид, что обновление выполнено.
#         # Если другой поток зайдет сюда, он увидит, что обновление уже выполнено
#         # В конце, если обновление не выполнялось, вернем старое значение
#         previous = settings.REDIS.pipeline().hget(ClickHouseModelConverter.REDIS_SYNC_KEY, redis_dict_key). \
#             hset(ClickHouseModelConverter.REDIS_SYNC_KEY, redis_dict_key, now_ts).execute()[0]
#
#         previous = int(previous) if previous else 0
#         result = bool(previous + cls.auto_sync < now_ts)
#         if not result:
#             # Возвращаем старое значение флагу, который мы изменили выше
#             settings.REDIS.hset(ClickHouseModelConverter.REDIS_SYNC_KEY, redis_dict_key, previous)
#
#         return result
#
#
