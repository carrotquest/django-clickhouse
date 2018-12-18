"""
This file defines base abstract models to inherit from
"""
import datetime
from collections import defaultdict
from itertools import chain
from typing import List, Tuple, Iterable

from django.db.models import Model as DjangoModel
from django.utils.timezone import now
from infi.clickhouse_orm.database import Database
from infi.clickhouse_orm.engines import CollapsingMergeTree
from infi.clickhouse_orm.models import Model as InfiModel, ModelBase as InfiModelBase
from six import with_metaclass
from statsd.defaults.django import statsd

from .configuration import config
from .database import connections
from .exceptions import RedisLockTimeoutError
from .models import ClickHouseSyncModel
from .query import QuerySet
from .serializers import Django2ClickHouseModelSerializer
from .utils import lazy_class_import


class ClickHouseModelMeta(InfiModelBase):
    def __new__(cls, *args, **kwargs):
        res = super().__new__(cls, *args, **kwargs)  # type: ClickHouseModel

        if res.django_model and not issubclass(res.django_model, ClickHouseSyncModel):
            raise TypeError('django_model must be ClickHouseSyncModel subclass')

        if res.django_model and res.sync_enabled:
            res.django_model.register_clickhouse_sync_model(res)

        res.objects = QuerySet(res)

        return res


class ClickHouseModel(with_metaclass(ClickHouseModelMeta, InfiModel)):
    """
    Base model for all other models
    """
    django_model = None
    django_model_serializer = Django2ClickHouseModelSerializer

    read_db_aliases = (config.DEFAULT_DB_ALIAS,)
    write_db_aliases = (config.DEFAULT_DB_ALIAS,)
    migrate_replicated_db_aliases = (config.DEFAULT_DB_ALIAS,)
    migrate_non_replicated_db_aliases = (config.DEFAULT_DB_ALIAS,)

    sync_enabled = False
    sync_batch_size = None
    sync_storage = None
    sync_delay = None
    sync_database_alias = None
    sync_lock_timeout = None

    # This attribute is initialized in metaclass, as it must get model class as a parameter
    objects = None  # type: QuerySet

    @classmethod
    def objects_in(cls, database):  # type: (Database) -> QuerySet
        return QuerySet(cls, database)

    @classmethod
    def get_database_alias(cls, for_write=False):
        # type: (bool) -> str
        """
        Gets database alias for read or write purposes
        :param for_write: Boolean flag if database is neede for read or for write
        :return: Database alias to use
        """
        db_router = lazy_class_import(config.DATABASE_ROUTER)()
        if for_write:
            return db_router.db_for_write(cls)
        else:
            return db_router.db_for_read(cls)

    @classmethod
    def get_database(cls, for_write=False):
        # type: (bool) -> Database
        """
        Gets database alias for read or write purposes
        :param for_write: Boolean flag if database is neede for read or for write
        :return: infi.clickhouse_orm.Database instance
        """
        db_alias = cls.get_database_alias(for_write=for_write)
        return connections[db_alias]

    @classmethod
    def get_django_model_serializer(cls, writable=False):  # type: (bool) -> Django2ClickHouseModelSerializer
        serializer_cls = lazy_class_import(cls.django_model_serializer)
        return serializer_cls(cls, writable=writable)

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
    def get_lock_timeout(cls):
        return cls.sync_lock_timeout or cls.get_sync_delay() * 10

    @classmethod
    def get_import_key(cls):
        return cls.__name__

    def _prepare_val_for_eq(self, field_name, field, val):
        if isinstance(val, datetime.datetime):
            return val.replace(microsecond=0)

        # Sign column for collapsing should be ignored
        if isinstance(self.engine, CollapsingMergeTree) and field_name == self.engine.sign_col:
            return True

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
        if not operations:
            return []

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
    def get_insert_batch(cls, import_objects):  # type: (Iterable[DjangoModel]) -> List[ClickHouseModel]
        """
        Formats django model objects to batch of ClickHouse objects
        :param import_objects: DjangoModel objects to import
        :return: ClickHouseModel objects to import
        """
        return cls.engine.get_insert_batch(cls, import_objects)

    @classmethod
    def insert_batch(cls, batch):
        """
        Inserts batch into database
        :param batch:
        :return:
        """
        if batch:
            conn = connections[cls.sync_database_alias]
            conn.insert(batch)

    @classmethod
    def sync_batch_from_storage(cls):
        """
        Gets one batch from storage and syncs it.
        :return:
        """
        try:
            statsd_key = "%s.sync.%s.{0}" % (config.STATSD_PREFIX, cls.__name__)
            with statsd.timer(statsd_key.format('total')):

                storage = cls.get_storage()
                import_key = cls.get_import_key()

                with statsd.timer(statsd_key.format('pre_sync')):
                    storage.pre_sync(import_key, lock_timeout=cls.get_lock_timeout())

                with statsd.timer(statsd_key.format('get_operations')):
                    operations = storage.get_operations(import_key, cls.get_sync_batch_size())

                if operations:
                    with statsd.timer(statsd_key.format('get_sync_objects')):
                        import_objects = cls.get_sync_objects(operations)
                else:
                    import_objects = []

                if import_objects:
                    with statsd.timer(statsd_key.format('get_insert_batch')):
                        batch = cls.get_insert_batch(import_objects)

                    with statsd.timer(statsd_key.format('insert')):
                        cls.insert_batch(batch)

                with statsd.timer(statsd_key.format('post_sync')):
                    storage.post_sync(import_key)

                    storage.set_last_sync_time(import_key, now())
        except RedisLockTimeoutError:
            pass  # skip this sync round if lock is acquired by another thread

    @classmethod
    def need_sync(cls):  # type: () -> bool
        """
        Checks if this model needs synchronization: sync is enabled and delay has passed
        :return: Boolean
        """
        if not cls.sync_enabled:
            return False

        last_sync_time = cls.get_storage().get_last_sync_time(cls.get_import_key())

        if last_sync_time is None:
            return True

        return (last_sync_time - datetime.datetime.now()).total_seconds() >= cls.get_sync_delay()


class ClickHouseMultiModel(ClickHouseModel):
    """
    This model syncs one django model with multiple ClickHouse sub-models
    """
    sub_models = []

    @classmethod
    def sync_batch_from_storage(cls):
        """
        Gets one batch from storage and syncs it.
        :return:
        """
        try:
            statsd_key = "%s.sync.%s.{0}" % (config.STATSD_PREFIX, cls.__name__)
            with statsd.timer(statsd_key.format('total')):

                storage = cls.get_storage()
                import_key = cls.get_import_key()

                with statsd.timer(statsd_key.format('pre_sync')):
                    storage.pre_sync(import_key, lock_timeout=cls.get_lock_timeout())

                with statsd.timer(statsd_key.format('get_operations')):
                    operations = storage.get_operations(import_key, cls.get_sync_batch_size())

                if operations:
                    with statsd.timer(statsd_key.format('get_sync_objects')):
                        import_objects = cls.get_sync_objects(operations)
                else:
                    import_objects = []

                if import_objects:
                    batches = {}
                    with statsd.timer(statsd_key.format('get_insert_batch')):
                        for model_cls in cls.sub_models:
                            batches[model_cls] = model_cls.get_insert_batch(import_objects)

                    with statsd.timer(statsd_key.format('insert')):
                        for model_cls, batch in batches.items():
                            model_cls.insert_batch(batch)

                with statsd.timer(statsd_key.format('post_sync')):
                    storage.post_sync(import_key)
                    storage.set_last_sync_time(import_key, now())

        except RedisLockTimeoutError:
            pass  # skip this sync round if lock is acquired by another thread
