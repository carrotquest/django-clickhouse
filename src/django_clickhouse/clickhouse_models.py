"""
This file defines base abstract models to inherit from
"""
import datetime
import logging
from collections import defaultdict
from copy import deepcopy
from itertools import chain
from typing import List, Tuple, Iterable, Set, Any, Optional

from django.db.models import Model as DjangoModel, QuerySet as DjangoQuerySet
from infi.clickhouse_orm.engines import CollapsingMergeTree
from infi.clickhouse_orm.models import Model as InfiModel, ModelBase as InfiModelBase
from statsd.defaults.django import statsd

from .compatibility import namedtuple
from .configuration import config
from .database import connections, Database
from .exceptions import RedisLockTimeoutError
from .models import ClickHouseSyncModel
from .query import QuerySet
from .serializers import Django2ClickHouseModelSerializer
from .utils import lazy_class_import, exec_multi_arg_func

logger = logging.getLogger('django-clickhouse')


class ClickHouseModelMeta(InfiModelBase):
    def __new__(cls, *args, **kwargs):
        res = super().__new__(cls, *args, **kwargs)  # type: ClickHouseModel

        if res.django_model and not issubclass(res.django_model, ClickHouseSyncModel):
            raise TypeError('django_model must be ClickHouseSyncModel subclass')

        if res.django_model and res.sync_enabled:
            res.django_model.register_clickhouse_sync_model(res)

        res.objects = QuerySet(res)

        return res


class ClickHouseModel(InfiModel, metaclass=ClickHouseModelMeta):
    """
    Base model for all other models
    """
    django_model = None
    django_model_serializer = Django2ClickHouseModelSerializer

    # Servers, model is replicated to.
    # Router takes random database to read or write from.
    read_db_aliases = (config.DEFAULT_DB_ALIAS,)
    write_db_aliases = (config.DEFAULT_DB_ALIAS,)

    # Databases to perform replicated migration queries, such as ALTER TABLE.
    # Migration is applied to random database from the list.
    migrate_replicated_db_aliases = (config.DEFAULT_DB_ALIAS,)

    # Databases to perform non-replicated migrations (CREATE TABLE, DROP TABLE).
    # Migration is applied to all databases from the list.
    migrate_non_replicated_db_aliases = (config.DEFAULT_DB_ALIAS,)

    sync_enabled = False
    sync_batch_size = None
    sync_storage = None
    sync_delay = None
    sync_lock_timeout = None

    # This flag gives ability to disable to_db_string while inserting data, if it is already formatted
    sync_formatted_tuples = False

    # This attribute is initialized in metaclass, as it must get model class as a parameter
    objects = None  # type: QuerySet

    @classmethod
    def get_tuple_class(cls, field_names=None, defaults=None):
        field_names = field_names or cls.fields(writable=False).keys()

        if defaults:
            defaults_new = deepcopy(cls._defaults)
            defaults_new.update(defaults)
        else:
            defaults_new = cls._defaults

        # defaults should be rightmost arguments
        required_field_names = tuple(name for name in field_names if name not in defaults_new)

        default_field_names, default_values = zip(*sorted(defaults_new.items(), key=lambda t: t[0]))

        # Strange, but sometimes the columns are in different order...
        field_names = tuple(sorted(required_field_names)) + default_field_names

        return namedtuple("%sTuple" % cls.__name__, field_names, defaults=default_values)

    @classmethod
    def objects_in(cls, database: Database) -> QuerySet:
        return QuerySet(cls, database)

    @classmethod
    def get_database_alias(cls, for_write: bool = False) -> str:
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
    def get_database(cls, for_write: bool = False) -> Database:
        """
        Gets database alias for read or write purposes
        :param for_write: Boolean flag if database is neede for read or for write
        :return: infi.clickhouse_orm.Database instance
        """
        db_alias = cls.get_database_alias(for_write=for_write)
        return connections[db_alias]

    @classmethod
    def get_django_model_serializer(cls, writable: bool = False, defaults: Optional[dict] = None
                                    ) -> Django2ClickHouseModelSerializer:
        serializer_cls = lazy_class_import(cls.django_model_serializer)
        return serializer_cls(cls, writable=writable, defaults=defaults)

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
    def get_sync_query_set(cls, using: str, pk_set: Set[Any]) -> DjangoQuerySet:
        """
        Forms django queryset to fetch for sync
        :param using: Database to fetch from
        :param pk_set: A set of primary keys to fetch
        :return: QuerySet
        """
        return cls.django_model.objects.filter(pk__in=pk_set).using(using)

    @classmethod
    def get_sync_objects(cls, operations: List[Tuple[str, str]]) -> List[DjangoModel]:
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

        # Selecting data from multiple databases should work faster in parallel, if connections are independent.
        objs = exec_multi_arg_func(
            lambda db_alias: list(cls.get_sync_query_set(db_alias, pk_by_db[db_alias])),
            pk_by_db.keys()
        )
        return list(chain(*objs))

    @classmethod
    def get_insert_batch(cls, import_objects: Iterable[DjangoModel]) -> List['ClickHouseModel']:
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
        :param batch: Batch of tuples to insert
        :return:
        """
        if batch:
            cls.get_database(for_write=True).insert_tuples(cls, batch, formatted=cls.sync_formatted_tuples)

    @classmethod
    def sync_batch_from_storage(cls):
        """
        Gets one batch from storage and syncs it.
        :return:
        """
        import_key = cls.get_import_key()
        storage = cls.get_storage()
        statsd_key = "%s.sync.%s.{0}" % (config.STATSD_PREFIX, import_key)

        try:
            with statsd.timer(statsd_key.format('total')):
                with statsd.timer(statsd_key.format('steps.pre_sync')):
                    storage.pre_sync(import_key, lock_timeout=cls.get_lock_timeout())

                with statsd.timer(statsd_key.format('steps.get_operations')):
                    operations = storage.get_operations(import_key, cls.get_sync_batch_size())

                statsd.incr(statsd_key.format('operations'), len(operations))
                logger.debug('django-clickhouse: got %d operations from storage (key: %s)'
                             % (len(operations), import_key))

                if operations:
                    with statsd.timer(statsd_key.format('steps.get_sync_objects')):
                        import_objects = cls.get_sync_objects(operations)
                else:
                    import_objects = []

                statsd.incr(statsd_key.format('import_objects'), len(import_objects))
                logger.debug('django-clickhouse: got %d objects to import from database (key: %s)'
                             % (len(import_objects), import_key))

                if import_objects:
                    with statsd.timer(statsd_key.format('steps.get_insert_batch')):
                        # NOTE I don't use generator pattern here, as it move all time into insert.
                        #  That makes hard to understand where real problem is in monitoring
                        batch = tuple(cls.get_insert_batch(import_objects))

                    logger.debug('django-clickhouse: formed %d ClickHouse objects to insert (key: %s)'
                                 % (len(batch), import_key))

                    with statsd.timer(statsd_key.format('steps.insert')):
                        cls.insert_batch(batch)

                with statsd.timer(statsd_key.format('steps.post_sync')):
                    storage.post_sync(import_key)
        except RedisLockTimeoutError:
            pass  # skip this sync round if lock is acquired by another thread
        except Exception as ex:
            with statsd.timer(statsd_key.format('steps.post_sync')):
                storage.post_sync_failed(import_key)
            raise ex

    @classmethod
    def need_sync(cls) -> bool:
        """
        Checks if this model needs synchronization: sync is enabled and delay has passed
        :return: Boolean
        """
        if not cls.sync_enabled:
            logger.debug('django-clickhouse: need_sync returned False for class %s as sync is disabled' % cls.__name__)
            return False

        last_sync_time = cls.get_storage().get_last_sync_time(cls.get_import_key())

        if last_sync_time is None:
            logger.debug('django-clickhouse: need_sync returned True for class %s as no last sync found' % cls.__name__)
            return True

        res = (datetime.datetime.now() - last_sync_time).total_seconds() >= cls.get_sync_delay()
        logger.debug('django-clickhouse: need_sync returned %s for class %s as no last sync found'
                     ' (now: %s, last: %s, delay: %d)'
                     % (res, cls.__name__, datetime.datetime.now().isoformat(), last_sync_time.isoformat(),
                        cls.get_sync_delay()))

        return res


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
        import_key = cls.get_import_key()
        storage = cls.get_storage()
        statsd_key = "%s.sync.%s.{0}" % (config.STATSD_PREFIX, import_key)

        try:
            with statsd.timer(statsd_key.format('total')):
                with statsd.timer(statsd_key.format('steps.pre_sync')):
                    storage.pre_sync(import_key, lock_timeout=cls.get_lock_timeout())

                with statsd.timer(statsd_key.format('steps.get_operations')):
                    operations = storage.get_operations(import_key, cls.get_sync_batch_size())

                statsd.incr(statsd_key.format('operations'), len(operations))
                logger.debug('django-clickhouse: got %d operations from storage (key: %s)'
                             % (len(operations), import_key))

                if operations:
                    with statsd.timer(statsd_key.format('steps.get_sync_objects')):
                        import_objects = cls.get_sync_objects(operations)
                else:
                    import_objects = []

                statsd.incr(statsd_key.format('import_objects'), len(import_objects))
                logger.debug('django-clickhouse: got %d objects to import from database (key: %s)'
                             % (len(import_objects), import_key))

                if import_objects:
                    batches = {}
                    with statsd.timer(statsd_key.format('steps.get_insert_batch')):
                        def _sub_model_func(model_cls):
                            model_statsd_key = "%s.sync.%s.{0}" % (config.STATSD_PREFIX, model_cls.__name__)
                            with statsd.timer(model_statsd_key.format('steps.get_insert_batch')):
                                # NOTE I don't use generator pattern here, as it move all time into insert.
                                # That makes hard to understand where real problem is in monitoring
                                batch = tuple(model_cls.get_insert_batch(import_objects))

                            logger.debug('django-clickhouse: formed %d ClickHouse objects to insert'
                                         ' (model_cls: %s, key: %s)' % (len(batch), model_cls.__name__, import_key))
                            return model_cls, batch

                        res = exec_multi_arg_func(_sub_model_func, cls.sub_models, threads_count=len(cls.sub_models))
                        batches = dict(res)

                    with statsd.timer(statsd_key.format('steps.insert')):
                        def _sub_model_func(model_cls):
                            model_statsd_key = "%s.sync.%s.{0}" % (config.STATSD_PREFIX, model_cls.__name__)
                            with statsd.timer(model_statsd_key.format('steps.insert')):
                                model_cls.insert_batch(batches[model_cls])

                        exec_multi_arg_func(_sub_model_func, cls.sub_models, threads_count=len(cls.sub_models))

                with statsd.timer(statsd_key.format('steps.post_sync')):
                    storage.post_sync(import_key)

        except RedisLockTimeoutError:
            pass  # skip this sync round if lock is acquired by another thread
        except Exception as ex:
            with statsd.timer(statsd_key.format('steps.post_sync')):
                storage.post_sync_failed(import_key)
            raise ex
