"""
This file defines different storages.
Storage saves intermediate data about database events - inserts, updates, delete.
This data is periodically fetched from storage and applied to ClickHouse tables.

Important:
Storage should be able to restore current importing batch, if something goes wrong.
"""
import datetime
import logging
from typing import Any, Optional, List, Tuple

import os
from statsd.defaults.django import statsd

from .configuration import config
from .exceptions import ConfigurationError, RedisLockTimeoutError
from .redis import redis_zadd
from .utils import check_pid, get_subclasses, SingletonMeta

logger = logging.getLogger('django-clickhouse')


class Storage:
    """
    Base abstract storage class, defining interface for all storages.
    The storage work algorithm:
    1) pre_sync()
    2) get_import_batch(). If batch is present go to 5)
    3) If batch is None, call get_operations()
    4) Transform operations to batch and call write_import_batch()
    5) Import batch to ClickHouse
    6) call post_sync(). If succeeded, it should remove the batch and it's data from sync_queue.

    If anything goes wrong before write_import_batch(), it is guaranteed that ClickHouse import hasn't been started yet,
    And we can repeat the procedure from the beginning.
    If anything goes wrong after write_import_batch(), we don't know it the part has been imported to ClickHouse.
    But ClickHouse is idempotent to duplicate inserts. So we can insert one batch twice correctly.
    """

    def pre_sync(self, import_key: str, **kwargs) -> None:
        """
        This method is called before import process starts
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param kwargs: Storage dependant arguments
        :return: None
        """
        pass

    def post_sync(self, import_key: str, **kwargs) -> None:
        """
        This method is called after import process has finished.
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param kwargs: Storage dependant arguments
        :return: None
        """
        pass

    def post_sync_failed(self, import_key: str, **kwargs) -> None:
        """
        This method is called after import process has finished with exception.
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param kwargs: Storage dependant arguments
        :return: None
        """
        pass

    def post_batch_removed(self, import_key: str, batch_size: int) -> None:
        """
        This method marks that batch has been removed in statsd
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param batch_size: Batch size to subtract from queue counter
        :return: None
        """
        key = "%s.sync.%s.queue" % (config.STATSD_PREFIX, import_key)
        statsd.gauge(key, self.operations_count(import_key))

    def operations_count(self, import_key: str, **kwargs) -> int:
        """
        Returns sync queue size
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param kwargs: Storage dependant arguments
        :return: Number of records in queue
        """
        raise NotImplementedError()

    def get_operations(self, import_key: str, count: int, **kwargs) -> List[Tuple[str, str]]:
        """
        Must return a list of operations on the model.
        Method should be error safe - if something goes wrong, import data should not be lost.
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param count: A batch size to get
        :param kwargs: Storage dependant arguments
        :return: A list of tuples (operation, pk) in incoming order.
        """
        raise NotImplementedError()

    def register_operations(self, import_key: str, operation: str, *pks: Any) -> int:
        """
        Registers new incoming operation
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param operation: One of insert, update, delete
        :param pk: Primary key to find records in main database. Should be string-serializable with str() method.
        :return: Number of registered operations
        """
        raise NotImplementedError()

    def register_operations_wrapped(self, import_key: str, operation: str, *pks: Any) -> int:
        """
        This is a wrapper for register_operation method, checking main parameters.
        This method should be called from inner functions.
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param operation: One of insert, update, delete
        :param pks: Primary keys to find records in main database. Should be string-serializable with str() method.
        :return: Number of registered operations
        """
        if operation not in {'insert', 'update', 'delete'}:
            raise ValueError('operation must be one of [insert, update, delete]')

        statsd_key = "%s.sync.%s.register_operations" % (config.STATSD_PREFIX, import_key)
        statsd.incr(statsd_key + '.%s' % operation, len(pks))
        with statsd.timer(statsd_key):
            ops_count = self.register_operations(import_key, operation, *pks)

        statsd_key = "%s.sync.%s.queue" % (config.STATSD_PREFIX, import_key)
        statsd.gauge(statsd_key, ops_count, delta=True)
        logger.debug('django-clickhouse: registered %s on %d items (%s) to storage'
                     % (operation, len(pks), import_key))

        return ops_count

    def flush(self):
        """
        This method is used in tests to drop all storage data
        :return: None
        """
        raise NotImplementedError()

    def get_last_sync_time(self, import_key: str) -> Optional[datetime.datetime]:
        """
        Gets the last time, sync has been executed
        :return: datetime.datetime if last sync has been. Otherwise - None.
        """
        raise NotImplementedError()

    def set_last_sync_time(self, import_key: str, dt: datetime.datetime) -> None:
        """
        Sets successful sync time
        :return: None
        """
        raise NotImplementedError()


class RedisStorage(Storage, metaclass=SingletonMeta):
    """
    Fast in-memory storage made on bases of redis and redis-py library.
    Requires:
        1) REDIS database
        2) CLICKHOUSE_REDIS_CONFIG parameter defined. This should be a dict of kwargs for redis.StrictRedis(**kwargs).
    """
    REDIS_KEY_OPS_TEMPLATE = 'clickhouse_sync:operations:{import_key}'
    REDIS_KEY_RANK_TEMPLATE = 'clickhouse_sync:timstamp:{import_key}'
    REDIS_KEY_LOCK = 'clickhouse_sync:lock:{import_key}'
    REDIS_KEY_LOCK_PID = 'clickhouse_sync:lock_pid:{import_key}'
    REDIS_KEY_LAST_SYNC_TS = 'clickhouse_sync:last_sync:{import_key}'

    def __init__(self):
        # Create redis library connection. If redis is not connected properly errors should be raised
        if config.REDIS_CONFIG is None:
            raise ConfigurationError('REDIS_CONFIG')

        from redis import StrictRedis
        self._redis = StrictRedis(**config.REDIS_CONFIG)
        self._locks = {}

    def register_operations(self, import_key, operation, *pks):
        key = self.REDIS_KEY_OPS_TEMPLATE.format(import_key=import_key)
        score = datetime.datetime.now().timestamp()

        items = {'%s:%s' % (operation, str(pk)): score for pk in pks}
        return redis_zadd(self._redis, key, items)

    def operations_count(self, import_key, **kwargs):
        ops_key = self.REDIS_KEY_OPS_TEMPLATE.format(import_key=import_key)
        return self._redis.zcard(ops_key)

    def get_operations(self, import_key, count, **kwargs):
        ops_key = self.REDIS_KEY_OPS_TEMPLATE.format(import_key=import_key)
        res = self._redis.zrangebyscore(ops_key, '-inf', datetime.datetime.now().timestamp(), start=0, num=count,
                                        withscores=True)

        if res:
            ops, scores = zip(*res)

            rank_key = self.REDIS_KEY_RANK_TEMPLATE.format(import_key=import_key)
            self._redis.set(rank_key, len(ops) - 1)

            return list(tuple(op.decode().split(':')) for op in ops)
        else:
            return []

    def get_lock(self, import_key, **kwargs):
        if self._locks.get(import_key) is None:
            from .redis import RedisLock
            lock_key = self.REDIS_KEY_LOCK.format(import_key=import_key)
            lock_timeout = kwargs.get('lock_timeout', config.SYNC_DELAY * 10)
            blocking_timeout = kwargs.get('blocking_timeout', config.SYNC_DELAY)
            self._locks[import_key] = RedisLock(self._redis, lock_key, timeout=lock_timeout,
                                                blocking_timeout=blocking_timeout, thread_local=False)

        return self._locks[import_key]

    def pre_sync(self, import_key, **kwargs):
        # Block process to be single threaded. Default sync delay is 10 * default sync delay.
        # It can be changed for model, by passing `lock_timeout` argument to pre_sync
        lock = self.get_lock(import_key, **kwargs)
        lock_pid_key = self.REDIS_KEY_LOCK_PID.format(import_key=import_key)
        try:
            lock.acquire()
            self._redis.set(lock_pid_key, os.getpid())
        except RedisLockTimeoutError:
            statsd.incr('%s.sync.%s.lock.timeout' % (config.STATSD_PREFIX, import_key))
            # Lock is busy. But If the process has been killed, I don't want to wait any more.
            # Let's check if pid exists
            pid = int(self._redis.get(lock_pid_key) or 0)
            if pid and not check_pid(pid):
                statsd.incr('%s.sync.%s.lock.hard_release' % (config.STATSD_PREFIX, import_key))
                logger.warning('django-clickhouse: hard releasing lock "%s" locked by pid %d (process is dead)'
                               % (import_key, pid))
                self._redis.delete(lock_pid_key)
                lock.hard_release()
                self.pre_sync(import_key, **kwargs)
            else:
                raise

    def post_sync(self, import_key, **kwargs):
        rank_key = self.REDIS_KEY_RANK_TEMPLATE.format(import_key=import_key)
        ops_key = self.REDIS_KEY_OPS_TEMPLATE.format(import_key=import_key)

        top_rank = self._redis.get(rank_key)
        if top_rank:
            res = self._redis.zremrangebyrank(ops_key, 0, int(top_rank))
            batch_size = int(res)
        else:
            batch_size = 0

        # unblock lock after sync completed
        lock_pid_key = self.REDIS_KEY_LOCK_PID.format(import_key=import_key)
        self._redis.delete(lock_pid_key)
        self.post_batch_removed(import_key, batch_size)
        self.get_lock(import_key, **kwargs).release()

        logger.info('django-clickhouse: removed %d operations from storage (key: %s)' % (batch_size, import_key))

    def post_sync_failed(self, import_key, **kwargs):
        # unblock lock after sync completed
        lock_pid_key = self.REDIS_KEY_LOCK_PID.format(import_key=import_key)
        self._redis.delete(lock_pid_key)
        self.get_lock(import_key, **kwargs).release()

    def flush(self):
        key_tpls = [
            self.REDIS_KEY_RANK_TEMPLATE.format(import_key='*'),
            self.REDIS_KEY_OPS_TEMPLATE.format(import_key='*'),
            self.REDIS_KEY_LOCK.format(import_key='*'),
            self.REDIS_KEY_LAST_SYNC_TS.format(import_key='*')
        ]
        for tpl in key_tpls:
            keys = self._redis.keys(tpl)
            if keys:
                self._redis.delete(*keys)

        from .clickhouse_models import ClickHouseModel
        for model in get_subclasses(ClickHouseModel):
            if isinstance(model.get_storage(), self.__class__):
                key = "%s.sync.%s.queue" % (config.STATSD_PREFIX, model.get_import_key())
                statsd.gauge(key, 0)

    def flush_import_key(self, import_key):
        keys = [
            self.REDIS_KEY_RANK_TEMPLATE.format(import_key=import_key),
            self.REDIS_KEY_OPS_TEMPLATE.format(import_key=import_key),
            self.REDIS_KEY_LOCK.format(import_key=import_key),
            self.REDIS_KEY_LAST_SYNC_TS.format(import_key=import_key)
        ]
        self._redis.delete(*keys)
        statsd.gauge("%s.sync.%s.queue" % (config.STATSD_PREFIX, import_key), 0)

    def get_last_sync_time(self, import_key):
        sync_ts_key = self.REDIS_KEY_LAST_SYNC_TS.format(import_key=import_key)
        res = self._redis.get(sync_ts_key)
        if res is None:
            return None

        return datetime.datetime.fromtimestamp(float(res))

    def set_last_sync_time(self, import_key, dt):
        sync_ts_key = self.REDIS_KEY_LAST_SYNC_TS.format(import_key=import_key)
        self._redis.set(sync_ts_key, dt.timestamp())
