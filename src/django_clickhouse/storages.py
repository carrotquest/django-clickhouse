"""
This file defines different storages.
Storage saves intermediate data about database events - inserts, updates, delete.
This data is periodically fetched from storage and applied to ClickHouse tables.

Important:
Storage should be able to restore current importing batch, if something goes wrong.
"""
import datetime
from itertools import chain
from typing import Any, Optional, List, Tuple, Iterable

from .exceptions import ConfigurationError
from .configuration import config


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

    def pre_sync(self, import_key, **kwargs):  # type: (str, **dict) -> None
        """
        This method is called before import process starts
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param kwargs: Storage dependant arguments
        :return: None
        """
        pass

    def post_sync(self, import_key, **kwargs):  # type: (str, **dict) -> None
        """
        This method is called after import process has finished.
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param kwargs: Storage dependant arguments
        :return: None
        """
        pass

    def get_import_batch(self, import_key, **kwargs):
        # type: (str, **dict) -> Optional[Tuple[str]]
        """
        Returns a saved batch for ClickHouse import or None, if it was not found
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param kwargs: Storage dependant arguments
        :return: None, if no batch has been formed. A tuple strings, saved in write_import_batch() method.
        """
        raise NotImplemented()

    def write_import_batch(self, import_key, batch, **kwargs):
        # type: (str, Iterable[str], **dict) -> None
        """
        Saves batch for ClickHouse import
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param batch: An iterable of strings to save as a batch
        :param kwargs: Storage dependant arguments
        :return: None
        """
        raise NotImplemented()

    def get_operations(self, import_key, count, **kwargs):
        # type: (str, int, **dict) -> List[Tuple[str, str]]
        """
        Must return a list of operations on the model.
        Method should be error safe - if something goes wrong, import data should not be lost.
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param count: A batch size to get
        :param kwargs: Storage dependant arguments
        :return: A list of tuples (operation, pk) in incoming order.
        """
        raise NotImplemented()

    def register_operations(self, import_key, operation, *pks):  # type: (str, str, *Any) -> None
        """
        Registers new incoming operation
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param operation: One of insert, update, delete
        :param pk: Primary key to find records in main database. Should be string-serializable with str() method.
        :return: None
        """
        raise NotImplementedError()

    def register_operations_wrapped(self, import_key, operation, *pks):
        # type: (str, str, *Any)  -> None
        """
        This is a wrapper for register_operation method, checking main parameters.
        This method should be called from inner functions.
        :param import_key: A key, returned by ClickHouseModel.get_import_key() method
        :param operation: One of insert, update, delete
        :param pk: Primary key to find records in main database. Should be string-serializable with str() method.
        :return: None
        """
        if operation not in {'insert', 'update', 'delete'}:
            raise ValueError('operation must be one of [insert, update, delete]')

        return self.register_operations(import_key, operation, *pks)

    def flush(self):
        """
        This method is used in tests to drop all storage data
        :return: None
        """
        raise NotImplemented()


class RedisStorage(Storage):
    """
    Fast in-memory storage made on bases of redis and redis-py library.
    Requires:
        1) REDIS database
        2) CLICKHOUSE_REDIS_CONFIG parameter defined. This should be a dict of kwargs for redis.StrictRedis(**kwargs).
    """
    REDIS_KEY_OPS_TEMPLATE = 'clickhouse_sync:operations:{import_key}'
    REDIS_KEY_TS_TEMPLATE = 'clickhouse_sync:timstamp:{import_key}'
    REDIS_KEY_BATCH_TEMPLATE = 'clickhouse_sync:batch:{import_key}'

    def __init__(self):
        # Create redis library connection. If redis is not connected properly errors should be raised
        if config.REDIS_CONFIG is None:
            raise ConfigurationError('REDIS_CONFIG')

        from redis import StrictRedis
        self._redis = StrictRedis(**config.REDIS_CONFIG)

    def register_operations(self, import_key, operation, *pks):
        key = self.REDIS_KEY_OPS_TEMPLATE.format(import_key=import_key)
        score = datetime.datetime.now().timestamp()

        items = chain(*((score, '%s:%s' % (operation, str(pk))) for pk in pks))

        # key, score1, value1, score2, value2, ...
        self._redis.zadd(key, *items)

    def get_operations(self, import_key, count, **kwargs):
        ops_key = self.REDIS_KEY_OPS_TEMPLATE.format(import_key=import_key)
        res = self._redis.zrangebyscore(ops_key, '-inf', datetime.datetime.now().timestamp(), start=0, num=count,
                                        withscores=True)

        if res:
            ops, scores = zip(*res)

            ts_key = self.REDIS_KEY_TS_TEMPLATE.format(import_key=import_key)
            self._redis.set(ts_key, max(scores))

            return list(tuple(op.decode().split(':')) for op in ops)
        else:
            return []

    def get_import_batch(self, import_key, **kwargs):
        batch_key = self.REDIS_KEY_BATCH_TEMPLATE.format(import_key=import_key)
        return tuple(item.decode() for item in self._redis.lrange(batch_key, 0, -1))

    def write_import_batch(self, import_key, batch, **kwargs):
        batch_key = self.REDIS_KEY_BATCH_TEMPLATE.format(import_key=import_key)

        # Elements are pushed to the head, so we need to invert batch in order to save correct order
        self._redis.lpush(batch_key, *reversed(batch))

    def post_sync(self, import_key, **kwargs):
        ts_key = self.REDIS_KEY_TS_TEMPLATE.format(import_key=import_key)
        ops_key = self.REDIS_KEY_OPS_TEMPLATE.format(import_key=import_key)
        batch_key = self.REDIS_KEY_BATCH_TEMPLATE.format(import_key=import_key)

        score = float(self._redis.get(ts_key))
        self._redis.pipeline()\
            .zremrangebyscore(ops_key, '-inf', score)\
            .delete(batch_key)\
            .execute()

    def flush(self):
        key_tpls = [
            self.REDIS_KEY_TS_TEMPLATE.format(import_key='*'),
            self.REDIS_KEY_OPS_TEMPLATE.format(import_key='*'),
            self.REDIS_KEY_BATCH_TEMPLATE.format(import_key='*')
        ]
        for tpl in key_tpls:
            keys = self._redis.keys(tpl)
            if keys:
                self._redis.delete(*keys)

