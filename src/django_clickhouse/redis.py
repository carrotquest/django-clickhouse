"""
Contains additional components for redis-py to use in RedisStorage
"""
import os
from itertools import chain
import logging

from .exceptions import RedisLockTimeoutError


logger = logging.getLogger('django-clickhouse')


class RedisLock:
    """
    Fixes issue of https://github.com/andymccurdy/redis-py/issues/621
    """

    def __init__(self, redis_client, *args, **kwargs):
        self.lock = redis_client.lock(*args, **kwargs)

    def __enter__(self):
        return self.lock.acquire()

    def __exit__(self, type, value, tb):
        self.lock.release()

    def acquire(self):
        logger.debug('django-clickhouse: acquiring lock "%s" with pid %d' % (self.lock.name, os.getpid()))
        if self.lock.acquire():
            logger.debug('django-clickhouse:  acquired lock "%s" with pid %d' % (self.lock.name, os.getpid()))
            return self
        else:
            logger.warning('django-clickhouse: timeout lock "%s" with pid %d' % (self.lock.name, os.getpid()))
            raise RedisLockTimeoutError()

    def release(self):
        logger.debug('django-clickhouse: releasing lock "%s" with pid %d' % (self.lock.name, os.getpid()))
        self.lock.release()

    def hard_release(self) -> bool:
        """
        Drops the lock, not looking if it is acquired by anyone.
        :return: Boolean - if lock has been acquired before releasing or not
        """
        logger.warning('django-clickhouse: hard releasing lock "%s" with pid %d' % (self.lock.name, os.getpid()))
        return bool(self.lock.redis.delete(self.lock.name))


def redis_zadd(redis_client, key, mapping, **kwargs):
    """
    In redis-py 3.* interface of zadd changed to mapping
    :return:
    """
    import redis
    if int(redis.__version__.split('.', 1)[0]) < 3:
        # key, score1, value1, score2, value2, ...
        items = chain(*((score, key) for key, score in mapping.items()))
    else:
        items = [mapping]

    return redis_client.zadd(key, *items, **kwargs)
