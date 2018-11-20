"""
Contains additional components for redis-py to use in RedisStorage
"""
from .exceptions import RedisLockTimeoutError


class RedisLock:
    """
    Fixes issue of https://github.com/andymccurdy/redis-py/issues/621
    """

    def __init__(self, redis_client, *args, **kwargs):
        self.lock = redis_client.lock(*args, **kwargs)

    def __enter__(self):
        if self.lock.acquire():
            return self
        else:
            raise RedisLockTimeoutError()

    def __exit__(self, type, value, tb):
        self.lock.release()

    def acquire(self):
        self.lock.acquire()

    def release(self):
        self.lock.release()

    def hard_release(self) -> bool:
        """
        Drops the lock, not looking if it is acquired by anyone.
        :return: Boolean - if lock has been acquired before releasing or not
        """
        return bool(self.lock.redis.delete(self.lock.name))
