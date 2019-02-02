import sys
from collections import namedtuple as basenamedtuple
from functools import lru_cache

from copy import deepcopy


class NamedTuple:
    __slots__ = ('_data', '_data_iterator')
    _defaults = {}
    _data_cls = None

    @classmethod
    @lru_cache(maxsize=32)
    def _get_defaults(cls, exclude):
        res = cls._defaults
        for k in exclude:
            res.pop(k, None)
        return res

    def __init__(self, *args, **kwargs):
        new_kwargs = deepcopy(self._get_defaults(self._data_cls._fields[:len(args)]))
        new_kwargs.update(kwargs)
        self._data = self._data_cls(*args, **new_kwargs)

    def __getattr__(self, item):
        return getattr(self._data, item)

    def __iter__(self):
        self._data_iterator = iter(self._data)
        return self

    def __next__(self):
        return next(self._data_iterator)


def namedtuple(*args, **kwargs):
    """
    Changes namedtuple to support defaults parameter as python 3.7 does
    https://docs.python.org/3.7/library/collections.html#collections.namedtuple
    :return: namedtuple class
    """
    if sys.version_info < (3, 7):
        defaults = kwargs.pop('defaults', {})
        return type('namedtuple', (NamedTuple,), {'_defaults': defaults, '_data_cls': basenamedtuple(*args, **kwargs)})
    else:
        return basenamedtuple(*args, **kwargs)
