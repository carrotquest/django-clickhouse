import sys
from collections import namedtuple as basenamedtuple


def namedtuple(*args, **kwargs):
    """
    Changes namedtuple to support defaults parameter as python 3.7 does
    https://docs.python.org/3.7/library/collections.html#collections.namedtuple
    See https://stackoverflow.com/questions/11351032/namedtuple-and-default-values-for-optional-keyword-arguments
    :return: namedtuple class
    """
    if sys.version_info < (3, 7):
        defaults = kwargs.pop('defaults', ())
        TupleClass = basenamedtuple(*args, **kwargs)
        TupleClass.__new__.__defaults__ = (None,) * (len(TupleClass._fields) - len(defaults)) + tuple(defaults)
        return TupleClass
    else:
        return basenamedtuple(*args, **kwargs)
