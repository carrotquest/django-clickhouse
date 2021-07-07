import sys
from collections import namedtuple as basenamedtuple
from typing import Any, Set

from django.db import transaction, connections
from django.db.models import QuerySet


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


def django_pg_returning_available(using: str) -> bool:
    """
    Checks if django-pg-returning library is installed and can be used with given databse
    :return: Boolean
    """
    try:
        import django_pg_returning  # noqa: F401
        return connections[using].vendor == 'postgresql'
    except ImportError:
        return False


def update_returning_pk(qs: QuerySet, updates: dict) -> Set[Any]:
    """
    Updates QuerySet items returning primary key values.
    This method should not depend on database engine, though can have optimization performances for some engines.
    :param qs: QuerySet to update
    :param updates: Update items as passed to QuerySet.update(**updates) method
    :return: A set of primary keys
    """
    qs._for_write = True
    if django_pg_returning_available(qs.db) and hasattr(qs, 'update_returning'):
        pk_name = qs.model._meta.pk.name
        qs = qs.only(pk_name).update_returning(**updates)
        pks = set(qs.values_list(pk_name, flat=True))
    else:
        with transaction.atomic(using=qs.db):
            pks = set(qs.select_for_update().values_list('pk', flat=True))
            QuerySet.update(qs, **updates)

    return pks
