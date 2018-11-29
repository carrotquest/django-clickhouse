import datetime
import os
from itertools import chain
from typing import Union, Any, Optional, TypeVar, Set, Dict, Iterable

import pytz
import six
from importlib import import_module
from importlib.util import find_spec
from django.db.models import Model as DjangoModel

from .database import connections


T = TypeVar('T')


def get_tz_offset(db_alias=None):  # type: (Optional[str]) -> int
    """
    Returns ClickHouse server timezone offset in minutes
    :param db_alias: The database alias used
    :return: Integer
    """
    db = connections[db_alias]
    return int(db.server_timezone.utcoffset(datetime.datetime.utcnow()).total_seconds() / 60)


def format_datetime(dt, timezone_offset=0, day_end=False, db_alias=None):
    # type: (Union[datetime.date, datetime.datetime], int, bool, Optional[str]) -> str
    """
    Formats datetime and date objects to format that can be used in WHERE conditions of query
    :param dt: datetime.datetime or datetime.date object
    :param timezone_offset: timezone offset (minutes)
    :param day_end: If datetime.date is given and flag is set, returns day end time, not day start.
    :param db_alias: The database alias used
    :return: A string representing datetime
    """
    assert isinstance(dt, (datetime.datetime, datetime.date)), "dt must be datetime.datetime instance"
    assert type(timezone_offset) is int, "timezone_offset must be integer"

    # datetime.datetime inherits datetime.date. So I can't just make isinstance(dt, datetime.date)
    if not isinstance(dt, datetime.datetime):
        t = datetime.time.max if day_end else datetime.time.min
        dt = datetime.datetime.combine(dt, t)

    # Convert datetime to UTC, if it has timezone
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        dt = pytz.utc.localize(dt)
    else:
        dt = dt.astimezone(pytz.utc)

    # Dates in ClickHouse are parsed in server local timezone. So I need to add server timezone
    server_dt = dt - datetime.timedelta(minutes=timezone_offset - get_tz_offset(db_alias))

    return server_dt.strftime("%Y-%m-%d %H:%M:%S")


def module_exists(module_name):  # type: (str) -> bool
    """
    Checks if moudle exists
    :param module_name: Dot-separated module name
    :return: Boolean
    """
    # Python 3.4+
    spam_spec = find_spec(module_name)
    return spam_spec is not None


def lazy_class_import(obj):  # type: (Union[str, Any]) -> Any
    """
    If string is given, imports object by given module path.
    Otherwise returns the object
    :param obj: A string class path or object to return
    :return: Imported object
    """
    if isinstance(obj, six.string_types):
        module_name, obj_name = obj.rsplit('.', 1)
        module = import_module(module_name)

        try:
            return getattr(module, obj_name)
        except AttributeError:
            raise ImportError('Invalid import path `%s`' % obj)
    else:
        return obj


def get_subclasses(cls, recursive=False):  # type: (T, bool) -> Set[T]
    """
    Gets all subclasses of given class
    Attention!!! Classes would be found only if they were imported before using this function
    :param cls: Class to get subcalsses
    :param recursive: If flag is set, returns subclasses of subclasses and so on too
    :return: A list of subclasses
    """
    subclasses = set(cls.__subclasses__())

    if recursive:
        for subcls in subclasses.copy():
            subclasses.update(get_subclasses(subcls, recursive=True))

    return subclasses


def model_to_dict(instance, fields=None, exclude_fields=None):
    # type: (DjangoModel, Optional[Iterable[str]], Optional[Iterable[str]]) -> Dict[str, Any]
    """
    Standard model_to_dict ignores some fields if they have invalid naming
    :param instance: Object to convert to dictionary
    :param fields: Field list to extract from instance
    :param exclude_fields: Filed list to exclude from extraction
    :return: Serialized dictionary
    """
    data = {}

    opts = instance._meta
    fields = fields or {f.name for f in chain(opts.concrete_fields, opts.private_fields, opts.many_to_many)}

    for name in set(fields) - set(exclude_fields or set()):
        val = getattr(instance, name, None)
        if val is not None:
            data[name] = val

    return data


def check_pid(pid):
    """
    Check For the existence of a unix pid.
    """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True