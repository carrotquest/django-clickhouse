from typing import Union, Any

import six
from importlib import import_module


def get_clickhouse_tz_offset():
    """
    Получает смещение временной зоны сервера ClickHouse в минутах
    :return: Integer
    """
    # Если даты форматируются вручную, то сервер воспринимает их как локаль сервера. Надо ее вычесть.
    return int(settings.CLICKHOUSE_DB.server_timezone.utcoffset(datetime.datetime.utcnow()).total_seconds() / 60)


def format_datetime(dt, timezone_offset=0, day_end=False):
    """
    Форматирует datetime.datetime в строковое представление, которое можно использовать в запросах к ClickHouse
    :param dt: Объект datetime.datetime или datetime.date
    :param timezone_offset: Смещение временной зоны в минутах
    :param day_end: Если флаг установлен, то будет взято время окончания дня, а не начала
    :return: Строковое представление даты-времени
    """
    assert isinstance(dt, (datetime.datetime, datetime.date)), "dt must be datetime.datetime instance"
    assert type(timezone_offset) is int, "timezone_offset must be integer"

    # datetime.datetime наследует datetime.date. Поэтому нельзя делать условие без отрицания
    if not isinstance(dt, datetime.datetime):
        t = datetime.time.max if day_end else datetime.time.min
        dt = datetime.datetime.combine(dt, t)

    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        dt = pytz.utc.localize(dt)
    else:
        dt = dt.astimezone(pytz.utc)

    # Если даты форматируются вручную, то сервер воспринимает их как локаль сервера.
    return (dt - datetime.timedelta(minutes=timezone_offset - get_clickhouse_tz_offset())).strftime("%Y-%m-%d %H:%M:%S")


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
