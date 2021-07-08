import datetime
from queue import Queue

import pytz
from django.test import TestCase
from six import with_metaclass

from django_clickhouse.models import ClickHouseSyncModel
from django_clickhouse.utils import get_tz_offset, format_datetime, lazy_class_import, int_ranges, exec_in_parallel, \
    SingletonMeta


def local_dt_str(dt) -> str:
    """
    Returns string representation of an aware datetime object, localized by adding system_tz_offset()
    :param dt: Datetime to change
    :return: Formatted string
    """
    return (dt + datetime.timedelta(minutes=get_tz_offset())).strftime('%Y-%m-%d %H:%M:%S')


class FormatDateTimeTest(TestCase):
    @staticmethod
    def _get_zone_time(dt):
        """
        На момент написания тестов в РФ было какое-то странное смещение (для Москвы, например +2:30, для Перми +4:03)
        :param dt: Объект datetime.datetime
        :return: Строковый ожидаемый результат
        """
        minute_offset = dt.utcoffset().total_seconds() / 60
        zone_h, zone_m = abs(int(minute_offset / 60)), int(minute_offset % 60)

        return local_dt_str(dt - datetime.timedelta(hours=zone_h, minutes=zone_m))

    def test_conversion(self):
        dt = datetime.datetime(2017, 1, 2, 3, 4, 5)
        self.assertEqual(format_datetime(dt), local_dt_str(dt))
        dt = datetime.datetime(2017, 1, 2, 3, 4, 5, tzinfo=pytz.utc)
        self.assertEqual(format_datetime(dt), local_dt_str(dt))
        dt = datetime.datetime(2017, 1, 2, 3, 4, 5, tzinfo=pytz.timezone('Europe/Moscow'))
        self.assertEqual(format_datetime(dt), self._get_zone_time(dt))
        dt = datetime.datetime(2017, 1, 2, 3, 4, 5, tzinfo=pytz.timezone('Europe/Moscow'))
        offset = int(pytz.timezone('Europe/Moscow').utcoffset(dt).total_seconds() / 60)
        self.assertEqual(format_datetime(dt, timezone_offset=offset),
                         local_dt_str(datetime.datetime(2017, 1, 2, 3, 4, 5) - datetime.timedelta(minutes=offset*2)))

    def test_date_conversion(self):
        dt = datetime.date(2017, 1, 2)
        self.assertEqual(format_datetime(dt), local_dt_str(datetime.datetime(2017, 1, 2, 0, 0, 0)))
        dt = datetime.date(2017, 1, 2)
        self.assertEqual(format_datetime(dt, day_end=True), local_dt_str(datetime.datetime(2017, 1, 2, 23, 59, 59)))
        dt = datetime.date(2017, 1, 2)
        self.assertEqual(format_datetime(dt, day_end=True, timezone_offset=60),
                         local_dt_str(datetime.datetime(2017, 1, 2, 22, 59, 59)))
        dt = datetime.date(2017, 1, 2)
        self.assertEqual(format_datetime(dt, timezone_offset=60), local_dt_str(datetime.datetime(2017, 1, 1, 23, 0, 0)))


class TestLazyClassImport(TestCase):
    def test_str(self):
        self.assertEqual(ClickHouseSyncModel, lazy_class_import('django_clickhouse.models.ClickHouseSyncModel'))

    def test_cls(self):
        self.assertEqual(ClickHouseSyncModel, lazy_class_import(ClickHouseSyncModel))


class TestIntRanges(TestCase):
    def test_simple(self):
        self.assertListEqual([(1, 3), (5, 6), (8, 10)],
                             list(int_ranges([1, 2, 3, 5, 6, 8, 9, 10])))

    def test_empty(self):
        self.assertListEqual([], list(int_ranges([])))

    def test_bounds(self):
        self.assertListEqual([(1, 1), (5, 6), (10, 10)],
                             list(int_ranges([1, 5, 6, 10])))


class TestExecInParallel(TestCase):
    base_classes = []

    def test_exec(self):
        q = Queue()
        for i in range(10):
            q.put(([i], {}))

        res = exec_in_parallel(lambda x: x*x, q, 4)
        self.assertSetEqual({x * x for x in range(10)}, set(res))

    def test_exec_no_count(self):
        q = Queue()
        for i in range(10):
            q.put(([i], {}))

        res = exec_in_parallel(lambda x: x * x, q)
        self.assertSetEqual({x * x for x in range(10)}, set(res))

    def test_exception(self):
        q = Queue()
        for i in range(10):
            q.put(([i], {}))

        def _test_func(x):
            raise TypeError("Exception in thread %d" % x)

        with self.assertRaises(TypeError):
            exec_in_parallel(_test_func, q)


class TestSingletonMeta(TestCase):
    def test_singleton(self):
        class Single(with_metaclass(SingletonMeta)):
            def __init__(self):
                self.test = 1

        a = Single()
        a.test += 1
        b = Single()
        self.assertEqual(a, b)
        self.assertEqual(2, b.test)
