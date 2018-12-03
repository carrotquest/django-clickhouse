import datetime

import pytz
from django.test import TestCase

from django_clickhouse.models import ClickHouseSyncModel
from django_clickhouse.utils import get_tz_offset, format_datetime, lazy_class_import, int_ranges


class GetTZOffsetTest(TestCase):
    def test_func(self):
        self.assertEqual(300, get_tz_offset())


class FormatDateTimeTest(TestCase):
    @staticmethod
    def _get_zone_time(dt):
        """
        На момент написания тестов в РФ было какое-то странное смещение (для Москвы, например +2:30, для Перми +4:03)
        :param dt: Объект datetime.datetime
        :return: Строковый ожидаемый результат
        """
        moscow_minute_offset = dt.utcoffset().total_seconds() / 60
        zone_h, zone_m = abs(int(moscow_minute_offset / 60)), int(moscow_minute_offset % 60)

        # +5 за счет времени тестового сервера ClickHouse
        return (dt - datetime.timedelta(hours=zone_h - 5, minutes=zone_m)).strftime("%Y-%m-%d %H:%M:%S")

    def test_conversion(self):
        dt = datetime.datetime(2017, 1, 2, 3, 4, 5)
        self.assertEqual(format_datetime(dt), '2017-01-02 08:04:05')
        dt = datetime.datetime(2017, 1, 2, 3, 4, 5, tzinfo=pytz.utc)
        self.assertEqual(format_datetime(dt), '2017-01-02 08:04:05')
        dt = datetime.datetime(2017, 1, 2, 3, 4, 5, tzinfo=pytz.timezone('Europe/Moscow'))
        self.assertEqual(format_datetime(dt), self._get_zone_time(dt))
        dt = datetime.datetime(2017, 1, 2, 3, 4, 5, tzinfo=pytz.timezone('Europe/Moscow'))
        offset = int(pytz.timezone('Europe/Moscow').utcoffset(dt).total_seconds() / 60)
        self.assertEqual(format_datetime(dt, timezone_offset=offset), '2017-01-02 03:04:05')

    def test_date_conversion(self):
        dt = datetime.date(2017, 1, 2)
        self.assertEqual(format_datetime(dt), '2017-01-02 05:00:00')
        dt = datetime.date(2017, 1, 2)
        self.assertEqual(format_datetime(dt, day_end=True), '2017-01-03 04:59:59')
        dt = datetime.date(2017, 1, 2)
        self.assertEqual(format_datetime(dt, day_end=True, timezone_offset=60), '2017-01-03 03:59:59')
        dt = datetime.date(2017, 1, 2)
        self.assertEqual(format_datetime(dt, timezone_offset=60), '2017-01-02 04:00:00')


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
