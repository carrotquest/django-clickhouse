import datetime

from django.test import TestCase

from django_clickhouse.serializers import Django2ClickHouseModelSerializer
from tests.clickhouse_models import ClickHouseTestModel
from tests.models import TestModel


class Django2ClickHouseModelSerializerTest(TestCase):
    fixtures = ['test_model']

    def setUp(self):
        self.obj = TestModel.objects.get(pk=1)

    def test_all(self):
        serializer = Django2ClickHouseModelSerializer(ClickHouseTestModel)
        res = serializer.serialize(self.obj)
        self.assertEqual(self.obj.id, res.id)
        self.assertEqual(self.obj.value, res.value)
        self.assertEqual(self.obj.created_date, res.created_date)

    def test_fields(self):
        serializer = Django2ClickHouseModelSerializer(ClickHouseTestModel, fields=('value',))
        res = serializer.serialize(self.obj)
        self.assertEqual(0, res.id)
        self.assertEqual(datetime.date(1970, 1, 1), res.created_date)
        self.assertEqual(self.obj.value, res.value)

    def test_exclude_fields(self):
        serializer = Django2ClickHouseModelSerializer(ClickHouseTestModel, exclude_fields=('created_date',))
        res = serializer.serialize(self.obj)
        self.assertEqual(datetime.date(1970, 1, 1), res.created_date)
        self.assertEqual(self.obj.id, res.id)
        self.assertEqual(self.obj.value, res.value)
