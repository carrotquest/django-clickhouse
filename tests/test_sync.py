import datetime

from django.test import TransactionTestCase

from django_clickhouse.database import connections
from django_clickhouse.migrations import migrate_app
from tests.clickhouse_models import ClickHouseTestModel, ClickHouseCollapseTestModel
from tests.models import TestModel


class SyncTest(TransactionTestCase):
    def setUp(self):
        self.db = connections['default']
        self.db.drop_database()
        self.db.db_exists = False
        self.db.create_database()
        migrate_app('tests', 'default')

    def test_simple(self):
        obj = TestModel.objects.create(value=1, created_date=datetime.date.today())
        ClickHouseTestModel.sync_batch_from_storage()

        synced_data = list(ClickHouseTestModel.objects_in(connections['default']))
        self.assertEqual(1, len(synced_data))
        self.assertEqual(obj.created_date, synced_data[0].created_date)
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

    def test_collapsing_update(self):
        obj = TestModel.objects.create(value=1, created_date=datetime.date.today())
        obj.value = 2
        obj.save()
        ClickHouseCollapseTestModel.sync_batch_from_storage()

        synced_data = list(ClickHouseCollapseTestModel.objects_in(connections['default']))
        self.assertEqual(1, len(synced_data))
        self.assertEqual(obj.created_date, synced_data[0].created_date)
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

        obj.value = 3
        obj.save()
        ClickHouseCollapseTestModel.sync_batch_from_storage()

        synced_data = list(self.db.select('SELECT * FROM $table FINAL', model_class=ClickHouseCollapseTestModel))
        self.assertGreaterEqual(1, len(synced_data))
        self.assertEqual(obj.created_date, synced_data[0].created_date)
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)
