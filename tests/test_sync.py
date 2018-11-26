import datetime
import signal

import os
from multiprocessing import Process
from time import sleep
from unittest import skip, expectedFailure

import random
from django.db import connections as django_connections
from django.db.models import F
from django.test import TransactionTestCase, override_settings

from django_clickhouse import config
from django_clickhouse.database import connections
from django_clickhouse.migrations import migrate_app
from tests.clickhouse_models import ClickHouseTestModel, ClickHouseCollapseTestModel, ClickHouseMultiTestModel
from tests.models import TestModel


class SyncTest(TransactionTestCase):
    def setUp(self):
        self.db = connections['default']
        self.db.drop_database()
        self.db.db_exists = False
        self.db.create_database()
        migrate_app('tests', 'default')
        ClickHouseTestModel.get_storage().flush()

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

        # sync_batch_from_storage uses FINAL, so data would be collapsed by now
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

    @expectedFailure
    def test_collapsing_delete(self):
        obj = TestModel.objects.create(value=1, created_date=datetime.date.today())
        ClickHouseCollapseTestModel.sync_batch_from_storage()
        obj.delete()
        ClickHouseCollapseTestModel.sync_batch_from_storage()

        # sync_batch_from_storage uses FINAL, so data would be collapsed by now
        synced_data = list(ClickHouseCollapseTestModel.objects_in(connections['default']))
        self.assertEqual(0, len(synced_data))

    def test_multi_model(self):
        obj = TestModel.objects.create(value=1, created_date=datetime.date.today())
        obj.value = 2
        obj.save()
        ClickHouseMultiTestModel.sync_batch_from_storage()

        synced_data = list(ClickHouseTestModel.objects_in(connections['default']))
        self.assertEqual(1, len(synced_data))
        self.assertEqual(obj.created_date, synced_data[0].created_date)
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

        # sync_batch_from_storage uses FINAL, so data would be collapsed by now
        synced_data = list(ClickHouseCollapseTestModel.objects_in(connections['default']))
        self.assertEqual(1, len(synced_data))
        self.assertEqual(obj.created_date, synced_data[0].created_date)
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

        obj.value = 3
        obj.save()
        ClickHouseMultiTestModel.sync_batch_from_storage()

        synced_data = list(self.db.select('SELECT * FROM $table FINAL', model_class=ClickHouseCollapseTestModel))
        self.assertGreaterEqual(1, len(synced_data))
        self.assertEqual(obj.created_date, synced_data[0].created_date)
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)


@skip("This doesn't work due to different threads connection problems")
class KillTest(TransactionTestCase):
    TEST_TIME = 30
    start = datetime.datetime.now()

    def setUp(self):
        ClickHouseTestModel.get_storage().flush()

    @staticmethod
    def _create_process(count=1000, test_time=60, period=1):
        for iteration in range(test_time):
            TestModel.objects.using('create').bulk_create([
                TestModel(created_date='2018-01-01', value=iteration * count + i) for i in range(count)])
            django_connections['create'].close()
            sleep(period)

    @staticmethod
    def _update_process(count=1000, test_time=60, period=1):
        for iteration in range(test_time):
            TestModel.objects.using('update').filter(id__gte=iteration * count).annotate(idmod10=F('id') % 10). \
                filter(idmod10=0).update(value=-1)
            django_connections['update'].close()
            sleep(period)

    @staticmethod
    def _delete_process(count=1000, test_time=60, period=1):
        for iteration in range(test_time):
            TestModel.objects.using('delete').filter(id__gte=iteration * count).annotate(idmod10=F('id') % 10). \
                filter(idmod10=1).delete()
            django_connections['delete'].close()
            sleep(period)

    @classmethod
    def _sync_process(cls, period=1):
        while (datetime.datetime.now() - cls.start).total_seconds() < cls.TEST_TIME:
            ClickHouseCollapseTestModel.sync_batch_from_storage()
            sleep(period)

    def _kill_process(self, p):
        # https://stackoverflow.com/questions/47553120/kill-a-multiprocessing-pool-with-sigkill-instead-of-sigterm-i-think
        os.kill(p.pid, signal.SIGKILL)
        p.terminate()

    def _check_data(self):
        ClickHouseCollapseTestModel.sync_batch_from_storage()

        ch_data = list(connections['default'].select('SELECT * FROM $table FINAL ORDER BY id',
                                                     model_class=ClickHouseCollapseTestModel))
        pg_data = list(TestModel.objects.all().order_by('id'))

        self.assertEqual(len(pg_data), len(ch_data))
        serizlier = ClickHouseCollapseTestModel.get_django_model_serializer()
        self.assertListEqual(ch_data, [serizlier.serialize(item) for item in pg_data])

    def test_kills(self):
        p_create = Process(target=self._create_process, kwargs={'test_time': 5})
        p_update = Process(target=self._update_process, kwargs={'test_time': 5})
        p_delete = Process(target=self._delete_process, kwargs={'test_time': 5})
        p_sync = Process(target=self._sync_process)

        self.start = datetime.datetime.now()
        p_create.start()
        p_update.start()
        p_delete.start()
        p_sync.start()

        # while (datetime.datetime.now() - start).total_seconds() < self.TEST_TIME:
        #     self._kill_process(p_sync)
        #     p_sync.start()
        #     sleep(random.randint(0, 5))

        p_create.join()
        p_update.join()
        p_delete.join()
        p_sync.join()

        # self._check_data()
