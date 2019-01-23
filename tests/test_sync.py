import datetime
import logging
from subprocess import Popen
from time import sleep
from unittest import expectedFailure, skip

import os
from django.test import TransactionTestCase
from django.utils.timezone import now
from random import randint

from django_clickhouse.database import connections
from django_clickhouse.migrations import migrate_app
from django_clickhouse.utils import int_ranges
from tests.clickhouse_models import ClickHouseTestModel, ClickHouseCollapseTestModel, ClickHouseMultiTestModel
from tests.models import TestModel

logger = logging.getLogger('django-clickhouse')


class SyncTest(TransactionTestCase):
    def setUp(self):
        self.db = connections['default']
        self.db.drop_database()
        self.db.create_database()
        migrate_app('tests', 'default')
        ClickHouseTestModel.get_storage().flush()

    def test_simple(self):
        obj = TestModel.objects.create(value=1, created=datetime.datetime.now(), created_date=datetime.date.today())
        ClickHouseTestModel.sync_batch_from_storage()

        synced_data = list(ClickHouseTestModel.objects.all())
        self.assertEqual(1, len(synced_data))
        self.assertEqual(obj.created_date, synced_data[0].created_date)
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

    def test_collapsing_update_by_final(self):
        obj = TestModel.objects.create(value=1, created=datetime.datetime.now(), created_date=datetime.date.today())
        obj.value = 2
        obj.save()
        ClickHouseCollapseTestModel.sync_batch_from_storage()

        # sync_batch_from_storage uses FINAL, so data would be collapsed by now
        synced_data = list(ClickHouseCollapseTestModel.objects.all())
        self.assertEqual(1, len(synced_data))
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

        obj.value = 3
        obj.save()
        ClickHouseCollapseTestModel.sync_batch_from_storage()

        synced_data = list(self.db.select('SELECT * FROM $table FINAL', model_class=ClickHouseCollapseTestModel))
        self.assertGreaterEqual(1, len(synced_data))
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

    def test_collapsing_update_by_version(self):
        ClickHouseCollapseTestModel.engine.version_col = 'version'

        obj = TestModel.objects.create(value=1, created=datetime.datetime.now(), created_date=datetime.date.today())
        obj.value = 2
        obj.save()
        ClickHouseCollapseTestModel.sync_batch_from_storage()

        # sync_batch_from_storage uses FINAL, so data would be collapsed by now
        synced_data = list(ClickHouseCollapseTestModel.objects.all())
        self.assertEqual(1, len(synced_data))
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

        obj.value = 3
        obj.save()
        ClickHouseCollapseTestModel.sync_batch_from_storage()

        synced_data = list(self.db.select('SELECT * FROM $table FINAL', model_class=ClickHouseCollapseTestModel))
        self.assertGreaterEqual(1, len(synced_data))
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

        ClickHouseCollapseTestModel.engine.version_col = None

    @expectedFailure
    def test_collapsing_delete(self):
        obj = TestModel.objects.create(value=1, created_date=datetime.date.today())
        ClickHouseCollapseTestModel.sync_batch_from_storage()
        obj.delete()
        ClickHouseCollapseTestModel.sync_batch_from_storage()

        # sync_batch_from_storage uses FINAL, so data would be collapsed by now
        synced_data = list(ClickHouseCollapseTestModel.objects.all())
        self.assertEqual(0, len(synced_data))

    def test_multi_model(self):
        obj = TestModel.objects.create(value=1, created=datetime.datetime.now(), created_date=datetime.date.today())
        obj.value = 2
        obj.save()
        ClickHouseMultiTestModel.sync_batch_from_storage()

        synced_data = list(ClickHouseTestModel.objects.all())
        self.assertEqual(1, len(synced_data))
        self.assertEqual(obj.created_date, synced_data[0].created_date)
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

        # sync_batch_from_storage uses FINAL, so data would be collapsed by now
        synced_data = list(ClickHouseCollapseTestModel.objects.all())
        self.assertEqual(1, len(synced_data))
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)

        obj.value = 3
        obj.save()
        ClickHouseMultiTestModel.sync_batch_from_storage()

        synced_data = list(self.db.select('SELECT * FROM $table FINAL', model_class=ClickHouseCollapseTestModel))
        self.assertGreaterEqual(1, len(synced_data))
        self.assertEqual(obj.value, synced_data[0].value)
        self.assertEqual(obj.id, synced_data[0].id)


class KillTest(TransactionTestCase):
    TEST_TIME = 60
    maxDiff = None

    def setUp(self):
        ClickHouseTestModel.get_storage().flush()
        connections['default'].drop_database()
        connections['default'].create_database()
        migrate_app('tests', 'default')

        # Disable sync for not interesting models
        ClickHouseMultiTestModel.sync_enabled = False
        ClickHouseTestModel.sync_enabled = False

    def tearDown(self):
        # Disable sync for not interesting models
        ClickHouseMultiTestModel.sync_enabled = True
        ClickHouseTestModel.sync_enabled = True

    def _check_data(self):
        logger.debug('django-clickhouse: syncing left test data')

        # Sync all data that is not synced
        # Data is expected to be in test_db, not default. So we need to call subprocess
        # in order everything works correctly
        import_key = ClickHouseCollapseTestModel.get_import_key()
        storage = ClickHouseCollapseTestModel.get_storage()
        sync_left = storage.operations_count(import_key)
        while sync_left:
            logger.debug('django-clickhouse: final sync (%d left)' % sync_left)
            self.sync_iteration(False)
            sync_left = storage.operations_count(import_key)

        ch_data = list(connections['default'].select('SELECT * FROM $table FINAL ORDER BY id',
                                                     model_class=ClickHouseCollapseTestModel))
        pg_data = list(TestModel.objects.all().order_by('id'))

        if len(pg_data) != len(ch_data):
            absent_ids = set(item.id for item in pg_data) - set(item.id for item in ch_data)
            logger.debug('django_clickhouse: absent ranges: %s (min: %d, max: %d)'
                         % (','.join(('(%d, %d)' % r) for r in int_ranges(absent_ids)),
                            min(item.id for item in pg_data), max(item.id for item in pg_data)))

        self.assertEqual(len(pg_data), len(ch_data))
        serializer = ClickHouseCollapseTestModel.get_django_model_serializer()
        self.assertListEqual(ch_data, serializer.serialize_many(pg_data))

    @classmethod
    def sync_iteration(cls, kill=True):
        test_script = os.path.join(os.path.dirname(__file__), 'kill_test_sub_process.py')
        if kill:
            args = ['--test-time', str(cls.TEST_TIME)]
        else:
            args = ['--once', 'true']
        p_sync = Popen(['python3', test_script, 'sync'] + args)

        if kill:
            sleep(randint(0, 5))
            logger.debug('django-clickhouse: test killing: %d' % p_sync.pid)
            p_sync.kill()
        else:
            p_sync.wait()

    def test_kills(self):
        test_script = os.path.join(os.path.dirname(__file__), 'kill_test_sub_process.py')
        p_create = Popen(['python3', test_script, 'create', '--test-time', str(self.TEST_TIME)])

        # Updates must be slower than inserts, or they will do nothing
        p_update = Popen(['python3', test_script, 'update', '--test-time', str(self.TEST_TIME), '--batch-size', '500'])

        start = now()
        while (now() - start).total_seconds() < self.TEST_TIME:
            self.sync_iteration()

        p_create.wait()
        p_update.wait()

        self._check_data()
