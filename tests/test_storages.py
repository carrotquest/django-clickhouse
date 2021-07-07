import datetime

from django.test import TestCase

from django_clickhouse.exceptions import RedisLockTimeoutError
from django_clickhouse.storages import RedisStorage
from tests.clickhouse_models import ClickHouseTestModel, ClickHouseCollapseTestModel


class StorageTest(TestCase):
    storage = RedisStorage()

    def setUp(self):
        self.storage.flush()

    def tearDown(self):
        self.storage.flush()

    def test_operation_pks(self):
        self.storage.register_operations_wrapped('test', 'insert', 100500)
        self.storage.register_operations_wrapped('test', 'insert', 100501)
        self.storage.register_operations_wrapped('test', 'insert', 100502)
        self.assertListEqual([
            ('insert', '100500'),
            ('insert', '100501'),
            ('insert', '100502'),
        ], self.storage.get_operations('test', 10))

    def test_operation_types(self):
        self.storage.register_operations_wrapped('test', 'insert', 100500)
        self.storage.register_operations_wrapped('test', 'update', 100500)
        self.storage.register_operations_wrapped('test', 'delete', 100500)
        self.assertListEqual([
            ('insert', '100500'),
            ('update', '100500'),
            ('delete', '100500'),
        ], self.storage.get_operations('test', 10))

    def test_operation_import_keys(self):
        self.storage.register_operations_wrapped('test1', 'insert', 100500)
        self.storage.register_operations_wrapped('test2', 'insert', 100500)
        self.storage.register_operations_wrapped('test2', 'insert', 100501)
        self.assertListEqual([
            ('insert', '100500')
        ], self.storage.get_operations('test1', 10))
        self.assertListEqual([
            ('insert', '100500'),
            ('insert', '100501'),
        ], self.storage.get_operations('test2', 10))

    def test_post_sync(self):
        self.storage.pre_sync('test')
        self.storage.register_operations_wrapped('test', 'insert', 100500)
        self.storage.register_operations_wrapped('test', 'insert', 100501)
        self.storage.get_operations('test', 10)
        self.storage.register_operations_wrapped('test', 'insert', 100502)

        self.storage.post_sync('test')
        self.assertListEqual([
            ('insert', '100502')
        ], self.storage.get_operations('test', 10))

    def test_last_sync(self):
        dt = datetime.datetime.now()
        self.storage.set_last_sync_time('test', dt)
        self.assertEqual(dt, self.storage.get_last_sync_time('test'))

    def test_operations_count(self):
        self.storage.register_operations_wrapped('test', 'insert', 100500)
        self.storage.register_operations_wrapped('test', 'insert', 100501)
        self.assertEqual(2, self.storage.operations_count('test'))
        self.storage.register_operations_wrapped('test', 'insert', 100502)
        self.assertEqual(3, self.storage.operations_count('test'))

    def test_locks(self):
        # Test that multiple can acquire locks in parallel
        # And single model can't
        lock = self.storage.get_lock(ClickHouseTestModel.get_import_key())
        lock.acquire()
        with self.assertRaises(RedisLockTimeoutError):
            lock.acquire()

        lock_2 = self.storage.get_lock(ClickHouseCollapseTestModel.get_import_key())
        lock_2.acquire()
