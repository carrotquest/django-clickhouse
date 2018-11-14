from django.test import TestCase

from django_clickhouse.storage import RedisStorage


class StorageTest(TestCase):
    storage = RedisStorage()

    def setUp(self):
        # Clean storage
        redis = self.storage._redis

        keys = redis.keys('clickhouse_sync*')
        if keys:
            redis.delete(*keys)

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

    def test_import_batch(self):
        self.storage.write_import_batch('test', [str(i) for i in range(10)])
        self.assertTupleEqual(tuple(str(i) for i in range(10)), self.storage.get_import_batch('test'))

    def test_post_sync(self):
        self.storage.register_operations_wrapped('test', 'insert', 100500)
        self.storage.register_operations_wrapped('test', 'insert', 100501)
        self.storage.get_operations('test', 10)
        self.storage.write_import_batch('test', [str(i) for i in range(10)])
        self.storage.register_operations_wrapped('test', 'insert', 100502)

        self.storage.post_sync('test')
        self.assertListEqual([
            ('insert', '100502')
        ], self.storage.get_operations('test', 10))
        self.assertTupleEqual(tuple(), self.storage.get_import_batch('test'))
