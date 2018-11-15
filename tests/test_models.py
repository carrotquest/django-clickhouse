import datetime

from django.test import TransactionTestCase

from tests.clickhouse_models import TestClickHouseModel
from tests.models import TestModel


# TestCase can't be used here:
# 1) TestCase creates transaction for inner usage
# 2) I call transaction.on_commit(), expecting no transaction at the moment
# 3) TestCase rollbacks transaction, on_commit not called
class ClickHouseDjangoModelTest(TransactionTestCase):
    fixtures = ['test_model']

    def setUp(self):
        self.storage = TestModel.get_clickhouse_storage()
        self.storage.flush()

    def tearDown(self):
        self.storage.flush()

    def test_save(self):
        # INSERT operation
        instance = TestModel(created_date=datetime.date.today(), value=2)
        instance.save()
        self.assertListEqual([('insert', "default.%d" % instance.pk)],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

        # UPDATE operation
        instance.save()
        self.assertListEqual([('insert', "default.%d" % instance.pk), ('update', "default.%d" % instance.pk)],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

    def test_create(self):
        instance = TestModel.objects.create(pk=100555, created_date=datetime.date.today(), value=2)
        self.assertListEqual([('insert', "default.%d" % instance.pk)],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

    def test_bulk_create(self):
        items = [TestModel(created_date=datetime.date.today(), value=i) for i in range(5)]
        items = TestModel.objects.bulk_create(items)
        self.assertEqual(5, len(items))
        self.assertListEqual([('insert', "default.%d" % instance.pk) for instance in items],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

    def test_get_or_create(self):
        instance, created = TestModel.objects. \
            get_or_create(pk=100, defaults={'created_date': datetime.date.today(), 'value': 2})

        self.assertTrue(created)
        self.assertListEqual([('insert', "default.%d" % instance.pk)],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

        instance, created = TestModel.objects. \
            get_or_create(pk=100, defaults={'created_date': datetime.date.today(), 'value': 2})

        self.assertFalse(created)
        self.assertListEqual([('insert', "default.%d" % instance.pk)],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

    def test_update_or_create(self):
        instance, created = TestModel.objects. \
            update_or_create(pk=100, defaults={'created_date': datetime.date.today(), 'value': 2})
        self.assertTrue(created)
        self.assertListEqual([('insert', "default.%d" % instance.pk)],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

        instance, created = TestModel.objects. \
            update_or_create(pk=100, defaults={'created_date': datetime.date.today(), 'value': 2})

        self.assertFalse(created)
        self.assertListEqual([('insert', "default.%d" % instance.pk), ('update', "default.%d" % instance.pk)],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

    def test_qs_update(self):
        TestModel.objects.filter(pk=1).update(created_date=datetime.date.today())
        self.assertListEqual([('update', 'default.1')], self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

        # Update, after which updated element will not suit update conditions
        TestModel.objects.filter(created_date__lt=datetime.date.today()). \
            update(created_date=datetime.date.today())
        self.assertListEqual([('update', 'default.1'), ('update', 'default.2')],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

    def test_qs_update_returning(self):
        TestModel.objects.filter(pk=1).update_returning(created_date=datetime.date.today())
        self.assertListEqual([('update', 'default.1')], self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

        # Update, after which updated element will not suit update conditions
        TestModel.objects.filter(created_date__lt=datetime.date.today()). \
            update_returning(created_date=datetime.date.today())
        self.assertListEqual([('update', 'default.1'), ('update', 'default.2')],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

    def test_qs_delete_returning(self):
        TestModel.objects.filter(pk=1).delete_returning()
        self.assertListEqual([('delete', 'default.1')], self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

        # Update, после которого исходный фильтр уже не сработает
        TestModel.objects.filter(created_date__lt=datetime.date.today()).delete_returning()
        self.assertListEqual([('delete', 'default.1'), ('delete', 'default.2')],
                             self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

    def test_delete(self):
        instance = TestModel.objects.get(pk=1)
        instance.delete()
        self.assertListEqual([('delete', 'default.1')], self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))

    def test_qs_delete(self):
        TestModel.objects.filter(pk=1).delete()
        self.assertListEqual([('delete', 'default.1')], self.storage.get_operations(TestClickHouseModel.get_import_key(), 10))
