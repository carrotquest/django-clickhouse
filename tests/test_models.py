import datetime

from django.test import TransactionTestCase

from tests.clickhouse_models import ClickHouseTestModel, ClickHouseSecondTestModel, ClickHouseCollapseTestModel, \
    ClickHouseMultiTestModel
from tests.models import TestModel, SecondaryTestModel


# TestCase can't be used here:
# 1) TestCase creates transaction for inner usage
# 2) I call transaction.on_commit(), expecting no transaction at the moment
# 3) TestCase rollbacks transaction, on_commit not called
class ClickHouseDjangoModelTest(TransactionTestCase):
    def test_clickhouse_sync_models(self):
        self.assertSetEqual({ClickHouseSecondTestModel}, SecondaryTestModel.get_clickhouse_sync_models())
        self.assertSetEqual({ClickHouseTestModel, ClickHouseCollapseTestModel, ClickHouseMultiTestModel},
                            TestModel.get_clickhouse_sync_models())


class TestOperations(TransactionTestCase):
    fixtures = ['test_model']
    django_model = TestModel
    clickhouse_model = ClickHouseTestModel
    db_alias = 'default'
    multi_db = True

    def setUp(self):
        self.storage = self.django_model.get_clickhouse_storage()
        self.storage.flush()

    def tearDown(self):
        self.storage.flush()

    def test_save(self):
        # INSERT operation
        instance = self.django_model(created_date=datetime.date.today(), created=datetime.datetime.now(), value=2)
        instance.save()
        self.assertListEqual([('insert', "%s.%d" % (self.db_alias, instance.pk))],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

        # UPDATE operation
        instance.save()
        self.assertListEqual([('insert', "%s.%d" % (self.db_alias, instance.pk)),
                              ('update', "%s.%d" % (self.db_alias, instance.pk))],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_create(self):
        instance = self.django_model.objects.create(pk=100555, created_date=datetime.date.today(),
                                                    created=datetime.datetime.now(), value=2)
        self.assertListEqual([('insert', "%s.%d" % (self.db_alias, instance.pk))],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_bulk_create(self):
        items = [self.django_model(created_date=datetime.date.today(), created=datetime.datetime.now(), value=i)
                 for i in range(5)]
        items = self.django_model.objects.bulk_create(items)
        self.assertEqual(5, len(items))
        self.assertListEqual([('insert', "%s.%d" % (self.db_alias, instance.pk)) for instance in items],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_get_or_create(self):
        instance, created = self.django_model.objects. \
            get_or_create(pk=100, defaults={'created_date': datetime.date.today(), 'created': datetime.datetime.now(),
                                            'value': 2})

        self.assertTrue(created)
        self.assertListEqual([('insert', "%s.%d" % (self.db_alias, instance.pk))],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

        instance, created = self.django_model.objects. \
            get_or_create(pk=100, defaults={'created_date': datetime.date.today(), 'value': 2})

        self.assertFalse(created)
        self.assertListEqual([('insert', "%s.%d" % (self.db_alias, instance.pk))],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_update_or_create(self):
        instance, created = self.django_model.objects. \
            update_or_create(pk=100, defaults={'created_date': datetime.date.today(),
                                               'created': datetime.datetime.now(), 'value': 2})
        self.assertTrue(created)
        self.assertListEqual([('insert', "%s.%d" % (self.db_alias, instance.pk))],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

        instance, created = self.django_model.objects. \
            update_or_create(pk=100, defaults={'created_date': datetime.date.today(), 'value': 2})

        self.assertFalse(created)
        self.assertListEqual(
            [('insert', "%s.%d" % (self.db_alias, instance.pk)), ('update', "%s.%d" % (self.db_alias, instance.pk))],
            self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_qs_update(self):
        self.django_model.objects.filter(pk=1).update(created_date=datetime.date.today())
        self.assertListEqual([('update', "%s.1" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

        # Update, after which updated element will not suit update conditions
        self.django_model.objects.filter(created_date__lt=datetime.date.today()). \
            update(created_date=datetime.date.today())
        self.assertListEqual([('update', "%s.1" % self.db_alias), ('update', "%s.2" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_qs_update_returning(self):
        self.django_model.objects.filter(pk=1).update_returning(created_date=datetime.date.today())
        self.assertListEqual([('update', "%s.1" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

        # Update, after which updated element will not suit update conditions
        self.django_model.objects.filter(created_date__lt=datetime.date.today()). \
            update_returning(created_date=datetime.date.today())
        self.assertListEqual([('update', "%s.1" % self.db_alias), ('update', "%s.2" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_qs_delete_returning(self):
        self.django_model.objects.filter(pk=1).delete_returning()
        self.assertListEqual([('delete', "%s.1" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

        # Update, после которого исходный фильтр уже не сработает
        self.django_model.objects.filter(created_date__lt=datetime.date.today()).delete_returning()
        self.assertListEqual([('delete', "%s.1" % self.db_alias), ('delete', "%s.2" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_delete(self):
        instance = self.django_model.objects.get(pk=1)
        instance.delete()
        self.assertListEqual([('delete', "%s.1" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_qs_delete(self):
        self.django_model.objects.filter(pk=1).delete()
        self.assertListEqual([('delete', "%s.1" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))


class TestSecondaryOperations(TestOperations):
    fixtures = ['test_secondary_model']
    django_model = SecondaryTestModel
    clickhouse_model = ClickHouseSecondTestModel
    db_alias = 'secondary'
