import datetime
from unittest import skipIf

import django
from django.test import TransactionTestCase
from django.utils.timezone import now

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

    databases = ['default', 'secondary']
    db_alias = 'default'
    multi_db = True

    def setUp(self):
        self.storage = self.django_model.get_clickhouse_storage()
        self.storage.flush()
        self.before_op_items = list(self.django_model.objects.all())

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
        self.assertSetEqual({('insert', "%s.%d" % (self.db_alias, instance.pk)) for instance in items},
                            set(self.storage.get_operations(self.clickhouse_model.get_import_key(), 10)))

    @skipIf(django.VERSION < (2, 2), "bulk_update method has been introduced in django 2.2")
    def test_native_bulk_update(self):
        items = list(self.django_model.objects.filter(pk__in={1, 2}))
        for instance in items:
            instance.value = instance.pk * 10

        self.django_model.native_objects.bulk_update(items, ['value'])

        items = list(self.django_model.objects.filter(pk__in={1, 2}))
        self.assertEqual(2, len(items))
        for instance in items:
            self.assertEqual(instance.value, instance.pk * 10)

        self.assertSetEqual({('update', "%s.%d" % (self.db_alias, instance.pk)) for instance in items},
                            set(self.storage.get_operations(self.clickhouse_model.get_import_key(), 10)))

    def test_pg_bulk_create(self):
        now_dt = now()
        res = self.django_model.objects.pg_bulk_create([
            {'value': i, 'created': now_dt, 'created_date': now_dt.date()}
            for i in range(5)
        ])
        self.assertEqual(5, res)

        items = list(self.django_model.objects.filter(value__lt=100).order_by('value'))
        self.assertEqual(5, len(items))
        for i, instance in enumerate(items):
            self.assertEqual(instance.created, now_dt)
            self.assertEqual(instance.created_date, now_dt.date())
            self.assertEqual(i, instance.value)

        self.assertSetEqual({('insert', "%s.%d" % (self.db_alias, instance.pk)) for instance in items},
                            set(self.storage.get_operations(self.clickhouse_model.get_import_key(), 10)))

    def test_pg_bulk_update(self):
        items = list(self.django_model.objects.filter(pk__in={1, 2}))

        self.django_model.objects.pg_bulk_update([
            {'id': instance.pk, 'value': instance.pk * 10}
            for instance in items
        ])

        items = list(self.django_model.objects.filter(pk__in={1, 2}))
        self.assertEqual(2, len(items))
        for instance in items:
            self.assertEqual(instance.value, instance.pk * 10)

        self.assertSetEqual({('update', "%s.%d" % (self.db_alias, instance.pk)) for instance in items},
                            set(self.storage.get_operations(self.clickhouse_model.get_import_key(), 10)))

    def test_pg_bulk_update_or_create(self):
        items = list(self.django_model.objects.filter(pk__in={1, 2}))

        data = [{
            'id': instance.pk,
            'value': instance.pk * 10,
            'created_date': instance.created_date,
            'created': instance.created
        } for instance in items] + [{'id': 11, 'value': 110, 'created_date': datetime.date.today(), 'created': now()}]

        self.django_model.objects.pg_bulk_update_or_create(data)

        items = list(self.django_model.objects.filter(pk__in={1, 2, 11}))
        self.assertEqual(3, len(items))
        for instance in items:
            self.assertEqual(instance.value, instance.pk * 10)

        self.assertSetEqual({('update', "%s.%d" % (self.db_alias, instance.pk)) for instance in items},
                            set(self.storage.get_operations(self.clickhouse_model.get_import_key(), 10)))

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

        self.django_model.objects.filter(created_date__lt=datetime.date.today()). \
            update(created_date=datetime.date.today())
        self.assertListEqual([('update', "%s.%d" % (self.db_alias, item.id)) for item in self.before_op_items],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_bulk_create_returning(self):
        items = [
            self.django_model(created_date=datetime.date.today(), created=datetime.datetime.now(), value=i)
            for i in range(5)
        ]
        items = self.django_model.objects.bulk_create_returning(items)
        self.assertEqual(5, len(items))
        self.assertSetEqual({('insert', "%s.%d" % (self.db_alias, instance.pk)) for instance in items},
                            set(self.storage.get_operations(self.clickhouse_model.get_import_key(), 10)))

    def test_qs_update_returning(self):
        self.django_model.objects.filter(pk=1).update_returning(created_date=datetime.date.today())
        self.assertListEqual([('update', "%s.1" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

        # Update, after which updated element will not suit update conditions
        self.django_model.objects.filter(created_date__lt=datetime.date.today()). \
            update_returning(created_date=datetime.date.today())
        self.assertListEqual([('update', "%s.%d" % (self.db_alias, item.id)) for item in self.before_op_items],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_qs_delete_returning(self):
        self.django_model.objects.filter(pk=1).delete_returning()
        self.assertListEqual([('delete', "%s.1" % self.db_alias)],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

        # Delete, after which updated element will not suit update conditions
        self.django_model.objects.filter(created_date__lt=datetime.date.today()).delete_returning()
        self.assertListEqual([('delete', "%s.%d" % (self.db_alias, item.id)) for item in self.before_op_items],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

    def test_save_returning(self):
        # INSERT operation
        instance = self.django_model(created_date=datetime.date.today(), created=datetime.datetime.now(), value=2)
        instance.save_returning()
        self.assertListEqual([('insert', "%s.%d" % (self.db_alias, instance.pk))],
                             self.storage.get_operations(self.clickhouse_model.get_import_key(), 10))

        # UPDATE operation
        instance.save_returning()
        self.assertListEqual([('insert', "%s.%d" % (self.db_alias, instance.pk)),
                              ('update', "%s.%d" % (self.db_alias, instance.pk))],
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
