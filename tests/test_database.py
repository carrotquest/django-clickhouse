from datetime import date

from django.test import TestCase

from django_clickhouse.migrations import migrate_app
from tests.clickhouse_models import ClickHouseTestModel


class CollapsingMergeTreeTest(TestCase):
    maxDiff = None

    def setUp(self):
        self.db = ClickHouseTestModel.get_database(for_write=True)
        self.db.drop_database()
        self.db.create_database()
        migrate_app('tests', 'default')

    def test_insert_tuples(self):
        tuple_class = ClickHouseTestModel.get_tuple_class()
        data = [
            tuple_class(id=i, created_date=date.today(), value=i, str_field=str(i))
            for i in range(10)
        ]
        self.db.insert_tuples(ClickHouseTestModel, data)

        qs = ClickHouseTestModel.objects.order_by('id').all()
        self.assertListEqual([{
            'id': i,
            'created_date': date.today(),
            'value': i,
            'str_field': str(i)
        } for i in range(10)], [item.to_dict() for item in qs])

    def test_insert_tuples_defaults(self):
        tuple_class = ClickHouseTestModel.get_tuple_class(defaults={'created_date': date.today()})
        data = [
            tuple_class(id=i, str_field=str(i))
            for i in range(10)
        ]
        self.db.insert_tuples(ClickHouseTestModel, data)

        qs = ClickHouseTestModel.objects.order_by('id').all()
        self.assertListEqual([{
            'id': i,
            'created_date': date.today(),
            'value': 100500,
            'str_field': str(i)
        } for i in range(10)], [item.to_dict() for item in qs])

    def test_insert_tuples_batch_size(self):
        tuple_class = ClickHouseTestModel.get_tuple_class()
        data = [
            tuple_class(id=i, created_date=date.today(), value=i, str_field=str(i))
            for i in range(10)
        ]
        self.db.insert_tuples(ClickHouseTestModel, data, batch_size=2)

        qs = ClickHouseTestModel.objects.order_by('id').all()
        self.assertListEqual([{
            'id': i,
            'created_date': date.today(),
            'value': i,
            'str_field': str(i)
        } for i in range(10)], [item.to_dict() for item in qs])

    def test_insert_tuples_special_characters(self):
        tuple_class = ClickHouseTestModel.get_tuple_class()
        data = [tuple_class(id=1, created_date=date.today(), value=1, str_field='\t')]
        self.db.insert_tuples(ClickHouseTestModel, data)

        item = ClickHouseTestModel.objects.filter(id=1)[0]
        self.assertEqual('\t', item.str_field)

        data = [tuple_class(id=2, created_date=date.today(), value=2, str_field='\n')]
        self.db.insert_tuples(ClickHouseTestModel, data)

        item = ClickHouseTestModel.objects.filter(id=2)[0]
        self.assertEqual('\n', item.str_field)

    def test_select_tuples(self):
        ClickHouseTestModel.objects.bulk_create([
            ClickHouseTestModel(id=i, created_date=date.today(), value=i, str_field=str(i))
            for i in range(10)
        ])

        res = self.db.select_tuples('SELECT * FROM $table ORDER BY id', ClickHouseTestModel)
        tuple_class = ClickHouseTestModel.get_tuple_class()
        self.assertListEqual([
            tuple_class(id=i, created_date=date.today(), value=i, str_field=str(i))
            for i in range(10)
        ], list(res))
