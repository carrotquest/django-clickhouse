from unittest import TestCase

import datetime

from django_clickhouse.database import connections
from django_clickhouse.migrations import migrate_app
from django_clickhouse.query import QuerySet
from tests.clickhouse_models import ClickHouseTestModel


class TestQuerySet(TestCase):
    def _recreate_db(self, db_alias):
        db = connections[db_alias]
        db.drop_database()
        db.db_exists = False
        db.create_database()

        migrate_app('tests', db_alias)
        return db

    def setUp(self):
        self.db = self._recreate_db('default')
        self._recreate_db('secondary')

    def test_all(self):
        self.db.insert([ClickHouseTestModel(id=i, created_date=datetime.date.today(), value=i) for i in range(1, 4)])
        qs = ClickHouseTestModel.objects.all()

        self.assertIsInstance(qs, QuerySet)
        self.assertEqual(3, qs.count())

    def test_create(self):
        ClickHouseTestModel.objects.create(id=1, created_date=datetime.date.today(), value=2)
        res = list(self.db.select('SELECT * FROM $table', model_class=ClickHouseTestModel))
        self.assertEqual(1, len(res))

        self.assertEqual(1, res[0].id)
        self.assertEqual(datetime.date.today(), res[0].created_date)
        self.assertEqual(2, res[0].value)

    def test_bulk_create(self):
        ClickHouseTestModel.objects.bulk_create([
            ClickHouseTestModel(id=i, created_date=datetime.date.today(), value=i) for i in range(1, 4)
        ])
        res = list(self.db.select('SELECT * FROM $table ORDER BY id', model_class=ClickHouseTestModel))
        self.assertEqual(3, len(res))

        for i in range(0, 3):
            self.assertEqual(i + 1, res[i].id)
            self.assertEqual(datetime.date.today(), res[0].created_date)
            self.assertEqual(i + 1, res[i].value)

    def test_using(self):
        self.db.insert(
            [ClickHouseTestModel(id=i, created_date=datetime.date.today(), value=i) for i in range(1, 4)]
        )
        connections['secondary'].insert([
            ClickHouseTestModel(id=i, created_date=datetime.date.today(), value=i) for i in range(10, 12)
        ])

        self.assertEqual(3, ClickHouseTestModel.objects.count())
        self.assertEqual(3, ClickHouseTestModel.objects_in(self.db).count())
        self.assertEqual(2, ClickHouseTestModel.objects_in(self.db).using('secondary').count())

        self.assertEqual(2, ClickHouseTestModel.objects.using('secondary').count())
        self.assertEqual(3, ClickHouseTestModel.objects.using('default').count())
