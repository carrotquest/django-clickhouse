from django.test import SimpleTestCase

from django_clickhouse.migrations import RunSQL, CreateTable
from django_clickhouse.routers import DefaultRouter
from tests.clickhouse_models import ClickHouseTestModel


class DefaultRouterAllowMigrateTest(SimpleTestCase):
    def setUp(self):
        self.router = DefaultRouter()
        self.operation = RunSQL('SELECT 1')

    def test_hints_model_class(self):
        hints = {'model': ClickHouseTestModel}

        with self.subTest('Allow migrate'):
            res = self.router.allow_migrate('default', 'tests', self.operation, **hints)
            self.assertTrue(res)

        with self.subTest('Reject migrate'):
            res = self.router.allow_migrate('other', 'tests', self.operation, **hints)
            self.assertFalse(res)

    def test_hints_model_name(self):
        hints = {'model': 'ClickHouseTestModel'}

        with self.subTest('Allow migrate'):
            res = self.router.allow_migrate('default', 'tests', self.operation, **hints)
            self.assertTrue(res)

        with self.subTest('Reject migrate'):
            res = self.router.allow_migrate('other', 'tests', self.operation, **hints)
            self.assertFalse(res)

    def test_hints_force_migrate_on_databases(self):
        hints = {'force_migrate_on_databases': ['secondary']}

        with self.subTest('Allow migrate'):
            res = self.router.allow_migrate('secondary', 'apps', self.operation, **hints)
            self.assertTrue(res)

        with self.subTest('Reject migrate'):
            res = self.router.allow_migrate('default', 'apps', self.operation, **hints)
            self.assertFalse(res)

    def test_model_operation(self):
        with self.subTest('Allow migrate'):
            operation = CreateTable(ClickHouseTestModel)
            res = self.router.allow_migrate('default', 'apps', operation)
            self.assertTrue(res)

        with self.subTest('Reject migrate'):
            operation = CreateTable(ClickHouseTestModel)
            res = self.router.allow_migrate('other', 'apps', operation)
            self.assertFalse(res)

    def test_no_model(self):
        with self.assertRaises(ValueError):
            self.router.allow_migrate('default', 'apps', self.operation)
