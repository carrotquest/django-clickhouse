from django.test import TestCase, override_settings
from django_clickhouse.migrations import MigrationHistory

from django_clickhouse.database import connections
from django_clickhouse.migrations import migrate_app
from django_clickhouse.routers import DefaultRouter
from tests.clickhouse_models import ClickHouseTestModel


class NoMigrateRouter(DefaultRouter):
    def allow_migrate(self, db_alias, app_label, operation, model=None, **hints):
        return False


def table_exists(db, model_class):
    res = db.select(
        "SELECT * FROM system.tables WHERE `database`='%s' AND `name`='%s'"
        % (db.db_name, model_class.table_name())
    )
    res = list(res)
    return bool(res)


@override_settings(CLICKHOUSE_MIGRATE_WITH_DEFAULT_DB=False)
class MigrateAppTest(TestCase):
    def setUp(self):
        self.db = connections['default']

        # Clean all database data
        self.db.drop_database()
        self.db.db_exists = False
        self.db.create_database()

    def test_migrate_app(self):
        migrate_app('tests', 'default')
        self.assertTrue(table_exists(self.db, ClickHouseTestModel))

        self.assertEqual(1, self.db.count(MigrationHistory))

        # Migrations are already applied no actions should be done
        migrate_app('tests', 'default')
        self.assertEqual(1, self.db.count(MigrationHistory))

    @override_settings(CLICKHOUSE_DATABASE_ROUTER=NoMigrateRouter)
    def test_router_not_allowed(self):
        migrate_app('tests', 'default')
        self.assertFalse(table_exists(self.db, ClickHouseTestModel))

    def test_no_migrate_connections(self):
        migrate_app('tests', 'no_migrate')
        self.assertFalse(table_exists(connections['no_migrate'], ClickHouseTestModel))

    def test_readonly_connections(self):
        migrate_app('tests', 'readonly')
        self.assertFalse(table_exists(connections['readonly'], ClickHouseTestModel))
