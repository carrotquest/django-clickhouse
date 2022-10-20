from typing import List, Dict, Any
from unittest import mock

from django.conf import settings
from django.test import TestCase, override_settings

from django_clickhouse.configuration import config
from django_clickhouse.database import connections
from django_clickhouse.management.commands.clickhouse_migrate import Command
from django_clickhouse.migrations import MigrationHistory, migrate_app
from django_clickhouse.routers import DefaultRouter
from tests.clickhouse_models import ClickHouseTestModel


class NoMigrateRouter(DefaultRouter):
    def allow_migrate(self, db_alias, app_label, operation, **hints):
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


@override_settings(CLICKHOUSE_MIGRATE_WITH_DEFAULT_DB=False)
@mock.patch('django_clickhouse.management.commands.clickhouse_migrate.migrate_app', return_value=True)
class MigrateDjangoCommandTest(TestCase):
    APP_LABELS = ('src', 'tests')

    def setUp(self) -> None:
        self.cmd = Command()

    def test_handle_all(self, migrate_app_mock):
        self.cmd.handle(verbosity=3, app_label=None, database=None, migration_number=None)

        self.assertEqual(len(config.DATABASES.keys()) * len(self.APP_LABELS), migrate_app_mock.call_count)
        for db_alias in config.DATABASES.keys():
            for app_label in self.APP_LABELS:
                migrate_app_mock.assert_any_call(app_label, db_alias, verbosity=3)

    def test_handle_app(self, migrate_app_mock):
        self.cmd.handle(verbosity=3, app_label='tests', database=None, migration_number=None)

        self.assertEqual(len(config.DATABASES.keys()), migrate_app_mock.call_count)
        for db_alias in config.DATABASES.keys():
            migrate_app_mock.assert_any_call('tests', db_alias, verbosity=3)

    def test_handle_database(self, migrate_app_mock):
        self.cmd.handle(verbosity=3, database='default', app_label=None, migration_number=None)

        self.assertEqual(len(settings.INSTALLED_APPS), migrate_app_mock.call_count)
        for app_label in self.APP_LABELS:
            migrate_app_mock.assert_any_call(app_label, 'default', verbosity=3)

    def test_handle_app_and_database(self, migrate_app_mock):
        self.cmd.handle(verbosity=3, app_label='tests', database='default', migration_number=None)

        migrate_app_mock.assert_called_with('tests', 'default', verbosity=3)

    def test_handle_migration_number(self, migrate_app_mock):
        self.cmd.handle(verbosity=3, database='default', app_label='tests', migration_number=1)

        migrate_app_mock.assert_called_with('tests', 'default', up_to=1, verbosity=3)

    def _test_parser_results(self, argv: List[str], expected: Dict[str, Any]) -> None:
        """
        Tests if parser process input correctly.
        Checks only expected parameters, ignores others.
        :param argv: List of string arguments from command line
        :param expected: Dictionary of expected results
        :return: None
        :raises AssertionError: If expected result is incorrect
        """
        parser = self.cmd.create_parser('./manage.py', 'clickhouse_migrate')

        options = parser.parse_args(argv)

        # Copied from django.core.management.base.BaseCommand.run_from_argv('...')
        cmd_options = vars(options)
        cmd_options.pop('args', ())

        self.assertDictEqual(expected, {opt: cmd_options[opt] for opt in expected.keys()})

    def test_parser(self, _):
        with self.subTest('Simple'):
            self._test_parser_results([], {
                'app_label': None,
                'database': None,
                'migration_number': None,
                'verbosity': 1
            })

        with self.subTest('App label'):
            self._test_parser_results(['tests'], {
                'app_label': 'tests',
                'database': None,
                'migration_number': None,
                'verbosity': 1
            })

        with self.subTest('App label and migration number'):
            self._test_parser_results(['tests', '123'], {
                'app_label': 'tests',
                'database': None,
                'migration_number': 123,
                'verbosity': 1
            })

        with self.subTest('Database'):
            self._test_parser_results(['--database', 'default'], {
                'app_label': None,
                'database': 'default',
                'migration_number': None,
                'verbosity': 1
            })

        with self.subTest('Verbosity'):
            self._test_parser_results(['--verbosity', '2'], {
                'app_label': None,
                'database': None,
                'migration_number': None,
                'verbosity': 2
            })
