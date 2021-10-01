"""
Django command that applies migrations for ClickHouse database
"""
import json

from django.apps import apps as django_apps
from django.core.management import BaseCommand, CommandParser

from ...configuration import config
from ...migrations import migrate_app


class Command(BaseCommand):
    help = 'Migrates ClickHouse databases'
    requires_migrations_checks = False

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument('app_label', nargs='?', type=str,
                            help='Django App name to migrate. By default all found apps are migrated.')

        parser.add_argument('migration_number', nargs='?', type=int,
                            help='Migration number in selected django app to migrate to.'
                                 ' By default all available migrations are applied.'
                                 ' Note that library currently have no ability rollback migrations')

        parser.add_argument('--database', '-d', nargs='?', type=str, required=False, choices=config.DATABASES.keys(),
                            help='ClickHouse database alias key from CLICKHOUSE_DATABASES django setting.'
                                 ' By default migrations are applied to all databases.')

    def handle(self, *args, **options) -> None:
        apps = [options['app_label']] if options['app_label'] else [app.name for app in django_apps.get_app_configs()]
        databases = [options['database']] if options['database'] else list(config.DATABASES.keys())
        kwargs = {'up_to': options['migration_number']} if options['migration_number'] else {}

        self.stdout.write(self.style.MIGRATE_HEADING(
            "Applying ClickHouse migrations for apps %s in databases %s" % (json.dumps(apps), json.dumps(databases))))

        any_migrations_applied = False
        for app_label in apps:
            for db_alias in databases:
                res = migrate_app(app_label, db_alias, verbosity=options['verbosity'], **kwargs)
                any_migrations_applied = any_migrations_applied or res

        if not any_migrations_applied:
            self.stdout.write("No migrations to apply")
