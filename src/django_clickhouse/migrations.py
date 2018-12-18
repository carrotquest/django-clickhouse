"""
Migrating database
"""
import datetime
from typing import Optional

from django.db import DEFAULT_DB_ALIAS as DJANGO_DEFAULT_DB_ALIAS
from django.db.models.signals import post_migrate
from django.dispatch import receiver
from infi.clickhouse_orm.migrations import *
from infi.clickhouse_orm.utils import import_submodules

from .configuration import config
from .database import connections, Database
from .utils import lazy_class_import, module_exists


class Migration:
    """
    Base class for migrations
    """
    operations = []

    def apply(self, db_alias, database=None):  # type: (str, Optional[Database]) -> None
        """
        Applies migration to given database
        :param db_alias: Database alias to apply migration to
        :param database: Sometimes I want to pass db object directly for testing purposes
        :return: None
        """
        db_router = lazy_class_import(config.DATABASE_ROUTER)()

        for op in self.operations:
            model_class = getattr(op, 'model_class', None)
            hints = getattr(op, 'hints', {})

            if db_router.allow_migrate(db_alias, self.__module__, model=model_class, **hints):
                database = database or connections[db_alias]
                op.apply(database)


def migrate_app(app_label, db_alias, up_to=9999, database=None):
    # type: (str, str, int, Optional[Database]) -> None
    """
    Migrates given django app
    :param app_label: App label to migrate
    :param db_alias: Database alias to migrate
    :param up_to: Migration number to migrate to
    :param database: Sometimes I want to pass db object directly for testing purposes
    :return: None
    """
    # Can't migrate such connection, just skip it
    if config.DATABASES[db_alias].get('readonly', False):
        return

    migrations_package = "%s.%s" % (app_label, config.MIGRATIONS_PACKAGE)

    if module_exists(migrations_package):
        database = database or connections[db_alias]
        applied_migrations = database._get_applied_migrations(migrations_package)
        modules = import_submodules(migrations_package)

        unapplied_migrations = set(modules.keys()) - applied_migrations

        for name in sorted(unapplied_migrations):
            print('Applying ClickHouse migration %s for app %s in database %s' % (name, app_label, db_alias))
            migration = modules[name].Migration()
            migration.apply(db_alias, database=database)

            database.insert([
                MigrationHistory(package_name=migrations_package, module_name=name, applied=datetime.date.today())
            ])

            if int(name[:4]) >= up_to:
                break


@receiver(post_migrate)
def clickhouse_migrate(sender, **kwargs):
    if not config.MIGRATE_WITH_DEFAULT_DB:
        # If auto migration is enabled
        return

    if kwargs.get('using', DJANGO_DEFAULT_DB_ALIAS) != DJANGO_DEFAULT_DB_ALIAS:
        # Не надо выполнять синхронизацию для каждого шарда. Только один раз.
        return

    app_name = kwargs['app_config'].name

    for db_alias in config.DATABASES:
        migrate_app(app_name, db_alias)
