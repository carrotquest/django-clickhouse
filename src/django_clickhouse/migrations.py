"""
Migrating database
"""
import datetime

from django.db.models.signals import post_migrate
from django.dispatch import receiver
from django.db import DEFAULT_DB_ALIAS as DJANGO_DEFAULT_DB_ALIAS

from infi.clickhouse_orm.migrations import *
from infi.clickhouse_orm.utils import import_submodules

from django_clickhouse.utils import lazy_class_import, module_exists
from .configuration import config
from .database import connections


class Migration:
    """
    Base class for migrations
    """
    operations = []

    def apply(self, db_alias):  # type: (str) -> None
        """
        Applies migration to given database
        :param db_alias: Database alias to apply migration to
        :return: None
        """
        db_router = lazy_class_import(config.DATABASE_ROUTER)()

        for op in self.operations:
            model_class = getattr(op, 'model_class', None)
            hints = getattr(op, 'hints', {})

            if db_router.allow_migrate(db_alias, self.__module__, model=model_class, **hints):
                op.apply(connections[db_alias])


def migrate_app(app_label, db_alias, up_to=9999):
    # type: (str, str, int) -> None
    """
    Migrates given django app
    :param app_label: App label to migrate
    :param db_alias: Database alias to migrate
    :param up_to: Migration number to migrate to
    :return: None
    """
    db = connections[db_alias]
    migrations_package = "%s.%s" % (app_label, config.MIGRATIONS_PACKAGE)

    if module_exists(migrations_package):
        applied_migrations = db._get_applied_migrations(migrations_package)
        modules = import_submodules(migrations_package)

        unapplied_migrations = set(modules.keys()) - applied_migrations

        for name in sorted(unapplied_migrations):
            migration = modules[name].Migration()
            migration.apply(db_alias)

            db.insert([
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
