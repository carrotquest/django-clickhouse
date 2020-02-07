"""
This file is a wrapper for django settings,
which searches for library properties and sets defaults
"""

from django.conf import settings
from typing import Any

# Prefix of all library parameters
PREFIX = getattr(settings, 'CLICKHOUSE_SETTINGS_PREFIX', 'CLICKHOUSE_')

# Default values for all library parameters
DEFAULTS = {
    'DATABASES': {},
    'SYNC_BATCH_SIZE': 10000,
    'SYNC_STORAGE': 'django_clickhouse.storages.RedisStorage',
    'SYNC_DELAY': 5,
    'REDIS_CONFIG': None,
    'MODELS_MODULE': 'clickhouse_models',
    'DATABASE_ROUTER': 'django_clickhouse.routers.DefaultRouter',
    'STATSD_PREFIX': 'clickhouse',
    'MIGRATIONS_PACKAGE': 'clickhouse_migrations',
    'MIGRATION_HISTORY_MODEL': 'django_clickhouse.migrations.MigrationHistory',
    'MIGRATE_WITH_DEFAULT_DB': True,
    'CELERY_QUEUE': 'celery',
    'DEFAULT_DB_ALIAS': 'default'
}


class Config:
    def __getattr__(self, item: str) -> Any:
        if item not in DEFAULTS:
            raise AttributeError('Unknown config parameter `%s`' % item)

        name = PREFIX + item
        return getattr(settings, name, DEFAULTS[item])


config = Config()
