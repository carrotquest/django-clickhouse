# Configuration

Library configuration is made in settings.py. All parameters start with `CLICKHOUSE_` prefix.
Prefix can be changed using `CLICKHOUSE_SETTINGS_PREFIX` parameter.

### <a name="databases">CLICKHOUSE_SETTINGS_PREFIX</a>
Defaults to: `'CLICKHOUSE_'`  
You can change `CLICKHOUSE_` prefix in settings using this parameter to anything your like.

### <a name="databases">CLICKHOUSE_DATABASES</a>
Defaults to: `{}`  
A dictionary, defining databases in django-like style.  
<!--- TODO Add link  --->
Key is an alias to communicate with this database in [connections]() and [using]().  
Value is a configuration dict with parameters:
* [infi.clickhouse_orm database parameters](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/class_reference.md#database)
<!--- TODO Add link  --->
* `migrate: bool` - indicates if this database should be migrated. See [migrations]().  

Example:
```python
CLICKHOUSE_DATABASES = {
    'default': {
        'db_name': 'test',
        'username': 'default',
        'password': ''
    }
}
```

### <a name="default_db_alias">CLICKHOUSE_DEFAULT_DB_ALIAS</a>
Defaults to: `'default'`  
<!--- TODO Add link  --->
A database alias to use in [QuerySets]() if direct [using]() is not specified.

### <a name="sync_storage">CLICKHOUSE_SYNC_STORAGE</a>
Defaults to: `'django_clickhouse.storages.RedisStorage'`  
An intermediate storage class to use. Can be a string or class. [More info about storages](storages.md).

### <a name="redis_config">CLICKHOUSE_REDIS_CONFIG</a>
Default to: `None`  
Redis configuration for [RedisStorage](storages.md#redis_storage).  
If given, should be a dictionary of parameters to pass to [redis-py](https://redis-py.readthedocs.io/en/latest/#redis.Redis).    

Example:  
```python
CLICKHOUSE_REDIS_CONFIG = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 8,
    'socket_timeout': 10
}
```

### <a name="sync_batch_size">CLICKHOUSE_SYNC_BATCH_SIZE</a>
Defaults to: `10000`  
Maximum number of operations, fetched by sync process from intermediate storage per sync round.

### <a name="sync_delay">CLICKHOUSE_SYNC_DELAY</a>
Defaults to: `5`
A delay in seconds between two sync rounds start.

### <a name="models_module">CLICKHOUSE_MODELS_MODULE</a>
Defaults to: `'clickhouse_models'`  
<!--- TODO Add link  --->
Module name inside [django app](https://docs.djangoproject.com/en/2.2/intro/tutorial01/), 
where [ClickHouseModel]() classes are search during migrations.

### <a name="database_router">CLICKHOUSE_DATABASE_ROUTER</a>
Defaults to: `'django_clickhouse.routers.DefaultRouter'`  
<!--- TODO Add link  --->
A dotted path to class, representing [database router]().

### <a name="migrations_package">CLICKHOUSE_MIGRATIONS_PACKAGE</a>
Defaults to: `'clickhouse_migrations'`
A python package name inside [django app](https://docs.djangoproject.com/en/2.2/intro/tutorial01/), 
where migration files are searched.

### <a name="migration_history_model">CLICKHOUSE_MIGRATION_HISTORY_MODEL</a>
Defaults to: `'django_clickhouse.migrations.MigrationHistory'`  
<!--- TODO Add link  --->
A dotted name of a ClickHouseModel subclass (including module path), representing [MigrationHistory]() model.

### <a name="migrate_with_default_db">CLICKHOUSE_MIGRATE_WITH_DEFAULT_DB</a>
Defaults to: `True`  
A boolean flag enabling automatic ClickHouse migration, 
when you call [`migrate`](https://docs.djangoproject.com/en/2.2/ref/django-admin/#django-admin-migrate) on default database.

### <a name="statd_prefix">CLICKHOUSE_STATSD_PREFIX</a>
Defaults to: `clickhouse`  
<!--- TODO Add link  --->
A prefix in [statsd](https://pythonhosted.org/python-statsd/) added to each library metric. See [metrics]()

### <a name="celery_queue">CLICKHOUSE_CELERY_QUEUE</a>
Defaults to: `'celery'`  
A name of a queue, used by celery to plan library sync tasks.
