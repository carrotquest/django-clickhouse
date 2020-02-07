# Configuration

Library configuration is made in settings.py. All parameters start with `CLICKHOUSE_` prefix.
Prefix can be changed using `CLICKHOUSE_SETTINGS_PREFIX` parameter.

### CLICKHOUSE_SETTINGS_PREFIX
Defaults to: `'CLICKHOUSE_'`  
You can change `CLICKHOUSE_` prefix in settings using this parameter to anything your like.

### CLICKHOUSE_DATABASES
Defaults to: `{}`  
A dictionary, defining databases in django-like style.  
Key is an alias to communicate with this database in [connections](databases.md#getting-database-objects) and [using](routing.md#settings-database-in-queryset).  
Value is a configuration dict with parameters:
* [infi.clickhouse_orm database parameters](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/class_reference.md#database)
* `migrate: bool` - indicates if this database should be migrated. See [migrations](migrations.md).  

Example:
```python
CLICKHOUSE_DATABASES = {
    'default': {
        'db_name': 'test',
        'username': 'default',
        'password': ''
    },
    'reader': {
        'db_name': 'read_only',
        'username': 'reader',
        'readonly': True,
        'password': ''
    }   
}
```

### CLICKHOUSE_DEFAULT_DB_ALIAS
Defaults to: `'default'`  
A database alias to use in [QuerySets](queries.md) if direct [using](routing.md#settings-database-in-queryset) is not specified.

### CLICKHOUSE_SYNC_STORAGE
Defaults to: `'django_clickhouse.storages.RedisStorage'`  
An [intermediate storage](storages.md) class to use. Can be a string or class.

### CLICKHOUSE_REDIS_CONFIG
Default to: `None`  
Redis configuration for [RedisStorage](storages.md#redisstorage).  
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

### CLICKHOUSE_SYNC_BATCH_SIZE
Defaults to: `10000`  
Maximum number of operations, fetched by sync process from [intermediate storage](storages.md) per [sync](sync.md)) round.

### CLICKHOUSE_SYNC_DELAY
Defaults to: `5`
A delay in seconds between two [sync](synchronization.md) rounds start.

### CLICKHOUSE_MODELS_MODULE
Defaults to: `'clickhouse_models'`  
Module name inside [django app](https://docs.djangoproject.com/en/3.0/intro/tutorial01/), 
where [ClickHouseModel](models.md#clickhousemodel) classes are search during migrations.

### CLICKHOUSE_DATABASE_ROUTER
Defaults to: `'django_clickhouse.routers.DefaultRouter'`  
A dotted path to class, representing [database router](routing.md#router).

### CLICKHOUSE_MIGRATIONS_PACKAGE
Defaults to: `'clickhouse_migrations'`
A python package name inside [django app](https://docs.djangoproject.com/en/3.0/intro/tutorial01/), 
where migration files are searched.

### CLICKHOUSE_MIGRATION_HISTORY_MODEL
Defaults to: `'django_clickhouse.migrations.MigrationHistory'`  
A dotted name of a ClickHouseModel subclass (including module path),
 representing [MigrationHistory model](migrations.md#migrationhistory-clickhousemodel).

### CLICKHOUSE_MIGRATE_WITH_DEFAULT_DB
Defaults to: `True`  
A boolean flag enabling automatic ClickHouse migration, 
when you call [`migrate`](https://docs.djangoproject.com/en/2.2/ref/django-admin/#django-admin-migrate) on `default` database.

### CLICKHOUSE_STATSD_PREFIX
Defaults to: `clickhouse`  
A prefix in [statsd](https://pythonhosted.org/python-statsd/) added to each library metric. See [monitoring](monitoring.md).

### CLICKHOUSE_CELERY_QUEUE
Defaults to: `'celery'`  
A name of a queue, used by celery to plan library sync tasks.
