# Databases
Direct usage of `Database` objects is not expected in this library. But in some cases, you may still need them.
This section describes `Database` objects and there usage.

`django_clickhouse.database.Database` is a class, describing a ClickHouse database connection.

## Getting database objects
To get a `Database` object by its alias name in [CLICKHOUSE_DATABASES](configuration.md#clickhouse_databases) 
 use `django_clickhouse.database.connections` object. 
This object is a `django_clickhouse.database.ConnectionProxy` instance:
 it creates `Database` objects when they are used for the first time and stores them in memory.
 
Example:
```python
from django_clickhouse.database import connections

# Database objects are inited on first call
db = connections['default']
secondary = connections['secondary']

# Already inited - object is returned from memory 
db_link = connections['default']
```

You can also get database objects from [QuerySet](queries.md) and [ClickHouseModel](models.md) instances by calling `get_database(for_write: bool = False)` method.
This database may differ, depending on [routing](routing.md#router) you use.

## Database object
Database class is based on [infi.clickhouse_orm Database object](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/models_and_databases.md#models-and-databases),
but extends it with some extra attributes and methods:

### Database migrations are restricted
I expect this library [migration system](migrations.md) to be used.
Direct database migration will lead to migration information errors.

### `insert_tuples` and `select_tuples` methods
Methods to work with [ClickHouseModel namedtuples](models.md#clickhousemodel-namedtuple-form).
