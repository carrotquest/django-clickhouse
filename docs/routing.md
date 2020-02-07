# Database routing
One of this libraries goals was to create easy and extendable automatic database routing.

## Motivation
In original [infi.clickhouse-orm](https://github.com/Infinidat/infi.clickhouse_orm) 
 you had to explicitly create [Database](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/models_and_databases.md#inserting-to-the-database) objects
 and set database to each query with `objects_in(db)` method.
 But common projects use a quite little number of database connections.
 As a result, it's easier to setup routing once and use it as [django](https://docs.djangoproject.com/en/2.2/topics/db/multi-db/) does.  
Unlike traditional relational databases, [ClickHouse](https://clickhouse.yandex/docs/en/)
 has per table replication.
 This means that:
 1) Each model can have it's own replication scheme
 2) Some migration queries are replicated automatically, others - not.
 3) To make system more extendable we need default routing, per model routing and router class for complex cases.
 
## Introduction
All database connections are defined in [CLICKHOUSE_DATABASES](configuration.md#clickhouse_databases) setting.
 Each connection has it's alias name to refer with.
 If no routing is configured, [CLICKHOUSE_DEFAULT_DB_ALIAS](configuration.md#clickhouse_default_db_alias) is used.
 
## Router
Router is a class, defining 3 methods:
* `def db_for_read(self, model: ClickHouseModel, **hints) -> str`  
  Returns `database alias` to use for given `model` for `SELECT` queries.
* `def db_for_write(self, model: ClickHouseModel, **hints) -> str`  
  Returns `database alias` to use for given `model` for `INSERT` queries.
* `def allow_migrate(self, db_alias: str, app_label: str, operation: Operation, model: Optional[ClickHouseModel] = None, **hints: dict) -> bool`
  Checks if migration `operation` should be applied in django application `app_label` on database `db_alias`.
  Optional `model` field can be used to determine migrations on concrete model.

By default [CLICKHOUSE_DATABASE_ROUTER](configuration.md#clickhouse_database_router) is used.
 It gets routing information from model fields, described below.  
 
## ClickHouseModel routing attributes
Default database router reads routing settings from model attributes.
```python
from django_clickhouse.configuration import config
from django_clickhouse.clickhouse_models import ClickHouseModel

class MyModel(ClickHouseModel):
    # Servers, model is replicated to. 
    # Router takes random database to read or write from.
    read_db_aliases = (config.DEFAULT_DB_ALIAS,)
    write_db_aliases = (config.DEFAULT_DB_ALIAS,)
    
    # Databases to perform replicated migration queries, such as ALTER TABLE.
    # Migration is applied to random database from the list.
    migrate_replicated_db_aliases = (config.DEFAULT_DB_ALIAS,)
    
    # Databases to perform non-replicated migrations (CREATE TABLE, DROP TABLE).
    # Migration is applied to all databases from the list.
    migrate_non_replicated_db_aliases = (config.DEFAULT_DB_ALIAS,)
 ```

## Settings database in QuerySet
Database can be set in each [QuerySet](queries.md) explicitly by using one of methods:
* With [infi approach](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/querysets.md#querysets): `MyModel.objects_in(db_object).filter(id__in=[1,2,3]).count()`
* With `using()` method: `MyModel.objects.filter(id__in=[1,2,3]).using(db_alias).count()`

If no explicit database is provided, database connection to use is determined lazily with router's `db_for_read` or `db_for_write`
 method, depending on query type.  