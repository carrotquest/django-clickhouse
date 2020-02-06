# Migrations
Migration system allows to make migrate ClickHouse table schema based on `ClickHouseModel`.
Library migrations are based on [infi.clickhouse_orm migration system](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/schema_migrations.md),
but makes it a little bit more django-like.

## File structure
Each django app can have optional `clickhouse_migrations` package.
 This is a default package name, it can be changed with [CLICKHOUSE_MIGRATIONS_PACKAGE](configuration.md#clickhouse_migrations_package) setting.

Package contains py files, starting with 4-digit number. 
A number gives an order in which migrations will be applied.

Example:
```
my_app
>> clickhouse_migrations
>>>> __init__.py
>>>> 0001_initial.py
>>>> 0002_add_new_field_to_my_model.py
>> clickhouse_models.py
>> urls.py
>> views.py
```

## Migration files
Each file must contain a `Migration` class, inherited from `django_clickhouse.migrations.Migration`.
The class should define an `operations` attribute - a list of operations to apply one by one.
Operation is one of [operations, supported by infi.clickhouse-orm](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/schema_migrations.md).

```python
from django_clickhouse import migrations
from my_app.clickhouse_models import ClickHouseUser

class Migration(migrations.Migration):
    operations = [
        migrations.CreateTable(ClickHouseUser)
    ]
```

## MigrationHistory ClickHouseModel
This model stores information about applied migrations.  
By default, library uses `django_clickhouse.migrations.MigrationHistory` model,
 but this can be changed using `CLICKHOUSE_MIGRATION_HISTORY_MODEL` setting.
For instance, if you want to make it replicated, you have to redeclare tables engine.
 
MigrationHistory model is stored in default database.  


## Automatic migrations
When library is installed, it tries applying migrations every time,
you call [django migrate](https://docs.djangoproject.com/en/3.0/ref/django-admin/#django-admin-migrate). If you want to disable this, use [CLICKHOUSE_MIGRATE_WITH_DEFAULT_DB](configuration.md#clickhouse_migrate_with_default_db) setting.
  
By default migrations are applied to all [CLICKHOUSE_DATABASES](configuration.md#clickhouse_databases), which have no flags:
* `'migrate': False`
* `'readonly': True`

Note: migrations are only applied, with django `default` database.  
So if you call `python manage.py migrate --database=secondary` they wouldn't be applied.

## Migration algorithm
- Get a list of databases from `CLICKHOUSE_DATABASES` setting. Migrate them one by one.  
  - Find all django apps from `INSTALLED_APPS` setting, which have no `readonly=True` attribute and have `migrate=True` attribute. Migrate them one by one.  
    * Iterate over `INSTAALLED_APPS`, searching for [clickhouse_migrations package](#file-structure)  
    * If package was not found, skip app.  
    * Get a list of migrations applied from [MigrationHistory model](#migrationhistory-clickhousemodel)   
    * Get a list of unapplied migrations
    * Get [Migration class](#migration-files) from each migration and call it `apply()` method
    * `apply()` iterates operations, checking if it should be applied with [router](routing.md)
    * If migration should be applied, it is applied
    * Mark migration as applied in [MigrationHistory model](#migrationhistory-clickhousemodel)

## Security notes
1) ClickHouse has no transaction system, as django relational databases. 
  As a result, if migration fails, it would be partially applied and there's no correct way to rollback.
  I recommend to make migrations as small as possible, so it should be easier to determine and correct the result if something goes wrong.
2) Unlike django, this library is enable to unapply migrations. 
  This functionality may be implemented in the future.
