# Making queries

QuerySet system used by this library looks very similar to django, but it is implemented separately.
You can read reasons for this design [here](motivation.md#separate-from-django-database-setting-queryset-and-migration-system).

## Usage
Library query system extends [infi.clickhouse-orm QuerySet system](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/querysets.md) and supports all it features.  
In most cases you have no need to create querysets explicitly - just use `objects` attribute or `objects_in(db)` method of `ClickHouseModel`.  
At the same time `django-clickhouse` adds some extra features to `QuerySet` and `AggregateQuerySet`. 
They are available if your model inherits `django_clickhouse.clickhouse_models.ClickHouseModel`.

## Extra features
### Django-like routing system
There's no need to set database object explicitly with `objects_in(...)` method, as original QuerySet expects.
Database is determined based on library configuration and [router](routing.md#router) used.

If you want to set database explicitly you can use any of approaches:
* [infi approach](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/querysets.md#querysets)
* Django like `QuerySet.using(db_alias)` method

Example:  
```python
from django_clickhouse.database import connections
from my_app.clickhouse_models import ClickHouseUser

# This query will choose database using current router.
# By default django_clickhouse.routers.DefaultRouter is used.
# It gets one random database, from ClickHouseUser.read_db_aliases for read queries
ClickHouseUser.objects.filter(id__in=[1,2,3]).count()

# These queries do the same thing, using 'secondary' connection from CLICKHOUSE_DATABASES setting
ClickHouseUser.objects_in(connections['secondary']).filter(id__in=[1,2,3]).count()
ClickHouseUser.objects.filter(id__in=[1,2,3]).using('secondary').count()

# You can get database to use with get_database(for_write: bool = False) method
# Note that it if you have multiple database in model settings,
#  DefaultRouter can return any of them each time function is called, function is stateless
ClickHouseUser.objects.get_database(for_write=False)
```

### QuerySet create methods
This library adds methods to add objects like django does without direct Database object usage.

Example:  
```python
from datetime import date
from my_app.clickhouse_models import ClickHouseUser

# This queries will choose database using current router.
# By default django_clickhouse.routers.DefaultRouter is used.
# It gets one random database, from ClickHouseUser.write_db_aliases for write queries
# You can set database explicitly with using(...) or objects_in(...) methods
instance = ClickHouseUser.objects.create(id=1, first_name='Alice', visits=1, birthday=date(2003, 6, 1))
objs = ClickHouseUser.objects.bulk_create([
    ClickHouseUser(id=2, first_name='Bob', visits=2, birthday=date(2001, 5, 1)),
    ClickHouseUser(id=3, first_name='Jhon', visits=3, birthday=date(2002, 7, 11))
], batch_size=10)
```

### Getting all objects
`QuerySet.all()` method returns copy of current QuerySet:
```python
from my_app.clickhouse_models import ClickHouseUser

qs = ClickHouseUser.objects.all()
```