# Usage overview
## Requirements
At the begging I expect, that you already have:
1. [ClickHouse](https://clickhouse.tech/docs/en/) (with [ZooKeeper](https://zookeeper.apache.org/), if you use replication)
2. Relational database used with [Django](https://www.djangoproject.com/). For instance, [PostgreSQL](https://www.postgresql.org/)
3. [Django database set up](https://docs.djangoproject.com/en/3.0/ref/databases/)
4. [Intermediate storage](storages.md) set up. For instance, [Redis](https://redis.io/)
5. [Celery set up](https://docs.celeryproject.org/en/stable/django/first-steps-with-django.html) in order to sync data automatically.

## Configuration
Add required parameters to [Django settings.py](https://docs.djangoproject.com/en/3.0/topics/settings/):
1. Add `'django_clickhouse'` to `INSTALLED_APPS`
2. [CLICKHOUSE_DATABASES](configuration.md#clickhouse_databases)
3. [Intermediate storage](storages.md) configuration. For instance, [RedisStorage](storages.md#redisstorage)
4. It's recommended to change [CLICKHOUSE_CELERY_QUEUE](configuration.md#clickhouse_celery_queue)
5. Add sync task to [celerybeat schedule](http://docs.celeryproject.org/en/v2.3.3/userguide/periodic-tasks.html).  
  Note, that executing planner every 2 seconds doesn't mean sync is executed every 2 seconds.
  Sync time depends on model sync_delay attribute value and [CLICKHOUSE_SYNC_DELAY](configuration.md#clickhouse_sync_delay) configuration parameter.
  You can read more in [sync section](synchronization.md).

You can also change other [configuration parameters](configuration.md) depending on your project.

#### Example
```python
INSTALLED_APPS = (
    # Your apps may go here
    'django_clickhouse',
    # Your apps may go here
)

# django-clickhouse library setup
CLICKHOUSE_DATABASES = {
    # Connection name to refer in using(...) method 
    'default': {
        'db_name': 'test',
        'username': 'default',
        'password': ''
    }
}
CLICKHOUSE_REDIS_CONFIG = {
    'host': '127.0.0.1',
    'port': 6379,
    'db': 8,
    'socket_timeout': 10
}
CLICKHOUSE_CELERY_QUEUE = 'clickhouse'

# If you have no any celerybeat tasks, define a new dictionary
# More info: http://docs.celeryproject.org/en/v2.3.3/userguide/periodic-tasks.html
from datetime import timedelta
CELERYBEAT_SCHEDULE = {
    'clickhouse_auto_sync': {
        'task': 'django_clickhouse.tasks.clickhouse_auto_sync',
        'schedule': timedelta(seconds=2),  # Every 2 seconds
        'options': {'expires': 1, 'queue': CLICKHOUSE_CELERY_QUEUE}
    }
}
```

## Adopting django model
Read [ClickHouseSyncModel](models.md#djangomodel) section.
Inherit all [django models](https://docs.djangoproject.com/en/3.0/topics/db/models/) 
 you want to sync with ClickHouse from `django_clickhouse.models.ClickHouseSyncModel` or sync mixins.

```python
from django_clickhouse.models import ClickHouseSyncModel
from django.db import models

class User(ClickHouseSyncModel):
    first_name = models.CharField(max_length=50)
    visits = models.IntegerField(default=0)
    birthday = models.DateField()
```

## Create ClickHouseModel
1. Read [ClickHouseModel section](models.md#clickhousemodel)
2. Create `clickhouse_models.py` in your django app.
3. Add `ClickHouseModel` class there:
```python
from django_clickhouse.clickhouse_models import ClickHouseModel
from django_clickhouse.engines import MergeTree
from infi.clickhouse_orm import fields
from my_app.models import User

class ClickHouseUser(ClickHouseModel):
    django_model = User
    
    # Uncomment the line below if you want your models to be synced automatically
    # sync_enabled = True
    
    id = fields.UInt32Field()
    first_name = fields.StringField()
    birthday = fields.DateField()
    visits = fields.UInt32Field(default=0)

    engine = MergeTree('birthday', ('birthday',))
```

## Migration to create table in ClickHouse
1. Read [migrations](migrations.md) section
2. Create `clickhouse_migrations` package in your django app
3. Create `0001_initial.py` file inside the created package. Result structure should be:
    ```
    my_app
    | clickhouse_migrations
    |-- __init__.py
    |-- 0001_initial.py
    | clickhouse_models.py
    | models.py
    ```

4. Add content to file `0001_initial.py`:
    ```python
    from django_clickhouse import migrations
    from my_app.cilckhouse_models import ClickHouseUser
    
    class Migration(migrations.Migration):
        operations = [
            migrations.CreateTable(ClickHouseUser)
        ]
    ```

## Run migrations
Call [django migrate](https://docs.djangoproject.com/en/3.0/ref/django-admin/#django-admin-migrate)
 to apply created migration and create table in ClickHouse.

## Set up and run celery sync process
Set up [celery worker](https://docs.celeryproject.org/en/latest/userguide/workers.html#starting-the-worker) for [CLICKHOUSE_CELERY_QUEUE](configuration.md#clickhouse_celery_queue) and [celerybeat](https://docs.celeryproject.org/en/latest/userguide/periodic-tasks.html#starting-the-scheduler).  

## Test sync and write analytics queries
1. Read [monitoring section](monitoring.md) in order to set up your monitoring system.
2. Read [query section](queries.md) to understand how to query database.
2. Create some data in source table with django.
3. Check, if it is synced.

#### Example
```python
import time
from my_app.models import User
from my_app.clickhouse_models import ClickHouseUser

u = User.objects.create(first_name='Alice', birthday=datetime.date(1987, 1, 1), visits=1)

# Wait for celery task is executed at list once
time.sleep(6)

assert ClickHouseUser.objects.filter(id=u.id).count() == 1, "Sync is not working"
```

## Congratulations
Tune your integration to achieve better performance if needed: [docs](performance.md).
