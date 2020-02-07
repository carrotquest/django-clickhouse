# Synchronization

## Design motivation
Read [here](motivation.md#sync-over-intermediate-storage).


## Algorithm
<!--- ![General scheme](https://octodex.github.com/images/yaktocat.png) --->
1. [Celery beat](https://docs.celeryproject.org/en/latest/userguide/periodic-tasks.html) schedules `django_clickhouse.tasks.clickhouse_auto_sync` task every second or near.
2. [Celery workers](https://docs.celeryproject.org/en/latest/userguide/workers.html) execute `clickhouse_auto_sync`.
 It searches for `ClickHouseModel` subclasses which need sync (if `Model.need_sync()` method returns `True`).
2. `django_clickhouse.tasks.sync_clickhouse_model` task is scheduled for each `ClickHouseModel` which needs sync.
3. `sync_clickhouse_model` saves sync start time in [storage](storages.md) and calls `ClickHouseModel.sync_batch_from_storage()` method.
4. `ClickHouseModel.sync_batch_from_storage()`:
    * Gets [storage](storages.md) model works with using `ClickHouseModel.get_storage()` method
    * Calls `Storage.pre_sync(import_key)` for model [storage](storages.md).
        This may be used to prevent parallel execution with locks or some other operations.
    * Gets a list of operations to sync from [storage](storages.md).
    * Fetches objects from relational database calling `ClickHouseModel.get_sync_objects(operations)` method.
    * Forms a batch of tuples to insert into ClickHouse using `ClickHouseModel.get_insert_batch(import_objects)` method.
    * Inserts batch of tuples into ClickHouse using `ClickHouseModel.insert_batch(batch)` method.
    * Calls `Storage.post_sync(import_key)` method to clean up storage after syncing batch.
        This method also removes synced operations from storage.
    * If some exception occurred during execution, `Storage.post_sybc_failed(import_key)` method is called.
        Note, that process can be killed without exception, for instance by OOM killer.
        And this method will not be called. 
   
    
## Configuration
Sync configuration can be set globally using django settings.py parameters or redeclared for each `ClickHouseModel` class.
`ClickHouseModel` configuration is prior to settings configuration.

### Settings configuration
* [CLICKHOUSE_CELERY_QUEUE](configuration.md#clickhouse_celery_queue)  
Defaults to: `'celery'`  
A name of a queue, used by celery to plan library sync tasks.
    
* [CLICKHOUSE_SYNC_STORAGE](configuration.md#clickhouse_sync_storage)  
Defaults to: `'django_clickhouse.storages.RedisStorage'`  
An [intermediate storage](storages.md) class to use. Can be a string or class.
    
* [CLICKHOUSE_SYNC_BATCH_SIZE](configuration.md#clickhouse_sync_storage)  
Defaults to: `10000`  
Maximum number of operations, fetched by sync process from [intermediate storage](storages.md) per sync round.
    
* [CLICKHOUSE_SYNC_DELAY](configuration.md#clickhouse_sync_storage)  
Defaults to: `5`
A delay in seconds between two sync rounds start.

### ClickHouseModel configuration
Each `ClickHouseModel` subclass can define sync arguments and methods:
* `django_model: django.db.models.Model`  
Required.
Django model this ClickHouseModel class is synchronized with.

* `django_model_serializer: django.db.models.Model`  
Defaults to: `django_clickhouse.serializers.Django2ClickHouseModelSerializer`  
[Serializer class](models.md#serializers) to convert DjangoModel to ClickHouseModel.  

* `sync_enabled: bool`  
Defaults to: `False`.
Is sync for this model enabled?

* `sync_batch_size: int`  
Defaults to: [CLICKHOUSE_SYNC_BATCH_SIZE](configuration.md#clickhouse_sync_storage)  
Maximum number of operations, fetched by sync process from [storage](storages.md) per sync round.  

* `sync_delay: float`  
Defaults to: [CLICKHOUSE_SYNC_DELAY](configuration.md#clickhouse_sync_storage)  
A delay in seconds between two sync rounds start.  

* `sync_storage: Union[str, Storage]`  
Defaults to: [CLICKHOUSE_SYNC_STORAGE](configuration.md#clickhouse_sync_storage)  
An [intermediate storage](storages.md) class to use. Can be a string or class.  

Example:  
```python
from django_clickhouse.clickhouse_models import ClickHouseModel
from django_clickhouse.engines import ReplacingMergeTree
from infi.clickhouse_orm import fields 
from my_app.models import User

class ClickHouseUser(ClickHouseModel):
    django_model = User
    sync_enabled = True
    sync_delay = 5
    sync_batch_size = 1000

    id = fields.UInt32Field()
    first_name = fields.StringField()
    birthday = fields.DateField()
    visits = fields.UInt32Field(default=0)

    engine = ReplacingMergeTree('birthday', ('birthday',))
```


## Fail resistance
Fail resistance is based on several points:
1. [Storage](storages.md) should not loose data in any case. It's not this library goal to keep it stable.
2. Data is removed from [storage](storages.md) only if import succeeds. Otherwise import attempt is repeated.
3. It's recommended to use ReplacingMergeTree or CollapsingMergeTree [engines](models.md#engines) 
    instead of simple MergeTree, so it removes duplicates if batch is imported twice.
4. Each `ClickHouseModel` is synced in separate process. 
    If one model fails, it should not affect other models.
