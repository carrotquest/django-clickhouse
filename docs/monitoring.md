# Monitoring
In order to monitor [synchronization](synchronization.md) process, [statsd](https://pypi.org/project/statsd/) is used.
Data from statsd then can be used by [Prometheus exporter](https://github.com/prometheus/statsd_exporter) 
 or [Graphite](https://graphite.readthedocs.io/en/latest/).

## Configuration
Library expects statsd to be configured as written in [statsd docs for django](https://statsd.readthedocs.io/en/latest/configure.html#in-django).  
You can set a common prefix for all keys in this library using [CLICKHOUSE_STATSD_PREFIX](configuration.md#clickhouse_statsd_prefix) parameter.

## Exported metrics
## Gauges
* `<prefix>.sync.<model_name>.queue`  
    Number of elements in [intermediate storage](storages.md) queue waiting for import.
    Queue should not be big. It depends on [sync_delay](synchronization.md#configuration) configured and time for syncing single batch.   
    It is a good parameter to watch and alert on.

## Timers
All time is sent in milliseconds.

* `<prefix>.sync.<model_name>.total`  
    Total time of single batch task execution.
    
* `<prefix>.sync.<model_name>.steps.<step_name>`  
    `<step_name>` is one of `pre_sync`, `get_operations`, `get_sync_objects`, `get_insert_batch`, `get_final_versions`,
     `insert`, `post_sync`. Read [here](synchronization.md) for more details.  
    Time of each sync step. Can be useful to debug reasons of long sync process.  
    
* `<prefix>.inserted_tuples.<model_name>`  
    Time of inserting batch of data into ClickHouse.
    It excludes as much python code as it could to distinguish real INSERT time from python data preparation.
    
* `<prefix>.sync.<model_name>.register_operations`  
    Time of inserting sync operations into storage.
    
## Counters
 * `<prefix>.sync.<model_name>.register_operations.<op_name>`   
    `<op_name>` is one or `create`, `update`, `delete`.  
    Number of DML operations added by DjangoModel methods calls to sync queue.

* `<prefix>.sync.<model_name>.operations`   
    Number of operations, fetched from [storage](storages.md) for sync in one batch. 
    
* `<prefix>.sync.<model_name>.import_objects`   
    Number of objects, fetched from relational storage (based on operations) in order to sync with ClickHouse models.
    
* `<prefix>.inserted_tuples.<model_name>`   
    Number of rows inserted to ClickHouse.

* `<prefix>.sync.<model_name>.lock.timeout`  
    Number of locks in [RedisStorage](storages.md#redisstorage), not acquired and skipped by timeout.
    This value should be zero. If not, it means your model sync takes longer then sync task call interval.
    
* `<prefix>.sync.<model_name>.lock.hard_release`  
    Number of locks in [RedisStorage](storages.md#redisstorage), released hardly (as process which required a lock is dead).
    This value should be zero. If not, it means your sync tasks are killed hardly during the sync process (by OutOfMemory killer, for instance).
