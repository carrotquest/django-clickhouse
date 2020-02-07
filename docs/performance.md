# Sync performance
Every real life system may have its own performance problems. 
They depend on:
* You ClickHouse servers configuration
* Number of ClickHouse instances in your cluster
* Your data formats
* Import speed
* Network
* etc

I recommend to use [monitoring](monitoring.md) in order to understand where is the bottle neck and act accordingly.

This chapter gives a list of known problems which can slow down your import.

## ClickHouse tuning
Read this [doc](https://clickhouse.tech/docs/en/introduction/performance/#performance-when-inserting-data)
 and tune it both for read and write.

## ClickHouse cluster
As ClickHouse is a [multimaster database](https://clickhouse.tech/docs/en/introduction/distinctive_features/#data-replication-and-data-integrity-support),
 you can import and read from any node when you have a cluster.
In order to read and import to multiple nodes you can use [CHProxy](https://github.com/Vertamedia/chproxy)
or add multiple databases to [routing configuration](routing.md#clickhousemodel-routing-attributes).

## CollapsingMergeTree engine and previous versions
In order to reduce number of stored data in [intermediate storage](storages.md),
 this library doesn't store old versions of data on update or delete.
 Another point is that getting previous data versions from relational storages is a hard operation.
Engines like `CollapsingMergeTree` get old versions from ClickHouse:
1. Using `version_col` if it is set in engine's parameters. 
 This is a special field which stores incremental row versions and is filled by the library.
 It should be of any unsigned integer type (depending on how many row versions you may have).
2. Using `FINAL` query modification.
 This way is much more slow, but doesn't require additional column.  

## Know your data
In common case library user uses python types to form ClickHouse data.
Library is responsible for converting this data into format ClickHouse expects to receive.
This leads to great number of convert operations when you import data in big batches.
In order to reduce this time, you can:
* Set `MyClickHouseModel.sync_formatted_tuples` to True
* Override `MyClickHouseModel.get_insert_batch(, import_objects: Iterable[DjangoModel])` method:  
  It should get `cls.get_tuple_class()` and yield (it is a [generator](https://wiki.python.org/moin/Generators))
  so it generates tuples of string values, already prepared to insert into ClickHouse.  
  **Important note**: `ClickHouseModel.get_insert_batch(...)` can perform additional functionality depending on model [engine](models.md#engines).
  Be careful.
