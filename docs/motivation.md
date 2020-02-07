# Design motivation
## Separate from django database setting, QuerySet and migration system
ClickHouse SQL and DML language is near to standard, but does not follow it exactly ([docs](https://clickhouse.tech/docs/en/introduction/distinctive_features/#sql-support)).  
As a result, it can not be easily integrated into django query subsystem as it expects databases to support:
1. Transactions.
2. INNER/OUTER JOINS by condition.
3. Full featured updates and deletes.
4. Per database replication (ClickHouse has per table replication)
5. Other features, not supported in ClickHouse.

In order to have more functionality, [infi.clickhouse-orm](https://github.com/Infinidat/infi.clickhouse_orm) 
  is used as base library for databases, querysets and migrations. The most part of it is compatible and can be used without any changes.

## Sync over intermediate storage
This library has several goals which lead to intermediate storage:
1. Fail resistant import, does not matter what the fail reason is:
 ClickHouse fail, network fail, killing import process by system (OOM, for instance).
2. ClickHouse does not like single row inserts: [docs](https://clickhouse.tech/docs/en/introduction/performance/#performance-when-inserting-data).
 So it's worth batching data somewhere before inserting it. 
 ClickHouse provide BufferEngine for this, but it can loose data if ClickHouse fails - and no one will now about it.
3. Better scalability. Different intermediate storages may be implemented in the future, based on databases, queue systems or even BufferEngine.
 
## Replication and routing
In primitive cases people just have single database or cluster with same tables on each replica.
But as ClickHouse has per table replication a more complicated structure can be built:
1. Model A is stored on servers 1 and 2
2. Model B is stored on servers 2, 3 and 5
3. Model C is stored on servers 1, 3 and 4 
 
Moreover, migration operations in ClickHouse can also be auto-replicated (`ALTER TABLE`, for instance)  or not (`CREATE TABLE`).
  
In order to make replication scheme scalable:
1. Each model has it's own read / write / migrate [routing configuration](routing.md#clickhousemodel-routing-attributes).
2. You can use [router](routing.md#router) like django does to set basic routing rules for all models or model groups.  
 