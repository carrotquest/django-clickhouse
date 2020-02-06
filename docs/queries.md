# Making queries

## Motivation
ClickHouse SQL language is near to standard, but does not follow it exactly ([docs](https://clickhouse.tech/docs/en/introduction/distinctive_features/#sql-support)).  
It can not be easily integrated into django query subsystem as it expects databases to support standard SQL language features like transactions and INNER/OUTER JOINS by condition.  

In order to fit it 



Libraries query system extends [infi.clickhouse-orm](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/querysets.md).

TODO
