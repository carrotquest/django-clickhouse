# Models
Model is a pythonic class representing database table in your code.
 It also defines an interface (methods) to perform operations on this table
 and describes its configuration inside framework.
 
This library operates 2 kinds of models:  
* DjangoModel, describing tables in source relational database (PostgreSQL, MySQL, etc.)  
* ClickHouseModel, describing models in [ClickHouse](https://clickhouse.yandex/docs/en) database
  
In order to distinguish them, I will refer them as ClickHouseModel and DjangoModel in further documentation.

## DjangoModel
Django provides a [model system](https://docs.djangoproject.com/en/3.0/topics/db/models/) 
 to interact with relational databases. 
 In order to perform [synchronization](synchronization.md) we need to "catch" all [DML operations](https://en.wikipedia.org/wiki/Data_manipulation_language)
 on source django model and save information about them in [storage](storages.md).
 To achieve this, library introduces abstract `django_clickhouse.models.ClickHouseSyncModel` class.
 Each model, inherited from `ClickHouseSyncModel` will automatically save information, needed to sync to storage.  
Read [synchronization](synchronization.md) section for more info.

`ClickHouseSyncModel` saves information about:
* `Model.objects.create()`, `Model.objects.bulk_create()`
* `Model.save()`, `Model.delete()`
* `QuerySet.update()`, `QuerySet.delete()`
* All queries of [django-pg-returning](https://pypi.org/project/django-pg-returning/) library
* All queries of [django-pg-bulk-update](https://pypi.org/project/django-pg-bulk-update/) library

You can also combine your custom django manager and queryset using mixins from `django_clickhouse.models` package:
  
**Important note**: Operations are saved in [transaction.on_commit()](https://docs.djangoproject.com/en/2.2/topics/db/transactions/#django.db.transaction.on_commit). 
 The goal is avoiding syncing operations, not committed to relational database.
 But this may also provide bad effect: situation, when transaction is committed,
 but it hasn't been registered, if something went wrong during registration. 

Example:
```python
from django_clickhouse.models import ClickHouseSyncModel
from django.db import models
from datetime import date

class User(ClickHouseSyncModel):
    first_name = models.CharField(max_length=50)
    age = models.IntegerField()
    birthday = models.DateField()

# All operations will be registered to sync with ClickHouse models:
User.objects.create(first_name='Alice', age=16, birthday=date(2003, 6, 1))
User(first_name='Bob', age=17, birthday=date(2002, 1, 1)).save()
User.objects.update(first_name='Candy')

# Custom manager

```

## ClickHouseModel
This kind of model is based on [infi.clickhouse_orm Model](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/models_and_databases.md#defining-models)
 and represents table in [ClickHouse database](https://clickhouse.yandex/docs/en).

You should define `ClickHouseModel` subclass for each table you want to access and sync in ClickHouse.
Each model should be inherited from `django_clickhouse.clickhouse_models.ClickHouseModel`.
By default, models are searched in `clickhouse_models` module of each django app.
You can change modules name, using setting [CLICKHOUSE_MODELS_MODULE](configuration.md#clickhouse_models_module)
 
You can read more about creating models and fields [here](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/models_and_databases.md#defining-models):
all capabilities are supported. At the same time, django-clickhouse libraries adds:
* [routing attributes and methods](routing.md)
* [sync attributes and methods](synchronization.md)

Example:
```python
from django_clickhouse.clickhouse_models import ClickHouseModel
from django_clickhouse.engines import MergeTree
from infi.clickhouse_orm import fields
from my_app.models import User


class HeightData(ClickHouseModel):
    django_model = User

    first_name = fields.StringField()
    birthday = fields.DateField()
    height = fields.Float32Field()

    engine = MergeTree('birthday', ('first_name', 'last_name', 'birthday'))


class AgeData(ClickHouseModel):
    django_model = User

    first_name = fields.StringField()
    birthday = fields.DateField()
    age = fields.UInt32Field()

    engine = MergeTree('birthday', ('first_name', 'last_name', 'birthday'))
```

### ClickHouseMultiModel
In some cases you may need to sync single DjangoModel to multiple ClickHouse models.
This model gives ability to reduce number of relational database operations.
You can read more in [sync](synchronization.md) section.

Example:
```python
from django_clickhouse.clickhouse_models import ClickHouseMultiModel
from my_app.models import User

class MyMultiModel(ClickHouseMultiModel):
    django_model = User
    sub_models = [AgeData, HeightData]
```

## ClickHouseModel namedtuple form
[infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm) stores data rows in special Model objects.
It works well on hundreds of records. 
But when you sync 100k records in a batch, initializing 100k model instances will be slow.  
Too optimize this process `ClickHouseModel` class have `get_tuple_class()` method.
It generates a [namedtuple](https://docs.python.org/3/library/collections.html#collections.namedtuple) class,
with same data fields a model has. 
Initializing such tuples takes much less time, then initializing Model objects.

## Engines
Engine is a way of storing, indexing, replicating and sorting data ClickHouse ([docs](https://clickhouse.yandex/docs/en/operations/table_engines/)).  
Engine system is based on [infi.clickhouse_orm engine system](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/table_engines.md#table-engines).  
This library extends original engine classes as each engine can have it's own synchronization mechanics. 
Engines are defined in `django_clickhouse.engines` module.

Currently supported engines (with all infi functionality, [more info](https://github.com/Infinidat/infi.clickhouse_orm/blob/develop/docs/table_engines.md#data-replication)):
* `MergeTree`
* `ReplacingMergeTree`
* `SummingMergeTree`
* `CollapsingMergeTree`


## Serializers
Serializer is a class which translates django model instances to [namedtuples, inserted into ClickHouse](#clickhousemodel-namedtuple-form).
`django_clickhouse.serializers.Django2ClickHouseModelSerializer` is used by default in all models.
 All serializers must inherit this class. 

Serializer must implement next interface:
```python
from django_clickhouse.serializers import Django2ClickHouseModelSerializer
from django.db.models import Model as DjangoModel
from typing import *

class CustomSerializer(Django2ClickHouseModelSerializer):
    def __init__(self, model_cls: Type['ClickHouseModel'], fields: Optional[Iterable[str]] = None,
                 exclude_fields: Optional[Iterable[str]] = None, writable: bool = False,
                 defaults: Optional[dict] = None) -> None:
        super().__init__(model_cls, fields=fields, exclude_fields=exclude_fields, writable=writable, defaults=defaults)

    def serialize(self, obj: DjangoModel) -> NamedTuple:
        pass
```
