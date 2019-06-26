from infi.clickhouse_orm import fields

from django_clickhouse.clickhouse_models import ClickHouseModel, ClickHouseMultiModel
from django_clickhouse.engines import ReplacingMergeTree, CollapsingMergeTree
from tests.models import TestModel, SecondaryTestModel


class ClickHouseTestModel(ClickHouseModel):
    django_model = TestModel
    sync_delay = 2
    sync_enabled = True

    id = fields.Int32Field()
    created_date = fields.DateField()
    value = fields.Int32Field(default=100500)
    str_field = fields.StringField()

    engine = ReplacingMergeTree('created_date', ('id',))
    migrate_replicated_db_aliases = ('default', 'secondary')
    migrate_non_replicated_db_aliases = ('default', 'secondary')


class ClickHouseCollapseTestModel(ClickHouseModel):
    django_model = TestModel
    sync_delay = 2
    sync_enabled = True

    id = fields.Int32Field()
    created = fields.DateTimeField()
    value = fields.Int32Field()
    sign = fields.Int8Field(default=1)
    version = fields.Int8Field(default=1)

    engine = CollapsingMergeTree('created', ('id',), 'sign')


class ClickHouseMultiTestModel(ClickHouseMultiModel):
    django_model = TestModel
    sub_models = [ClickHouseTestModel, ClickHouseCollapseTestModel]
    sync_delay = 2
    sync_enabled = True


class ClickHouseSecondTestModel(ClickHouseModel):
    django_model = SecondaryTestModel
    sync_delay = 2
    sync_enabled = True

    id = fields.Int32Field()
    created_date = fields.DateField()
    value = fields.Int32Field()

    engine = ReplacingMergeTree('created_date', ('id',))
