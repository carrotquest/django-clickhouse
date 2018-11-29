from infi.clickhouse_orm import fields

from django_clickhouse.clickhouse_models import ClickHouseModel, ClickHouseMultiModel
from django_clickhouse.engines import ReplacingMergeTree, CollapsingMergeTree
from tests.models import TestModel


class ClickHouseTestModel(ClickHouseModel):
    django_model = TestModel
    sync_delay = 2

    id = fields.Int32Field()
    created_date = fields.DateField()
    value = fields.Int32Field()

    engine = ReplacingMergeTree('created_date', ('id',))
    migrate_db_aliases = ('default', 'secondary')


class ClickHouseCollapseTestModel(ClickHouseModel):
    django_model = TestModel
    sync_delay = 2
    sync_enabled = True

    id = fields.Int32Field()
    created_date = fields.DateField()
    value = fields.Int32Field()
    sign = fields.Int8Field()

    engine = CollapsingMergeTree('created_date', ('id',), 'sign')


class ClickHouseMultiTestModel(ClickHouseMultiModel):
    django_model = TestModel
    sub_models = [ClickHouseTestModel, ClickHouseCollapseTestModel]
