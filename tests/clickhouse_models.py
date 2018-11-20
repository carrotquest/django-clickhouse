from django_clickhouse.clickhouse_models import ClickHouseModel
from django_clickhouse.engines import MergeTree, CollapsingMergeTree
from infi.clickhouse_orm import fields

from tests.models import TestModel


class ClickHouseTestModel(ClickHouseModel):
    django_model = TestModel
    sync_delay = 2

    id = fields.Int32Field()
    created_date = fields.DateField()
    value = fields.Int32Field()

    engine = MergeTree('created_date', ('id',))


class ClickHouseCollapseTestModel(ClickHouseModel):
    django_model = TestModel
    sync_delay = 2

    id = fields.Int32Field()
    created_date = fields.DateField()
    value = fields.Int32Field()
    sign = fields.Int8Field()

    engine = CollapsingMergeTree('created_date', ('id',), 'sign')