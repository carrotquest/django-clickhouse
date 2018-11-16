from django_clickhouse.clickhouse_models import ClickHouseModel
from django_clickhouse.engines import MergeTree
from infi.clickhouse_orm import fields

from tests.models import TestModel


class TestClickHouseModel(ClickHouseModel):
    django_model = TestModel
    sync_delay = 5

    id = fields.Int32Field()
    created_date = fields.DateField()
    value = fields.Int32Field()

    engine = MergeTree('created_date', ('id',))
