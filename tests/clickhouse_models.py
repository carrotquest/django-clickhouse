from django_clickhouse.clickhouse_models import ClickHouseModel
from tests.models import TestModel


class TestClickHouseModel(ClickHouseModel):
    django_model = TestModel
    sync_delay = 5
