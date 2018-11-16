
from infi.clickhouse_orm.migrations import CreateTable
from django_clickhouse import migrations
from tests.clickhouse_models import ClickHouseTestModel, ClickHouseCollapseTestModel


class Migration(migrations.Migration):
    operations = [
        CreateTable(ClickHouseTestModel),
        CreateTable(ClickHouseCollapseTestModel)
    ]
