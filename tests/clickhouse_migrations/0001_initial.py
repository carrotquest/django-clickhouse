
from django_clickhouse import migrations, migration_operators
from tests.clickhouse_models import ClickHouseTestModel, ClickHouseCollapseTestModel


class Migration(migrations.Migration):
    operations = [
        migration_operators.CreateTable(ClickHouseTestModel),
        migration_operators.CreateTable(ClickHouseCollapseTestModel)
    ]
