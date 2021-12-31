
from django_clickhouse import migrations
from tests.clickhouse_models import ClickHouseTestModel, ClickHouseCollapseTestModel


class Migration(migrations.Migration):
    operations = [
        migrations.CreateTable(ClickHouseTestModel),
        migrations.CreateTable(ClickHouseCollapseTestModel)
    ]
