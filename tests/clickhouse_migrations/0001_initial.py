
from django_clickhouse import migrations
from tests.clickhouse_models import ClickHouseTestModel, ClickHouseCollapseTestModel


def python_exec(database):
    pass


class Migration(migrations.Migration):
    operations = [
        migrations.CreateTable(ClickHouseTestModel),
        migrations.CreateTable(ClickHouseCollapseTestModel),
        migrations.RunPython(python_exec, hints={'force_migrate_on_databases': ['default']})
    ]
