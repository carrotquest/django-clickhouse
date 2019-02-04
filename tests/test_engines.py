from django.test import TestCase

from django_clickhouse.database import connections
from django_clickhouse.migrations import migrate_app
from tests.clickhouse_models import ClickHouseCollapseTestModel
from tests.models import TestModel


class CollapsingMergeTreeTest(TestCase):
    fixtures = ['test_model']
    maxDiff = None

    collapse_fixture = [{
        "id": 1,
        "created": "2018-01-01 00:00:00",
        "value": 0,
        "sign": 1,
        "version": 1
    }, {
        "id": 1,
        "created": "2018-01-01 00:00:00",
        "value": 0,
        "sign": -1,
        "version": 1
    }, {
        "id": 1,
        "created": "2018-01-01 00:00:00",
        "value": 0,
        "sign": 1,
        "version": 2
    }, {
        "id": 1,
        "created": "2018-01-01 00:00:00",
        "value": 0,
        "sign": -1,
        "version": 2
    }, {
        "id": 1,
        "created": "2018-01-01 00:00:00",
        "value": 0,
        "sign": 1,
        "version": 3
    }, {
        "id": 1,
        "created": "2018-01-01 00:00:00",
        "value": 0,
        "sign": -1,
        "version": 3
    }, {
        "id": 1,
        "created": "2018-01-01 00:00:00",
        "value": 0,
        "sign": 1,
        "version": 4
    }]

    def setUp(self):
        self.db = connections['default']
        self.db.drop_database()
        self.db.create_database()
        migrate_app('tests', 'default')
        ClickHouseCollapseTestModel.get_storage().flush()

        ClickHouseCollapseTestModel.objects.bulk_create([
            ClickHouseCollapseTestModel(**item) for item in self.collapse_fixture
        ])
        self.objects = TestModel.objects.filter(id=1)

    def _test_final_versions(self, final_versions):
        final_versions = list(final_versions)
        self.assertEqual(1, len(final_versions))
        item = (final_versions[0].id, final_versions[0].sign, final_versions[0].version, final_versions[0].value)
        self.assertTupleEqual((1, -1, 4, 0), item)

    def test_get_final_versions_by_final_date(self):
        final_versions = ClickHouseCollapseTestModel.engine.get_final_versions(ClickHouseCollapseTestModel,
                                                                               self.objects)
        self._test_final_versions(final_versions)

    def test_get_final_versions_by_version_date(self):
        ClickHouseCollapseTestModel.engine.version_col = 'version'
        final_versions = ClickHouseCollapseTestModel.engine.get_final_versions(ClickHouseCollapseTestModel,
                                                                               self.objects)
        self._test_final_versions(final_versions)

    def test_get_final_versions_by_final_datetime(self):
        final_versions = ClickHouseCollapseTestModel.engine.get_final_versions(ClickHouseCollapseTestModel,
                                                                               self.objects, date_col='created')
        self._test_final_versions(final_versions)

    def test_get_final_versions_by_version_datetime(self):
        ClickHouseCollapseTestModel.engine.version_col = 'version'
        final_versions = ClickHouseCollapseTestModel.engine.get_final_versions(ClickHouseCollapseTestModel,
                                                                               self.objects, date_col='created')
        self._test_final_versions(final_versions)

    def test_versions(self):
        ClickHouseCollapseTestModel.engine.version_col = 'version'
        batch = ClickHouseCollapseTestModel.get_insert_batch(self.objects)
        batch = list(batch)
        self.assertEqual(2, len(batch))
        self.assertEqual(4, batch[0].version)
        self.assertEqual(-1, batch[0].sign)
        self.assertEqual(5, batch[1].version)
        self.assertEqual(1, batch[1].sign)
