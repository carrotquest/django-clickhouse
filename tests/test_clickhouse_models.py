import datetime

from django.test import TestCase

from tests.clickhouse_models import ClickHouseTestModel


class ClickHouseModelTest(TestCase):
    def setUp(self):
        self.storage = ClickHouseTestModel.get_storage()
        self.storage.flush()

    def test_need_sync(self):
        # sync is disabled by default
        ClickHouseTestModel.sync_enabled = False
        self.assertFalse(ClickHouseTestModel.need_sync())

        # There were no syncs. So it should be done
        ClickHouseTestModel.sync_enabled = True
        self.assertTrue(ClickHouseTestModel.need_sync())

        # Time hasn't passed - no sync
        self.storage.set_last_sync_time(ClickHouseTestModel.get_import_key(), datetime.datetime.now())
        self.assertFalse(ClickHouseTestModel.need_sync())

        # Time has passed
        sync_delay = ClickHouseTestModel.get_sync_delay()
        self.storage.set_last_sync_time(ClickHouseTestModel.get_import_key(),
                                        datetime.datetime.now() - datetime.timedelta(seconds=sync_delay + 1))
        self.assertTrue(ClickHouseTestModel.need_sync())
