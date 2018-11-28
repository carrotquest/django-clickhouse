from django.test import TestCase

from django_clickhouse.configuration import config


class ConfigTest(TestCase):
    def test_default(self):
        self.assertEqual(5, config.SYNC_DELAY)

    def test_value(self):
        self.assertEqual(5000, config.SYNC_BATCH_SIZE)

    def test_not_lib_prop(self):
        with self.assertRaises(AttributeError):
            config.SECRET_KEY
