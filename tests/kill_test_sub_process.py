import logging
import sys

import argparse
import django
import os
from time import sleep

import datetime


# set Django environment
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
os.environ.setdefault("DJANGO_SETTINGS_MODULE", 'tests.settings')
django.setup()

# This imports must be after django activation
from django.db.models import F  # noqa: E402
from tests.clickhouse_models import ClickHouseCollapseTestModel  # noqa: E402
from tests.models import TestModel  # noqa: E402

logger = logging.getLogger('django-clickhouse')


def create(batch_size=1000, test_time=60, period=1, **kwargs):
    for iteration in range(int(test_time / period)):
        res = TestModel.objects.db_manager('test_db').bulk_create([
            TestModel(created=datetime.datetime.now(), created_date='2018-01-01', value=iteration * batch_size + i)
            for i in range(batch_size)
        ])
        logger.info('django-clickhouse: test created %d records' % len(res))
        sleep(period)


def update(batch_size=500, test_time=60, period=1, **kwargs):
    for iteration in range(int(test_time / period)):
        updated = TestModel.objects.db_manager('test_db').\
            filter(value__gte=iteration * batch_size, value__lt=(iteration + 1) * batch_size).\
            annotate(valmod10=F('value') % 10).filter(valmod10=0).update(value=-1)
        logger.debug('django-clickhouse: test updated %d records' % updated)
        sleep(period)


def delete(batch_size=500, test_time=60, period=1, **kwargs):
    for iteration in range(int(test_time / period)):
        deleted, _ = TestModel.objects.db_manager('test_db'). \
            filter(value__gte=iteration * batch_size, value__lt=(iteration + 1) * batch_size). \
            annotate(valmod10=F('value') % 10).filter(valmod10=1).delete()
        logger.debug('django-clickhouse: test deleted %d records' % deleted)
        sleep(period)


def sync(period=1, test_time=60, **kwargs):
    if kwargs['once']:
        ClickHouseCollapseTestModel.sync_batch_from_storage()
    else:
        start = datetime.datetime.now()
        while (datetime.datetime.now() - start).total_seconds() < test_time:
            ClickHouseCollapseTestModel.sync_batch_from_storage()
            sleep(period)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('process', type=str, choices=('sync', 'create', 'update', 'delete'))
    parser.add_argument('--test-time', type=int, required=False, default=60)
    parser.add_argument('--batch-size', type=int, required=False, default=1000)
    parser.add_argument('--period', type=int, required=False, default=1)
    parser.add_argument('--once', type=bool, required=False, default=False)
    params = vars(parser.parse_args())

    # Disable registering not needed models
    TestModel._clickhouse_sync_models = {ClickHouseCollapseTestModel}

    func_name = params['process']
    method = locals()[func_name]
    method(**params)
