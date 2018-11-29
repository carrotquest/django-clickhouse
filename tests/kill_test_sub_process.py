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
from django.db.models import F
from tests.clickhouse_models import ClickHouseCollapseTestModel
from tests.models import TestModel


def create(batch_size=1000, test_time=60, period=1, **kwargs):
    for iteration in range(int(test_time / period)):
        TestModel.objects.db_manager('test_db').bulk_create([
            TestModel(created_date='2018-01-01', value=iteration * batch_size + i) for i in range(batch_size)
        ])
        sleep(period)


def update(batch_size=1000, test_time=60, period=1, **kwargs):
    for iteration in range(int(test_time / period)):
        TestModel.objects.db_manager('test_db').filter(id__gte=iteration * batch_size).annotate(idmod10=F('id') % 10). \
            filter(idmod10=0).update(value=-1)
        sleep(period)


def delete(batch_size=1000, test_time=60, period=1, **kwargs):
    for iteration in range(int(test_time / period)):
        TestModel.objects.db_manager('test_db').filter(id__gte=iteration * batch_size).annotate(idmod10=F('id') % 10). \
            filter(idmod10=1).delete()
        sleep(period)


def sync(period=1, test_time=60, **kwargs):
    start = datetime.datetime.now()
    while (datetime.datetime.now() - start).total_seconds() < test_time:
        ClickHouseCollapseTestModel.sync_batch_from_storage()
        sleep(period)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('process', type=str, choices=('sync', 'create', 'update', 'delete'))
    parser.add_argument('--test-time', type=int, required=False, default=60)
    parser.add_argument('--batch-size', type=str, required=False, default=1000)
    parser.add_argument('--period', type=str, required=False, default=1)
    params = vars(parser.parse_args())

    func_name = params['process']
    method = locals()[func_name]
    method(**params)
