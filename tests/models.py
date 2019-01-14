"""
This file contains sample models to use in tests
"""
from django.db import models

from django_clickhouse.models import ClickHouseSyncModel


class TestModel(ClickHouseSyncModel):
    value = models.IntegerField()
    created_date = models.DateField()
    created = models.DateTimeField()


class SecondaryTestModel(ClickHouseSyncModel):
    value = models.IntegerField()
    created_date = models.DateField()
    created = models.DateTimeField()
