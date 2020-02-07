"""
This file contains sample models to use in tests
"""
from django.db import models
from django.db.models import QuerySet
from django.db.models.manager import BaseManager
from django_pg_returning import UpdateReturningModel

from django_clickhouse.models import ClickHouseSyncModel, ClickHouseSyncQuerySet, ClickHouseSyncQuerySetMixin


class NativeQuerySet(ClickHouseSyncQuerySetMixin, QuerySet):
    pass


class TestQuerySet(ClickHouseSyncQuerySet):
    pass


class TestManager(BaseManager.from_queryset(TestQuerySet)):
    pass


class NativeManager(BaseManager.from_queryset(NativeQuerySet)):
    pass


class TestModel(UpdateReturningModel, ClickHouseSyncModel):
    objects = TestManager()
    native_objects = NativeManager()

    value = models.IntegerField()
    created_date = models.DateField()
    created = models.DateTimeField()


class SecondaryTestModel(UpdateReturningModel, ClickHouseSyncModel):
    objects = TestManager()
    native_objects = NativeManager()

    value = models.IntegerField()
    created_date = models.DateField()
    created = models.DateTimeField()
