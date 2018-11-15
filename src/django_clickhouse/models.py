"""
This file contains base django model to be synced with ClickHouse.
It saves all operations to storage in order to write them to ClickHouse later.
"""

from typing import Optional, Any, List, Type

import six
from django.db import transaction
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django.db.models import QuerySet as DjangoQuerySet, Manager as DjangoManager, Model as DjangoModel

from .configuration import config
from .storages import Storage
from .utils import lazy_class_import


try:
    from django_pg_returning.manager import UpdateReturningMixin
except ImportError:
    class UpdateReturningMixin:
        fake = True


try:
    from django_pg_bulk_update.manager import BulkUpdateManagerMixin
except ImportError:
    class BulkUpdateManagerMixin:
        fake = True


class ClickHouseSyncUpdateReturningQuerySetMixin(UpdateReturningMixin):
    """
    This mixin adopts methods of django-pg-returning library
    """

    def _register_ops(self, operation, result):
        pk_name = self.model._meta.pk.name
        pk_list = result.values_list(pk_name, flat=True)
        self.model.register_clickhouse_operations(operation, *pk_list, using=self.db)

    def update_returning(self, **updates):
        result = super().update_returning(**updates)
        self._register_ops('update', result)
        return result

    def delete_returning(self):
        result = super().delete_returning()
        self._register_ops('delete', result)
        return result


class ClickHouseSyncBulkUpdateManagerMixin(BulkUpdateManagerMixin):
    """
    This mixin adopts methods of django-pg-bulk-update library
    """

    def _update_returning_param(self, returning):
        pk_name = self.model._meta.pk.name
        if returning is None:
            returning = pk_name
        elif isinstance(returning, six.string_types):
            returning = [pk_name, returning]
        else:
            returning = list(returning) + [pk_name]

        return returning

    def _register_ops(self, result):
        pk_name = self.model._meta.pk.name
        pk_list = [getattr(item, pk_name) for item in result]
        self.model.register_clickhouse_operations('update', *pk_list, using=self.db)

    def bulk_update(self, *args, **kwargs):
        original_returning = kwargs.pop('returning', None)
        kwargs['returning'] = self._update_returning_param(original_returning)
        result = super().bulk_update(*args, **kwargs)
        self._register_ops(result)
        return result.count() if original_returning is None else result

    def bulk_update_or_create(self, *args, **kwargs):
        original_returning = kwargs.pop('returning', None)
        kwargs['returning'] = self._update_returning_param(original_returning)
        result = super().bulk_update_or_create(*args, **kwargs)
        self._register_ops(result)
        return result.count() if original_returning is None else result


class ClickHouseSyncQuerySetMixin:
    def update(self, **kwargs):
        # BUG I use update_returning method here. But it is not suitable for databases other then PostgreSQL
        # and requires django-pg-update-returning installed
        pk_name = self.model._meta.pk.name
        res = self.only(pk_name).update_returning(**kwargs).values_list(pk_name, flat=True)
        self.model.register_clickhouse_operations('update', *res, using=self.db)
        return len(res)

    def bulk_create(self, objs, batch_size=None):
        objs = super().bulk_create(objs, batch_size=batch_size)
        self.model.register_clickhouse_operations('insert', *[obj.pk for obj in objs], using=self.db)

        return objs


# I add library dependant mixins to base classes only if libraries are installed
qs_bases = [ClickHouseSyncQuerySetMixin, DjangoQuerySet]

if not getattr(UpdateReturningMixin, 'fake', False):
    qs_bases.append(ClickHouseSyncUpdateReturningQuerySetMixin)

if not getattr(BulkUpdateManagerMixin, 'fake', False):
    qs_bases.append(ClickHouseSyncBulkUpdateManagerMixin)

ClickHouseSyncModelQuerySet = type('ClickHouseSyncModelQuerySet', tuple(qs_bases), {})


class ClickHouseSyncModelMixin:
    def get_queryset(self):
        return ClickHouseSyncModelQuerySet(model=self.model, using=self._db)


class ClickHouseSyncModelManager(ClickHouseSyncModelMixin, DjangoManager):
    pass


class ClickHouseSyncModel(DjangoModel):
    """
    Base model for syncing data. Each django model synced with data must inherit this
    """
    _clickhouse_sync_models = []
    objects = ClickHouseSyncModelManager()

    class Meta:
        abstract = True

    @classmethod
    def get_clickhouse_storage(cls):  # type: () -> Storage
        """
        Returns Storage instance to save clickhouse sync data to
        :return:
        """
        storage_cls = lazy_class_import(config.SYNC_STORAGE)
        return storage_cls()

    @classmethod
    def register_clickhouse_sync_model(cls, model_cls):
        # type: (Type['django_clickhouse.clickhouse_models.ClickHouseModel']) -> None
        """
        Registers ClickHouse model to listen to this model updates
        :param model_cls: Model class to register
        :return: None
        """
        cls._clickhouse_sync_models.append(model_cls)

    @classmethod
    def get_clickhouse_sync_models(cls):  # type: () -> List['django_clickhouse.clickhouse_models.ClickHouseModel']
        """
        Returns all clickhouse models, listening to this class
        :return:
        """
        return cls._clickhouse_sync_models

    @classmethod
    def register_clickhouse_operations(cls, operation, *model_pks, using=None):
        # type: (str, *Any, Optional[str]) -> None
        """
        Registers model operation in storage
        :param operation: Operation type - one of [insert, update, delete)
        :param model_pks: Elements to import
        :param using: Database alias registered instances are from
        :return: None
        """
        def _on_commit():
            for model_cls in cls.get_clickhouse_sync_models():
                storage.register_operations_wrapped(model_cls.get_import_key(), operation, *model_pks)

        if len(model_pks) > 0:
            storage = cls.get_clickhouse_storage()
            transaction.on_commit(_on_commit, using=using)

    def post_save(self, created, using=None):  # type: (bool, Optional[str]) -> None
        self.register_clickhouse_operations('insert' if created else 'update', self.pk, using=using)

    def post_delete(self, using=None):  # type: (Optional[str]) -> None
        self.register_clickhouse_operations('delete', self.pk, using=using)


@receiver(post_save)
def post_save(sender, instance, **kwargs):
    if issubclass(sender, ClickHouseSyncModel):
        instance.post_save(kwargs.get('created', False), using=kwargs.get('using'))


@receiver(post_delete)
def post_delete(sender, instance, **kwargs):
    if issubclass(sender, ClickHouseSyncModel):
        instance.post_delete(using=kwargs.get('using'))
