"""
This file contains base django model to be synced with ClickHouse.
It saves all operations to storage in order to write them to ClickHouse later.
"""

from typing import Optional, Any, Type, Set

import six
from django.db import transaction
from django.db.models import QuerySet as DjangoQuerySet, Model as DjangoModel, Manager as DjangoManager
from django.db.models.manager import BaseManager
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from statsd.defaults.django import statsd

from .compatibility import update_returning_pk
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


class ClickHouseSyncRegisterMixin:
    def _register_ops(self, operation, result, as_int: bool = False):
        pk_name = self.model._meta.pk.name
        pk_list = [getattr(item, pk_name) if isinstance(item, DjangoModel) else item for item in result]
        self.model.register_clickhouse_operations(operation, *pk_list, using=self.db)


class ClickHouseSyncUpdateReturningQuerySetMixin(ClickHouseSyncRegisterMixin, UpdateReturningMixin):
    """
    This mixin adopts methods of django-pg-returning library
    """

    def update_returning(self, **updates):
        result = super().update_returning(**updates)
        self._register_ops('update', result)
        return result

    def delete_returning(self):
        result = super().delete_returning()
        self._register_ops('delete', result)
        return result


class ClickHouseSyncBulkUpdateQuerySetMixin(ClickHouseSyncRegisterMixin, BulkUpdateManagerMixin):
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

    def _decorate_method(self, name: str, operation: str, args, kwargs):
        if not hasattr(super(), name):
            raise AttributeError("QuerySet has no attribute %s. Is django-pg-bulk-update library installed?" % name)

        func = getattr(super(), name)
        original_returning = kwargs.pop('returning', None)
        kwargs['returning'] = self._update_returning_param(original_returning)
        result = func(*args, **kwargs)
        self._register_ops(operation, result)
        return result.count() if original_returning is None else result

    def pg_bulk_update(self, *args, **kwargs):
        return self._decorate_method('pg_bulk_update', 'update', args, kwargs)

    def pg_bulk_update_or_create(self, *args, **kwargs):
        return self._decorate_method('pg_bulk_update_or_create', 'update', args, kwargs)

    def pg_bulk_create(self, *args, **kwargs):
        return self._decorate_method('pg_bulk_create', 'insert', args, kwargs)


class ClickHouseSyncQuerySetMixin(ClickHouseSyncRegisterMixin):
    def update(self, **kwargs):
        pks = update_returning_pk(self, kwargs)
        self._register_ops('update', pks)
        return len(pks)

    def bulk_create(self, objs, batch_size=None):
        objs = super().bulk_create(objs, batch_size=batch_size)
        self._register_ops('insert', objs)
        return objs

    def bulk_update(self, objs, *args, **kwargs):
        objs = list(objs)

        # No need to register anything, if there are no objects.
        # If objects are not models, django-pg-bulk-update method is called and pg_bulk_update will register items
        if len(objs) == 0 or not isinstance(objs[0], DjangoModel):
            return super().bulk_update(objs, *args, **kwargs)

        # native django bulk_update requires each object to have a primary key
        res = super().bulk_update(objs, *args, **kwargs)
        self._register_ops('update', objs)
        return res


# I add library dependant mixins to base classes only if libraries are installed
qs_bases = [ClickHouseSyncQuerySetMixin]

if not getattr(UpdateReturningMixin, 'fake', False):
    qs_bases.append(ClickHouseSyncUpdateReturningQuerySetMixin)

if not getattr(BulkUpdateManagerMixin, 'fake', False):
    qs_bases.append(ClickHouseSyncBulkUpdateQuerySetMixin)


# QuerySet must be the last one, so it can be redeclared in mixins
qs_bases.append(DjangoQuerySet)
ClickHouseSyncQuerySet = type('ClickHouseSyncModelQuerySet', tuple(qs_bases), {})


class ClickHouseSyncManager(BaseManager.from_queryset(ClickHouseSyncQuerySet), DjangoManager):
    pass


class ClickHouseSyncModel(DjangoModel):
    """
    Base model for syncing data. Each django model synced with data must inherit this
    """
    objects = ClickHouseSyncManager()

    class Meta:
        abstract = True

    @classmethod
    def get_clickhouse_storage(cls) -> Storage:
        """
        Returns Storage instance to save clickhouse sync data to
        :return:
        """
        storage_cls = lazy_class_import(config.SYNC_STORAGE)
        return storage_cls()

    @classmethod
    def register_clickhouse_sync_model(cls, model_cls: Type['ClickHouseModel']) -> None:  # noqa: F821
        """
        Registers ClickHouse model to listen to this model updates
        :param model_cls: Model class to register
        :return: None
        """
        if not hasattr(cls, '_clickhouse_sync_models'):
            cls._clickhouse_sync_models = set()

        cls._clickhouse_sync_models.add(model_cls)

    @classmethod
    def get_clickhouse_sync_models(cls) -> Set['ClickHouseModel']:  # noqa: F821
        """
        Returns all clickhouse models, listening to this class
        :return: A set of model classes to sync
        """
        return getattr(cls, '_clickhouse_sync_models', set())

    @classmethod
    def register_clickhouse_operations(cls, operation: str, *model_pks: Any, using: Optional[str] = None) -> None:
        """
        Registers model operation in storage
        :param operation: Operation type - one of [insert, update, delete)
        :param model_pks: Elements to import
        :param using: Database alias registered instances are from
        :return: None
        """
        model_pks = ['%s.%s' % (using or config.DEFAULT_DB_ALIAS, pk) for pk in model_pks]

        def _on_commit():
            for model_cls in cls.get_clickhouse_sync_models():
                if model_cls.django_model == cls:
                    storage.register_operations_wrapped(model_cls.get_import_key(), operation, *model_pks)

        if len(model_pks) > 0:
            storage = cls.get_clickhouse_storage()
            transaction.on_commit(_on_commit, using=using)

    def post_save(self, created: bool, using: Optional[str] = None) -> None:
        self.register_clickhouse_operations('insert' if created else 'update', self.pk, using=using)

    def post_delete(self, using: Optional[str] = None) -> None:
        self.register_clickhouse_operations('delete', self.pk, using=using)


@receiver(post_save)
def post_save(sender, instance, **kwargs):
    statsd.incr('%s.sync.post_save' % config.STATSD_PREFIX, 1)
    if issubclass(sender, ClickHouseSyncModel):
        instance.post_save(kwargs.get('created', False), using=kwargs.get('using'))


@receiver(post_delete)
def post_delete(sender, instance, **kwargs):
    if issubclass(sender, ClickHouseSyncModel):
        instance.post_delete(using=kwargs.get('using'))
