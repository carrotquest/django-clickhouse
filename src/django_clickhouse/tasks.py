import datetime
import importlib
from typing import Type, Union

from celery import shared_task
from django.apps import apps as django_apps
from infi.clickhouse_orm.utils import import_submodules

from django_clickhouse.clickhouse_models import ClickHouseModel
from .configuration import config
from .utils import get_subclasses, lazy_class_import


@shared_task(queue=config.CELERY_QUEUE)
def sync_clickhouse_model(model_cls: Union[Type[ClickHouseModel], str]) -> None:
    """
    Syncs one batch of given ClickHouseModel
    :param model_cls: ClickHouseModel subclass or python path to it
    :return: None
    """
    model_cls = lazy_class_import(model_cls)

    # If sync will not finish it is not fatal to set up sync period here: sync will be executed next time
    model_cls.get_storage().set_last_sync_time(model_cls.get_import_key(), datetime.datetime.now())
    model_cls.sync_batch_from_storage()


@shared_task(queue=config.CELERY_QUEUE)
def clickhouse_auto_sync() -> None:
    """
    Plans syncing models
    :return: None
    """
    # Import all model modules
    for app in django_apps.get_app_configs():
        package_name = "%s.%s" % (app.name, config.MODELS_MODULE)
        try:
            module = importlib.import_module(package_name)
            if hasattr(module, '__path__'):
                import_submodules(package_name)
        except ImportError:
            pass

    for cls in get_subclasses(ClickHouseModel, recursive=True):
        if cls.need_sync():
            # I pass class as a string in order to make it JSON serializable
            cls_path = "%s.%s" % (cls.__module__, cls.__name__)
            sync_clickhouse_model.delay(cls_path)
