from celery import shared_task
from django.conf import settings
from infi.clickhouse_orm.utils import import_submodules
from statsd.defaults.django import statsd

from django_clickhouse.clickhouse_models import ClickHouseModel
from .configuration import config
from .utils import get_subclasses


@shared_task(queue=config.CELERY_QUEUE)
def sync_clickhouse_converter(cls):  # type: (ClickHouseModel) -> None
    """
    Syncs one batch of given ClickHouseModel
    :param cls: ClickHouseModel subclass
    :return: None
    """
    statsd_key = "%s.sync.%s.time" % (config.STATSD_PREFIX, cls.__name__)
    with statsd.timing(statsd_key):
        cls.sync_batch_from_storage()


@shared_task(queue=config.CELERY_QUEUE)
def clickhouse_auto_sync():
    """
    Plans syncing models
    :return: None
    """
    # Import all model modules
    for app in settings.INSTALLED_APPS:
        import_submodules("%s.%s" % (app, config.MODELS_MODULE))

    # Start
    for cls in get_subclasses(ClickHouseModel, recursive=True):
        if cls.need_sync():
            # Даже если синхронизация вдруг не выполнится, не страшно, что мы установили период синхронизации
            # Она выполнится следующей таской через интервал.
            sync_clickhouse_converter.delay(cls)
