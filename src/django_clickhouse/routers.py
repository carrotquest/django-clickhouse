"""
This file defines router to find appropriate database
"""
from typing import Type

import random
from infi.clickhouse_orm.migrations import Operation, DropTable, CreateTable

from .clickhouse_models import ClickHouseModel
from .configuration import config
from .utils import lazy_class_import


class DefaultRouter:
    def db_for_read(self, model: Type[ClickHouseModel], **hints) -> str:
        """
        Gets database to read from for model
        :param model: Model to decide for
        :param hints: Some hints to make correct choice
        :return: Database alias
        """
        return random.choice(model.read_db_aliases)

    def db_for_write(self, model: Type[ClickHouseModel], **hints) -> str:
        """
        Gets database to write to for model
        :param model: Model to decide for
        :param hints: Some hints to make correct choice
        :return: Database alias
        """
        return random.choice(model.write_db_aliases)

    def allow_migrate(self, db_alias: str, app_label: str, operation: Operation, **hints) -> bool:
        """
        Checks if migration can be applied to given database
        :param db_alias: Database alias to check
        :param app_label: App from which migration is got
        :param operation: Operation object to perform
        :param hints: Hints to make correct decision
        :return: boolean
        """
        if hints.get("force_migrate_on_databases", None):
            return db_alias in hints["force_migrate_on_databases"]

        model = hints.get('model') or getattr(operation, 'model_class', None)
        if model is None:
            raise ValueError('"model_class" attribute is not defined for operation "%s". '
                             'Please provide "force_migrate_on_databases" or "model" in hints.'
                             % operation.__class__.__name__)

        model = '%s.%s.%s' % (app_label, config.MODELS_MODULE, model) \
            if isinstance(model, str) else model
        model = lazy_class_import(model)

        if operation.__class__ not in {CreateTable, DropTable}:
            return db_alias in model.migrate_replicated_db_aliases
        else:
            return db_alias in model.migrate_non_replicated_db_aliases
