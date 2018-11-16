"""
This file defines router to find appropriate database
"""
import random
from typing import Optional

import six

from .clickhouse_models import ClickHouseModel
from .configuration import config
from .database import connections
from .utils import lazy_class_import


class DefaultRouter:
    def db_for_read(self, model, **hints):
        # type: (ClickHouseModel, **dict) -> str
        """
        Gets database to read from for model
        :param model: Model to decide for
        :param hints: Some hints to make correct choice
        :return: Database alias
        """
        return random.choice(model.read_db_aliases)

    def db_for_write(self, model, **hints):
        # type: (ClickHouseModel, **dict) -> str
        """
        Gets database to write to for model
        :param model: Model to decide for
        :param hints: Some hints to make correct choice
        :return: Database alias
        """
        return random.choice(model.write_db_aliases)

    def allow_migrate(self, db_alias, app_label, model=None, **hints):
        # type: (str, str, Optional[ClickHouseModel], **dict) -> bool
        """
        Checks if migration can be applied to given database
        :param db_alias: Database alias to check
        :param app_label: App from which migration is got
        :param model: Model migration is applied to
        :param hints: Hints to make correct decision
        :return: boolean
        """
        if connections[db_alias].readonly:
            return False

        if hints.get("force_migrate_on_databases", None):
            return db_alias in hints["force_migrate_on_databases"]

        if hints.get('model'):
            model = '%s.%s.%s' % (app_label, config.MODELS_MODULE, hints['model']) \
                if isinstance(hints['model'], six.string_types) else hints['model']

        model = lazy_class_import(model)
        return db_alias in model.write_db_aliases
