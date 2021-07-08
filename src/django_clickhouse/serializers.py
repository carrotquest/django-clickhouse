from typing import NamedTuple, Optional, Iterable, Type

import pytz
from django.db.models import Model as DjangoModel

from .utils import model_to_dict


class Django2ClickHouseModelSerializer:
    def __init__(self, model_cls: Type['ClickHouseModel'], fields: Optional[Iterable[str]] = None,  # noqa: F821
                 exclude_fields: Optional[Iterable[str]] = None, writable: bool = False,
                 defaults: Optional[dict] = None) -> None:
        """
        Initializes serializer
        :param model_cls: ClickHouseModel subclass to serialize to
        :param fields: Optional. A list of fields to add into result tuple
        :param exclude_fields: Fields to exclude from result tuple
        :param writable: If fields parameter is not set directly,
          this flags determines if only writable or all fields should be taken from model_cls
        :param defaults: A dictionary of field: value which are taken as default values for model_cls instances
        :return: None
        """
        self._model_cls = model_cls
        if fields is not None:
            self.serialize_fields = fields
        else:
            self.serialize_fields = model_cls.fields(writable=writable).keys()

        self.exclude_serialize_fields = exclude_fields
        self._result_class = self._model_cls.get_tuple_class(defaults=defaults)
        self._fields = self._model_cls.fields(writable=False)

    def _get_serialize_kwargs(self, obj: DjangoModel) -> dict:
        data = model_to_dict(obj, fields=self.serialize_fields, exclude_fields=self.exclude_serialize_fields)

        # Remove None values, they should be initialized as defaults
        result = {
            key: self._fields[key].to_python(value, pytz.utc)
            for key, value in data.items() if value is not None
        }

        return result

    def serialize(self, obj: DjangoModel) -> NamedTuple:
        return self._result_class(**self._get_serialize_kwargs(obj))
