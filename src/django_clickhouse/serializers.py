from typing import Type

from django.db.models import Model as DjangoModel
from django.forms import model_to_dict


class Django2ClickHouseModelSerializer:
    def __init__(self, fields=None, exclude_fields=None):
        self.serialize_fields = fields
        self.exclude_serialize_fields = exclude_fields

    def serialize(self, obj, model_cls):
        # type: (DjangoModel, Type['ClickHouseModel']) -> 'ClickHouseModel'
        data = model_to_dict(obj, self.serialize_fields, self.exclude_serialize_fields)

        # Remove None values, they should be initialized as defaults
        for key, value in data.items():
            if value is None:
                del data[key]

        return model_cls(**data)
