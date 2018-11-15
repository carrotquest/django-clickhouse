from typing import Type

from django.db.models import Model as DjangoModel
from django.forms import model_to_dict


class Django2ClickHouseModelSerializer:
    serialize_fields = None
    exclude_serialize_fields = None

    def serialize(self, obj, model_cls, **kwargs):
        # type: (DjangoModel, Type['ClickHouseModel'], **dict) -> 'ClickHouseModel'
        data = model_to_dict(obj, self.serialize_fields, self.exclude_serialize_fields)

        # Remove None values, they should be initialized as defaults
        for key, value in data.items():
            if value is None:
                del data[key]

        return model_cls(**data)
