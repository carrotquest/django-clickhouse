from django.db.models import Model as DjangoModel
from django.forms import model_to_dict


class Django2ClickHouseModelSerializer:
    def __init__(self, model_cls, fields=None, exclude_fields=None, writable=False):
        self._model_cls = model_cls
        if fields is not None:
            self.serialize_fields = fields
        else:
            self.serialize_fields = model_cls.fields(writable=writable).keys()

        self.exclude_serialize_fields = exclude_fields

    def serialize(self, obj):  # type: (DjangoModel) -> 'ClickHouseModel'
        # Standard model_to_dict ignores some fields if they have invalid naming
        data = {}
        for name in set(self.serialize_fields) - set(self.exclude_serialize_fields):
            val = getattr(obj, name, None)
            if val is not None:
                data[name] = val

        # Remove None values, they should be initialized as defaults
        params = {}
        for key, value in data.items():
            if value is None:
                pass
            elif isinstance(value, bool):
                params[key] = int(value)
            else:
                params[key] = value

        return self._model_cls(**params)
