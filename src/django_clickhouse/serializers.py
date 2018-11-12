from typing import Any
from django.db.models import DjangoModel

from .clickhouse_models import ClickHouseModel


class Serializer:
    def serialize(self, value: Any) -> Any:
        pass


class DefaultDjango2ClickHouseSerializer(Serializer):
    @staticmethod
    def _convert_none(values, fields_dict):
        """
        ClickHouse не хранит значения NULL, потэтому для них сохраняются невалидные значения параметра.
        Преобразует все значения, воспринимаемые как None в None
        :param values: Словарь с данными, которые надо преобразовывать
        :param fields_dict: Итерируемый объект имен полей, которые надо преобразовывать
        :return: Преобразованный словарь
        """
        assert isinstance(values, dict), "values parameter must be a dict instance"
        result = values.copy()
        for key in fields_dict:
            if isinstance(values[key], datetime.date) and values[key] == datetime.date(1970, 1, 1) \
                    or (isinstance(values[key], datetime.datetime)
                        and (values[key] in (datetime.datetime(1970, 1, 1, 0, 0, 0),
                                             datetime.datetime(1970, 1, 1, 0, 0, 0, tzinfo=pytz.utc)))) \
                    or type(values[key]) is int and values[key] == 0 \
                    or type(values[key]) is str and values[key] == '':
                result[key] = None
        return result

    @staticmethod
    def _convert_bool(values, fields_dict):
        """
        ClickHouse не хранит значения Bool, потэтому для них сохраняются UInt8.
        :param values: Словарь с данными, которые надо преобразовывать
        :param fields_dict: Итерируемый объект имен полей, которые надо преобразовывать
        :return: Преобразованный словарь
        """
        assert isinstance(values, dict), "values parameter must be a dict instance"
        result = values.copy()
        for key in fields_dict:
            result[key] = bool(result[key])
        return result

    @staticmethod
    def _enum_to_str(values):
        """
        Преобразует все значения типа Enum в их строковые представления
        :param values: Словарь с данными, которые надо преобразовывать
        :return: Преобразованный словарь
        """
        assert isinstance(values, dict), "values parameter must be a dict instance"
        return {key: val.name if isinstance(val, Enum) else val for key, val in values.items()}

    @classmethod
    def from_django_model(cls, obj):
        """
        Создает объект модели ClickHouse из модели django
        При переопределении метода желательно проверить аргументы, вызвав:
            cls._validate_django_model_instance(obj)
        :param obj: Объект модели django
        :return: Объект модели ClickHouse или список таких объектов
        """
        cls._validate_django_model_instance(obj)
        raise ClickHouseError('Method "from_django_model" is  not implemented')

    def to_django_model(self, obj=None):
        """
        Конвертирует эту модель в объект модели django, если это возможно.
        Если невозможно - должен поднять исключение.
        :param obj: Если передан, то надо не создать новый объект модели django, а обновить существующий
        :return: Объект модели django
        """
        if obj is not None:
            self._validate_django_model_instance(obj)
        else:
            self._validate_cls_attributes()
        raise ClickHouseError('Method "to_django_model" is not implemented')

    def serialize(self, value: DjangoModel) -> ClickHouseModel:
        pass