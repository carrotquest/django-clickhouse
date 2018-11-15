"""
This file defines base abstract models to inherit from
"""
import datetime

from django.utils.timezone import now
from infi.clickhouse_orm.models import Model as InfiModel, ModelBase as InfiModelBase
from typing import Set, Union

from six import with_metaclass

from .models import ClickHouseSyncModel
from .utils import lazy_class_import
from . import config


class ClickHouseModelMeta(InfiModelBase):
    def __new__(cls, *args, **kwargs):
        res = super().__new__(cls, *args, **kwargs)  # type: ClickHouseModel

        if res.django_model and not issubclass(res.django_model, ClickHouseSyncModel):
            raise TypeError('django_model must be ClickHouseSyncModel subclass')

        if res.django_model and res.get_sync_delay():
            res.django_model.register_clickhouse_sync_model(res)

        return res


class ClickHouseModel(with_metaclass(ClickHouseModelMeta, InfiModel)):
    """
    Base model for all other models
    """
    django_model = None

    sync_batch_size = None
    sync_storage = None
    sync_delay = None

    @classmethod
    def get_sync_batch_size(cls):
        return cls.sync_batch_size or config.SYNC_BATCH_SIZE

    @classmethod
    def get_storage(cls):
        return lazy_class_import(cls.sync_storage or config.SYNC_STORAGE)

    @classmethod
    def get_sync_delay(cls):
        return cls.sync_delay or config.SYNC_DELAY

    @classmethod
    def get_import_key(cls):
        return cls.__name__

    def _prepare_val_for_eq(self, field_name, field, val):
        if isinstance(val, datetime.datetime):
            return val.replace(microsecond=0)
        elif field_name == '_version':
            return True  # Независимо от версии должно сравнение быть истиной
        return val

    def __eq__(self, other):
        if other.__class__ != self.__class__:
            return False

        for name, f in self.fields().items():
            val_1 = getattr(self, name, None)
            val_2 = getattr(other, name, None)
            if self._prepare_val_for_eq(name, f, val_1) != self._prepare_val_for_eq(name, f, val_2):
                return False

        return True

    def import_batch(self):
        """
        Imports batch to ClickHouse
        :return:
        """
        pass


    def sync_batch_from_storage(self):
        """
        Gets one batch from storage and syncs it.
        :return:
        """
        storage = self.get_storage()
        import_key = self.get_import_key()
        storage.pre_sync(import_key)
        #     1) pre_sync()
        #     2) get_import_batch(). If batch is present go to 5)
        #     3) If batch is None, call get_operations()
        #     4) Transform operations to batch and call write_import_batch()
        #     5) Import batch to ClickHouse

        batch = storage.get_import_batch(import_key)
        if batch is None:
            operations = storage.get_operations(import_key, self.get_sync_batch_size())
            batch = self.engine.get_batch(operations)
            storage.write_import_batch(import_key, batch)

        self.import_batch(batch)
        storage.post_sync(import_key)


# class ClickHouseModelConverter:
#     """
#     Абстрактный класс, описывающий процесс конвертации модели django в модель ClickHouse и обратно.
#     """
#     # Ключ REDIS, по которому синхронизируются модели
#     # Содержит словарь, где ключи = module_name:ModelConverterName,
#     # а значения - last_recalc_timestamp
#     #REDIS_SYNC_KEY = 'clickhouse_models_sync'
#     #sync_batch_size = settings.CLICKHOUSE_INSERT_SIZE
#
#     # Если этот атрибут установлен, это должно быть вермя в секундах - частота,
#     # с которой будет вызываться синхронизация модели в celery задаче
#     #auto_sync = None
#     #sync_type = settings.CLICKHOUSE_DEFAULT_SYNC_TYPE
#
#     # Возможность создавать таблицы не в миграциях, а по необходимости перед импортом
#     #auto_create_tables = False
#
#     # django_model = None
#
#     @classmethod
#     def _validate_cls_attributes(cls):
#         assert inspect.isclass(cls.django_model), \
#             "django_model static attribute must be django.db.model subclass"
#         assert issubclass(cls.django_model, django_models.Model), \
#             "django_model static attribute must be django.db.model subclass"
#         assert isinstance(cls.sync_batch_size, int) and cls.sync_batch_size > 0,\
#             "sync_batch_size must be positive integer"
#         assert cls.auto_sync is None or isinstance(cls.auto_sync, int) and cls.auto_sync > 0, \
#             "auto_sync must be positive integer, if set"
#         assert cls.sync_type in {'redis', 'postgres'}, "sync_type must be one of [redis, postgres]"
#
#     @classmethod
#     def _validate_django_model_instance(cls, obj):
#         cls._validate_cls_attributes()
#         assert isinstance(obj, cls.django_model), \
#             "obj must be instance of {0}".format(cls.django_model.__name__)
#
#     @classmethod
#     @transaction.atomic
#     def import_data(cls, using: Optional[str] = None, inserted_items=None, updated_items=None)-> int:
#         """
#         Это сервисный метод, который синхронизирует данный Converter c ClickHouse при включенной авто-синхронизации
#         :param using: Данная функция получает id только для одной БД.
#         :param inserted_items: Элементы, которые были вставлены в БД
#         :param updated_items: Элементы, которые были обновлены.
#             Оба параметра должны быть задан или не заданы.
#             Если не заданы будут получены через get_sync_items()
#             Если заданы, то необходимо указать корректный using.
#         :return: Количество импортированных элементов
#         """
#         def _shard_func(shard):
#             return cls.import_sync_items(*cls.get_sync_items(using=shard), using=shard)
#
#         cls._validate_cls_attributes()
#         assert inserted_items is None and updated_items is None \
#                or inserted_items is not None and updated_items is not None, \
#             "You must specify both inserted_items and updated_items or none of them"
#
#         from utils.sharding.models import BaseShardedManager
#         if inserted_items is not None:
#             return cls.import_sync_items(inserted_items, updated_items, using=using)
#         elif isinstance(cls.django_model.objects, BaseShardedManager):
#             res = exec_all_shards_func(_shard_func)
#             return sum(res)
#         else:
#             return _shard_func(using)
#
#     @classmethod
#     def import_sync_items(cls, inserted_items, updated_items, using: Optional[str] = None) -> int:
#         """
#         Это сервисный метод, который загружает данные в ClickHouse. Не должен делать запросов к БД.
#         sync_items уже получены ранее.
#         :param inserted_items: Элементы, которые были вставлены в БД
#         :param updated_items: Элементы, которые были обновлены
#         :param using: Данная функция получает id только для одной БД.
#         :return: Количество импортированных элементов
#         """
#         # Эта процедура игнорирует update-ы и delete-ы. Используйте для этого другие модели
#
#         if len(inserted_items) > 0:
#             res = import_data_from_queryset(cls, inserted_items, statsd_key_prefix='upload_clickhouse.' + cls.__name__,
#                                             batch_size=cls.sync_batch_size, no_qs_validation=True,
#                                             create_table_if_not_exist=cls.auto_create_tables)
#             statsd.incr('clickhouse.{0}.inserts'.format(cls.__name__), res)
#         else:
#             # Если данный вариант возникает слишком часто, это означает, что время auto_sync слишком мелнькое
#             # И вызывать синхронизацию так часто не имеет смысла.
#             statsd.incr('clickhouse.{0}.empty_inserts'.format(cls.__name__))
#             res = 0
#
#         return res
#
#     @classmethod
#     def get_sync_items(cls):
#         cls._validate_cls_attributes()
#
#         if cls.sync_type == 'postgres':
#             insert_ids, update_ids, delete_ids = cls._get_sync_ids_postgres(database=using)
#         else:  # if cls.sync_type == 'redis'
#             insert_ids, update_ids, delete_ids = cls._get_sync_ids_redis(database=using)
#
#         if update_ids:
#             # Операция UPDATE недоступна для обычной модели. Поэтому Если произошло обновление данных в БД,
#             # мы должны залогировать WARNING и проигнорировать запись
#             statsd.incr('clickhouse.{0}.invalid_operation'.format(cls.__name__))
#             log('clickhouse_import_data_warning', 'update_operation on insert only model', data={
#                 'model': cls.__name__,
#                 'model_ids': list(update_ids)
#             })
#
#         if len(insert_ids) > 0:
#             qs = cls.django_model.objects.filter(pk__in=insert_ids).using(using).nocache()
#             if select_related:
#                 qs = qs.select_related(*select_related)
#             if prefetch_related:
#                 qs = qs.prefetch_related(*prefetch_related)
#
#             insert_items = list(qs)
#         else:
#             insert_items = []
#
#         return insert_items, []
#
#     @classmethod
#     def start_sync(cls):
#         """
#         Проверяет, нужна ли модели синхронизация.
#         Если синхронизация нужна, отмечает, что синхронизация началась.
#         :return: Boolean, надо ли начинать синхронизацию
#         """
#         if cls.auto_sync is None:
#             return False
#
#         assert type(cls.auto_sync) is int and cls.auto_sync > 0, \
#             "auto_sync attribute must be positive integer if given"
#
#         # Получаем результаты предыдущей синхронизации
#         redis_dict_key = "{0}:{1}".format(cls.__module__, cls.__name__)
#
#         now_ts = int(now().timestamp())
#
#         # Сразу же делаем вид, что обновление выполнено.
#         # Если другой поток зайдет сюда, он увидит, что обновление уже выполнено
#         # В конце, если обновление не выполнялось, вернем старое значение
#         previous = settings.REDIS.pipeline().hget(ClickHouseModelConverter.REDIS_SYNC_KEY, redis_dict_key). \
#             hset(ClickHouseModelConverter.REDIS_SYNC_KEY, redis_dict_key, now_ts).execute()[0]
#
#         previous = int(previous) if previous else 0
#         result = bool(previous + cls.auto_sync < now_ts)
#         if not result:
#             # Возвращаем старое значение флагу, который мы изменили выше
#             settings.REDIS.hset(ClickHouseModelConverter.REDIS_SYNC_KEY, redis_dict_key, previous)
#
#         return result
#
#
# class ClickHouseModel(InfiModel):
#
#     @classmethod
#     def _validate_cls_attributes(cls):
#         super()._validate_cls_attributes()
#         assert inspect.isclass(cls.django_model), \
#             "django_model static attribute must be ClickHouseDjangoModel subclass"
#         assert issubclass(cls.django_model, ClickHouseDjangoModel), \
#             "django_model static attribute must be ClickHouseDjangoModel subclass"
#
#         assert type(cls.clickhouse_pk_field) is str, "pk_field attribute must be string"
#
#     @classmethod
#     def form_query(cls, select_items: Union[str, Set[str], List[str], Tuple[str]], table: Optional[str] = None,
#                    final: bool = False, date_filter_field: str = '', date_in_prewhere: bool = True,
#                    prewhere: Union[str, Set[str], List[str], Tuple[str]] = '',
#                    where: Union[str, Set[str], List[str], Tuple[str]] = '',
#                    group_fields: Union[str, Set[str], List[str], Tuple[str]] = '', group_with_totals: bool = False,
#                    order_by: Union[str, Set[str], List[str], Tuple[str]] = '',
#                    limit: Optional[int] = None, prewhere_app: bool = True):
#         """
#         Формирует запрос к данной таблице
#         :param select_items: строка или массив строк, которые надо выбрать в запросе
#         :param table: Таблица данного класса по-умолчанию
#         :param final: Позволяет выбрать из CollapsingMergeTree только последнюю версию записи.
#         :param date_filter_field: Поле, которое надо отфильтровать от start_date до date_end, если задано
#         :param date_in_prewhere: Если флаг указан и задано поле date_filter_field,
#             то условие будет помещено в секцию PREWHERE, иначе - в WHERE
#         :param prewhere: Условие, которое добавляется в prewhere
#         :param where: Условие, которое добавляется в where
#         :param group_fields: Поля, по которым будет производится группировка
#         :param group_with_totals: Позволяет добавить к группировке модификатор with_totals
#         :param order_by: Поле или массив полей, по которым сортируется результат
#         :param limit: Лимит на количество записей
#         :param prewhere_app: Автоматически добавляет в prewhere фильтр по app_id
#         :return: Запрос, в пределах приложения
#         """
#         assert isinstance(select_items, (str, list, tuple, set)), "select_items must be string, list, tuple or set"
#         assert table is None or isinstance(table, str), "table must be string or None"
#         assert isinstance(final, bool), "final must be boolean"
#         assert isinstance(date_filter_field, str), "date_filter_field must be string"
#         assert isinstance(date_in_prewhere, bool), "date_in_prewhere must be boolean"
#         assert isinstance(prewhere, (str, list, tuple, set)), "prewhere must be string, list, tuple or set"
#         assert isinstance(where, (str, list, tuple, set)), "where must be string, list, tuple or set"
#         assert isinstance(group_fields, (str, list, tuple, set)), "group_fields must be string, list, tuple or set"
#         assert isinstance(group_with_totals, bool), "group_with_totals must be boolean"
#         assert isinstance(order_by, (str, list, tuple, set)), "group_fields must be string, list, tuple or set"
#         assert limit is None or isinstance(limit, int) and limit > 0, "limit must be None or positive integer"
#         assert isinstance(prewhere_app, bool), "prewhere_app must be boolean"
#
#         table = table or '$db.`{0}`'.format(cls.table_name())
#         final = 'FINAL' if final else ''
#
#         if prewhere:
#             if not isinstance(prewhere, str):
#                 prewhere = '(%s)' % ') AND ('.join(prewhere)
#
#         if prewhere_app:
#             prewhere = '`app_id`={app_id} AND (' + prewhere + ')' if prewhere else '`app_id`={app_id}'
#
#         if prewhere:
#             prewhere = 'PREWHERE ' + prewhere
#
#         if where:
#             if not isinstance(where, str):
#                 where = ' AND '.join(where)
#             where = 'WHERE ' + where
#
#         if not isinstance(select_items, str):
#             # Исключим пустые строки
#             select_items = [item for item in select_items if item]
#             select_items = ', '.join(select_items)
#
#         if group_fields:
#             if not isinstance(group_fields, str):
#                 group_fields = ', '.join(group_fields)
#
#             group_fields = 'GROUP BY %s' % group_fields
#
#             if group_with_totals:
#                 group_fields += ' WITH TOTALS'
#
#         if order_by:
#             if not isinstance(order_by, str):
#                 order_by = ', '.join(order_by)
#
#             order_by = 'ORDER BY ' + order_by
#
#         if date_filter_field:
#             cond = "`%s` >= '{start_date}' AND `%s` < '{end_date}'" % (date_filter_field, date_filter_field)
#             if date_in_prewhere:
#                 prewhere += ' AND ' + cond
#             elif where:
#                 where += ' AND ' + cond
#             else:
#                 where = 'WHERE ' + cond
#
#         limit = "LIMIT {0}".format(limit) if limit else ''
#
#         query = '''
#           SELECT %s
#           FROM %s %s
#
#             %s
#           %s
#          %s %s %s
#         ''' % (select_items, table, final, prewhere, where, group_fields, order_by, limit)
#
#         # Моя спец функция, сокращающая запись проверки даты на Null в запросах.
#         # В скобках не может быть других скобок
#         # (иначе надо делать сложную проверку скобочной последовательности, решил пока не заморачиваться)
#         # Фактически, функция приводит выражение в скобках к timestamp и смотрит, что оно больше 0
#         query = re.sub(r'\$dateIsNotNull\s*(\([^()]*\))', r'toUInt64(toDateTime(\1)) > 0', query)
#
#         return re.sub(r'\s+', ' ', query.strip())
#
#     @classmethod
#     def init_insert(cls, model_ids, database=None):
#         """
#         Ининциирует обновление данных для объектов с укзаанными id-шниками
#         :param model_ids: Список id моделей, которые надо обновлять
#         :param database: Данная функция получает id только для одной БД.
#         :return: None
#         """
#         assert isinstance(model_ids, Iterable), "model_ids must be iterable"
#
#         if len(model_ids) > 0:
#             if cls.sync_type == 'redis':
#                 cls.django_model.register_clickhouse_operations('INSERT', *model_ids, database=(database or 'default'))
#             else:  # if self.sync_type == 'postgres'
#                 from utils.models import ClickHouseModelOperation
#                 ClickHouseModelOperation.objects.bulk_update_or_create([
#                     {'table': cls.django_model._meta.db_table,
#                      'model_id': int(x),
#                      'operation': 'INSERT',
#                      'database': (database or 'default')
#                      } for x in model_ids
#                 ], update=True, key_fields=('table', 'model_id'))
#
#     @classmethod
#     def recheck(cls, qs: DjangoBaseQuerySet) -> int:
#         """
#         В ходе импорта могут возникнуть ошибки - какие-то записи могут не быть вставлены
#         Данная функция проверяет правильность импорта данных между указанными id
#         :param qs: QuerySet с данными, которые надо проверить
#         :return: Количество некорректных записей
#         """
#         cls._validate_cls_attributes()
#         assert isinstance(qs, DjangoBaseQuerySet), "qs must be a QuerySet"
#         assert qs.model == cls.django_model, "qs model must be equal to current django_model"
#
#         item_ids = {item.pk for item in qs}
#
#         query = cls.form_query('id', where='id IN (%s)' % ','.join([str(idx) for idx in item_ids]), prewhere_app=False)
#         ch_ids = {item.id for item in settings.CLICKHOUSE_DB.select(query, model_class=cls)}
#
#         cls.init_insert(list(item_ids - ch_ids), qs.db)
#
#         return len(item_ids - ch_ids)
#
#     def _prepare_val_for_eq(self, field_name, field, val):
#         if isinstance(val, datetime.datetime):
#             return val.replace(microsecond=0)
#         elif field_name == '_version':
#             return True  # Независимо от версии должно сравнение быть истиной
#         return val
#
#     def __eq__(self, other):
#         if other.__class__ != self.__class__:
#             return False
#
#         for name, f in self.fields().items():
#             val_1 = getattr(self, name, None)
#             val_2 = getattr(other, name, None)
#             if self._prepare_val_for_eq(name, f, val_1) != self._prepare_val_for_eq(name, f, val_2):
#                 return False
#
#         return True
#
#
# class ClickHouseMultiModelConverter(ClickHouseModelConverter):
#     """
#     Поскольку в ClickHouse нельзя изменять данные, нередко одна django-модель будет разделяться
#     на несколько моделей в ClickHouse. Этот класс упрощает преобразование между моделями.
#     Например, его можно использовать в функции import_data, чтобы не выбирать исходную модель несколько раз
#     """
#     ch_models = None
#
#     def __init__(self, *ch_objects):
#         """
#         Инициирует multi-модель из объектов моделей.
#         Порядок ch_objects должен соответствовать порядку ch_models
#         :param ch_objects: Набор объектов моделей ClickHouse
#         """
#         self._validate_ch_objects(ch_objects)
#         self._ch_objects = ch_objects
#
#     @classmethod
#     def _validate_cls_attributes(cls):
#         super()._validate_cls_attributes()
#         assert isinstance(cls.ch_models, (tuple, list)), "ch_models must be tuple or list instance"
#         assert len(cls.ch_models) > 0, "ch_models can't be empty"
#         for m in cls.ch_models:
#             assert issubclass(m, ClickHouseModelConverter), \
#                 "ch_models must be a list of ClickHouseModelConverter subclasses"
#
#     @classmethod
#     def _validate_ch_objects(cls, ch_objects):
#         cls._validate_cls_attributes()
#         for index, obj in enumerate(ch_objects):
#             assert isinstance(obj, tuple(cls.ch_models)), "Parameter {0} must be instance of [{1}]". \
#                 format(index + 1, ','.join(klass.__name__ for klass in cls.ch_models))
#
#     @classmethod
#     def from_django_model(cls, obj):
#         """
#         Создает объекты модели ClickHouse из модели django
#         При переопределении метода желательно проверить аргументы, вызвав:
#             cls._validate_django_model_instance(obj)
#         :param obj: Объект модели django
#         :return: Список объектов моделей ClickHouse в том порядке, в каком они указаны в ch_models
#         """
#         cls._validate_django_model_instance(obj)
#         cls._validate_cls_attributes()
#
#         res = []
#         for m in cls.ch_models:
#             item = m.from_django_model(obj)
#             if isinstance(item, list):
#                 res.extend(item)
#             else:
#                 res.append(item)
#         return res
#
#     def to_django_model(self, obj=None):
#         # Валидируем входящие данные
#         if obj is not None:
#             self._validate_django_model_instance(obj)
#         else:
#             self._validate_cls_attributes()
#             obj = self.django_model()
#
#         # Обновление по всем моделям
#         for model_obj in self._ch_objects:
#             model_obj.to_django_model(obj)
#
#         return obj
#
#     @classmethod
#     def import_sync_items(cls, inserted_items, updated_items, using: Optional[str] = None) -> int:
#         return sum([
#             m.import_sync_items(inserted_items=inserted_items, updated_items=updated_items, using=using)
#             for m in cls.ch_models
#         ])
#
#     @classmethod
#     def get_sync_items(cls, using=None, select_related=None, prefetch_related=None):
#         """
#         Формирует 2 списка объектов модели для вставки и обновления данных.
#         :param using: Данная функция получает id только для одной БД.
#         :param select_related: Позволяет выбрать данные через QuerySet.select_related()
#         :param prefetch_related: Позволяет выбрать данные через QuerySet.prefetch_related()
#         :return: Insert items, Update items
#         """
#         cls._validate_cls_attributes()
#
#         if cls.sync_type == 'postgres':
#             insert_ids, update_ids, delete_ids = cls._get_sync_ids_postgres(database=using)
#         else:  # if cls.sync_type == 'redis'
#             insert_ids, update_ids, delete_ids = cls._get_sync_ids_redis(database=using)
#
#         # Выберем одним запросом из БД, а затем преераспределим
#         ids_set = insert_ids | update_ids | delete_ids
#         if len(ids_set) > 0:
#             qs = cls.django_model.objects.filter(pk__in=ids_set).using(using).nocache()
#             if select_related:
#                 qs = qs.select_related(*select_related)
#             if prefetch_related:
#                 qs = qs.prefetch_related(*prefetch_related)
#
#             insert_items, update_items, delete_items = [], [], []
#             for pk, instance in qs.models_dict().items():
#                 if pk in insert_ids:
#                     insert_items.append(instance)
#                 else:  # if pk in update_ids:
#                     # Вставка данных приоритетна перед update-ом
#                     update_items.append(instance)
#         else:
#             insert_items, update_items = [], []
#
#         return insert_items, update_items
#
#
# class ClickHouseCollapseModel(ClickHouseModel):
#     """
#     Добавлеяет операции для таблиц с engine CollapsingMergeTree
#     https://clickhouse.yandex/reference_ru.html#CollapsingMergeTree
#     """
#     # Это поле в ClickHouse, в котором хранится знак строки для подсчета
#     clickhouse_sign_field = "sign"
#
#     # Это номер версии вставленной записи. Добавляется автоматически.
#     # Служит для определения последней вставленной записи с одинаковым первичным ключом
#     _version = fields.UInt32Field(default=0)
#
#     @classmethod
#     def _validate_cls_attributes(cls):
#         super()._validate_cls_attributes()
#         assert inspect.isclass(cls.django_model), \
#             "django_model static attribute must be ClickHouseDjangoModel subclass"
#         assert issubclass(cls.django_model, ClickHouseDjangoModel), \
#             "django_model static attribute must be ClickHouseDjangoModel subclass"
#         assert type(cls.clickhouse_sign_field) is str, "sign_field attribute must be string"
#
#     @classmethod
#     def get_sync_items(cls, using=None, select_related=None, prefetch_related=None):
#         """
#         Формирует 2 списка объектов модели для вставки и обновления данных.
#         :param using: Данная функция получает id только для одной БД.
#         :param select_related: Позволяет выбрать данные через QuerySet.select_related()
#         :param prefetch_related: Позволяет выбрать данные через QuerySet.prefetch_related()
#         :return: Insert items, Update items
#         """
#         cls._validate_cls_attributes()
#
#         if cls.sync_type == 'postgres':
#             insert_ids, update_ids, delete_ids = cls._get_sync_ids_postgres(database=using)
#         else:  # if cls.sync_type == 'redis'
#             insert_ids, update_ids, delete_ids = cls._get_sync_ids_redis(database=using)
#
#         # Выберем одним запросом из БД, а затем преераспределим
#         ids_set = insert_ids | update_ids | delete_ids
#         if len(ids_set) > 0:
#             qs = cls.django_model.objects.filter(pk__in=ids_set).using(using).nocache()
#             if select_related:
#                 qs = qs.select_related(*select_related)
#             if prefetch_related:
#                 qs = qs.prefetch_related(*prefetch_related)
#
#             insert_items, update_items, delete_items = [], [], []
#             for pk, instance in qs.models_dict().items():
#                 if pk in insert_ids:
#                     insert_items.append(instance)
#                 else:  # if pk in update_ids:
#                     # Вставка данных приоритетна перед update-ом
#                     update_items.append(instance)
#         else:
#             insert_items, update_items = [], []
#
#         return insert_items, update_items
#
#     @classmethod
#     def import_sync_items(cls, inserted_items, updated_items, using: Optional[str] = None):
#         if len(inserted_items) > 0 or len(updated_items) > 0:
#             # Эта операция удалит данные из ClickHouse, но не вставит обновленные данные
#             update_list = cls.update(updated_items, fake_insert=True)
#
#             # Посчитаем, сколько еще можем сделать INSERT-ов
#             updated_count = len(update_list)
#             # Умножим update_count на 2, поскольку мы уже вставили записи с -1
#             insert_count = cls.sync_batch_size - updated_count * 2
#
#             # Импорт новых моделей
#             insert_data = inserted_items[:insert_count]
#             update_list.extend(insert_data)
#
#             # Возвращаем в БД невсатвленные данные
#             restore_model_ids = {item.pk for item in inserted_items[insert_count:]}
#             cls.init_insert(restore_model_ids, database=using)
#
#             # Вставляем данные
#             res = import_data_from_queryset(cls, update_list, statsd_key_prefix='upload_clickhouse.' + cls.__name__,
#                                             batch_size=cls.sync_batch_size, no_qs_validation=True,
#                                             create_table_if_not_exist=cls.auto_create_tables)
#
#             # Отмечаем в графане
#             statsd.incr('clickhouse.{0}.inserts'.format(cls.__name__), len(insert_data))
#             statsd.incr('clickhouse.{0}.updates'.format(cls.__name__), updated_count)
#
#             res += updated_count
#         else:
#             # Если данный вариант возникает слишком часто, это означает, что время auto_sync слишком мелнькое
#             # И вызывать синхронизацию так часто не имеет смысла.
#             statsd.incr('clickhouse.{0}.empty_inserts'.format(cls.__name__))
#             res = 0
#
#         return res
#
#     @classmethod
#     @transaction.atomic
#     def delete(cls, django_qs):
#         """
#         Удаляет в ClickHouse все записи из указанного QuerySet
#         :param django_qs: iterable объектов моделей, которые будут удаляться
#         :return: Последние версии удаленных данных (словарь id: version)
#         """
#         assert isinstance(django_qs, Iterable), "django_qs must be iterable"
#         upd_list = list(django_qs)
#
#         if len(upd_list) == 0:
#             return {}
#
#         # Если используется ClickHousePartitionedModel, удаление должно производится из нужных таблиц, а не общей.
#         # В этом случае в запросе max_query движком Merge будет выбран столбец _table, содержащий имя таблицы.
#         # Запрос на удаление из каждой таблицы мы будем делать по отдельности.
#         is_merge_table = issubclass(cls, ClickHousePartitionedModel)
#
#         # Получаем данные для вставки
#         field_list = cls._get_model_fields()
#         field_str = ','.join(field_list)
#
#         params = {
#             'table': cls.table_name(),
#             'fields_with_sign': field_str + ',' + cls.clickhouse_sign_field,
#             'pk_values': ','.join(str(upd.pk) for upd in upd_list),
#             'fields': field_str,
#             'sign_field': cls.clickhouse_sign_field,
#             'pk_field': cls.clickhouse_pk_field,
#             'merge_table': ', _table' if is_merge_table else ''
#         }
#
#         # Запрос на получение последних актуальных версий данных, которые надо зачистить.
#         # _version содержит удаляемую версию данных
#         # Если модель наследует ClickHousePartitionedModel, запрос сделаем к Merge-таблице, получим столбец _table.
#         max_query = "SELECT {pk_field} AS pk, MAX(_version) AS _version{merge_table} FROM $db.{table} " \
#                     "WHERE {pk_field} IN ({pk_values}) GROUP BY {pk_field}{merge_table}".format(**params)
#
#         t1 = time.time()
#         max_ids_list = list(settings.CLICKHOUSE_DB.select(max_query))
#         t2 = time.time()
#         statsd.timing('clickhouse.{0}.get_max_ids_list'.format(cls.__name__), t2 - t1)
#
#         if len(max_ids_list):
#             # Разделяем данные по таблицам, из которых реально удаляются данные
#             if is_merge_table:
#                 max_version_ids_by_table = defaultdict(list)
#                 for item in max_ids_list:
#                     max_version_ids_by_table[item._table].append("({0}, {1})".format(item.pk, item._version))
#             else:
#                 # Тут всегда одна таблица
#                 max_version_ids_by_table = {cls.table_name(): ["({0}, {1})".format(item.pk, item._version)
#                                                                for item in max_ids_list]}
#
#             for tbl, ids_list in max_version_ids_by_table.items():
#                 params['max_version_ids'] = ','.join(ids_list)
#                 params['table'] = tbl
#
#                 # "Удаляем" старые данные, вставляя записи с отрицательным знаком
#                 # Поскольку выборка происходит из ClickHouse удаление еще недобавленных туда записей невозможно
#                 query = "INSERT INTO $db.{table} ({fields_with_sign}, _version) " \
#                         "SELECT {fields}, -1 AS sign, _version FROM $db.{table} " \
#                         "WHERE ({pk_field}, _version) IN ({max_version_ids})". \
#                     format(**params)
#
#                 t1 = time.time()
#                 settings.CLICKHOUSE_DB.raw(query)
#                 t2 = time.time()
#                 statsd.timing('clickhouse.{0}.delete'.format(cls.__name__), t2 - t1)
#
#         return {item.pk: item._version for item in max_ids_list}
#
#     @classmethod
#     @transaction.atomic
#     def update(cls, django_qs, fake_insert=False):
#         """
#         Обновляет в ClickHouse все записи из указанного QuerySet
#         :param django_qs: iterable объектов моделей, которые будут обновляться
#         :param fake_insert: Если флаг установлен, то данные будут удалены, но не будут вставлены (удобнов в импорте).
#             Вместо количества обновлений будет возвращен список объектов для вставки через import_data_from_queryset
#         :return: Количество обновленных данных, если fake_insert=False, иначе список объектов django-модели
#         """
#         upd_list = list(django_qs)
#
#         # Удаляем предыдущие данные
#         max_ids_dict = cls.delete(django_qs)
#
#         # Теперь вставим обновленные данные
#
#         # Устанавливаем версию для новых элементов
#         insert_list = []
#         for item in django_qs:
#             item._version = max_ids_dict.get(item.pk, -1) + 1
#             insert_list.append(item)
#
#         if fake_insert:
#             return insert_list
#         else:
#             res = import_data_from_queryset(cls, insert_list, statsd_key_prefix='upload_clickhouse.' + cls.__name__,
#                                             batch_size=cls.sync_batch_size, no_qs_validation=True,
#                                             create_table_if_not_exist=cls.auto_create_tables)
#             statsd.incr('clickhouse.{0}.updates'.format(cls.__name__), len(upd_list))
#             return res
#
#     @classmethod
#     def optimize(cls, partition=None, final=False):
#         """
#         Вызывает оптимизацию таблицы в ClickHouse.
#         https://clickhouse.yandex/reference_ru.html#OPTIMIZE
#         :param final: Если указан, то оптимизация будет производиться даже когда все данные уже лежат в одном куске
#         :param partition: Если указана, то оптимизация будет производиться только для указаной партиции
#         :return: None
#         """
#         assert partition is None or isinstance(partition, django_models.six.string_types)
#         assert type(final) is bool, "final must be boolean"
#         assert not final or partition, "final flag is meaningful only wit specified partition"
#
#         partition_string = 'PARTITION ' + partition if partition else ''
#         final_string = 'FINAL' if final else ''
#         settings.CLICKHOUSE_DB.raw("OPTIMIZE TABLE $db.`{0}` {1} {2};".format(cls.table_name(), partition_string,
#                                                                               final_string))
#
#     @classmethod
#     def init_update(cls, model_ids: IterableType[int], database: Optional[str] = None) -> None:
#         """
#         Ининциирует обновление данных для объектов с укзаанными id-шниками
#         :param model_ids: Список id моделей, которые надо обновлять
#         :param database: Данная функция получает id только для одной БД.
#         :return: None
#         """
#         assert isinstance(model_ids, Iterable), "model_ids must be iterable"
#
#         if len(model_ids) > 0:
#             if cls.sync_type == 'redis':
#                 cls.django_model.register_clickhouse_operations('UPDATE', *list(model_ids), database=database)
#             else:  # if self.sync_type == 'postgres'
#                 from utils.models import ClickHouseModelOperation
#                 ClickHouseModelOperation.objects.bulk_update_or_create([
#                     {'table': cls.django_model._meta.db_table,
#                      'model_id': int(x),
#                      'operation': 'UPDATE',
#                      'database': (database or 'default')}
#                     for x in model_ids
#                 ], update=False, key_fields=('table', 'model_id'))
#
#     @classmethod
#     def _get_model_fields(cls) -> List[str]:
#         """
#         Возвращает поля модели без учета readonly, sign и _version
#         :return: List имен полей
#         """
#         if not isinstance(cls._fields, dict):
#             # DEPRECATED для старой infi.clickhouse-orm
#             field_list = [f[0] for f in cls._fields
#                           if not f[1].readonly and f[0] != cls.clickhouse_sign_field and f[0] != '_version']
#         else:
#             field_list = [f_name for f_name, f in cls._fields.items()
#                           if not f.readonly and f_name != cls.clickhouse_sign_field and f_name != '_version']
#         return field_list
#
#     @classmethod
#     def correct_minus_records(cls):
#         """
#         Проверяет правильность импорта элементов модели и исправляет их при необходимости.
#         Импорт неправилен, если сумма sign по первичному ключу меньше 0
#         :return: Количество исправлений
#         """
#         # Ищем кривые данные
#         query = 'SELECT DISTINCT(id), SUM(sign) AS sum FROM $db.`{table}` GROUP BY id HAVING sum <= 0'. \
#             format(table=cls.table_name())
#         data = set(settings.CLICKHOUSE_DB.select(query))
#         minus_ids = {item.id for item in data if item.sum < 0}
#         zero_ids = {item.id for item in data if item.sum == 0}
#         if len(minus_ids) > 0 or len(zero_ids) > 0:
#             joined_minus_ids = ', '.join([str(x) for x in minus_ids])
#             joined_zero_ids = ', '.join([str(x) for x in zero_ids])
#             print('Ids with minus sum ({0} items): {1}'.format(len(minus_ids), joined_minus_ids or 'not found'))
#             print('Ids with zero sum ({0} items): {1}'.format(len(zero_ids), joined_zero_ids or 'not found'))
#
#             # Дополняем до корректных записей
#             field_list = cls._get_model_fields()
#             field_str = ','.join(field_list)
#
#             # 1000 здесь взята от балды, как большой _version, который больше всех существующих в системе
#             if zero_ids and minus_ids:
#                 joined_zero_ids = ', ' + joined_zero_ids
#
#             if minus_ids:
#                 minus_ids_select = '''
#                     SELECT DISTINCT {fields} FROM $db.`{table}`
#                     WHERE id IN ({minus_ids})
#                     UNION ALL
#                 '''.format(fields=field_str, table=cls.table_name(), minus_ids=joined_minus_ids)
#             else:
#                 minus_ids_select = ''
#
#             upd_query = '''
#                 INSERT INTO $db.`{table}` ({fields}, _version, sign)
#                 SELECT *, toUInt32(1000) AS _version, toInt8(1) AS sign FROM (
#                     {minus_ids_select}
#                     SELECT DISTINCT {fields} FROM $db.`{table}`
#                     WHERE id IN ({minus_ids}{zero_ids})
#                 )
#             '''.format(fields=field_str, table=cls.table_name(), minus_ids=joined_minus_ids, zero_ids=joined_zero_ids,
#                        minus_ids_select=minus_ids_select)
#             settings.CLICKHOUSE_DB.raw(upd_query)
#
#             print("Data corrected")
#
#             # Инициируем обновление
#             cls.init_update(minus_ids | zero_ids)
#             print("Updates were planned")
#
#     @classmethod
#     def recheck(cls, qs: DjangoBaseQuerySet) -> int:
#         """
#         В ходе импорта могут возникнуть ошибки - какие-то записи могут не быть вставлены или некорректны
#         Данная функция проверяет правильность импорта данных между указанными id
#         :param qs: QuerySet с данными, которые надо проверить
#         :return: Количество некорректных записей
#         """
#         cls._validate_cls_attributes()
#         assert isinstance(qs, DjangoBaseQuerySet), "qs must be a QuerySet"
#         assert qs.model == cls.django_model, "qs model must be equal to current django_model"
#
#         items = list(qs)
#         item_ids = [str(item.pk) for item in items]
#
#         query = cls.form_query('*', where='id IN (%s)' % ','.join(item_ids), final=True, prewhere_app=False)
#         ch_qs = {item.id: item for item in settings.CLICKHOUSE_DB.select(query, model_class=cls)}
#
#         insert_ids, update_ids = set(), set()
#         for item in items:
#             if item.id in ch_qs:
#                 if ch_qs[item.id] != cls.from_django_model(item):
#                     update_ids.add(item.id)
#             else:
#                 insert_ids.add(item.id)
#
#         cls.init_insert(insert_ids, database=qs.db)
#         cls.init_update(update_ids, database=qs.db)
#
#         return len(insert_ids) + len(update_ids)
#
#
# class ClickHousePartitionedModelMeta(models.ModelBase):
#     def __new__(cls, clsname, superclasses, attributedict):
#         res = super().__new__(cls, clsname, superclasses, attributedict)
#
#         # Далее нужны if-ы, чтобы корректно отработать ситуации:
#         # - с неопределенным свойством
#         # - с зацикливанием при отрабатываании _base_class_factory
#
#         if clsname != 'ClickHousePartitionedModel':
#             # Подменяем engine - модель будет работать с движком Merge
#             if hasattr(res, 'engine') and not hasattr(res, 'base_engine'):
#                 res.base_engine = res.engine
#                 res.engine = Merge('^' + res.table_name() + '_')
#
#             # Подменяем from_django_model, чтобы он подменял таблицу в генерируемом инстансе
#             if hasattr(res, 'from_django_model') and not hasattr(res, 'base_from_django_model'):
#                 res.base_from_django_model = res.from_django_model
#                 res.from_django_model = res._from_django_model
#
#         return res
#
#
# class ClickHousePartitionedModel(models.MergeModel, ClickHouseModel, metaclass=ClickHousePartitionedModelMeta):
#     """
#     Этот класс служит для разделения данных на несколько таблиц в ClickHouse.
#     При этом объекты базовой модели автоматически распределяются по таблицам
#     с помощью метода get_partition_key(django_instance)
#
#     Стандартный db.insert будет поднимать исключение (так как движок Merge), что нас устраивает.
#     Стандартный db.select(model_class=THIS) будет делать запросы к Merge таблице, что не эффективно, но работает:
#         делает запросы параллельно во все таблицы, получая результат
#     Для получения данных из конкретной таблицы лучше использовать метод select_by_partition_key(key).
#
#     Для того, чтобы исползовать другой базовый подкласс ClickHouseModelConverter достаточно унаследовать дочерний класс
#     от обоих классов.
#
#     Пример:
#     class TestPartitionedModel(ClickHousePartitionedModel, ClickHouseCollapseModel):
#         auto_sync = 5
#         django_model = TestModel
#
#         @classmethod
#         def get_partition_key(cls, django_instance):
#             ...
#
#         def to_django_model(self, obj=None):
#             ...
#
#         @classmethod
#         def from_django_model(cls, obj)
#             ...
#
#     """
#     auto_create_tables = True
#
#     @classmethod
#     def get_partition_key(cls, django_instance):
#         """
#         Получает из объекта исходной django-модели строковый ключ, по которому разделяются данные.
#         :param django_instance: Объект Django
#         :return: Строка - ключ разделения таблиц
#         """
#         raise NotImplementedError('Method get_partition_key(django_instance) must is not implemented')
#
#     @classmethod
#     def base_class_factory(cls, partition_key):
#         """
#         Factory для подкласса с конкретной таблицей
#         :param partition_key: Ключ разделения таблиц
#         :return:
#         """
#         BaseClass = models.ModelBase(cls.__name__, (cls,), {})
#
#         # Подмена имени таблицы
#         BaseClass.base_table_name = cls.table_name
#         BaseClass.table_name = lambda _=None: cls.table_name() + '_' + partition_key
#
#         # Движок у базовой таблицы должен быть исходный, а не Merge
#         BaseClass.engine = cls.base_engine
#
#         # В базовую модель можно вставлять данные
#         BaseClass.readonly = False
#
#         # Метод не должен проверять движок Merge и устанавливать для него таблицу
#         BaseClass.set_database = ClickHouseModel.set_database
#
#         # HACK Закостылял переопределением метода
#         # BaseClass.create_table_sql = super(models.MergeModel, cls).create_table_sql
#
#         # У базовой модели этого поля нет
#         if hasattr(BaseClass, '_fields'):
#             BaseClass._fields.pop('_table', None)
#
#         return BaseClass
#
#     @classmethod
#     def _from_django_model(cls, obj):
#         res = cls.base_from_django_model(obj)
#         partition_key = cls.get_partition_key(obj)
#
#         # Подменяем класс на базовый
#         BaseClass = cls.base_class_factory(partition_key)
#
#         # Некоторые преобразователи могут вернуть списки значений, а не одно.
#         if isinstance(res, list):
#             for item in res:
#                 if isinstance(item, cls):
#                     item.__class__ = BaseClass
#         else:
#             res.__class__ = BaseClass
#
#         return res
#
#     @classmethod
#     def select_by_partition_key(cls, db, partition_key, query):
#         """
#         Делает select из конкретной таблицы (не через движок Merge)
#         :param db: База данных, из которой делается выборка
#         :param partition_key: Ключ конкретной таблицы (как его возвращает метод get_partition_key)
#         :param query: Запрос к БД
#         :return: Результат select-а
#         """
#         assert isinstance(db, Database), "db parameter must be infi.clickhouse_orm.database.Database model instance"
#         assert isinstance(partition_key, six.string_types), "partition_key parameter must be string"
#         assert isinstance(query, six.string_types), "query parameter must be string"
#
#         BaseClass = cls.base_class_factory(partition_key)
#
#         return db.select(query, model_class=BaseClass)
#
#     @property
#     def partition_key(self):
#         if hasattr(self, 'base_table_name'):
#             table = self.base_table_name()
#         else:
#             table = self.table_name()
#         div_index = len(table) + 1
#         return self._table[div_index:]
#
#     @classmethod
#     def create_table_sql(cls, db):
#         # HACK Мне надо избежать assert-а на Merge модель, чтобы создавать таблицы динамически
#         from infi.clickhouse_orm.models import MergeModel
#         return super(MergeModel, cls).create_table_sql(db)
#
