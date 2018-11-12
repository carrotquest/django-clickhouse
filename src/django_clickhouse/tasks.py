
@shared_task(queue='service')
@statsd.timer('clickhouse.import_clickhouse_model_objects')
def import_clickhouse_model_objects(model_objects, statsd_key_prefix='upload_clickhouse.all',
                                    batch_size=settings.CLICKHOUSE_INSERT_SIZE,
                                    create_table_if_not_exist=False):
    """
    Загрузка данных в Yandex ClickHouse
    :param model_objects: Список объектов модели ClickHouse
    :param statsd_key_prefix: Префикс для statsd, чтобы выделить какую-то группу запросов с отдельным ключом.
        Пишется время обработки данных (data_prepare) и время выполнения запроса (request)
    :param batch_size: Максимальное количество данных, вставляемых за 1 INSERT запрос
    :param create_table_if_not_exist: Если флаг установлен, перед вставкой данных проверит существование таблицы
    и создаст ее, если ее нет
    :return: Количество импортированных элементов
    """
    assert isinstance(model_objects, Iterable), "model_objects must be Iterable"
    for obj in model_objects:
        assert isinstance(obj, models.Model), "model_objects must contain infi.clickhouse_orm.models.Model instances"

    if not model_objects:
        return 0

    t1 = time.time()
    # Поскольку составные модели могут вернуть не один объект, а список объектов,
    # то необходимо разбить их по таблице назначения
    convert_items = defaultdict(list)
    for obj in model_objects:
        convert_items[(obj.__class__, obj.table_name())].append(obj)

    t2 = time.time()
    statsd.timing(statsd_key_prefix + '.import_clickhouse_model_objects.convert', t2 - t1)

    for (model_class, _), model_group in convert_items.items():
        if create_table_if_not_exist:
            settings.CLICKHOUSE_DB.create_table(model_class)
        settings.CLICKHOUSE_DB.insert(model_group, batch_size=batch_size)
    t3 = time.time()
    statsd.timing(statsd_key_prefix + '.import_clickhouse_model_objects.insert', t3 - t2)

    return len(model_objects)


@shared_task(queue='service')
@statsd.timer('clickhouse.import_data_from_queryset')
def import_data_from_queryset(ch_model, django_qs, statsd_key_prefix='upload_clickhouse.all',
                              batch_size=settings.CLICKHOUSE_INSERT_SIZE, no_qs_validation=False,
                              create_table_if_not_exist=False):
    """
    Загрузка данных в Yandex ClickHouse
    :param ch_model: infi model to import data to. It must have from_django_model() method
    :param django_qs: QuerySet элементов, которые надо загрузить
    :param statsd_key_prefix: Префикс для statsd, чтобы выделить какую-то группу запросов с отдельным ключом.
        Пишется время обработки данных (data_prepare) и время выполнения запроса (request)
    :param batch_size: Максимальное количество данных, вставляемых за 1 INSERT запрос
    :param no_qs_validation: Иногда удобно передать не QuerySet, а сразу список объектов модели.
        Тогда мы его не будем валидировать.
    :param create_table_if_not_exist: Если флаг установлен, перед вставкой данных проверит существование таблицы
        и создаст ее, если ее нет
    :return: Количество импортированных элементов
    """

    def _update_version(model_obj, django_obj):
        if hasattr(model_obj, '_version'):
            model_obj._version = getattr(django_obj, '_version', 0)

        return model_obj

    assert inspect.isclass(ch_model), "ch_model must be a subclass of ClickHouseModelConverter"
    assert issubclass(ch_model, ClickHouseModelConverter), "ch_model must be a subclass of ClickHouseModelConverter"
    if not no_qs_validation:
        assert isinstance(django_qs, django_models.QuerySet) and django_qs.model == ch_model.django_model, \
            "django_qs must be queryset of {0}".format(ch_model.django_model.__name__)

    t1 = time.time()
    items = list(django_qs)
    t2 = time.time()
    if not items:
        statsd.timing(statsd_key_prefix + '.import_data_from_queryset.empty_inserts', t2 - t1)
        return

    statsd.timing(statsd_key_prefix + '.import_data_from_queryset.fetch', t2 - t1)

    insert_items = []
    for item in items:
        convert_res = ch_model.from_django_model(item)
        if isinstance(convert_res, Iterable):
            insert_items.extend(
                [_update_version(model_obj, item) for model_obj in convert_res if model_obj is not None])
        else:
            insert_items.append(_update_version(convert_res, item))

    t3 = time.time()
    statsd.timing(statsd_key_prefix + '.import_data_from_queryset.convert', t3 - t2)

    res = import_clickhouse_model_objects(insert_items, statsd_key_prefix=statsd_key_prefix, batch_size=batch_size,
                                          create_table_if_not_exist=create_table_if_not_exist)
    t4 = time.time()
    statsd.timing(statsd_key_prefix + '.import_data_from_queryset.insert', t4 - t3)

    return res


@shared_task(queue='service')
@statsd.timer('clickhouse.import_data')
def import_data(ch_model, pk_start, pk_end, statsd_key_prefix='upload_clickhouse.all',
                batch_size=settings.CLICKHOUSE_INSERT_SIZE):
    """
    Загрузка данных в Yandex ClickHouse
    :param ch_model: infi model to import data to. It must have from_django_model() method
    :param pk_start: Стартовый pk выборки
    :param pk_end: Конечный pk выборки
    :param statsd_key_prefix: Префикс для statsd, чтобы выделить какую-то группу запросов с отдельным ключом.
        Пишется время обработки данных (data_prepare) и время выполнения запроса (request)
    :param batch_size: Максимальное количество данных, вставляемых за 1 INSERT запрос
    :return: Количество импортированных элементов
    """
    assert inspect.isclass(ch_model), "ch_model must be a subclass of ClickHouseModelConverter"
    assert issubclass(ch_model, ClickHouseModelConverter), "ch_model must be a subclass of ClickHouseModelConverter"
    return import_data_from_queryset(ch_model, ch_model.django_model.objects.filter(pk__gte=pk_start, pk__lt=pk_end),
                                     statsd_key_prefix=statsd_key_prefix, batch_size=batch_size)


@shared_task(queue='service')
def sync_clickhouse_converter(cls):
    """
    Синхронизирует один указанный класс с ClickHouse
    :param cls: Наследник ClickHouseModelConverter
    :return: Количество загруженных в ClickHouse записей
    """
    assert inspect.isclass(cls), "cls must be ClickHouseModelConverter subclass"
    assert issubclass(cls, ClickHouseModelConverter), "cls must be ClickHouseModelConverter subclass"

    t1 = time.time()
    result = cls.import_data()
    t2 = time.time()

    statsd.timing('sync_clickhouse_model.' + cls.__name__, t2 - t1)

    return result


@shared_task(queue='service')
@statsd.timer('clickhouse.auto_sync')
def clickhouse_auto_sync():
    """
    Выгружает в ClickHouse данные всех наследников ClickHouseModelConverter, у которых установлен атрибут auto_sync
    :return:
    """
    # Импортируем все модели, иначе get_subclasses их не увидит
    for app in settings.INSTALLED_APPS:
        pack = app + ".ch_models"
        spam_spec = importlib.util.find_spec(pack)
        if spam_spec is not None:
            importlib.import_module(pack)

    # Запускаем
    for cls in get_subclasses(ClickHouseModelConverter, recursive=True):
        if cls.start_sync():
            # Даже если синхронизация вдруг не выполнится, не страшно, что мы установили период синхронизации
            # Она выполнится следующей таской через интервал.
            sync_clickhouse_converter.delay(cls)


class ReSyncClickHouseModel(PkLongMigration):
    ch_model = None  # type: ClickHouseBaseModel

    @property
    def hints(self):
        return {'model_name': self.ch_model.django_model.__class__.__name__}

    def forwards_batch(self, qs, using: Optional[str] = None):
        result = self.ch_model.recheck(qs)

        items = list(qs)

        next_start_id = None if len(items) < self.batch_size else min(item.id for item in items)
        return result, next_start_id

    def backwords_batch(self, qs, using: Optional[str] = None):
        return 0, None

    def get_queryset(self):
        return self.ch_model.django_model.objects.all()