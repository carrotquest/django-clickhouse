from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver


class ClickHouseDjangoModelQuerySet(DjangoBaseQuerySet):
    """
    Переопределяет update, чтобы он сгенерировал данные для обновления ClickHouse
    """

    def __init__(self, *args, **kwargs):
        super(ClickHouseDjangoModelQuerySet, self).__init__(*args, **kwargs)

    def update(self, **kwargs):
        if self.model.clickhouse_sync_type == 'redis':
            pk_name = self.model._meta.pk.name
            res = self.only(pk_name).update_returning(**kwargs).values_list(pk_name, flat=True)
            self.model.register_clickhouse_operation('UPDATE', *res, database=(self._db or 'default'))
            return len(res)
        else:
            return super(ClickHouseDjangoModelQuerySet, self).update(**kwargs)

    def update_returning(self, **updates):
        result = super(ClickHouseDjangoModelQuerySet, self).update_returning(**updates)
        if self.model.clickhouse_sync_type == 'redis':
            pk_name = self.model._meta.pk.name
            pk_list = result.values_list(pk_name, flat=True)
            self.model.register_clickhouse_operation('UPDATE', *pk_list, database=(self._db or 'default'))
        return result

    def delete_returning(self):
        result = super(ClickHouseDjangoModelQuerySet, self).delete_returning()
        if self.model.clickhouse_sync_type == 'redis':
            pk_name = self.model._meta.pk.name
            pk_list = result.values_list(pk_name, flat=True)
            self.model.register_clickhouse_operation('DELETE', *pk_list, database=(self._db or 'default'))
        return result


class ClickHouseDjangoModelManager(DjangoBaseManager):
    def get_queryset(self):
        """
        Инициализирует кастомный QuerySet
        :return: BaseQuerySet модели
        """
        return ClickHouseDjangoModelQuerySet(model=self.model, using=self._db)

    def bulk_create(self, objs, batch_size=None):
        objs = super(ClickHouseDjangoModelManager, self).bulk_create(objs, batch_size=batch_size)
        self.model.register_clickhouse_operation('INSERT', *[obj.pk for obj in objs], database=(self._db or 'default'))

        return objs


class ClickHouseDjangoModel(DjangoBaseModel):
    """
    Определяет базовую абстрактную модель, синхронизируемую с кликхаусом
    """
    # TODO PostgreSQL, используемый сейчас не поддерживает UPSERT. Эта функция появилась в PostgreSQL 9.5
    # INSERT INTO "{clickhouse_update_table}" ("table", "model_id", "operation")
    # VALUES (TG_TABLE_NAME, NEW.{pk_field_name}, TG_OP) ON CONFILICT DO NOTHING;

    # DEPRECATED Пока не удаляю, вдруг все таки решим переписать
    # Синхронизация через Postgres основана на триггерах, которые не работают меж шардами
    CREATE_TRIGGER_SQL_TEMPLATE = """
        CREATE OR REPLACE FUNCTION {table}_clickhouse_update() RETURNS TRIGGER AS ${table}_clickhouse_update$
        BEGIN
            INSERT INTO "{clickhouse_update_table}" ("table", "model_id", "operation", "database") 
              SELECT TG_TABLE_NAME, NEW.{pk_field_name}, TG_OP, 'default' WHERE NOT EXISTS (
                SELECT id FROM "{clickhouse_update_table}" WHERE "table"=TG_TABLE_NAME AND "model_id"=NEW.{pk_field_name}
              );
            RETURN NEW;
        END;
    ${table}_clickhouse_update$ LANGUAGE plpgsql;

    DROP TRIGGER IF EXISTS {table}_collapsing_model_update ON {table};
    CREATE TRIGGER {table}_collapsing_model_update AFTER INSERT OR UPDATE ON {table}
    FOR EACH ROW EXECUTE PROCEDURE {table}_clickhouse_update();
    """

    # DEPRECATED Пока не удаляю, вдруг все таки решим переписать
    # Синхронизация через Postgres основана на триггерах, которые не работают меж шардами
    DROP_TRIGGER_SQL_TEMPLATE = """
        DROP TRIGGER IF EXISTS {table}_collapsing_model_update ON {table};
        DROP FUNCTION IF EXISTS {table}_clickhouse_update();
    """

    clickhouse_sync_type = None
    objects = ClickHouseDjangoModelManager()

    class Meta:
        abstract = True

    def __init__(self, *args, **kwargs):
        # Добавил, чтобы PyCharm не ругался на неопределенный __init__
        super().__init__(*args, **kwargs)

    @classmethod
    def register_clickhouse_operation(cls, operation, *model_ids, database=None):
        """
        Добавляет в redis запись о том, что произошел Insert, update или delete модели
        :param operation: Тип операции INSERT, UPDATE, DELETE
        :param model_ids: Id элементов для регистрации
        :param database: База данных, в которой лежит данное значение
        :return: None
        """
        if cls.clickhouse_sync_type != 'redis':
            return

        assert operation in {'INSERT', 'UPDATE', 'DELETE'}, 'operation must be one of [INSERT, UPDATE, DELETE]'
        model_ids = get_parameter_pk_list(model_ids)

        if len(model_ids) > 0:
            key = 'clickhouse_sync:{database}:{table}:{operation}'.format(table=cls._meta.db_table, operation=operation,
                                                                          database=(database or 'default'))
            on_transaction_commit(settings.REDIS.sadd, args=[key] + model_ids)

    @classmethod
    def get_trigger_sql(cls, drop=False, table=None):
        """
        Формирует SQL для создания или удаления триггера на обновление модели синхронизации с ClickHouse
        :param drop: Если флаг указан, формирует SQL для удаления триггера. Иначе - для создания
        :return: Строка SQL
        """
        # DEPRECATED Пока не удаляю, вдруг все таки решим переписать
        # Синхронизация через Postgres основана на триггерах, которые не работают меж шардами
        raise Exception('This method is deprecated due to sharding released')

        # table = table or cls._meta.db_table
        # from utils.models import ClickHouseModelOperation
        # sql = cls.DROP_TRIGGER_SQL_TEMPLATE if drop else cls.CREATE_TRIGGER_SQL_TEMPLATE
        # sql = sql.format(table=table, pk_field_name=cls._meta.pk.name,
        #                  clickhouse_update_table=ClickHouseModelOperation._meta.db_table)
        # return sql

    def post_save(self, created, using=None):
        self.register_clickhouse_operation('INSERT' if created else 'UPDATE', self.pk, database=(using or 'default'))

    def post_delete(self, using=None):
        self.register_clickhouse_operation('DELETE', self.pk, database=(using or 'default'))


@receiver(post_save)
def post_save(sender, instance, **kwargs):
    if issubclass(sender, ClickHouseDjangoModel):
        instance.post_save(kwargs.get('created'), using=kwargs.get('using'))


@receiver(post_delete)
def post_delete(sender, instance, **kwargs):
    if issubclass(sender, ClickHouseDjangoModel):
        instance.post_delete(using=kwargs.get('using'))
