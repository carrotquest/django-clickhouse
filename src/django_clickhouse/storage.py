"""
This file defines different storages.
Storage saves intermediate data about database events - inserts, updates, delete.
This data is periodically fetched from storage and applied to ClickHouse tables.

Important:
Storage should be able to restore current importing batch, if something goes wrong.
"""


class Storage:

    def pre_sync(self):  # type: () -> None
        """
        This method is called before import process starts
        :return: None
        """
        pass

    def post_sync(self, success):  # type: (bool) -> None
        """
        This method is called after import process has finished.
        :param success: A flag, if process ended with success or error
        :return: None
        """
        pass

    def get_sync_ids(self, **kwargs):  # type(**dict) -> Tuple[Set[Any], Set[Any], Set[Any]]
        """
        Must return 3 sets of ids: to insert, update and delete records.
        Method should be error safe - if something goes wrong, import data should not be lost
        :param kwargs: Storage dependant arguments
        :return: 3 sets of primary keys
        """
        raise NotImplemented()


class RedisStorage(Storage):

    def __init__(self):


    @classmethod
    def get_sync_ids(cls, **kwargs):
        # Шардинговый формат
        key = 'clickhouse_sync:{using}:{table}:{operation}'.format(table=cls.django_model._meta.db_table,
                                                                   operation='*', using=(using or 'default'))

        # Множества id для вставки, обновления, удаления
        insert_model_ids, update_model_ids, delete_model_ids = set(), set(), set()

        for key in settings.REDIS.keys(key):
            model_ids = settings.REDIS.pipeline().smembers(key).delete(key).execute()[0]
            model_ids = {int(mid) for mid in model_ids}

            op = key.decode('utf-8').split(':')[-1]
            if op == 'INSERT':
                insert_model_ids = set(model_ids)
            elif op == 'UPDATE':
                update_model_ids = set(model_ids)
            else:  # if op == 'DELETE'
                delete_model_ids = set(model_ids)

        return insert_model_ids, update_model_ids, delete_model_ids