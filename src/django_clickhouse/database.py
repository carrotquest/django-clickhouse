from infi.clickhouse_orm.database import Database as InfiDatabase

from .configuration import config
from .exceptions import DBAliasError

DEFAULT_DB_ALIAS = 'default'


class Database(InfiDatabase):
    def __init__(self, **kwargs):
        infi_kwargs = {
            k: kwargs[k]
            for k in ('db_name', 'db_url', 'username', 'password', 'readonly', 'autocreate')
            if k in kwargs
        }
        super(Database, self).__init__(**infi_kwargs)

    def migrate(self, migrations_package_name, up_to=9999):
        raise NotImplementedError('This method is not supported by django-clickhouse.'
                                  ' Use django_clickhouse.migrations module instead.')


class ConnectionProxy:
    _connections = {}

    def get_connection(self, alias):
        if alias is None:
            alias = DEFAULT_DB_ALIAS

        if alias not in self._connections:
            if alias not in config.DATABASES:
                raise DBAliasError(alias)

            self._connections[alias] = Database(**config.DATABASES[alias])

        return self._connections[alias]

    def __getitem__(self, item):
        return self.get_connection(item)


connections = ConnectionProxy()
