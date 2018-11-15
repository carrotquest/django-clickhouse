from infi.clickhouse_orm.database import Database

from .exceptions import DBAliasError
from .configuration import config


class ConnectionProxy:
    _connections = {}

    def get_connection(self, alias):
        if alias not in self._connections:
            if alias not in config.DATABASES:
                raise DBAliasError(alias)

            self._connections[alias] = Database(**config.DATABASES[alias])

        return self._connections[alias]

    def __getattr__(self, item):
        return self.get_connection(item)


connections = ConnectionProxy()
