from infi.clickhouse_orm.database import Database as InfiDatabase
from infi.clickhouse_orm.utils import parse_tsv
from six import next

from .configuration import config
from .exceptions import DBAliasError


class Database(InfiDatabase):
    def __init__(self, **kwargs):
        infi_kwargs = {
            k: kwargs[k]
            for k in ('db_name', 'db_url', 'username', 'password', 'readonly', 'autocreate')
            if k in kwargs
        }
        super(Database, self).__init__(**infi_kwargs)

    def drop_database(self):
        # BUG fix https://github.com/Infinidat/infi.clickhouse_orm/issues/89
        super(Database, self).drop_database()
        self.db_exists = False

    def migrate(self, migrations_package_name, up_to=9999):
        raise NotImplementedError('This method is not supported by django-clickhouse.'
                                  ' Use django_clickhouse.migrations module instead.')

    def _get_applied_migrations(self, migrations_package_name):
        raise NotImplementedError("This method is not supported by django_clickhouse.")

    def select_init_many(self, query, model_class, settings=None):
        """
        Base select doesn't use init_mult which is ineffective on big result lists
        """
        query += ' FORMAT TabSeparatedWithNames'
        query = self._substitute(query, model_class)
        r = self._send(query, settings, True)
        lines = r.iter_lines()
        field_names = parse_tsv(next(lines))

        kwargs_list = []
        for line in lines:
            # skip blank line left by WITH TOTALS modifier
            if line:
                values = iter(parse_tsv(line))
                kwargs = {}
                for name in field_names:
                    field = getattr(model_class, name)
                    kwargs[name] = field.to_python(next(values), self.server_timezone)

                kwargs_list.append(kwargs)

        return model_class.init_many(kwargs_list, database=self)


class ConnectionProxy:
    _connections = {}

    def get_connection(self, alias):
        if alias is None:
            alias = config.DEFAULT_DB_ALIAS

        if alias not in self._connections:
            if alias not in config.DATABASES:
                raise DBAliasError(alias)

            self._connections[alias] = Database(**config.DATABASES[alias])

        return self._connections[alias]

    def __getitem__(self, item):
        return self.get_connection(item)


connections = ConnectionProxy()
