import logging
from typing import Optional, Type, Iterable

from infi.clickhouse_orm.database import Database as InfiDatabase, DatabaseException
from infi.clickhouse_orm.utils import parse_tsv
from six import next
from io import BytesIO
from statsd.defaults.django import statsd

from .configuration import config
from .exceptions import DBAliasError

logger = logging.getLogger('django-clickhouse')


class Database(InfiDatabase):
    def __init__(self, **kwargs):
        infi_kwargs = {
            k: kwargs[k]
            for k in ('db_name', 'db_url', 'username', 'password', 'readonly', 'autocreate', 'timeout',
                      'verify_ssl_cert')
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

    def select_tuples(self, query: str, model_class: Type['ClickHouseModel'],  # noqa: F821
                      settings: Optional[dict] = None) -> Iterable[tuple]:
        """
        This method selects model_class namedtuples, instead of class instances.
        Less memory consumption, greater speed
        :param query: Query to execute. Can contain substitution variables.
        :param model_class: A class of model to get fields from
        :param settings: Optional connection settings
        :return: Generator of namedtuple objects
        """
        query += ' FORMAT TabSeparatedWithNames'
        query = self._substitute(query, model_class)
        r = self._send(query, settings, True)
        lines = r.iter_lines()
        field_names = parse_tsv(next(lines))
        fields = [
            field for name, field in model_class.fields(writable=True).items()
            if name in field_names
        ]
        res_class = model_class.get_tuple_class(field_names)

        for line in lines:
            # skip blank line left by WITH TOTALS modifier
            if line:
                values = iter(parse_tsv(line))
                item = res_class(**{
                    field_name: fields[i].to_python(next(values), self.server_timezone)
                    for i, field_name in enumerate(field_names)
                })

                yield item

    def insert_tuples(self, model_class: Type['ClickHouseModel'], model_tuples: Iterable[tuple],  # noqa: F821
                      batch_size: Optional[int] = None, formatted: bool = False) -> None:
        """
        Inserts model_class namedtuples
        :param model_class: ClickHouse model, namedtuples are made from
        :param model_tuples: An iterable of tuples to insert
        :param batch_size: Size of batch
        :param formatted: If flag is set, tuples are expected to be ready to insert without calling field.to_db_string
        :return: None
        """
        tuples_iterator = iter(model_tuples)

        try:
            first_tuple = next(tuples_iterator)
        except StopIteration:
            return  # model_instances is empty

        if model_class.is_read_only() or model_class.is_system_model():
            raise DatabaseException("You can't insert into read only and system tables")

        fields_list = ','.join('`%s`' % name for name in first_tuple._fields)
        fields_dict = model_class.fields(writable=True)
        statsd_key = "%s.inserted_tuples.%s" % (config.STATSD_PREFIX, model_class.__name__)

        query = 'INSERT INTO `%s`.`%s` (%s) FORMAT TabSeparated\n' \
                % (self.db_name, model_class.table_name(), fields_list)
        query_enc = query.encode('utf-8')

        def tuple_to_csv(tup):
            if formatted:
                str_gen = (getattr(tup, field_name) for field_name in first_tuple._fields)
            else:
                str_gen = (fields_dict[field_name].to_db_string(getattr(tup, field_name), quote=False)
                           for field_name in first_tuple._fields)

            return '%s\n' % '\t'.join(str_gen)

        def gen():
            buf = BytesIO()
            buf.write(query_enc)
            buf.write(tuple_to_csv(first_tuple).encode('utf-8'))

            # Collect lines in batches of batch_size
            lines = 1
            for t in tuples_iterator:
                buf.write(tuple_to_csv(t).encode('utf-8'))

                lines += 1
                if batch_size is not None and lines >= batch_size:
                    # Return the current batch of lines
                    statsd.incr(statsd_key, lines)
                    yield buf.getvalue()
                    # Start a new batch
                    buf = BytesIO()
                    buf.write(query_enc)
                    lines = 0

            # Return any remaining lines in partial batch
            if lines:
                statsd.incr(statsd_key, lines)
                yield buf.getvalue()

        # For testing purposes
        for data in gen():
            with statsd.timer(statsd_key):
                logger.debug('django-clickhouse: insert tuple: %s' % data)
                self._send(data)


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
