# Basic information
## About
This project's goal is to build [Yandex ClickHouse](https://clickhouse.yandex/) database into [Django](https://www.djangoproject.com/) project.  
It is based on [infi.clickhouse-orm](https://github.com/Infinidat/infi.clickhouse_orm) library.  

## Features
* Multiple ClickHouse database configuration in [settings.py](https://docs.djangoproject.com/en/2.1/ref/settings/)
* ORM to create and manage ClickHouse models.
* ClickHouse migration system.
* Scalable serialization of django model instances to ORM model instances.
* Effective periodical synchronization of django models to ClickHouse without loosing data.
* Synchronization process monitoring.

## Requirements
* [Python 3](https://www.python.org/downloads/)
* [Django](https://docs.djangoproject.com/) 1.7+
* [Yandex ClickHouse](https://clickhouse.yandex/)
* [infi.clickhouse-orm](https://github.com/Infinidat/infi.clickhouse_orm)
* [pytz](https://pypi.org/project/pytz/)
* [six](https://pypi.org/project/six/)
* [typing](https://pypi.org/project/typing/)
* [psycopg2](https://www.psycopg.org/)
* [celery](http://www.celeryproject.org/)
* [statsd](https://pypi.org/project/statsd/)

### Optional libraries
* [redis-py](https://redis-py.readthedocs.io/en/latest/) for [RedisStorage](storages.md#redisstorage)
* [django-pg-returning](https://github.com/M1hacka/django-pg-returning) 
  for optimizing registering updates in [PostgreSQL](https://www.postgresql.org/)
* [django-pg-bulk-update](https://github.com/M1hacka/django-pg-bulk-update)
  for performing effective bulk update and create operations in [PostgreSQL](https://www.postgresql.org/)

## Installation
Install via pip:  
`pip install django-clickhouse`    
or via setup.py:  
`python setup.py install`
