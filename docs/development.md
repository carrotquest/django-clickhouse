# Development
## Basic info
This is an Open source project developed by `Carrot quest` team under MIT license. 
Feel free to create issues and make pull requests.  
Query and database system wraps [infi.clickhouse_orm](https://github.com/Infinidat/infi.clickhouse_orm) library. 
If you want to develop QuerySet system, it is better to contribute there.  


## General info about testing
Library test system is based on [django.test](https://docs.djangoproject.com/en/3.2/topics/testing/overview/).
You can find them in `tests` directory. 

## Tests requirements  
* [Redis](https://redis.io/)  
* [Yandex ClickHouse](https://clickhouse.yandex/)  
* [PostgreSQL](https://www.postgresql.org/)  
* Pypi libraries listed in `requirements-test.txt` file

## Running tests
### Running in docker
1. Install [docker and docker-compose](https://www.docker.com/)  
2. Run `docker-compose run run_tests` in project directory

### Running in virtual environment
1. Install all requirements listed above 
2. [Create virtual environment](https://docs.python.org/3/tutorial/venv.html)
3. Install requirements  
  `pip3 install -U -r requirements-test.txt`  
4. Start tests  
  `python3 runtests.py`
