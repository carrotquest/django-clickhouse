import datetime
from queue import Queue, Empty
from threading import Thread

import os
from itertools import chain
from typing import Union, Any, Optional, TypeVar, Set, Dict, Iterable, Tuple, Iterator, Callable, List

import pytz
from importlib import import_module
from importlib.util import find_spec
from django.db.models import Model as DjangoModel

from .database import connections


T = TypeVar('T')


def get_tz_offset(db_alias: Optional[str] = None) -> int:
    """
    Returns ClickHouse server timezone offset in minutes
    :param db_alias: The database alias used
    :return: Integer
    """
    db = connections[db_alias]
    return int(db.server_timezone.utcoffset(datetime.datetime.utcnow()).total_seconds() / 60)


def format_datetime(dt: Union[datetime.date, datetime.datetime], timezone_offset: int = 0, day_end: bool = False,
                    db_alias: Optional[str] = None) -> str:
    """
    Formats datetime and date objects to format that can be used in WHERE conditions of query
    :param dt: datetime.datetime or datetime.date object
    :param timezone_offset: timezone offset (minutes)
    :param day_end: If datetime.date is given and flag is set, returns day end time, not day start.
    :param db_alias: The database alias used
    :return: A string representing datetime
    """
    assert isinstance(dt, (datetime.datetime, datetime.date)), "dt must be datetime.datetime instance"
    assert type(timezone_offset) is int, "timezone_offset must be integer"

    # datetime.datetime inherits datetime.date. So I can't just make isinstance(dt, datetime.date)
    if not isinstance(dt, datetime.datetime):
        t = datetime.time.max if day_end else datetime.time.min
        dt = datetime.datetime.combine(dt, t)

    # Convert datetime to UTC, if it has timezone
    if dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None:
        dt = pytz.utc.localize(dt)
    else:
        dt = dt.astimezone(pytz.utc)

    # Dates in ClickHouse are parsed in server local timezone. So I need to add server timezone
    server_dt = dt - datetime.timedelta(minutes=timezone_offset - get_tz_offset(db_alias))

    return server_dt.strftime("%Y-%m-%d %H:%M:%S")


def module_exists(module_name: str) -> bool:
    """
    Checks if module exists
    :param module_name: Dot-separated module name
    :return: Boolean
    """
    # Python 3.4+
    spam_spec = find_spec(module_name)
    return spam_spec is not None


def lazy_class_import(obj: Union[str, Any]) -> Any:
    """
    If string is given, imports object by given module path.
    Otherwise returns the object
    :param obj: A string class path or object to return
    :return: Imported object
    """
    if isinstance(obj, str):
        module_name, obj_name = obj.rsplit('.', 1)
        module = import_module(module_name)

        try:
            return getattr(module, obj_name)
        except AttributeError:
            raise ImportError('Invalid import path `%s`' % obj)
    else:
        return obj


def get_subclasses(cls: T, recursive: bool = False) -> Set[T]:
    """
    Gets all subclasses of given class
    Attention!!! Classes would be found only if they were imported before using this function
    :param cls: Class to get subcalsses
    :param recursive: If flag is set, returns subclasses of subclasses and so on too
    :return: A list of subclasses
    """
    subclasses = set(cls.__subclasses__())

    if recursive:
        for subcls in subclasses.copy():
            subclasses.update(get_subclasses(subcls, recursive=True))

    return subclasses


def model_to_dict(instance: DjangoModel, fields:  Optional[Iterable[str]] = None,
                  exclude_fields:  Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """
    Standard model_to_dict ignores some fields if they have invalid naming
    :param instance: Object to convert to dictionary
    :param fields: Field list to extract from instance
    :param exclude_fields: Filed list to exclude from extraction
    :return: Serialized dictionary
    """
    data = {}

    if not fields:
        opts = instance._meta
        fields = {f.name for f in chain(opts.concrete_fields, opts.private_fields, opts.many_to_many)}

    for name in set(fields) - set(exclude_fields or set()):
        val = getattr(instance, name, None)
        if val is not None:
            data[name] = val

    return data


def check_pid(pid):
    """
    Check For the existence of a unix pid.
    """
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    else:
        return True


def int_ranges(items: Iterable[int]) -> Iterator[Tuple[int, int]]:
    """
    Finds continuous intervals in integer iterable.
    :param items: Items to search in
    :return: Iterator over Tuple[start, end]
    """
    interval_start = None
    prev_item = None
    for item in sorted(items):
        if prev_item is None:
            interval_start = prev_item = item
        elif prev_item + 1 == item:
            prev_item = item
        else:
            interval = interval_start, prev_item
            interval_start = prev_item = item
            yield interval

    if interval_start is None:
        return
    else:
        yield interval_start, prev_item


class ExceptionThread(Thread):
    """
    Thread objects, which catches thread exceptions and raises them in main thread
    """
    def __init__(self, *args, **kwargs):
        super(ExceptionThread, self).__init__(*args, **kwargs)
        self.exc = None

    def _close_django_db_connections(self):
        """
        In Django every thread has its own database connection pool.
        But django does not close them automatically in child threads.
        As a result, this can cause database connection leaking.
        Here we close connections manually when thread execution is finished.
        """
        try:
            from django.db import connections as db_connections
        except (ModuleNotFoundError, ImportError):
            db_connections = None

        if db_connections:
            db_connections.close_all()

    def run(self):
        try:
            return super(ExceptionThread, self).run()
        except Exception as e:
            self.exc = e
        finally:
            self._close_django_db_connections()

    def join(self, timeout=None):
        super(ExceptionThread, self).join(timeout=timeout)
        if self.exc:
            raise self.exc


def exec_in_parallel(func: Callable, args_queue: Queue, threads_count: Optional[int] = None) -> List[Any]:
    """
    Executes func in multiple threads in parallel
    Functions are expected to be thread safe. If it needs some locks, func must provide them.
    :param func: Function to execute in thread
    :param args_queue: A queue with arguments for separate function call. Each element is tuple of (args, kwargs)
    :param threads_count: Maximum number of parallel threads tho run
    :return: A list of results. Order of results is not guaranteed. Element types depends func return type.
    """
    results = []

    # If thread_count is not given, we execute all tasks in parallel.
    # If queue has less elements than threads_count, take queue size.
    threads_count = min(args_queue.qsize(), threads_count) if threads_count else args_queue.qsize()

    def _worker():
        """
        Thread worker, gets next arguments from queue and processes them.
        Results are put into results array using thread safe lock
        :return: None
        """
        finished = False
        while not finished:
            try:
                # Get arguments
                args, kwargs = args_queue.get_nowait()

                # Execute function
                local_res = func(*args, **kwargs)

                # Write result. appending a list is thread safe operation according to:
                # http://effbot.org/pyfaq/what-kinds-of-global-value-mutation-are-thread-safe.htm
                results.append(local_res)

                # Mark task as complete
                args_queue.task_done()
            except Empty:
                # No data in queue, finish worker thread
                finished = True

    # Run threads
    threads = []
    for index in range(threads_count):
        t = ExceptionThread(target=_worker)
        threads.append(t)
        t.start()

    # Wait for threads to finish
    for t in threads:
        t.join()

    return results


def exec_multi_arg_func(func: Callable, split_args: Iterable[Any], *args, threads_count: Optional[int] = None,
                        **kwargs) -> List[Any]:
    """
    Executes function in parallel threads. Thread functions (func) receive one of split_args as first argument
    Another arguments passed to functions - args and kwargs
    If len(split_args) <= 0, separate threads are not run, main thread is used.
    :param func: Function to execute. Must accept split_arg as first parameter
    :param split_args: A list of arguments to split threads by
    :param threads_count: Maximum number of threads to run in parallel
    :return: A list of execution results. Order of execution is not guaranteed.
    """
    split_args = list(split_args)
    if len(split_args) == 0:
        return []
    elif len(split_args) == 1:
        return [func(split_args[0], *args, **kwargs)]
    else:
        q = Queue()
        for s in split_args:
            q.put(([s] + list(args), kwargs))

        return exec_in_parallel(func, q, threads_count=threads_count)


class SingletonMeta(type):
    """
    Realises singleton pattern
    """
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]
