# class ClickHouseModel(InfiModel):
#
#     @classmethod
#     def form_query(cls, select_items: Union[str, Set[str], List[str], Tuple[str]], table: Optional[str] = None,
#                    final: bool = False, date_filter_field: str = '', date_in_prewhere: bool = True,
#                    prewhere: Union[str, Set[str], List[str], Tuple[str]] = '',
#                    where: Union[str, Set[str], List[str], Tuple[str]] = '',
#                    group_fields: Union[str, Set[str], List[str], Tuple[str]] = '', group_with_totals: bool = False,
#                    order_by: Union[str, Set[str], List[str], Tuple[str]] = '',
#                    limit: Optional[int] = None, prewhere_app: bool = True):
#         """
#         Формирует запрос к данной таблице
#         :param select_items: строка или массив строк, которые надо выбрать в запросе
#         :param table: Таблица данного класса по-умолчанию
#         :param final: Позволяет выбрать из CollapsingMergeTree только последнюю версию записи.
#         :param date_filter_field: Поле, которое надо отфильтровать от start_date до date_end, если задано
#         :param date_in_prewhere: Если флаг указан и задано поле date_filter_field,
#             то условие будет помещено в секцию PREWHERE, иначе - в WHERE
#         :param prewhere: Условие, которое добавляется в prewhere
#         :param where: Условие, которое добавляется в where
#         :param group_fields: Поля, по которым будет производится группировка
#         :param group_with_totals: Позволяет добавить к группировке модификатор with_totals
#         :param order_by: Поле или массив полей, по которым сортируется результат
#         :param limit: Лимит на количество записей
#         :param prewhere_app: Автоматически добавляет в prewhere фильтр по app_id
#         :return: Запрос, в пределах приложения
#         """
#         assert isinstance(select_items, (str, list, tuple, set)), "select_items must be string, list, tuple or set"
#         assert table is None or isinstance(table, str), "table must be string or None"
#         assert isinstance(final, bool), "final must be boolean"
#         assert isinstance(date_filter_field, str), "date_filter_field must be string"
#         assert isinstance(date_in_prewhere, bool), "date_in_prewhere must be boolean"
#         assert isinstance(prewhere, (str, list, tuple, set)), "prewhere must be string, list, tuple or set"
#         assert isinstance(where, (str, list, tuple, set)), "where must be string, list, tuple or set"
#         assert isinstance(group_fields, (str, list, tuple, set)), "group_fields must be string, list, tuple or set"
#         assert isinstance(group_with_totals, bool), "group_with_totals must be boolean"
#         assert isinstance(order_by, (str, list, tuple, set)), "group_fields must be string, list, tuple or set"
#         assert limit is None or isinstance(limit, int) and limit > 0, "limit must be None or positive integer"
#         assert isinstance(prewhere_app, bool), "prewhere_app must be boolean"
#
#         table = table or '$db.`{0}`'.format(cls.table_name())
#         final = 'FINAL' if final else ''
#
#         if prewhere:
#             if not isinstance(prewhere, str):
#                 prewhere = '(%s)' % ') AND ('.join(prewhere)
#
#         if prewhere_app:
#             prewhere = '`app_id`={app_id} AND (' + prewhere + ')' if prewhere else '`app_id`={app_id}'
#
#         if prewhere:
#             prewhere = 'PREWHERE ' + prewhere
#
#         if where:
#             if not isinstance(where, str):
#                 where = ' AND '.join(where)
#             where = 'WHERE ' + where
#
#         if not isinstance(select_items, str):
#             # Исключим пустые строки
#             select_items = [item for item in select_items if item]
#             select_items = ', '.join(select_items)
#
#         if group_fields:
#             if not isinstance(group_fields, str):
#                 group_fields = ', '.join(group_fields)
#
#             group_fields = 'GROUP BY %s' % group_fields
#
#             if group_with_totals:
#                 group_fields += ' WITH TOTALS'
#
#         if order_by:
#             if not isinstance(order_by, str):
#                 order_by = ', '.join(order_by)
#
#             order_by = 'ORDER BY ' + order_by
#
#         if date_filter_field:
#             cond = "`%s` >= '{start_date}' AND `%s` < '{end_date}'" % (date_filter_field, date_filter_field)
#             if date_in_prewhere:
#                 prewhere += ' AND ' + cond
#             elif where:
#                 where += ' AND ' + cond
#             else:
#                 where = 'WHERE ' + cond
#
#         limit = "LIMIT {0}".format(limit) if limit else ''
#
#         query = '''
#           SELECT %s
#           FROM %s %s
#
#             %s
#           %s
#          %s %s %s
#         ''' % (select_items, table, final, prewhere, where, group_fields, order_by, limit)
#
#         # Моя спец функция, сокращающая запись проверки даты на Null в запросах.
#         # В скобках не может быть других скобок
#         # (иначе надо делать сложную проверку скобочной последовательности, решил пока не заморачиваться)
#         # Фактически, функция приводит выражение в скобках к timestamp и смотрит, что оно больше 0
#         query = re.sub(r'\$dateIsNotNull\s*(\([^()]*\))', r'toUInt64(toDateTime(\1)) > 0', query)
#
#         return re.sub(r'\s+', ' ', query.strip())