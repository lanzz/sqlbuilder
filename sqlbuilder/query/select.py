# -*- coding: utf-8 -*-

"""
SQL select query
"""

from __future__ import absolute_import
from ..sql.query import DataManipulationQuery
from ..sql.base import SQL, SQLIterator
from ..sql.alias import SubqueryAlias
from ..sql.name import F
from ..sql.window import Window
from ..utils import Const


class BaseSelect(DataManipulationQuery):
    """
    Base class for SELECT-like queries (actual SELECT statements and set operations)
    """

    def __init__(self):
        self.order = None
        self.limit = None
        self.offset = None

    # set operations
    def __or__(self, other): return SelectSet(self, other, SelectSet.OP.UNION)
    def __and__(self, other): return SelectSet(self, other, SelectSet.OP.INTERSECT)
    def __sub__(self, other): return SelectSet(self, other, SelectSet.OP.EXCEPT)

    def ORDER_BY(self, *exprs):
        self.order = exprs
        return self

    def LIMIT(self, limit, offset=None):
        self.limit = limit
        self.offset = offset
        return self

    def OFFSET(self, offset):
        self.offset = offset
        return self

    def AS(self, *args, **kwargs):
        return SubqueryAlias(self, *args, **kwargs)

    def count(self, connection):
        """
        Return count of rows in result
        """
        cursor = SELECT(F.count()).source(self.set).execute(connection)
        return cursor.fetchone()[0]

    def total_count(self, connection):
        """
        Return total count of rows in result with no limits applied
        """
        cursor = SELECT(F.count()).source(self.copy().limit(None, None)).execute(connection)
        return cursor.fetchone()[0]


class SELECT(BaseSelect):

    def __init__(self, *columns, **kwargs):
        super(SELECT, self).__init__()
        self.distinct = None
        self.columns = list(columns)
        self.source = None
        self.windows = {}

    def DISTINCT(self, *expr):
        self.distinct = expr
        return self

    def _as_sql(self, connection, context):
        if self.distinct is not None:
            distinct_sql, distinct_args = SQLIterator(self.distinct)._as_sql(connection, context)
            distinct_sql = u'DISTINCT {on}'.format(
                on=u'ON ({expr}) '.format(expr=distinct_sql) if distinct_sql else u'',
            )
        else:
            distinct_sql = u''
            distinct_args = ()

        if self.columns:
            columns_sql, columns_args = SQLIterator(self.columns)._as_sql(connection, context)
        else:
            columns_sql = u'*'
            columns_args = ()

        sql = u'SELECT {distinct}{columns}'.format(
            distinct=distinct_sql,
            columns=columns_sql,
        )
        args = distinct_args + columns_args

        if self.source is not None:
            source_sql, source_args = self.source._as_sql(connection, context)
            sql += source_sql
            args += source_args
        if self.windows:
            windows = []
            for name, window in sorted(self.windows.iteritems()):
                alias_sql, alias_args = SQL.wrap(name, id=True)._as_sql(connection, context)
                window_sql, window_args = window._as_sql(connection, context)
                windows.append(u'{name} AS {window}'.format(
                    name=alias_sql,
                    window=window_sql,
                ))
                args += alias_args + window_args
            sql += u' WINDOW {windows}'.format(
                windows=', '.join(windows),
            )
        if self.order is not None:
            order_sql, order_args = SQLIterator(self.order)._as_sql(connection, context)
            sql += u' ORDER BY {order}'.format(order=order_sql)
            args += order_args
        if self.limit is not None:
            limit_sql, limit_args = SQL.wrap(self.limit)._as_sql(connection, context)
            sql += u' LIMIT {limit}'.format(limit=limit_sql)
            args += limit_args
            if self.offset is not None:
                offset_sql, offset_args = SQL.wrap(self.offset)._as_sql(connection, context)
                sql += u' OFFSET {offset}'.format(offset=offset_sql)
                args += offset_args
        else:
            assert self.offset is None, 'Cannot specify OFFSET without LIMIT clause'

        return sql, args

    def copy(self):
        copy = self.set_copy()
        copy.order = self.order
        copy.limit = self.limit
        copy.offset = self.offset
        copy.set = [query.copy() for query in self.set]
        return copy

    def FROM(self, *args, **kwargs):
        self.source = From(*args, **kwargs)
        return self

    def CROSS_JOIN(self, *args, **kwargs):
        self.source.CROSS_JOIN(*args, **kwargs)
        return self

    def LEFT_JOIN(self, *args, **kwargs):
        self.source.LEFT_JOIN(*args, **kwargs)
        return self

    def RIGHT_JOIN(self, *args, **kwargs):
        self.source.RIGHT_JOIN(*args, **kwargs)
        return self

    def FULL_JOIN(self, *args, **kwargs):
        self.source.FULL_JOIN(*args, **kwargs)
        return self

    def INNER_JOIN(self, *args, **kwargs):
        self.source.INNER_JOIN(*args, **kwargs)
        return self

    def WHERE(self, *args, **kwargs):
        """
        Set up a WHERE clause on the data source
        """
        assert self.source is not None, 'Cannot filter query with no FROM clause'
        self.source.WHERE(*args, **kwargs)
        return self

    def GROUP_BY(self, *args, **kwargs):
        """
        Set up a GROUP BY clause on the data source
        """
        self.source.GROUP_BY(*args, **kwargs)
        return self

    def HAVING(self, *args, **kwargs):
        """
        Set up a HAVING clause on the data source
        """
        self.source.HAVING(*args, **kwargs)
        return self

    def WINDOW(self, name, *args, **kwargs):
        """
        Set up a named window definition
        """
        assert name not in self.windows, 'Duplicate window name: {name}'.format(name=name)
        self.windows[name] = Window(*args, **kwargs)
        return self


class SelectSet(BaseSelect):
    """
    Wrapper for a set operation on SELECT statements
    """

    OP = Const('OP', """Operators""",
        UNION=u'UNION',
        INTERSECT=u'INTERSECT',
        EXCEPT=u'EXCEPT',
    )

    DUP = Const('DUP', """Duplicate strategies""",
        ALL=u' ALL',
        DISTINCT=u' DISTINCT',
    )

    def __init__(self, left, right, op):
        self.left = left
        self.right = right
        self.op = op
        self.dup = None
        self.order = None
        self.limit = None
        self.offset = None

    def _as_sql(self, connection, context):
        left_sql, left_args = self.left._as_sql(connection, context)
        if isinstance(self.left, SelectSet):
            left_sql = u'({sql})'.format(sql=left_sql)
        right_sql, right_args = self.right._as_sql(connection, context)
        if isinstance(self.right, SelectSet):
            right_sql = u'({sql})'.format(sql=right_sql)
        sql = u'{left} {op}{dup} {right}'.format(
            left=left_sql,
            op=self.op,
            dup=self.dup or '',
            right=right_sql,
        )
        args = left_args + right_args
        if self.order is not None:
            order_sql, order_args = SQLIterator(self.order)._as_sql(connection, context)
            sql += u' ORDER BY {order}'.format(order=order_sql)
            args += order_args
        if self.limit is not None:
            limit_sql, limit_args = SQL.wrap(self.limit)._as_sql(connection, context)
            sql += u' LIMIT {limit}'.format(limit=limit_sql)
            args += limit_args
            if self.offset is not None:
                offset_sql, offset_args = SQL.wrap(self.offset)._as_sql(connection, context)
                sql += u' OFFSET {offset}'.format(offset=offset_sql)
                args += offset_args
        else:
            assert self.offset is None, 'Cannot specify OFFSET without LIMIT clause'
        return sql, args

    @property
    def ALL(self):
        self.dup = self.DUP.ALL
        return self

    @property
    def DISTINCT(self):
        self.dup = self.DUP.DISTINCT
        return self


class From(SQL):
    """
    FROM clause wrapper
    """

    def __init__(self, source):
        self.source = source
        self.where = None
        self.group_by = None
        self.having = None

    def _as_sql(self, connection, context):
        sql, args = SQL.wrap(self.source)._as_sql(connection, context)
        sql = u' FROM {source}'.format(
            source=sql,
        )
        if self.where:
            where_sql, where_args = SQL.wrap(self.where)._as_sql(connection, context)
            sql += u' WHERE {condition}'.format(
                condition=where_sql,
            )
            args += where_args
        if self.group_by:
            group_sql, group_args = SQLIterator(self.group_by)._as_sql(connection, context)
            sql += u' GROUP BY {columns}'.format(
                columns=group_sql,
            )
            args += group_args
        if self.having:
            having_sql, having_args = SQL.wrap(self.having)._as_sql(connection, context)
            sql += u' HAVING {condition}'.format(
                condition=having_sql,
            )
            args += having_args
        return sql, args

    def copy(self):
        copy = self.__class__(source=self.source.copy())
        copy.where = None if self.where is None else self.where.copy()
        copy.group_by = None if self.group_by is None else self.group_by.copy()
        copy.having = None if self.having is None else self.having.copy()
        return copy

    def CROSS_JOIN(self, *args, **kwargs):
        kwargs.setdefault('parens', False)
        self.source = self.source.CROSS_JOIN(*args, **kwargs)
        return self

    def LEFT_JOIN(self, *args, **kwargs):
        kwargs.setdefault('parens', False)
        self.source = self.source.LEFT_JOIN(*args, **kwargs)
        return self

    def RIGHT_JOIN(self, *args, **kwargs):
        kwargs.setdefault('parens', False)
        self.source = self.source.RIGHT_JOIN(*args, **kwargs)
        return self

    def FULL_JOIN(self, *args, **kwargs):
        kwargs.setdefault('parens', False)
        self.source = self.source.FULL_JOIN(*args, **kwargs)
        return self

    def INNER_JOIN(self, *args, **kwargs):
        kwargs.setdefault('parens', False)
        self.source = self.source.INNER_JOIN(*args, **kwargs)
        return self

    def WHERE(self, expr):
        """
        Set up a WHERE clause
        """
        self.where = expr
        return self

    def GROUP_BY(self, *columns):
        """
        Set up a GROUP BY clause
        """
        self.group_by = columns
        return self

    def HAVING(self, expr):
        """
        Set up a HAVING clause
        """
        self.having = expr
        return self
