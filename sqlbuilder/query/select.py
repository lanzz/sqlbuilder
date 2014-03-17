# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import DataManipulationQuery, F
from ..sql import SQL, SQLIterator, SubqueryAlias, wrap_source


class SetMember(SQL):
    """
    Member of a set of SELECT queries
    """

    # set operations
    UNION = u' UNION '
    INTERSECT = u' INTERSECT '
    EXCEPT = u' EXCEPT '

    def __init__(self, parent, op, all=None):
        self.parent = parent
        self.op = op
        self.all = all
        self.query = parent.set_copy()
        assert self.op in (SetMember.UNION, SetMember.INTERSECT, SetMember.EXCEPT), 'Invalid set operation: {op}'.format(op=self.op)

    def _as_sql(self, connection, context):
        sql, args = self.query._as_sql(connection, context)
        sql = u'{query}{op}{dups}'.format(
            query=sql,
            op=self.op,
            dups=u'' if self.all is None else u'ALL ' if self.all else u'DISTINCT ',
        )
        return sql, args

    def copy(self, new_parent=None):
        copy = self.__class__(new_parent or self.query, self.op, all=self.all)
        copy.query = self.query.copy()
        return copy

    @property
    def ALL(self):
        self.all = True
        return self

    @property
    def DISTINCT(self):
        self.all = False
        return self

    @property
    def SELECT(self):
        """
        Start the member query
        Weird property decoration allows for both `.SELECT(...)` and `.SELECT.DISTINCT(...)` invocations
        """
        def SELECT(*args, **kwargs):
            """
            Copy the current parent query as this member's query
            """
            return self.parent.set_add(self, *args, **kwargs)
        def DISTINCT(*args, **kwargs):
            kwargs['DISTINCT'] = True
            return SELECT(self, *args, **kwargs)
        SELECT.DISTINCT = DISTINCT
        return SELECT


class SELECT(DataManipulationQuery):

    def __init__(self, *columns, **kwargs):
        self.set_init(*columns, **kwargs)
        self.order = None
        self.limit = None
        self.offset = None
        self.set = []

    def set_init(self, *columns, **kwargs):
        """
        Limited initialization for a new set query
        """
        self.distinct = None
        self.columns = list(columns)
        self.source = None

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

        if self.set:
            set_sql, set_args = SQLIterator(self.set)._as_sql(connection, context)
            sql = set_sql + sql
            args = set_args + args

        return sql, args

    def set_copy(self):
        """
        Limited copy for use in set
        Ordering, limiting and previous set not included
        """
        copy = self.__class__(*self.columns, DISTINCT=self.distinct)
        copy.source = None if self.source is None else self.source.copy()
        return copy

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

    def NATURAL_JOIN(self, *args, **kwargs):
        self.source.NATURAL_JOIN(*args, **kwargs)
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

    @property
    def UNION(self):
        return SetMember(self, SetMember.UNION)

    @property
    def UNION_ALL(self):
        return SetMember(self, SetMember.UNION, all=True)

    @property
    def UNION_DISTINCT(self):
        return SetMember(self, SetMember.UNION, all=False)

    @property
    def INTERSECT(self):
        return SetMember(self, SetMember.INTERSECT)

    @property
    def INTERSECT_ALL(self):
        return SetMember(self, SetMember.INTERSECT, all=True)

    @property
    def INTERSECT_DISTINCT(self):
        return SetMember(self, SetMember.INTERSECT, all=False)

    @property
    def EXCEPT(self):
        return SetMember(self, SetMember.EXCEPT)

    @property
    def EXCEPT_ALL(self):
        return SetMember(self, SetMember.EXCEPT, all=True)

    @property
    def EXCEPT_DISTINCT(self):
        return SetMember(self, SetMember.EXCEPT, all=False)

    def set_add(self, member, *args, **kwargs):
        """
        Add a new query to the set
        """
        self.set.append(member)
        self.set_init(*args, **kwargs)
        return self

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
        cursor = SELECT(F.count()).source(self.set.copy().limit(None, None)).execute(connection)
        return cursor.fetchone()[0]


class From(SQL):
    """
    FROM clause wrapper
    """

    def __init__(self, source, AS=None, ONLY=None):
        self.source = wrap_source(source, AS=AS, ONLY=ONLY)
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
        self.source = self.source.CROSS_JOIN(*args, **kwargs)
        return self

    def NATURAL_JOIN(self, *args, **kwargs):
        self.source = self.source.NATURAL_JOIN(*args, **kwargs)
        return self

    def LEFT_JOIN(self, *args, **kwargs):
        self.source = self.source.LEFT_JOIN(*args, **kwargs)
        return self

    def RIGHT_JOIN(self, *args, **kwargs):
        self.source = self.source.RIGHT_JOIN(*args, **kwargs)
        return self

    def FULL_JOIN(self, *args, **kwargs):
        self.source = self.source.FULL_JOIN(*args, **kwargs)
        return self

    def INNER_JOIN(self, *args, **kwargs):
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
