# -*- coding: utf-8 -*-
from __future__ import absolute_import

from . import DataManipulationQuery
from ..helpers import SQL, to_sql, to_sql_iter
from ..source import wrap_source, TableAlias
from ..expression import F, Alias


class SetMember(SQL):
    """
    Member of a set of SELECT queries
    """

    __slots__ = 'parent', 'op', 'all', 'query'

    # set operations
    UNION = ' UNION '
    INTERSECT = ' INTERSECT '
    EXCEPT = ' EXCEPT '

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
            dups='' if self.all is None else 'ALL ' if self.all else 'DISTINCT ',
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

    __slots__ = 'distinct', 'columns', 'source', 'order', 'limit', 'offset'

    def __init__(self, *columns, **kwargs):
        super(SELECT, self).__init__()
        self.set_init(*columns, **kwargs)
        self.order = None
        self.limit = None
        self.offset = None
        self.set = []

    def set_init(self, *columns, **kwargs):
        """
        Limited initialization for a new set query
        """
        self.distinct = kwargs.get('DISTINCT', False)
        self.columns = list(columns)
        self.source = None

    @classmethod
    def DISTINCT(cls, *args, **kwargs):
        """
        Convenience constructor for SELECT DISTINCT queries
        """
        kwargs['DISTINCT'] = True
        return cls(*args, **kwargs)

    def _as_sql(self, connection, context):
        if self.columns:
            sql, args = to_sql_iter(self.columns, connection, context)
        else:
            sql = '*'
            args = ()
        sql = u'SELECT {distinct}{columns}'.format(
            distinct='DISTINCT ' if self.distinct else '',
            columns=sql,
        )
        if self.source is not None:
            source_sql, source_args = self.source._as_sql(connection, context)
            sql += source_sql
            args += source_args
        if self.order is not None:
            order_sql, order_args = to_sql_iter(self.order, connection, context)
            sql += u' ORDER BY {order}'.format(order=order_sql)
            args += order_args
        if self.limit is not None:
            limit_sql, limit_args = to_sql(self.limit, connection, context)
            sql += u' LIMIT {limit}'.format(limit=limit_sql)
            args += limit_args
            if self.offset is not None:
                offset_sql, offset_args = to_sql(self.offset, connection, context)
                sql += u' OFFSET {offset}'.format(offset=offset_sql)
                args += offset_args
        else:
            assert self.offset is None, 'Cannot specify OFFSET without LIMIT clause'

        if self.set:
            set_sql, set_args = to_sql_iter(self.set, connection, context)
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

    def __call__(self, expr, AS=None):
        """
        Add a column to the select list
        """
        self.columns.append(Alias(expr, AS) if AS else expr)
        return self

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

    def AS(self, alias):
        return SelectAlias(self, alias)

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

    __slots__ = 'source', 'where', 'group_by', 'having'

    def __init__(self, source, AS=None, ONLY=None):
        self.source = wrap_source(source, AS=AS, ONLY=ONLY)
        self.where = None
        self.group_by = None
        self.having = None

    def _as_sql(self, connection, context):
        sql, args = to_sql(self.source, connection, context)
        sql = u' FROM {source}'.format(
            source=sql,
        )
        if self.where:
            where_sql, where_args = to_sql(self.where, connection, context)
            sql += ' WHERE {condition}'.format(
                condition=where_sql,
            )
            args += where_args
        if self.group_by:
            group_sql, group_args = to_sql_iter(self.group_by, connection, context)
            sql += ' GROUP BY {columns}'.format(
                columns=group_sql,
            )
            args += group_args
        if self.having:
            having_sql, having_args = to_sql(self.having, connection, context)
            sql += ' HAVING {condition}'.format(
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


class SelectAlias(TableAlias):
    """
    Alias for a SELECT statement used as a subquery
    """

    def _as_sql(self, connection, context):
        sql, args = self.source._as_sql(connection, context)
        sql = u'({subquery}) AS {alias}'.format(
            subquery=sql,
            alias=connection.quote_identifier(self.alias),
        )
        return sql, args
