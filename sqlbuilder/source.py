# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helpers import SQL, to_sql, to_sql_iter, merge_sql, dummy_connection


def wrap_source(source, AS=None, ONLY=None):
    """
    Wrap join source in a `Table` instance if necessary
    """
    if not isinstance(source, Source):
        source = Table(source, ONLY=ONLY)
        if AS:
            source = source.AS(AS)
    else:
        assert AS is None, 'Cannot assign alias to an existing source'
    return source


class Source(SQL):
    """
    Abstract base class for data sources (tables, joins, queries)
    """

    def CROSS_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return CrossJoin(self, other, *args, **kwargs)

    def NATURAL_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return NaturalJoin(self, other, *args, **kwargs)

    def LEFT_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return ConditionalJoin(self, other, type=Join.LEFT, *args, **kwargs)

    def RIGHT_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return ConditionalJoin(self, other, type=Join.RIGHT, *args, **kwargs)

    def FULL_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return ConditionalJoin(self, other, type=Join.FULL, *args, **kwargs)

    def INNER_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return ConditionalJoin(self, other, type=Join.INNER, *args, **kwargs)


class TableLike(Source):
    """
    Abstract base class for table-like data sources
    """

    __slots__ = 'alias',

    def __init__(self, AS=None):
        super(TableLike, self).__init__()
        self.alias = AS
        assert (self.alias is None) or isinstance(self.alias, basestring), 'Alias must be a plain string'

    def copy(self):
        return self.__class__(AS=self.alias)

    @property
    def C(self):
        """
        Column identifier factory
        """
        assert self.alias is not None, 'Cannot reference columns on source with no alias'
        return getattr(F, self.alias)

    def AS(self, alias):
        return TableAlias(self, alias)


class Table(TableLike):

    __slots__ = 'name', 'only'

    def __init__(self, name, ONLY=None):
        super(Table, self).__init__()
        if not hasattr(name, '_as_sql'):
            # wrap raw name in an expression
            name = Name(name)
        self.name = name
        self.only = False if ONLY is None else ONLY

    def _as_sql(self, connection, context):
        sql, args = self.name._as_sql(connection, context)
        if self.only:
            sql = 'ONLY ' + sql
        return sql, args

    def copy(self):
        return self.__class__(self.name, ONLY=self.only)

    @property
    def C(self):
        """
        Column identifier factory
        """
        return getattr(F, self.alias or self.name)


class Values(TableLike):
    """
    VALUES expression
    """

    __slots__ = 'rows',

    def __init__(self, *columns):
        super(Values, self).__init__()
        self.rows = [columns]

    def __call__(self, *columns):
        self.rows.append(columns)
        return self

    def _as_sql(self, connection, context):
        if not len(self.rows):
            raise ValueError('No rows in VALUE expression')
        sql, args = merge_sql((to_sql_iter(row, connection, context) for row in self.rows), sep='), (')
        sql = u'VALUES ({rows})'.format(
            rows=sql,
        )
        return sql, args

    def AS(self, alias, columns=None):
        return SubqueryAlias(self, alias, columns=columns)


class TableAlias(Source):
    """
    Alias for a table-like source
    """

    def __init__(self, source, alias):
        self.source = source
        self.alias = alias

    def _as_sql(self, connection, context):
        sql, args = self.source._as_sql(connection, context)
        sql = u'{source} AS {alias}'.format(
            source=sql,
            alias=connection.quote_identifier(self.alias),
        )
        return sql, args


class SubqueryAlias(TableAlias):
    """
    Alias for a subquery
    """

    def __init__(self, source, alias, columns=None):
        super(SubqueryAlias, self).__init__(source, alias)
        self.columns = columns

    def _as_sql(self, connection, context):
        sql, args = self.source._as_sql(connection, context)
        sql = u'({subquery}) AS {alias}'.format(
            subquery=sql,
            alias=connection.quote_identifier(self.alias),
        )
        if self.columns is not None and len(self.columns):
            sql += u' ({columns})'.format(
                columns=', '.join(map(connection.quote_identifier, self.columns)),
            )
        return sql, args


class Join(Source):
    """
    Abstract base class for joins
    """

    __slots__ = 'left', 'right'

    # constants
    INNER = 'INNER'
    LEFT = 'LEFT OUTER'
    RIGHT = 'RIGHT OUTER'
    FULL = 'FULL OUTER'

    def __init__(self, left, right):
        super(Join, self).__init__()
        self.left = left
        self.right = right
        assert isinstance(self.left, Source) and isinstance(self.right, Source), 'Invalid join sources'

    def copy(self):
        return self.__class__(self.left.copy(), self.right.copy())


class QualifiedJoin(Join):
    """
    Abstract base class for qualified joins
    """

    __slots__ = 'type',

    def __init__(self, left, right, type=None):
        super(QualifiedJoin, self).__init__(left, right)
        self.type = type or self.INNER
        assert self.type in (self.INNER, self.LEFT, self.RIGHT, self.FULL), 'Invalid join type: {type}'.format(type=self.type)

    def copy(self):
        return self.__class__(self.left.copy(), self.right.copy(), type=self.type)


class CrossJoin(Join):

    def _as_sql(self, connection, context):
        left_sql, left_args = to_sql(self.left, connection, context)
        right_sql, right_args = to_sql(self.right, connection, context)
        sql = u'({left} CROSS JOIN {right})'.format(
            left=left_sql,
            right=right_sql,
        )
        return sql, left_args + right_args


class NaturalJoin(QualifiedJoin):

    def _as_sql(self, connection, context):
        left_sql, left_args = to_sql(self.left, connection, context)
        right_sql, right_args = to_sql(self.right, connection, context)
        sql = u'({left} NATURAL {type} JOIN {right})'.format(
            left=left_sql,
            right=right_sql,
            type=self.type,
        )
        return sql, args1 + args2


class ConditionalJoin(QualifiedJoin):

    __slots__ = 'on', 'using'

    def __init__(self, left, right, type=None, on=None, using=None):
        super(ConditionalJoin, self).__init__(left, right, type=type)
        self.on = on
        if using is not None and not isinstance(using, (list, tuple)):
            using = using,
        self.using = using
        assert (self.on is None or self.using is None), 'Cannot have both ON and USING clauses on a join'
        assert not (self.on is None and self.using is None), 'Either ON or USING clause is required for conditional join'

    def _as_sql(self, connection, context):
        if self.on:
            expr_sql, condition_args = to_sql(self.on, connection, context)
            condition_sql = u'ON {expression}'.format(expression=expr_sql)
        else:
            columns_sql, condition_args = to_sql_iter(self.using, connection, context)
            condition_sql = u'USING ({columns})'.format(columns=columns_sql)
            condition_args = sum(condition_args, ())
        left_sql, left_args = to_sql(self.left, connection, context)
        right_sql, right_args = to_sql(self.right, connection, context)
        sql = u'({left} {type} JOIN {right} {condition})'.format(
            left=left_sql,
            right=right_sql,
            type=self.type,
            condition=condition_sql,
        )
        return sql, left_args + right_args + condition_args

    def copy(self):
        return self.__class__(self.left.copy(), self.right.copy(), type=self.type, on=self.on, using=self.using)


from .expression import F, Expression, Name
