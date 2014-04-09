# -*- coding: utf-8 -*-

"""
SQL joins
"""

from __future__ import absolute_import
from .base import SQL, SQLIterator
from .query import Query
from ..utils import Const


class Joinable(SQL):
    """
    Base class for joinable classes (tables, subquery aliases)
    """

    def CROSS_JOIN(self, other, *args, **kwargs):
        return CrossJoin(self, other, *args, **kwargs)

    def LEFT_JOIN(self, other, *args, **kwargs):
        JoinClass = NaturalJoin if kwargs.pop('NATURAL', False) else ConditionalJoin
        return JoinClass(self, other, type=Join.TYPE.LEFT, *args, **kwargs)

    def RIGHT_JOIN(self, other, *args, **kwargs):
        JoinClass = NaturalJoin if kwargs.pop('NATURAL', False) else ConditionalJoin
        return JoinClass(self, other, type=Join.TYPE.RIGHT, *args, **kwargs)

    def FULL_JOIN(self, other, *args, **kwargs):
        JoinClass = NaturalJoin if kwargs.pop('NATURAL', False) else ConditionalJoin
        return JoinClass(self, other, type=Join.TYPE.FULL, *args, **kwargs)

    def INNER_JOIN(self, other, *args, **kwargs):
        JoinClass = NaturalJoin if kwargs.pop('NATURAL', False) else ConditionalJoin
        return JoinClass(self, other, type=Join.TYPE.INNER, *args, **kwargs)


class Table(Joinable):
    """
    Table reference
    """

    def __init__(self, name, ONLY=None):
        object.__setattr__(self, '_name', name)
        object.__setattr__(self, '_only', False if ONLY is None else ONLY)

    def _as_sql(self, connection, context):
        sql, args = SQL.wrap(self._name, id=True)._as_sql(connection, context)
        if self._only:
            sql = u'ONLY ' + sql
        return sql, args

    def __getattr__(self, name):
        return Table(u'{name}.{subname}'.format(
            name=self._name,
            subname=name,
        ), ONLY=self._only)

    def __setattr__(self, name, value):
        raise AttributeError('Names are not assignable')

    def __call__(self):
        """
        Column identifier factory
        """
        return NameFactory(Identifier, prefix=self._name + u'.', as_sql=lambda _, connection, context: Wildcard(self)._as_sql(connection, context))


class VALUES(Joinable, Query):
    """
    VALUES expression
    """

    def __init__(self, *values):
        self.rows = [ values ]

    def __call__(self, *values):
        """
        Add another row of values
        """
        self.rows.append(values)
        return self

    def _as_sql(self, connection, context):
        assert len(self.rows), 'No rows in VALUE expression'
        rows = []
        args = ()
        for row in self.rows:
            row_sql, row_args = SQLIterator(row)._as_sql(connection, context)
            rows.append(u'({row})'.format(row=row_sql))
            args += row_args
        sql = u'VALUES {rows}'.format(
            rows=u', '.join(rows),
        )
        return sql, args


class Wildcard(SQL):
    """
    `table.*` wildcard
    """

    def __init__(self, table=None):
        self.table = table

    def _as_sql(self, connection, context):
        if not self.table:
            return u'*', ()
        sql, args = self.table._as_sql(connection, context)
        sql += u'.*'
        return sql, args


class Join(Joinable):
    """
    Abstract base class for joins
    """

    TYPE = Const('TYPE', """Join types""",
        INNER=u'INNER',
        LEFT=u'LEFT OUTER',
        RIGHT=u'RIGHT OUTER',
        FULL=u'FULL OUTER',
    )

    def __init__(self, left, right, parens=True):
        self.left = left
        self.right = right
        self.parens = True if parens is None else parens
        assert isinstance(self.left, Joinable) and isinstance(self.right, Joinable), 'Invalid join sources'


class QualifiedJoin(Join):
    """
    Abstract base class for qualified joins
    """

    def __init__(self, left, right, parens=None, type=None):
        super(QualifiedJoin, self).__init__(left, right, parens=parens)
        self.type = type
        assert self.type in Join.TYPE, 'Invalid join type: {type}'.format(type=self.type)


class CrossJoin(Join):

    def _as_sql(self, connection, context):
        left_sql, left_args = SQL.wrap(self.left)._as_sql(connection, context)
        right_sql, right_args = SQL.wrap(self.right)._as_sql(connection, context)
        sql = u'{left} CROSS JOIN {right}'.format(
            left=left_sql,
            right=right_sql,
        )
        if self.parens:
            sql = u'({sql})'.format(sql=sql)
        return sql, left_args + right_args


class NaturalJoin(QualifiedJoin):

    def _as_sql(self, connection, context):
        left_sql, left_args = SQL.wrap(self.left)._as_sql(connection, context)
        right_sql, right_args = SQL.wrap(self.right)._as_sql(connection, context)
        sql = u'{left} NATURAL {type} JOIN {right}'.format(
            left=left_sql,
            right=right_sql,
            type=self.type,
        )
        if self.parens:
            sql = u'({sql})'.format(sql=sql)
        return sql, left_args + right_args


class ConditionalJoin(QualifiedJoin):

    def __init__(self, left, right, parens=None, type=None, ON=None, USING=None):
        super(ConditionalJoin, self).__init__(left, right, parens=parens, type=type)
        self.on = ON
        if USING is not None and not isinstance(USING, (list, tuple)):
            using = USING,
        self.using = USING
        assert (self.on is None or self.using is None), 'Cannot have both ON and USING clauses on a join'
        assert not (self.on is None and self.using is None), 'Either ON or USING clause is required for conditional join'

    def _as_sql(self, connection, context):
        if self.on:
            expr_sql, condition_args = SQL.wrap(self.on)._as_sql(connection, context)
            condition_sql = u'ON {expression}'.format(expression=expr_sql)
        else:
            columns_sql, condition_args = SQLIterator(self.using)._as_sql(connection, context)
            condition_sql = u'USING ({columns})'.format(columns=columns_sql)
        left_sql, left_args = SQL.wrap(self.left)._as_sql(connection, context)
        right_sql, right_args = SQL.wrap(self.right)._as_sql(connection, context)
        sql = u'{left} {type} JOIN {right} {condition}'.format(
            left=left_sql,
            right=right_sql,
            type=self.type,
            condition=condition_sql,
        )
        if self.parens:
            sql = u'({sql})'.format(sql=sql)
        return sql, left_args + right_args + condition_args


from .name import NameFactory
from .expression import Identifier
