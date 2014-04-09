# -*- coding: utf-8 -*-

"""
SQL aliases
"""

from __future__ import absolute_import
from .base import SQL, SQLIterator
from .table import Joinable, Table
from .query import Query


class Alias(SQL):
    """
    Alias of an expression
    """

    def __init__(self, origin, alias):
        self._origin = origin
        self._alias = alias

    def _as_sql(self, connection, context):
        origin_sql, origin_args = SQL.wrap(self._origin)._as_sql(connection, context)
        alias_sql, alias_args = SQL.wrap(self._alias, id=True)._as_sql(connection, context)
        sql = u'{origin} AS {alias}'.format(
            origin=origin_sql,
            alias=alias_sql,
        )
        return sql, origin_args + alias_args


class TableAlias(Alias, Joinable):
    """
    Alias of a table
    """

    def __init__(self, origin, alias, columns=None):
        super(TableAlias, self).__init__(origin, alias)
        self._columns = columns

    def __getattr__(self, name):
        return Identifier('{name}.{subname}'.format(
            name=self._alias,
            subname=name,
        ))

    def _as_sql(self, connection, context):
        origin_sql, origin_args = SQL.wrap(self._origin)._as_sql(connection, context)
        alias_sql, alias_args = SQL.wrap(self._alias, id=True)._as_sql(connection, context)
        sql = u'{origin} AS {alias}'.format(
            origin=origin_sql,
            alias=alias_sql,
        )
        if self._columns:
            columns_sql, columns_args = SQLIterator(self._columns, id=True)._as_sql(connection, context)
            sql += '({columns})'.format(columns=columns_sql)
        else:
            columns_args = ()
        return sql, origin_args + alias_args + columns_args


class SubqueryAlias(TableAlias):
    """
    Alias of a subquery
    """

    def __init__(self, origin, alias, columns=None, LATERAL=None):
        super(SubqueryAlias, self).__init__(origin, alias, columns=columns)
        self._lateral = LATERAL or False

    def _as_sql(self, connection, context):
        origin_sql, origin_args = SQL.wrap(self._origin)._as_sql(connection, context)
        alias_sql, alias_args = SQL.wrap(self._alias, id=True)._as_sql(connection, context)
        sql = u'{lateral}({origin}) AS {alias}'.format(
            lateral=u'LATERAL ' if self._lateral else u'',
            origin=origin_sql,
            alias=alias_sql,
        )
        if self._columns:
            columns_sql, columns_args = SQLIterator(self._columns, id=True)._as_sql(connection, context)
            sql += '({columns})'.format(columns=columns_sql)
        else:
            columns_args = ()
        return sql, origin_args + alias_args + columns_args


class AliasFactory(object):
    """
    Factory for alias names
    """

    def __getattribute__(self, name):
        return AliasName(name)
    def __setattr__(self, name, value):
        raise AttributeError('Alias names are not assignable')
    def __call__(self, name, expr):
        return AliasName(name)(expr)


class AliasName(object):
    """
    Wrapper for an alias name
    """

    def __init__(self, name):
        self.name = name

    def __call__(self, expr, *args, **kwargs):
        """
        Create the alias
        """
        if isinstance(expr, Table):
            return TableAlias(expr, self.name, *args, **kwargs)
        elif isinstance(expr, Query):
            return SubqueryAlias(expr, self.name, *args, **kwargs)
        else:
            return Alias(expr, self.name, *args, **kwargs)

A = AliasFactory()


from .expression import Identifier
