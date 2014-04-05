# -*- coding: utf-8 -*-

"""
SQL expressions
"""

from __future__ import absolute_import
from .base import SQL, SQLIterator


class VALUES(SQL):
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

    def AS(self, *args, **kwargs):
        return SubqueryAlias(self, *args, **kwargs)


class SubqueryAlias(TableAlias):
    """
    Alias of a subquery
    """

    def __init__(self, origin, alias, columns=None, LATERAL=None):
        super(SubqueryAlias, self).__init__(origin, alias)
        self._columns = columns
        self._lateral = LATERAL or False

    def _as_sql(self, connection, context):
        origin_sql, origin_args = SQL.wrap(self._origin)._as_sql(connection, context)
        alias_sql, alias_args = SQL.wrap(self._alias, id=True)._as_sql(connection, context)
        sql = u'{lateral}({origin}) AS {alias}'.format(
            lateral=u'LATERAL ' if self._lateral else u'',
            origin=origin_sql,
            alias=alias_sql,
        )
        if self._columns is not None and len(self._columns):
            sql += u' ({columns})'.format(
                columns=SQLIterator(self._columns, id=True)._as_sql(connection, context),
            )
        return sql, origin_args + alias_args
