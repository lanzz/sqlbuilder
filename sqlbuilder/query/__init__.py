# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..helpers import SQL, dummy_connection, dummy_context
from ..source import TableLike


class Query(SQL):
    """
    Abstract base class for queries
    """

    def _as_statement(self, connection, context):
        """
        Render the query as a top-level statement (no parens, no alias)
        """
        raise NotImplementedError()

    def execute(self, connection, *args, **context):
        """
        Allocate a cursor from the connection and execute the query
        """
        sql = self._as_statement(connection, context)
        cursor = connection.cursor()
        cursor.execute(sql, *args)
        return cursor


class DataManipulationQuery(Query, TableLike):
    """
    Abstract base class for data manipulation queries
    All DML queries are potential table-like data sources, though not all DBMS support non-SELECT data sources
    """

    def _as_statement(self, connection, context):
        """
        Render the statement as a top-level query
        """
        raise NotImplementedError()

    def _as_sql(self, connection, context):
        """
        Render the statement as a subquery
        """
        sql, args = self._as_statement(connection, context)
        sql = u'({sql})'.format(sql=sql)
        if self.alias:
            sql += u' AS ' + connection.quote_identifier(self.alias)
        return sql, args

    def __unicode__(self):
        sql, args = self._as_statement(dummy_connection, dummy_context)
        return sql % args

    def __repr__(self):
        sql, args = self._as_statement(dummy_connection, dummy_context)
        return '<{sql!r}, {args!r}>'.format(
            name=self.__class__.__name__,
            sql=sql,
            args=args,
        )


class DataDefinitionQuery(Query):
    """
    Abstract base class for data definition queries
    """


from .select import SELECT
from ..expression import F, C, V
