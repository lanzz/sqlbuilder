# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..helpers import SQL, dummy_connection, dummy_context
from ..source import TableLike


class Query(SQL):
    """
    Abstract base class for queries
    """

    def execute(self, connection, *args, **context):
        """
        Allocate a cursor from the connection and execute the query
        """
        sql = self._as_sql(connection, context)
        cursor = connection.cursor()
        cursor.execute(sql, *args)
        return cursor


class DataManipulationQuery(Query, TableLike):
    """
    Abstract base class for data manipulation queries
    All DML queries are potential table-like data sources, though not all DBMS support non-SELECT data sources
    """

class DataDefinitionQuery(Query):
    """
    Abstract base class for data definition queries
    """


from .select import SELECT
from ..expression import F, C, V
