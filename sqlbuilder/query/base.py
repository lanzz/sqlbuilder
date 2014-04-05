# -*- coding: utf-8 -*-

"""
SQL query base classes
"""

from __future__ import absolute_import
from ..sql.base import SQL


class Query(SQL):
    """
    Abstract base class for queries
    """

    def execute(self, connection, *args, **context):
        """
        Allocate a cursor from the connection and execute the query
        """
        sql, args = self._as_sql(connection, context)
        cursor = connection.cursor()
        cursor.execute(sql, *args)
        return cursor


class DataManipulationQuery(Query):
    """
    Abstract base class for data manipulation queries
    """


class DataDefinitionQuery(Query):
    """
    Abstract base class for data definition queries
    """
