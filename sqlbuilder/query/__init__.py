# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..sql import SQL, Identifier, IdentifierFactory, Variable, VariableFactory, Table, TableFactory


# importable references to identifier factory
# the "F" stands for "function" and "C" stands for "column", but there's no internal distinction
C = F = IdentifierFactory

# importable reference to table factory
T = TableFactory

# importable reference to variable factory
V = VariableFactory


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


from .select import SELECT
