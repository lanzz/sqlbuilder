# -*- coding: utf-8 -*-
from __future__ import absolute_import

from ..sql import SQL, NameFactory, Identifier, Variable, Table, Aliasable


# importable references to identifier factory
# the "F" stands for "function" and "C" stands for "column", but there's no internal distinction
C = F = NameFactory(Identifier)

# importable reference to table factory
T = NameFactory(Table)

# importable reference to variable factory
V = NameFactory(Variable)


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


class DataManipulationQuery(Query, Aliasable):
    """
    Abstract base class for data manipulation queries
    """

class DataDefinitionQuery(Query):
    """
    Abstract base class for data definition queries
    """


from .select import SELECT
