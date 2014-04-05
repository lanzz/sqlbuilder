# -*- coding: utf-8 -*-
from __future__ import absolute_import

"""
SQL sorting
"""

from __future__ import absolute_import
from .base import SQL
from ..utils import Const


class Sorting(SQL):
    """
    Sorting order on an expression
    Sorting orders are no longer expressions, as they are not allowed in operations, only in ORDER BY clauses
    """

    DIR = Const('DIR', """Sort direction""",
        ASC=u' ASC',
        DESC=u' DESC',
    )

    NULLS = Const('NULLS', """NULL ordering""",
        FIRST=u' NULLS FIRST',
        LAST=u' NULLS LAST',
    )

    def __init__(self, expr, direction=None, nulls=None):
        self.expr = expr
        self.direction = direction
        self.nulls = nulls
        assert self.direction is None or self.direction in self.DIR, 'Invalid sorting direction: {dir}'.format(dir=self.direction)
        assert self.nulls is None or self.nulls in self.NULLS, 'Invalid sorting of nulls: {nulls}'.format(nulls=self.nulls)

    def _as_sql(self, connection, context):
        sql, args = SQL.wrap(self.expr)._as_sql(connection, context)
        sql = u'{expr}{dir}{nulls}'.format(
            expr=sql,
            dir='' if self.direction is None else self.direction,
            nulls='' if self.nulls is None else self.nulls,
        )
        return sql, args

    @property
    def ASC(self):
        self.direction = self.DIR.ASC
        return self

    @property
    def DESC(self):
        self.direction = self.DIR.DESC
        return self

    @property
    def NULLS_FIRST(self):
        self.nulls = self.NULLS.FIRST
        return self

    @property
    def NULLS_LAST(self):
        self.nulls = self.NULLS.LAST
        return self
