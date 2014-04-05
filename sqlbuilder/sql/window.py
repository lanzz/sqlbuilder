# -*- coding: utf-8 -*-

"""
SQL windows
"""

from __future__ import absolute_import
from .base import SQL, SQLIterator
from ..utils import Const


class Window(SQL):
    """
    Window definition
    """

    TYPE = Const('TYPE', """Frame types""",
        RANGE=u'RANGE',
        ROWS=u'ROWS',
    )

    ENDPOINT = Const('ENDPOINT', """Reference endpoints""",
        START=u'UNBOUNDED PRECEDING',
        END=u'UNBOUNDED FOLLOWING',
    )

    def __init__(self, window=None, PARTITION_BY=None, ORDER_BY=None, RANGE=None, ROWS=None):
        self.window = window
        self.partition = PARTITION_BY
        self.order = ORDER_BY
        self.range = RANGE
        self.rows = ROWS
        assert not (self.range and self.rows), 'Cannot specify both RANGE and ROWS frames'

    def reference(self, offset, endpoint=None):
        if offset is None:
            return endpoint, ()
        if offset < 0:
            return u'%s PRECEDING', (abs(offset),)
        if offset > 0:
            return u'%s FOLLOWING', (offset,)
        return u'CURRENT ROW', ()

    def _as_sql(self, connection, context):
        clauses = []
        args = ()
        if self.window:
            window_sql, window_args = SQL.wrap(self.window, id=True)._as_sql(connection, context)
            clauses.append(window_sql)
            args += args
        if self.partition:
            partition_sql, partition_args = SQLIterator(self.partition)._as_sql(connection, context)
            clauses.append(u'PARTITION BY {expr}'.format(
                expr=partition_sql,
            ))
            args += partition_args
        if self.order:
            order_sql, order_args = SQLIterator(self.order)._as_sql(connection, context)
            clauses.append(u'ORDER BY {expr}'.format(
                expr=order_sql,
            ))
            args += order_args
        if self.range or self.rows:
            if self.range:
                frame_type = self.FRAME.RANGE
                frame = self.range
            else:
                frame_type = self.FRAME.ROWS
                frame = self.rows
            try:
                start, end = frame
            except TypeError:
                # single value
                start_sql, start_args = self.reference(frame, self.ENDPOINT.START)
                clauses.append(u'{type} {start}'.format(
                    type=frame_type,
                    start=start_sql,
                ))
                args += start_args
            else:
                # range
                start_sql, start_args = self.reference(start, self.ENDPOINT.START)
                end_sql, end_args = self.reference(end, self.ENDPOINT.END)
                clauses.append(u'{type} BETWEEN {start} AND {end}'.format(
                    type=frame_type,
                    start=start_sql,
                    end=end_sql,
                ))
                args += start_args + end_args
        sql = u'({clauses})'.format(
            clauses=' '.join(clauses),
        )
        return sql, args
