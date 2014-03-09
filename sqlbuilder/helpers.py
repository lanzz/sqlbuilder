# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .dummy import dummy_connection, dummy_context


def to_sql(obj, connection, context=None):
    """
    Render an SQL object
    """
    context = context or {}
    if hasattr(obj, '_as_sql'):
        # object knows how to render itself
        return obj._as_sql(connection, context)
    else:
        # dumb object, return as query argument
        return '%s', ( obj, )

def merge_sql(iterable, sep=', '):
    """
    Merge an interable of (sql, args) items into a single (sql, args) tuple
    """
    if iterable is None:
        return '', ()
    sql, args = zip(*list(iterable))
    sql = sep.join(sql)
    args = sum(args, ())
    return sql, args

def to_sql_iter(iterable, connection, context=None, sep=', '):
    """
    Render an interable of SQL objects
    """
    if iterable is None:
        return '', ()
    context = context or {}
    if hasattr(iterable, '_as_sql'):
        # iterable knows how to render itself
        return iterable._as_sql(connection, context)
    return merge_sql((to_sql(item, connection, context) for item in iterable), sep=sep)

class SQL(object):
    """
    Base for classes whose instances support the `to_sql` method
    """

    def _as_sql(self, connection, context):
        """
        Return SQL for this instance
        """
        raise NotImplementedError()

    def __unicode__(self):
        sql, args = self._as_sql(dummy_connection, dummy_context)
        return sql % args

    def __repr__(self):
        sql, args = self._as_sql(dummy_connection, dummy_context)
        return '<{name} {sql!r}, {args!r}>'.format(
            name=self.__class__.__name__,
            sql=sql,
            args=args,
        )
