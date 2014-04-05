# -*- coding: utf-8 -*-

"""
SQL base syntax
"""

from __future__ import absolute_import


def merge_sql(iterable, sep=', '):
    """
    Merge an interable of (sql, args) items into a single (sql, args) tuple
    The `sql` strings will be joind using `sep`
    """

    if iterable is None:
        return u'', ()
    iterable = list(iterable)
    if not len(iterable):
        return u'', ()
    sql, args = zip(*iterable)
    sql = sep.join(sql)
    args = sum(args, ())
    return sql, args


class SQL(object):
    """
    Base for classes that can be rendered as SQL
    Used as a wrapper for primitive values (values and identifiers)
    """

    @classmethod
    def wrap(cls, value, id=False):
        """
        Instantiate a `SQL` or `Identifier` instance if `value` is plain
        """
        if isinstance(value, SQL):
            # value is already an instance of SQL
            return value
        return Identifier(value) if id else Value(value)

    def _as_sql(self, connection, context):
        raise NotImplementedError()

    def __unicode__(self):
        sql, args = self._as_sql(dummy_connection, dummy_context)
        return sql % args

    def __repr__(self):
        sql, args = self._as_sql(dummy_connection, dummy_context)
        return u'<{name} {sql!r}, {args!r}>'.format(
            name=self.__class__.__name__,
            sql=sql,
            args=args,
        )


class SQLIterator(SQL):

    """
    Iterator of SQL objects
    """

    def __init__(self, iterable, sep=', ', id=False):
        self.iterable = iterable
        self.sep = sep
        self.id = id

    def __iter__(self):
        if hasattr(self.iterable, '_as_sql'):
            # iterable knows how to render itself
            yield self.iterable
            return
        for item in self.iterable:
            yield SQL.wrap(item, id=self.id)

    def iter(self):
        return self.__iter__()

    def _as_sql(self, connection, context):
        return merge_sql((item._as_sql(connection, context) for item in self), sep=self.sep)


from ..dummy import dummy_connection, dummy_context
from .expression import Identifier, Value
