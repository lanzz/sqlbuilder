# -*- coding: utf-8 -*-

"""
SQL name factories
"""

from __future__ import absolute_import
from .base import SQL
from .expression import Variable, Identifier
from .table import Table


def NameFactory(Class, prefix=None, as_sql=None, args=None, kwargs=None):
    """
    Factory that returns a new class that converts attribute access to Class instances
    """

    prefix = prefix or ''
    args = args or ()
    kwargs = kwargs or {}

    def __getattr__(self, name):
        return Class(prefix+name, *args, **kwargs)
    def __setattr__(self, name, value):
        raise AttributeError(name)
    def __delattr__(self, name):
        raise AttributeError(name)

    name = '{classname}Factory'.format(classname=Class.__name__)
    bases = (object,)
    attrs = dict(
        __getattr__=__getattr__,
        __setattr__=__setattr__,
        __delattr__=__delattr__,
    )

    if as_sql:
        # create factory that renders as SQL
        attrs['_as_sql'] = as_sql
        bases = (SQL,)

    return type(name, bases, attrs)()


# prepare importable shorthand names for the various name factories
T = TableFactory = NameFactory(Table)
ONLY = NameFactory(Table, kwargs={ 'ONLY': True })
V = VariableFactory = NameFactory(Variable)
C = F = IdentifierFactory = NameFactory(Identifier, as_sql=lambda self, connection, context: Wildcard()._as_sql(connection, context))
