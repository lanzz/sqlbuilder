# -*- coding: utf-8 -*-
from __future__ import absolute_import

"""
SQL name factories
"""

from __future__ import absolute_import
from .base import SQL
from .expression import Variable, Identifier
from .table import Table


def NameFactory(Class, prefix=None, as_sql=None):
    """
    Factory that returns a new class that converts attribute access to Class instances
    """

    prefix = prefix or ''

    def __getattr__(self, name):
        return Class(prefix+name)
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


TableFactory = NameFactory(Table)
VariableFactory = NameFactory(Variable)
IdentifierFactory = NameFactory(Identifier, as_sql=lambda self, connection, context: Wildcard()._as_sql(connection, context))
