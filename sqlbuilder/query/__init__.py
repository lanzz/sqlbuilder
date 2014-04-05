# -*- coding: utf-8 -*-

"""
SQL queries
"""

from __future__ import absolute_import

from ..sql.name import Identifier, IdentifierFactory, Variable, VariableFactory, Table, TableFactory


# importable references to identifier factory
# the "F" stands for "function" and "C" stands for "column", but there's no internal distinction
C = F = IdentifierFactory

# importable reference to table factory
T = TableFactory

# importable reference to variable factory
V = VariableFactory


from .select import SELECT
