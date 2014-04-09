# -*- coding: utf-8 -*-

"""
SQL syntax
"""

from __future__ import absolute_import
from .base import SQL
from .name import C, F, T, V, ONLY
from .expression import CASE, AND, XOR, OR, NOT, LIKE, NOT_LIKE, ILIKE, NOT_ILIKE, RLIKE, NOT_RLIKE, IN, NOT_IN, IS_NULL, IS_NOT_NULL
from .sort import ASC, DESC
from .table import VALUES
from .alias import A

L = SQL.wrap
