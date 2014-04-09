# -*- coding: utf-8 -*-

"""
SQL syntax
"""

from __future__ import absolute_import
from .base import SQL
from .name import C, F, T, V, ONLY
from .expression import CASE
from .query import VALUES
from .sort import ASC, DESC

L = SQL.wrap
