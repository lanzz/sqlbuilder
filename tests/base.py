# -*- coding: utf-8 -*-
from __future__ import absolute_import
import unittest
from sqlbuilder.dummy import dummy_connection, dummy_context


class TestCase(unittest.TestCase):
    """
    SQL-specific assertions
    """

    def as_sql(self, value, context=None):
        return value._as_sql(dummy_connection, context or {})

    def assertSQL(self, expr, sql, context=None):
        """
        Assert that expression evaluates to the given sql
        """
        self.assertEqual(self.as_sql(expr, context=context), sql)

    def assertSQLEquals(self, expr1, expr2, context=None):
        """
        Assert that two expressions evaluate to the same sql
        """
        self.assertEqual(self.as_sql(expr1, context=context), self.as_sql(expr2, context=context))
