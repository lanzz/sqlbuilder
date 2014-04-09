# -*- coding: utf-8 -*-
from __future__ import absolute_import
from ..base import TestCase
from sqlbuilder.query import *


class ColumnsTest(TestCase):

    def test_empty(self):
        self.assertSQL(SELECT(),
                    (u'SELECT *', ()))

    def test_wildcard(self):
        self.assertSQL(SELECT(C),
                    (u'SELECT *', ()))

    def test_table_wildcard(self):
        self.assertSQL(SELECT(T.table()),
                    (u'SELECT table.*', ()))

    def test_literal(self):
        self.assertSQL(SELECT('foo', 123, True, False, None),
                    (u'SELECT %s, %s, %s, %s, %s', ('foo', 123, True, False, None)))

    def test_literal_alias(self):
        self.assertSQL(SELECT(A.literal_alias('literal')),
                    (u'SELECT %s AS literal_alias', ('literal',)))

    def test_column(self):
        self.assertSQL(SELECT(C.foo, C.bar.baz, T.table().xyzzy),
                    (u'SELECT foo, bar.baz, table.xyzzy', ()))

    def test_column_alias(self):
        self.assertSQL(SELECT(A.foo_alias(C.foo)),
                    (u'SELECT foo AS foo_alias', ()))

    def test_expression(self):
        self.assertSQL(SELECT(F.sum(C.qty) * F.avg(C.price) * 1.2),
                    (u'SELECT ((sum(qty) * avg(price)) * %s)', (1.2,)))

    def test_expression_alias(self):
        self.assertSQL(SELECT(A.average(F.sum(C.qty) * F.avg(C.price) * 1.2)),
                    (u'SELECT ((sum(qty) * avg(price)) * %s) AS average', (1.2,)))

    def test_subquery(self):
        self.assertSQL(SELECT(A.average(SELECT(F.avg(C.price)))),
                    (u'SELECT (SELECT avg(price)) AS average', ()))


class FunctionTest(TestCase):

    def test_simple(self):
        self.assertSQL(SELECT(F.func_name()),
                    (u'SELECT func_name()', ()))

    def test_alias(self):
        self.assertSQL(SELECT(A.func_alias(F.func_name())),
                    (u'SELECT func_name() AS func_alias', ()))

    def test_literal(self):
        self.assertSQL(SELECT(F.abs(-1)),
                    (u'SELECT abs(%s)', (-1,)))

    def test_column(self):
        self.assertSQL(SELECT(F.count(C.id)),
                    (u'SELECT count(id)', ()))

    def test_wildcard(self):
        self.assertSQL(SELECT(F.count(C)),
                    (u'SELECT count(*)', ()))

    def test_distinct(self):
        self.assertSQL(SELECT(F.count(C.id).DISTINCT),
                    (u'SELECT count(DISTINCT id)', ()))

    def test_all(self):
        self.assertSQL(SELECT(F.count(C.id).ALL),
                    (u'SELECT count(ALL id)', ()))

    def test_window(self):
        self.assertSQL(SELECT(F.count(C.id).OVER(C.window)),
                    (u'SELECT count(id) OVER window', ()))


class ExpressionTest(TestCase):

    def test_lt(self):
        self.assertSQL(SELECT(C.foo < C.bar),
                    (u'SELECT (foo < bar)', ()))

    def test_le(self):
        self.assertSQL(SELECT(C.foo <= C.bar),
                    (u'SELECT (foo <= bar)', ()))

    def test_eq(self):
        self.assertSQL(SELECT(C.foo == C.bar),
                    (u'SELECT (foo = bar)', ()))

    def test_ne(self):
        self.assertSQL(SELECT(C.foo != C.bar),
                    (u'SELECT (foo <> bar)', ()))

    def test_gt(self):
        self.assertSQL(SELECT(C.foo > C.bar),
                    (u'SELECT (foo > bar)', ()))

    def test_ge(self):
        self.assertSQL(SELECT(C.foo >= C.bar),
                    (u'SELECT (foo >= bar)', ()))

    def test_add(self):
        self.assertSQL(SELECT(C.foo + C.bar),
                    (u'SELECT (foo + bar)', ()))

    def test_sub(self):
        self.assertSQL(SELECT(C.foo - C.bar),
                    (u'SELECT (foo - bar)', ()))

    def test_mul(self):
        self.assertSQL(SELECT(C.foo * C.bar),
                    (u'SELECT (foo * bar)', ()))

    def test_div(self):
        self.assertSQL(SELECT(C.foo / C.bar),
                    (u'SELECT (foo / bar)', ()))

    def test_floordiv(self):
        self.assertSQL(SELECT(C.foo // C.bar),
                    (u'SELECT (foo / bar)', ()))

    def test_mod(self):
        self.assertSQL(SELECT(C.foo % C.bar),
                    (u'SELECT mod(foo, bar)', ()))

    def test_pow(self):
        self.assertSQL(SELECT(C.foo ** C.bar),
                    (u'SELECT power(foo, bar)', ()))

    def test_lshift(self):
        self.assertSQL(SELECT(C.foo << C.bar),
                    (u'SELECT (foo << bar)', ()))

    def test_rshift(self):
        self.assertSQL(SELECT(C.foo >> C.bar),
                    (u'SELECT (foo >> bar)', ()))

    def test_and(self):
        self.assertSQL(SELECT(C.foo & C.bar),
                    (u'SELECT (foo & bar)', ()))

    def test_xor(self):
        self.assertSQL(SELECT(C.foo ^ C.bar),
                    (u'SELECT (foo ^ bar)', ()))

    def test_or(self):
        self.assertSQL(SELECT(C.foo | C.bar),
                    (u'SELECT (foo | bar)', ()))

    def test_neg(self):
        self.assertSQL(SELECT(-C.foo),
                    (u'SELECT (- foo)', ()))

    def test_pos(self):
        self.assertSQL(SELECT(+C.foo),
                    (u'SELECT (+ foo)', ()))

    def test_abs(self):
        self.assertSQL(SELECT(abs(C.foo)),
                    (u'SELECT abs(foo)', ()))

    def test_invert(self):
        self.assertSQL(SELECT(~C.foo),
                    (u'SELECT (~ foo)', ()))

    def test_bool_and(self):
        self.assertSQL(SELECT(AND(C.foo, C.bar)),
                    (u'SELECT (foo AND bar)', ()))

    def test_bool_xor(self):
        self.assertSQL(SELECT(XOR(C.foo, C.bar)),
                    (u'SELECT (foo XOR bar)', ()))

    def test_bool_or(self):
        self.assertSQL(SELECT(OR(C.foo, C.bar)),
                    (u'SELECT (foo OR bar)', ()))

    def test_bool_not(self):
        self.assertSQL(SELECT(NOT(C.foo)),
                    (u'SELECT (NOT foo)', ()))

    def test_like(self):
        self.assertSQL(SELECT(LIKE(C.foo, 'foobar')),
                    (u'SELECT (foo LIKE %s)', ('foobar',)))

    def test_not_like(self):
        self.assertSQL(SELECT(NOT_LIKE(C.foo, 'foobar')),
                    (u'SELECT (foo NOT LIKE %s)', ('foobar',)))

    def test_ilike(self):
        self.assertSQL(SELECT(ILIKE(C.foo, 'foobar')),
                    (u'SELECT (foo ILIKE %s)', ('foobar',)))

    def test_not_ilike(self):
        self.assertSQL(SELECT(NOT_ILIKE(C.foo, 'foobar')),
                    (u'SELECT (foo NOT ILIKE %s)', ('foobar',)))

    def test_rlike(self):
        self.assertSQL(SELECT(RLIKE(C.foo, 'foobar')),
                    (u'SELECT (foo RLIKE %s)', ('foobar',)))

    def test_not_rlike(self):
        self.assertSQL(SELECT(NOT_RLIKE(C.foo, 'foobar')),
                    (u'SELECT (foo NOT RLIKE %s)', ('foobar',)))

    def test_in(self):
        self.assertSQL(SELECT(IN(C.foo, (1, 2, 3))),
                    (u'SELECT (foo IN (%s, %s, %s))', (1, 2, 3)))

    def test_not_in(self):
        self.assertSQL(SELECT(NOT_IN(C.foo, (1, 2, 3))),
                    (u'SELECT (foo NOT IN (%s, %s, %s))', (1, 2, 3)))

    def test_is_null(self):
        self.assertSQL(SELECT(IS_NULL(C.foo)),
                    (u'SELECT (foo IS NULL)', ()))

    def test_is_not_null(self):
        self.assertSQL(SELECT(IS_NOT_NULL(C.foo)),
                    (u'SELECT (foo IS NOT NULL)', ()))

    def test_precedence(self):
        self.assertSQL(SELECT(C.foo + C.bar * C.baz),
                    (u'SELECT (foo + (bar * baz))', ()))

    def test_parens(self):
        self.assertSQL(SELECT((C.foo + C.bar) * C.baz),
                    (u'SELECT ((foo + bar) * baz)', ()))


class DistinctTest(TestCase):

    def test_distinct(self):
        self.assertSQL(SELECT().DISTINCT(),
                    (u'SELECT DISTINCT *', ()))

    def test_distinct_columns(self):
        self.assertSQL(SELECT().DISTINCT(C.foo, C.bar),
                    (u'SELECT DISTINCT ON (foo, bar) *', ()))

    def test_all(self):
        self.assertSQL(SELECT().ALL(),
                    (u'SELECT ALL *', ()))

    def test_all_columns(self):
        self.assertSQL(SELECT().ALL(C.foo, C.bar),
                    (u'SELECT ALL ON (foo, bar) *', ()))


class WithTest(TestCase):

    def test_single(self):
        self.assertSQL(SELECT().WITH(C.foo, SELECT()),
                    (u'WITH foo AS (SELECT *) SELECT *', ()))

    def test_multi(self):
        self.assertSQL(SELECT().WITH(C.foo, SELECT().FROM(T.table_foo)).WITH(C.bar, SELECT().FROM(T.table_bar)),
                    (u'WITH foo AS (SELECT * FROM table_foo), bar AS (SELECT * FROM table_bar) SELECT *', ()))

    def test_recursive(self):
        self.assertSQL(SELECT().WITH(C.foo(C.bar, C.baz), SELECT(C.bar, C.baz) | SELECT(C.bar, C.baz).FROM(C.foo), RECURSIVE=True),
                    (u'WITH RECURSIVE foo(bar, baz) AS (SELECT bar, baz UNION SELECT bar, baz FROM foo) SELECT *', ()))


class FromTest(TestCase):

    def test_table(self):
        self.assertSQL(SELECT().FROM(T.table),
                    (u'SELECT * FROM table', ()))

    def test_table_alias(self):
        self.assertSQL(SELECT().FROM(A.table_alias(T.table)),
                    (u'SELECT * FROM table AS table_alias', ()))

    def test_table_alias_columns(self):
        self.assertSQL(SELECT().FROM(A.table_alias(T.table, columns=(C.foo, C.bar))),
                    (u'SELECT * FROM table AS table_alias(foo, bar)', ()))

    def test_subquery(self):
        self.assertSQL(SELECT().FROM(A.subquery(SELECT(F.sum(F.qty), F.avg(C.price)))),
                    (u'SELECT * FROM (SELECT sum(qty), avg(price)) AS subquery', ()))

    def test_subquery_columns(self):
        self.assertSQL(SELECT().FROM(A.subquery(SELECT(F.sum(F.qty), F.avg(C.price)), columns=(C.qty, C.average))),
                    (u'SELECT * FROM (SELECT sum(qty), avg(price)) AS subquery(qty, average)', ()))

    def test_values(self):
        self.assertSQL(SELECT().FROM(A.val_alias(VALUES(1, 2, 3)(4, 5, 6))),
                    (u'SELECT * FROM (VALUES (%s, %s, %s), (%s, %s, %s)) AS val_alias', (1, 2, 3, 4, 5, 6)))

    def test_values_columns(self):
        self.assertSQL(SELECT().FROM(A.val_alias(VALUES(1, 2, 3)(4, 5, 6), columns=(C.foo, C.bar, C.baz))),
                    (u'SELECT * FROM (VALUES (%s, %s, %s), (%s, %s, %s)) AS val_alias(foo, bar, baz)', (1, 2, 3, 4, 5, 6)))


class OrderTest(TestCase):

    def test_empty(self):
        self.assertSQL(SELECT().ORDER_BY(),
                    (u'SELECT *', ()))

    def test_column(self):
        self.assertSQL(SELECT().ORDER_BY(C.foo, C.bar),
                    (u'SELECT * ORDER BY foo, bar', ()))

    def test_dir(self):
        self.assertSQL(SELECT().ORDER_BY(ASC(C.foo), DESC(C.bar)),
                    (u'SELECT * ORDER BY foo ASC, bar DESC', ()))

    def test_nulls(self):
        self.assertSQL(SELECT().ORDER_BY(ASC(C.foo).NULLS_FIRST, DESC(C.bar).NULLS_LAST),
                    (u'SELECT * ORDER BY foo ASC NULLS FIRST, bar DESC NULLS LAST', ()))


class LimitTest(TestCase):

    def test_limit(self):
        self.assertSQL(SELECT().LIMIT(10),
                    (u'SELECT * LIMIT %s', (10,)))

    def test_limit_offset(self):
        self.assertSQL(SELECT().LIMIT(10, 20),
                    (u'SELECT * LIMIT %s OFFSET %s', (10, 20)))

    def test_offset(self):
        self.assertSQL(SELECT().LIMIT(10).OFFSET(20),
                    (u'SELECT * LIMIT %s OFFSET %s', (10, 20)))


class JoinTest(TestCase):

    def test_cross(self):
        self.assertSQL(SELECT().FROM(T.foo).CROSS_JOIN(T.bar),
                    (u'SELECT * FROM foo CROSS JOIN bar', ()))

    def test_left_natural(self):
        self.assertSQL(SELECT().FROM(T.foo).LEFT_JOIN(T.bar, NATURAL=True),
                    (u'SELECT * FROM foo NATURAL LEFT OUTER JOIN bar', ()))

    def test_left_using_one(self):
        self.assertSQL(SELECT().FROM(T.foo).LEFT_JOIN(T.bar, USING=C.baz),
                    (u'SELECT * FROM foo LEFT OUTER JOIN bar USING (baz)', ()))

    def test_left_using_multi(self):
        self.assertSQL(SELECT().FROM(T.foo).LEFT_JOIN(T.bar, USING=(C.baz, C.xyzzy)),
                    (u'SELECT * FROM foo LEFT OUTER JOIN bar USING (baz, xyzzy)', ()))

    def test_left_on(self):
        self.assertSQL(SELECT().FROM(T.foo).LEFT_JOIN(T.bar, ON=(C.baz > 100)),
                    (u'SELECT * FROM foo LEFT OUTER JOIN bar ON (baz > %s)', (100,)))

    def test_right_natural(self):
        self.assertSQL(SELECT().FROM(T.foo).RIGHT_JOIN(T.bar, NATURAL=True),
                    (u'SELECT * FROM foo NATURAL RIGHT OUTER JOIN bar', ()))

    def test_right_using_one(self):
        self.assertSQL(SELECT().FROM(T.foo).RIGHT_JOIN(T.bar, USING=C.baz),
                    (u'SELECT * FROM foo RIGHT OUTER JOIN bar USING (baz)', ()))

    def test_right_using_multi(self):
        self.assertSQL(SELECT().FROM(T.foo).RIGHT_JOIN(T.bar, USING=(C.baz, C.xyzzy)),
                    (u'SELECT * FROM foo RIGHT OUTER JOIN bar USING (baz, xyzzy)', ()))

    def test_right_on(self):
        self.assertSQL(SELECT().FROM(T.foo).RIGHT_JOIN(T.bar, ON=(C.baz > 100)),
                    (u'SELECT * FROM foo RIGHT OUTER JOIN bar ON (baz > %s)', (100,)))

    def test_full_natural(self):
        self.assertSQL(SELECT().FROM(T.foo).FULL_JOIN(T.bar, NATURAL=True),
                    (u'SELECT * FROM foo NATURAL FULL OUTER JOIN bar', ()))

    def test_full_using_one(self):
        self.assertSQL(SELECT().FROM(T.foo).FULL_JOIN(T.bar, USING=C.baz),
                    (u'SELECT * FROM foo FULL OUTER JOIN bar USING (baz)', ()))

    def test_full_using_multi(self):
        self.assertSQL(SELECT().FROM(T.foo).FULL_JOIN(T.bar, USING=(C.baz, C.xyzzy)),
                    (u'SELECT * FROM foo FULL OUTER JOIN bar USING (baz, xyzzy)', ()))

    def test_full_on(self):
        self.assertSQL(SELECT().FROM(T.foo).FULL_JOIN(T.bar, ON=(C.baz > 100)),
                    (u'SELECT * FROM foo FULL OUTER JOIN bar ON (baz > %s)', (100,)))

    def test_inner_natural(self):
        self.assertSQL(SELECT().FROM(T.foo).INNER_JOIN(T.bar, NATURAL=True),
                    (u'SELECT * FROM foo NATURAL INNER JOIN bar', ()))

    def test_inner_using_one(self):
        self.assertSQL(SELECT().FROM(T.foo).INNER_JOIN(T.bar, USING=C.baz),
                    (u'SELECT * FROM foo INNER JOIN bar USING (baz)', ()))

    def test_inner_using_multi(self):
        self.assertSQL(SELECT().FROM(T.foo).INNER_JOIN(T.bar, USING=(C.baz, C.xyzzy)),
                    (u'SELECT * FROM foo INNER JOIN bar USING (baz, xyzzy)', ()))

    def test_inner_on(self):
        self.assertSQL(SELECT().FROM(T.foo).INNER_JOIN(T.bar, ON=(C.baz > 100)),
                    (u'SELECT * FROM foo INNER JOIN bar ON (baz > %s)', (100,)))


class WhereTest(TestCase):

    def test_no_from(self):
        with self.assertRaises(TypeError):
            SELECT().WHERE(C.foo > 100)

    def test_where(self):
        self.assertSQL(SELECT().FROM(T.table).WHERE(C.foo > 100),
                    (u'SELECT * FROM table WHERE (foo > %s)', (100,)))


class GroupTest(TestCase):

    def test_no_from(self):
        with self.assertRaises(TypeError):
            SELECT().GROUP_BY(C.foo)

    def test_group(self):
        self.assertSQL(SELECT().FROM(T.table).GROUP_BY(C.foo, F.count(C.bar)),
                    (u'SELECT * FROM table GROUP BY foo, count(bar)', ()))


class HavingTest(TestCase):

    def test_no_from(self):
        with self.assertRaises(TypeError):
            SELECT().HAVING(C.foo > 100)

    def test_having(self):
        self.assertSQL(SELECT().FROM(T.table).HAVING(C.foo > 100),
                    (u'SELECT * FROM table HAVING (foo > %s)', (100,)))


class WindowTest(TestCase):

    def test_empty(self):
        self.assertSQL(SELECT().WINDOW(C.name),
                    (u'SELECT * WINDOW name AS ()', ()))

    def test_named(self):
        self.assertSQL(SELECT().WINDOW(C.name, C.window_ref),
                    (u'SELECT * WINDOW name AS (window_ref)', ()))

    def test_partition_single(self):
        self.assertSQL(SELECT().WINDOW(C.name, PARTITION_BY=C.foo),
                    (u'SELECT * WINDOW name AS (PARTITION BY foo)', ()))

    def test_partition_multi(self):
        self.assertSQL(SELECT().WINDOW(C.name, PARTITION_BY=(C.foo, C.bar)),
                    (u'SELECT * WINDOW name AS (PARTITION BY foo, bar)', ()))

    def test_order_single(self):
        self.assertSQL(SELECT().WINDOW(C.name, ORDER_BY=C.foo),
                    (u'SELECT * WINDOW name AS (ORDER BY foo)', ()))

    def test_order_dir(self):
        self.assertSQL(SELECT().WINDOW(C.name, ORDER_BY=ASC(C.foo)),
                    (u'SELECT * WINDOW name AS (ORDER BY foo ASC)', ()))

    def test_order_dir_nulls(self):
        self.assertSQL(SELECT().WINDOW(C.name, ORDER_BY=ASC(C.foo).NULLS_FIRST),
                    (u'SELECT * WINDOW name AS (ORDER BY foo ASC NULLS FIRST)', ()))

    def test_order_multi(self):
        self.assertSQL(SELECT().WINDOW(C.name, ORDER_BY=(C.foo, ASC(C.bar), DESC(C.baz).NULLS_LAST)),
                    (u'SELECT * WINDOW name AS (ORDER BY foo, bar ASC, baz DESC NULLS LAST)', ()))

    def test_range_negative(self):
        self.assertSQL(SELECT().WINDOW(C.name, RANGE=-1),
                    (u'SELECT * WINDOW name AS (RANGE %s PRECEDING)', (1,)))

    def test_range_zero(self):
        self.assertSQL(SELECT().WINDOW(C.name, RANGE=0),
                    (u'SELECT * WINDOW name AS (RANGE CURRENT ROW)', ()))

    def test_range_positive(self):
        self.assertSQL(SELECT().WINDOW(C.name, RANGE=1),
                    (u'SELECT * WINDOW name AS (RANGE %s FOLLOWING)', (1,)))

    def test_range_negative_to_end(self):
        self.assertSQL(SELECT().WINDOW(C.name, RANGE=(-1, None)),
                    (u'SELECT * WINDOW name AS (RANGE BETWEEN %s PRECEDING AND UNBOUNDED FOLLOWING)', (1,)))

    def test_range_negative_to_zero(self):
        self.assertSQL(SELECT().WINDOW(C.name, RANGE=(-1, 0)),
                    (u'SELECT * WINDOW name AS (RANGE BETWEEN %s PRECEDING AND CURRENT ROW)', (1,)))

    def test_range_start_to_zero(self):
        self.assertSQL(SELECT().WINDOW(C.name, RANGE=(None, 0)),
                    (u'SELECT * WINDOW name AS (RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)', ()))

    def test_range_zero_to_end(self):
        self.assertSQL(SELECT().WINDOW(C.name, RANGE=(0, None)),
                    (u'SELECT * WINDOW name AS (RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)', ()))

    def test_range_zero_to_positive(self):
        self.assertSQL(SELECT().WINDOW(C.name, RANGE=(0, 1)),
                    (u'SELECT * WINDOW name AS (RANGE BETWEEN CURRENT ROW AND %s FOLLOWING)', (1,)))

    def test_range_start_to_positive(self):
        self.assertSQL(SELECT().WINDOW(C.name, RANGE=(None, 1)),
                    (u'SELECT * WINDOW name AS (RANGE BETWEEN UNBOUNDED PRECEDING AND %s FOLLOWING)', (1,)))

    def test_rows_negative(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=-1),
                    (u'SELECT * WINDOW name AS (ROWS %s PRECEDING)', (1,)))

    def test_rows_zero(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=0),
                    (u'SELECT * WINDOW name AS (ROWS CURRENT ROW)', ()))

    def test_rows_positive(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=1),
                    (u'SELECT * WINDOW name AS (ROWS %s FOLLOWING)', (1,)))

    def test_rows_negative_to_positive(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=(-1, 1)),
                    (u'SELECT * WINDOW name AS (ROWS BETWEEN %s PRECEDING AND %s FOLLOWING)', (1, 1)))

    def test_rows_negative_to_end(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=(-1, None)),
                    (u'SELECT * WINDOW name AS (ROWS BETWEEN %s PRECEDING AND UNBOUNDED FOLLOWING)', (1,)))

    def test_rows_negative_to_zero(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=(-1, 0)),
                    (u'SELECT * WINDOW name AS (ROWS BETWEEN %s PRECEDING AND CURRENT ROW)', (1,)))

    def test_rows_start_to_zero(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=(None, 0)),
                    (u'SELECT * WINDOW name AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)', ()))

    def test_rows_zero_to_end(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=(0, None)),
                    (u'SELECT * WINDOW name AS (ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)', ()))

    def test_rows_zero_to_positive(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=(0, 1)),
                    (u'SELECT * WINDOW name AS (ROWS BETWEEN CURRENT ROW AND %s FOLLOWING)', (1,)))

    def test_rows_start_to_positive(self):
        self.assertSQL(SELECT().WINDOW(C.name, ROWS=(None, 1)),
                    (u'SELECT * WINDOW name AS (ROWS BETWEEN UNBOUNDED PRECEDING AND %s FOLLOWING)', (1,)))

    def test_complex(self):
        self.assertSQL(SELECT().WINDOW(C.name, C.window_ref, PARTITION_BY=(C.foo, C.bar), ORDER_BY=(ASC(C.foo), DESC(C.bar)), RANGE=(-1, 1)),
                    (u'SELECT * WINDOW name AS (window_ref PARTITION BY foo, bar ORDER BY foo ASC, bar DESC RANGE BETWEEN %s PRECEDING AND %s FOLLOWING)', (1, 1)))
