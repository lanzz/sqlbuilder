# -*- coding: utf-8 -*-
from __future__ import absolute_import

"""
SQL expressions
"""

from __future__ import absolute_import
from .base import SQL, SQLIterator, merge_sql


class Expression(SQL):
    """
    Wrapper for an expression
    """

    def __lt__(self, other): return BinaryOperator(self, u' < ', other)
    def __le__(self, other): return BinaryOperator(self, u' <= ', other)
    def __eq__(self, other): return BinaryOperator(self, u' = ', other)
    def __ne__(self, other): return BinaryOperator(self, u' <> ', other)
    def __gt__(self, other): return BinaryOperator(self, u' > ', other)
    def __ge__(self, other): return BinaryOperator(self, u' >= ', other)

    def __add__(self, other): return BinaryOperator(self, u' + ', other)
    def __sub__(self, other): return BinaryOperator(self, u' - ', other)
    def __mul__(self, other): return BinaryOperator(self, u' * ', other)
    def __div__(self, other): return BinaryOperator(self, u' / ', other)
    def __truediv__(self, other): return BinaryOperator(self, u' / ', other)
    def __floordiv__(self, other): return BinaryOperator(self, u' / ', other)
    def __mod__(self, other): return FunctionCall(u'mod', self, other)
    def __pow__(self, other): return FunctionCall(u'power', self, other)
    def __lshift__(self, other): return BinaryOperator(self, u' << ', other)
    def __rshift__(self, other): return BinaryOperator(self, u' >> ', other)
    def __and__(self, other): return BinaryOperator(self, u' & ', other)
    def __xor__(self, other): return BinaryOperator(self, u' ^ ', other)
    def __or__(self, other): return BinaryOperator(self, u' | ', other)

    def __radd__(self, other): return BinaryOperator(other, u' + ', self)
    def __rsub__(self, other): return BinaryOperator(other, u' - ', self)
    def __rmul__(self, other): return BinaryOperator(other, u' * ', self)
    def __rdiv__(self, other): return BinaryOperator(other, u' / ', self)
    def __rtruediv__(self, other): return BinaryOperator(other, u' / ', self)
    def __rfloordiv__(self, other): return BinaryOperator(other, u' / ', self)
    def __rmod__(self, other): return FunctionCall(u'mod', other, self)
    def __rpow__(self, other): return FunctionCall(u'power', other, self)
    def __rlshift__(self, other): return BinaryOperator(other, u' << ', self)
    def __rrshift__(self, other): return BinaryOperator(other, u' >> ', self)
    def __rand__(self, other): return BinaryOperator(other, u' & ', self)
    def __rxor__(self, other): return BinaryOperator(other, u' ^ ', self)
    def __ror__(self, other): return BinaryOperator(other, u' | ', self)

    def __neg__(self): return UnaryOperator(u'-', self)
    def __pos__(self): return UnaryOperator(u'+', self)
    def __abs__(self): return FunctionCall(u'abs', self)
    def __invert__(self): return UnaryOperator(u'~', self)

    # Python doesn't allow overriding of the behavior of basic logical operators, so these are methods instead
    def AND(self, other): return BinaryOperator(self, u' AND ', other)
    def XOR(self, other): return BinaryOperator(self, u' XOR ', other)
    def OR(self, other): return BinaryOperator(self, u' OR ', other)
    @property
    def NOT(self): return UnaryOperator(u'NOT ', self)

    # common SQL operators
    def LIKE(self, other): return LikeOperator(self, other)
    def NOT_LIKE(self, other): return LikeOperator(self, other, invert=True)
    def ILIKE(self, other): return LikeOperator(self, other, nocase=True)
    def NOT_ILIKE(self, other): return LikeOperator(self, other, nocase=True, invert=True)
    def IN(self, other): return InOperator(self, other)
    def NOT_IN(self, other): return InOperator(self, other, invert=True)
    @property
    def IS_NULL(self): return IsNullOperator(self)
    @property
    def IS_NOT_NULL(self): return IsNullOperator(self, invert=True)

    # expressions are aliasable
    def AS(self, *args, **kwargs): return Alias(self, *args, **kwargs)

    # ORDER BY qualifiers
    @property
    def ASC(self): return Sorting(self, direction=Sorting.DIR.ASC)
    @property
    def DESC(self): return Sorting(self, direction=Sorting.DIR.DESC)
    @property
    def NULLS_FIRST(self): return Sorting(self, nulls=Sorting.NULLS.FIRST)
    @property
    def NULLS_LAST(self): return Sorting(self, nulls=Sorting.NULLS.LAST)


class Value(Expression):
    """
    Plain value
    """

    def __init__(self, value):
        self.value = value

    def _as_sql(self, connection, context):
        """
        Return SQL for this instance
        """
        return u'%s', ( self.value, )

    def __repr__(self):
        return u'<Value {value!r}>'.format(value=self.value)


class Variable(Expression):
    """
    Variable placeholder
    """

    def __init__(self, name):
        self.name = name

    def _as_sql(self, connection, context):
        return SQL.wrap(context[self.name])._as_sql(connection, context)

    def __repr__(self):
        return u'<Variable {name!r}>'.format(name=self.name)


class Identifier(Expression):
    """
    Raw name — can be a column reference or a function call
    """

    def __init__(self, name):
        self.name = name

    def _as_sql(self, connection, context):
        """
        Render name as identifier
        """
        return connection.quote_identifier(self.name), ()

    def __repr__(self):
        return u'<Identifier {name!r}>'.format(name=self.name)

    def __getattr__(self, name):
        return Identifier(u'{name}.{subname}'.format(
            name=self.name,
            subname=name,
        ))

    def __call__(self, *args, **kwargs):
        """
        Wrap name in a function call wrapper
        """
        return FunctionCall(self.name, *args, **kwargs)


class FunctionCall(Expression):
    """
    Function call wrapper
    """

    def __init__(self, name, *params, **kwargs):
        self.name = name
        self.params = params
        self.distinct = kwargs.get('DISTINCT', False)

    def _as_sql(self, connection, context):
        sql, args = SQLIterator(self.params)._as_sql(connection, context)
        sql = u'{name}({distinct}{params})'.format(
            name=connection.quote_function_name(self.name),
            distinct=u'DISTINCT ' if self.distinct else u'',
            params=sql,
        )
        return sql, args

    def OVER(self, *args, **kwargs):
        return WindowFunctionCall(self, *args, **kwargs)


class WindowFunctionCall(FunctionCall):
    """
    Window function call wrapper
    """

    def __init__(self, call, *args, **kwargs):
        self.call = call
        self.window = Window(*args, **kwargs) if (len(args) != 1) or kwargs else SQL.wrap(args[0], id=True)

    def _as_sql(self, connection, context):
        call_sql, call_args = self.call._as_sql(connection, context)
        window_sql, window_args = self.window._as_sql(connection, context)
        sql = u'{call} OVER {window}'.format(
            call=call_sql,
            window=window_sql,
        )
        return sql, call_args + window_args


class BinaryOperator(Expression):
    """
    Wrapper for a generic binary operator
    """

    def __init__(self, left, op, right):
        self.left = left
        self.op = op
        self.right = right

    def _as_sql(self, connection, context):
        override = connection.operator_to_sql(self.op, self.left, self.right, context=context)
        if override and (override != NotImplemented):
            # database driver overrides this operator
            return override
        left_sql, left_args = self.left_to_sql(connection, context)
        right_sql, right_args = self.right_to_sql(connection, context)
        try:
            sql = u'({left}{op}{right})'.format(
                left=left_sql,
                op=self.op,
                right=right_sql,
            )
        except Exception:
            raise Exception(map(type, (left_sql, self.op, right_sql)))
        return sql, left_args + right_args

    def left_to_sql(self, connection, context):
        return SQL.wrap(self.left)._as_sql(connection, context)

    def right_to_sql(self, connection, context):
        return SQL.wrap(self.right)._as_sql(connection, context)


class UnaryOperator(Expression):
    """
    Wrapper for a generic unary operation
    """

    def __init__(self, op, operand):
        self.op = op
        self.operand = operand

    def _as_sql(self, connection, context):
        override = connection.operator_to_sql(self.op, self.operand, context=context)
        if override and (override != NotImplemented):
            # database driver overrides this operator
            return override
        sql, args = SQL.wrap(self.operand)._as_sql(connection, context)
        sql = u'({op}{operand})'.format(
            op=self.op,
            operand=sql,
        )
        return sql, args


class UnaryPostfixOperator(UnaryOperator):
    """
    Wrapper for a generic unary postfix operation
    """

    def __init__(self, operand, op):
        super(UnaryPostfixOperator, self).__init__(op, operand)

    def _as_sql(self, connection, context):
        override = connection.operator_to_sql(self.op, self.operand, context=context)
        if override and (override != NotImplemented):
            # database driver overrides this operator
            return override
        sql, args = SQL.wrap(self.operand)._as_sql(connection, context)
        sql = u'({operand}{op})'.format(
            op=self.op,
            operand=sql,
        )
        return sql, args


class LikeOperator(BinaryOperator):
    """
    Wrapper for a LIKE operator
    """

    def __init__(self, left, right, nocase=False, invert=False):
        op = u' ILIKE ' if nocase else u' LIKE '
        if invert:
            op = u' NOT' + op
        super(LikeOperator, self).__init__(left, op, right)
        self.nocase = nocase
        self.invert = invert

    @property
    def NOT(self):
        return LikeOperator(self.left, self.right, nocase=self.nocase, invert=not self.invert)


class InOperator(BinaryOperator):
    """
    Wrapper for IN operator
    """

    def __init__(self, left, right, invert=False):
        super(InOperator, self).__init__(left, u' NOT IN ' if invert else u' IN ', right)
        self.invert = invert

    @property
    def NOT(self):
        return InOperator(self.left, self.right, invert=not self.invert)

    def right_to_sql(self, connection, context):
        sql, args = SQLIterator(self.right)._as_sql(connection, context)
        sql = u'({items})'.format(items=sql)
        return sql, args


class IsNullOperator(UnaryPostfixOperator):
    """
    Wrapper for IS NULL operator
    """

    def __init__(self, operand, invert=False):
        super(IsNullOperator, self).__init__(operand, u' IS NOT NULL' if invert else u' IS NULL')
        self.invert = invert

    def NOT(self):
        return IsNullOperator(self.operand, invert=not self.invert)


class CaseExpression(Expression):
    """
    CASE operator
    """

    def __init__(self):
        self.cases = []
        self.else_ = None

    def WHEN(self, condition, value):
        self.cases.append((condition, value))
        return self

    def ELSE(self, value):
        self.else_ = value
        return self

    def case_to_sql(self, cond, value, connection, context):
        """
        Render a single case to SQL
        """
        assert self.cases, 'CASE operator must have at least one WHEN clause'
        cond_sql, cond_args = SQL.wrap(cond)._as_sql(connection, context)
        value_sql, value_args = SQL.wrap(value)._as_sql(connection, context)
        sql = u'WHEN {condition} THEN {value}'.format(
            condition=cond_sql,
            value=value_sql,
        )
        return sql, cond_args + value_args

    def _as_sql(self, connection, context):
        cases_sql, cases_args = merge_sql(self.case_to_sql(cond, value, connection, context) for cond, value in self.cases)
        if self.else_ is not None:
            else_sql, else_args = SQL.wrap(self.else_)._as_sql(connection, context)
            else_sql = u' ELSE {value}'.format(value=else_sql)
        else:
            else_sql = u''
            else_args = ()
        sql = u'CASE {cases}{else_} END'.format(
            cases=cases_sql,
            else_=else_sql,
        )
        return sql, cases_args + else_args


class Alias(SQL):
    """
    Alias of an expression
    """

    def __init__(self, origin, alias):
        self._origin = origin
        self._alias = alias

    def _as_sql(self, connection, context):
        origin_sql, origin_args = SQL.wrap(self._origin)._as_sql(connection, context)
        alias_sql, alias_args = SQL.wrap(self._alias, id=True)._as_sql(connection, context)
        sql = u'{origin} AS {alias}'.format(
            origin=origin_sql,
            alias=alias_sql,
        )
        return sql, origin_args + alias_args


from .window import Window
