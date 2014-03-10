# -*- coding: utf-8 -*-
from __future__ import absolute_import

from .helpers import SQL, to_sql, to_sql_iter, merge_sql


sentinel = object()


class NameFactory(object):
    """
    Factory that converts attribute access to Name instances
    Includes a few additional facilities (CASE expressions and wrapping primitive values in value expressions)
    """

    def __init__(self, prefix=''):
        if prefix and not prefix.endswith('.'):
            prefix += '.'
        else:
            prefix = prefix or ''
        object.__setattr__(self, 'prefix', prefix)

    def __getattr__(self, name):
        return Name(object.__getattribute__(self, 'prefix') + name)
    def __setattr__(self, name, value):
        raise AttributeError(name)
    def __delattr__(self, name):
        raise AttributeError(name)

    @property
    def CASE(self):
        """
        Start a CASE expression
        """
        return CaseExpression()

    def VALUES(self, *columns):
        """
        VALUES expression
        """
        return Values(*columns)

    def __call__(self, expr):
        """
        Wrap a (possibly) primitive expression in an `Expression` instance
        """
        return expr if isinstance(expr, Expression) else Expression(expr)

# importable references to `Unquote` instance
# the "F" stands for "function", "C" stands for "column" and "T" stands for "table", but there's no internal distinction
C = F = T = NameFactory()


class VariableFactory(object):
    """
    Variable reference factory
    """

    def __getattribute__(self, name):
        return Variable(name)
    def __setattr__(self, name, value):
        raise AttributeError(name)
    def __delattr__(self, name):
        raise AttributeError(name)

# importable reference to variable factory
V = VariableFactory()


class Expression(SQL):
    """
    Wrapper for an expression
    """

    __slots__ = 'expr',

    def __init__(self, expr):
        self.expr = expr

    def _as_sql(self, connection, context):
        return to_sql(self.expr, connection, context)

    def __lt__(self, other): return BinaryOperator(self, ' < ', other)
    def __le__(self, other): return BinaryOperator(self, ' <= ', other)
    def __eq__(self, other): return BinaryOperator(self, ' = ', other)
    def __ne__(self, other): return BinaryOperator(self, ' <> ', other)
    def __gt__(self, other): return BinaryOperator(self, ' > ', other)
    def __ge__(self, other): return BinaryOperator(self, ' >= ', other)

    def __add__(self, other): return BinaryOperator(self, ' + ', other)
    def __sub__(self, other): return BinaryOperator(self, ' - ', other)
    def __mul__(self, other): return BinaryOperator(self, ' * ', other)
    def __div__(self, other): return BinaryOperator(self, ' / ', other)
    def __truediv__(self, other): return BinaryOperator(self, ' / ', other)
    def __floordiv__(self, other): return BinaryOperator(self, ' / ', other)
    def __mod__(self, other): return FunctionCall('mod', self, other)
    def __pow__(self, other): return FunctionCall('power', self, other)
    def __lshift__(self, other): return BinaryOperator(self, ' << ', other)
    def __rshift__(self, other): return BinaryOperator(self, ' >> ', other)
    def __and__(self, other): return BinaryOperator(self, ' & ', other)
    def __xor__(self, other): return BinaryOperator(self, ' ^ ', other)
    def __or__(self, other): return BinaryOperator(self, ' | ', other)

    def __radd__(self, other): return BinaryOperator(other, ' + ', self)
    def __rsub__(self, other): return BinaryOperator(other, ' - ', self)
    def __rmul__(self, other): return BinaryOperator(other, ' * ', self)
    def __rdiv__(self, other): return BinaryOperator(other, ' / ', self)
    def __rtruediv__(self, other): return BinaryOperator(other, ' / ', self)
    def __rfloordiv__(self, other): return BinaryOperator(other, ' / ', self)
    def __rmod__(self, other): return FunctionCall('mod', other, self)
    def __rpow__(self, other): return FunctionCall('power', other, self)
    def __rlshift__(self, other): return BinaryOperator(other, ' << ', self)
    def __rrshift__(self, other): return BinaryOperator(other, ' >> ', self)
    def __rand__(self, other): return BinaryOperator(other, ' & ', self)
    def __rxor__(self, other): return BinaryOperator(other, ' ^ ', self)
    def __ror__(self, other): return BinaryOperator(other, ' | ', self)

    def __neg__(self): return UnaryOperator('-', self)
    def __pos__(self): return UnaryOperator('+', self)
    def __abs__(self): return FunctionCall('abs', self)
    def __invert__(self): return UnaryOperator('~', self)

    def AND(self, other): return BinaryOperator(self, ' AND ', other)
    def XOR(self, other): return BinaryOperator(self, ' XOR ', other)
    def OR(self, other): return BinaryOperator(self, ' OR ', other)
    @property
    def NOT(self): return UnaryOperator('NOT ', self)

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

    def AS(self, alias): return Alias(self, alias)

    @property
    def ASC(self): return Sorting(self, Sorting.ASC)
    @property
    def DESC(self): return Sorting(self, Sorting.DESC)
    @property
    def NULLS_FIRST(self): return Sorting(self, nulls=Sorting.FIRST)
    @property
    def NULLS_LAST(self): return Sorting(self, nulls=Sorting.LAST)


class BinaryOperator(Expression):
    """
    Wrapper for a binary operator
    """

    __slots__ = 'left', 'op', 'right'

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
        sql = u'({left}{op}{right})'.format(
            left=left_sql,
            op=self.op,
            right=right_sql,
        )
        return sql, left_args + right_args

    def left_to_sql(self, connection, context):
        return to_sql(self.left, connection, context)

    def right_to_sql(self, connection, context):
        return to_sql(self.right, connection, context)


class LikeOperator(BinaryOperator):
    """
    Wrapper for a LIKE operator
    """

    __slots__ = 'nocase', 'invert'

    def __init__(self, left, right, nocase=False, invert=False):
        op = ' ILIKE ' if nocase else ' LIKE '
        if invert:
            op = ' NOT' + op
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

    __slots__ = 'invert'

    def __init__(self, left, right, invert=False):
        super(InOperator, self).__init__(left, ' NOT IN ' if invert else ' IN ', right)
        self.invert = invert

    @property
    def NOT(self):
        return InOperator(self.left, self.right, invert=not self.invert)

    def right_to_sql(self, connection, context):
        sql, args = to_sql_iter(self.right, connection, context)
        sql = '({})'.format(sql)
        return sql, args


class UnaryOperator(Expression):
    """
    Wrapper for an unary operation
    """

    __slots__ = 'op', 'operand'

    def __init__(self, op, operand):
        self.op = op
        self.operand = operand

    def _as_sql(self, connection, context):
        """
        Render operation
        """
        override = connection.operator_to_sql(self.op, self.operand, context=context)
        if override and (override != NotImplemented):
            # database driver overrides this operator
            return override
        sql, args = to_sql(self.operand, connection, context)
        sql = u'({op}{operand})'.format(
            op=self.op,
            operand=sql,
        )
        return sql, args


class UnaryPostfixOperator(UnaryOperator):
    """
    Wrapper for unary postfix operation
    """

    def __init__(self, operand, op):
        super(UnaryPostfixOperator, self).__init__(op, operand)

    def _as_sql(self, connection, context):
        """
        Render operation
        """
        override = connection.operator_to_sql(self.op, self.left, context=context)
        if override and (override != NotImplemented):
            # database driver overrides this operator
            return override
        sql, args = to_sql(self.operand, connection, context)
        sql = u'({operand}{op})'.format(
            op=self.op,
            operand=sql,
        )
        return sql, args


class IsNullOperator(UnaryPostfixOperator):
    """
    Wrapper for IS NULL operator
    """

    __slots__ = 'invert',

    def __init__(self, operand, invert=False):
        super(IsNullOperator, self).__init__(' IS NOT NULL' if invert else ' IS NULL', operand)
        self.invert = invert

    def NOT(self):
        return IsNullOperator(self.operand, invert=not self.invert)


class CaseExpression(Expression):
    """
    CASE operator
    """

    __slots__ = 'cases', 'else_'

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
        cond_sql, cond_args = to_sql(cond, connection, context)
        value_sql, value_args = to_sql(value, connection, context)
        sql = u'WHEN {condition} THEN {value}'.format(
            condition=cond_sql,
            value=value_sql,
        )
        return sql, cond_args + value_args

    def _as_sql(self, connection, context):
        cases_sql, cases_args = merge_sql(self.case_to_sql(cond, value, connection, context) for cond, value in self.cases)
        if self.else_ is not None:
            else_sql, else_args = to_sql(self.else_, connection, context)
            else_sql = u' ELSE {value}'.format(value=else_sql)
        else:
            else_sql = ''
            else_args = ()
        sql = u'CASE {cases}{else_} END'.format(
            cases=cases_sql,
            else_=else_sql,
        )
        return sql, cases_args + else_args


class Variable(Expression):
    """
    Variable placeholder
    """

    __slots__ = 'name',

    def __init__(self, name):
        self.name = name

    def _as_sql(self, connection, context):
        return to_sql(context[self.name], connection, context)


class Name(Expression):
    """
    Raw name — can be a column reference or a function call
    """

    __slots__ = 'name',

    def __init__(self, name):
        self.name = name

    def _as_sql(self, connection, context):
        """
        Render name as identifier
        """
        return connection.quote_identifier(self.name), ()

    def __getattr__(self, name):
        return Name('{name}.{subname}'.format(
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

    __slots__ = 'name', 'params', 'distinct'

    def __init__(self, name, *params, **kwargs):
        self.name = name
        self.params = params
        self.distinct = kwargs.get('DISTINCT', False)

    def _as_sql(self, connection, context):
        sql, args = to_sql_iter(self.params, connection, context)
        sql = u'{name}({distinct}{params})'.format(
            name=connection.quote_function_name(self.name),
            distinct='DISTINCT ' if self.distinct else '',
            params=sql,
        )
        args = sum(args, ())
        return sql, args


class Alias(SQL):
    """
    Alias for an expression
    Aliases are no longer expressions, as they are not allowed in operations, only in queries
    """

    __slts__ = 'expr', 'alias'

    def __init__(self, expr, alias):
        self.expr = expr
        self.alias = alias

    def _as_sql(self, connection, context):
        sql, args = to_sql(self.expr, connection, context)
        sql = u'{expr} AS {alias}'.format(
            expr=sql,
            alias=connection.quote_identifier(self.alias),
        )
        return sql, args


class Sorting(SQL):
    """
    Sorting order on an expression
    Sorting orders are no longer expressions, as they are not allowed in operations, only in ORDER BY clauses
    """

    __slots__ = 'expr', 'direction', 'nulls'

    # directions
    ASC = ' ASC'
    DESC = ' DESC'

    # nulls ordering
    FIRST = ' NULLS FIRST'
    LAST = ' NULLS LAST'

    def __init__(self, expr, direction=None, nulls=None):
        self.expr = expr
        self.direction = direction
        self.nulls = nulls
        assert self.direction in (None, self.ASC, self.DESC), 'Invalid sorting direction: {dir}'.format(dir=self.direction)
        assert self.nulls in (None, self.FIRST, self.LAST), 'Invalid sorting of nulls: {nulls}'.format(nulls=self.nulls)

    def _as_sql(self, connection, context):
        sql, args = to_sql(self.expr, connection, context)
        sql = u'{expr}{dir}{nulls}'.format(
            expr=sql,
            dir='' if self.direction is None else self.direction,
            nulls='' if self.nulls is None else self.nulls,
        )
        return sql, args

    @property
    def NULLS_FIRST(self):
        self.nulls = self.FIRST
        return self

    @property
    def NULLS_LAST(self):
        self.nulls = self.LAST
        return self

from .source import Values
