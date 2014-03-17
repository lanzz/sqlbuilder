# -*- coding: utf-8 -*-

"""
SQL syntax
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


def NameFactory(Class, prefix=None, as_sql=None):
    """
    Factory that converts attribute access to Identifier instances
    Includes a few additional facilities (CASE expressions and wrapping primitive values in value expressions)
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


def wrap_source(source, AS=None, ONLY=None):
    """
    Wrap join source in a `Table` instance if necessary
    """
    if not isinstance(source, Joinable):
        source = Table(source, ONLY=ONLY)
        if AS:
            source = source.AS(AS)
    else:
        assert AS is None, 'Cannot assign alias to an existing source'
    return source


class Joinable(SQL):
    """
    Base class for joinable classes (tables, subquery aliases)
    """

    def CROSS_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return CrossJoin(self, other, *args, **kwargs)

    def NATURAL_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return NaturalJoin(self, other, *args, **kwargs)

    def LEFT_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return ConditionalJoin(self, other, type=Join.LEFT, *args, **kwargs)

    def RIGHT_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return ConditionalJoin(self, other, type=Join.RIGHT, *args, **kwargs)

    def FULL_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return ConditionalJoin(self, other, type=Join.FULL, *args, **kwargs)

    def INNER_JOIN(self, other, *args, **kwargs):
        other = wrap_source(other, AS=kwargs.pop('AS', None))
        return ConditionalJoin(self, other, type=Join.INNER, *args, **kwargs)


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


class TableAlias(Alias):
    """
    Alias of a table
    """

    def __getattr__(self, name):
        return Identifier('{name}.{subname}'.format(
            name=self._alias,
            subname=name,
        ))


class SubqueryAlias(TableAlias, Joinable):
    """
    Alias of a subquery
    """

    def __init__(self, origin, alias, columns=None, LATERAL=None):
        super(SubqueryAlias, self).__init__(origin, alias)
        self._columns = columns
        self._lateral = LATERAL or False

    def _as_sql(self, connection, context):
        origin_sql, origin_args = SQL.wrap(self._origin)._as_sql(connection, context)
        alias_sql, alias_args = SQL.wrap(self._alias, id=True)._as_sql(connection, context)
        sql = u'{lateral}({origin}) AS {alias}'.format(
            lateral=u'LATERAL ' if self._lateral else u'',
            origin=origin_sql,
            alias=alias_sql,
        )
        if self._columns is not None and len(self._columns):
            sql += u' ({columns})'.format(
                columns=SQLIterator(self._columns, id=True)._as_sql(connection, context),
            )
        return sql, origin_args + alias_args


class Table(Joinable):
    """
    Table reference
    """

    def __init__(self, name, ONLY=None):
        self._name = name
        self._only = False if ONLY is None else ONLY

    def _as_sql(self, connection, context):
        sql, args = SQL.wrap(self._name, id=True)._as_sql(connection, context)
        if self._only:
            sql = u'ONLY ' + sql
        return sql, args

    def __getattr__(self, name):
        return Table(u'{name}.{subname}'.format(
            name=self._name,
            subname=name,
        ), ONLY=self._only)

    def AS(self, *args, **kwargs):
        return TableAlias(self, *args, **kwargs)

    def __call__(self):
        """
        Column identifier factory
        """
        return NameFactory(Identifier, prefix=self._name + u'.', as_sql=lambda _, connection, context: Wildcard(self)._as_sql(connection, context))

TableFactory = NameFactory(Table)


class Wildcard(SQL):
    """
    `table.*` wildcard
    """

    def __init__(self, table=None):
        self.table = table

    def _as_sql(self, connection, context):
        if not self.table:
            return u'*', ()
        sql, args = self.table._as_sql(connection, context)
        sql += u'.*'
        return sql, args


class VALUES(SQL):
    """
    VALUES expression
    """

    def __init__(self, *values):
        self.rows = [ values ]

    def __call__(self, *values):
        """
        Add another row of values
        """
        self.rows.append(values)
        return self

    def _as_sql(self, connection, context):
        assert len(self.rows), 'No rows in VALUE expression'
        rows = []
        args = ()
        for row in self.rows:
            row_sql, row_args = SQLIterator(row)._as_sql(connection, context)
            rows.append(u'({row})'.format(row=row_sql))
            args += row_args
        sql = u'VALUES {rows}'.format(
            rows=u', '.join(rows),
        )
        return sql, args

    def AS(self, *args, **kwargs):
        return SubqueryAlias(self, *args, **kwargs)


class Join(Joinable):
    """
    Abstract base class for joins
    """

    # constants
    INNER = u'INNER'
    LEFT = u'LEFT OUTER'
    RIGHT = u'RIGHT OUTER'
    FULL = u'FULL OUTER'

    def __init__(self, left, right):
        self.left = left
        self.right = right
        assert isinstance(self.left, Source) and isinstance(self.right, Source), 'Invalid join sources'


class QualifiedJoin(Join):
    """
    Abstract base class for qualified joins
    """

    def __init__(self, left, right, type=None):
        super(QualifiedJoin, self).__init__(left, right)
        self.type = type or self.INNER
        assert self.type in (self.INNER, self.LEFT, self.RIGHT, self.FULL), 'Invalid join type: {type}'.format(type=self.type)


class CrossJoin(Join):

    def _as_sql(self, connection, context):
        left_sql, left_args = SQL.wrap(self.left)._as_sql(connection, context)
        right_sql, right_args = SQL.wrap(self.right)._as_sql(connection, context)
        sql = u'({left} CROSS JOIN {right})'.format(
            left=left_sql,
            right=right_sql,
        )
        return sql, left_args + right_args


class NaturalJoin(QualifiedJoin):

    def _as_sql(self, connection, context):
        left_sql, left_args = SQL.wrap(self.left)._as_sql(connection, context)
        right_sql, right_args = SQL.wrap(self.right)._as_sql(connection, context)
        sql = u'({left} NATURAL {type} JOIN {right})'.format(
            left=left_sql,
            right=right_sql,
            type=self.type,
        )
        return sql, args1 + args2


class ConditionalJoin(QualifiedJoin):

    def __init__(self, left, right, type=None, on=None, using=None):
        super(ConditionalJoin, self).__init__(left, right, type=type)
        self.on = on
        if using is not None and not isinstance(using, (list, tuple)):
            using = using,
        self.using = using
        assert (self.on is None or self.using is None), 'Cannot have both ON and USING clauses on a join'
        assert not (self.on is None and self.using is None), 'Either ON or USING clause is required for conditional join'

    def _as_sql(self, connection, context):
        if self.on:
            expr_sql, condition_args = SQL.wrap(self.on)._as_sql(connection, context)
            condition_sql = u'ON {expression}'.format(expression=expr_sql)
        else:
            columns_sql, condition_args = SQLIterator(self.using)._as_sql(connection, context)
            condition_sql = u'USING ({columns})'.format(columns=columns_sql)
        left_sql, left_args = SQL.wrap(self.left)._as_sql(connection, context)
        right_sql, right_args = SQL.wrap(self.right)._as_sql(connection, context)
        sql = u'({left} {type} JOIN {right} {condition})'.format(
            left=left_sql,
            right=right_sql,
            type=self.type,
            condition=condition_sql,
        )
        return sql, left_args + right_args + condition_args


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
    def ASC(self): return Sorting(self, Sorting.ASC)
    @property
    def DESC(self): return Sorting(self, Sorting.DESC)
    @property
    def NULLS_FIRST(self): return Sorting(self, nulls=Sorting.FIRST)
    @property
    def NULLS_LAST(self): return Sorting(self, nulls=Sorting.LAST)


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

VariableFactory = NameFactory(Variable)


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

IdentifierFactory = NameFactory(Identifier, as_sql=lambda self, connection, context: Wildcard()._as_sql(connection, context))


class Value(Expression):

    def __init__(self, value):
        self.value = value

    def _as_sql(self, connection, context):
        """
        Return SQL for this instance
        """
        return u'%s', ( self.value, )

    def __repr__(self):
        return u'<Value {value!r}>'.format(value=self.value)


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
        override = connection.operator_to_sql(self.op, self.left, context=context)
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
        super(IsNullOperator, self).__init__(u' IS NOT NULL' if invert else u' IS NULL', operand)
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


class Sorting(SQL):
    """
    Sorting order on an expression
    Sorting orders are no longer expressions, as they are not allowed in operations, only in ORDER BY clauses
    """

    # directions
    ASC = u' ASC'
    DESC = u' DESC'

    # nulls ordering
    FIRST = u' NULLS FIRST'
    LAST = u' NULLS LAST'

    def __init__(self, expr, direction=None, nulls=None):
        self.expr = expr
        self.direction = direction
        self.nulls = nulls
        assert self.direction in (None, self.ASC, self.DESC), 'Invalid sorting direction: {dir}'.format(dir=self.direction)
        assert self.nulls in (None, self.FIRST, self.LAST), 'Invalid sorting of nulls: {nulls}'.format(nulls=self.nulls)

    def _as_sql(self, connection, context):
        sql, args = SQL.wrap(self.expr)._as_sql(connection, context)
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


from .dummy import dummy_connection, dummy_context
