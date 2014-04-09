## SQL Builder - Python module for incremental building of SQL queries
_Copyright 2014, Mihail Milushev <<mihail.milushev@lanzz.org>>_

SQL Builder gives you a Python syntax for describing SQL queries, which then can be evaluated to a `template, (arg1, arg2, ...)` tuple for execution by your database backend.

---

### Examples

```python
>>> from sqlbuilder.query import SELECT, C, T
>>> SELECT(C.column_name, C.another_column, 123, 'abc').FROM(T.table_name)
<SELECT u'SELECT column_name, another_column, %s, %s FROM table_name', (123, 'abc')>
```

`C` and `T` are _name factories_ — they turn attribute access into SQL Builder expressions. `C` is shorthand for "column", while `T` is shorthand for "table"; both of these factories generate identifier expressions, which render to literal SQL — you can see the "column_name", "another_column" and "table_name" identifiers are included verbatim in the generated SELECT query template. The plain Python values `123` and `'abc'`, on the other hand, are rendered as parameter placeholders (`%s`) and their actual values end up in the query parameter values tuple.

```python
>>> SELECT(C).FROM(T.table_name)
<SELECT u'SELECT * FROM table_name', ()>
```

The `C` name factory on itself generates a wildcard column list, when you just want to grab all columns from your data source.

```python
>>> SELECT(T.table_name(), T.table_name().column).FROM(T.table_name)
<SELECT u'SELECT table_name.*, table_name.column FROM table_name', ()>
```

Calling a table name (`T.table_name()`) returns a column name factory for that table — `T.table_name().column` renders as `table_name.column`, while `T.table_name()` with no further column name referenced renders as a wildcard `table_name.*` — useful when you want all columns from a single table.

```python
>>> SELECT(C.name).DISTINCT().FROM(T.users)
<SELECT u'SELECT DISTINCT name FROM users', ()>

>>> SELECT(C.name).DISTINCT(C.department, C.age).FROM(T.users)
<SELECT u'SELECT DISTINCT ON (department, age) name FROM users', ()>
```

Calling the `.DISTINCT()` method of a SELECT query adds a `DISTINCT` SQL clause; when called with parameters, it renders a `DISTINCT ON (columns)` clause.

```python
>>> col = 'column'
>>> tab = 'table'
>>> SELECT(C(col)).FROM(T(tab))
<SELECT u'SELECT column FROM table', ()>
```

Sometimes you already have a column name or a table name stored as a string in a variable, and you cannot reference it directly as an attribute on the `C` or `T` name factories; you could call `getattr(C, col)`, but `C(col)` is a shorthand notation for such cases.

```python
>>> SELECT((C.quantity + C.markup) * C.price).FROM(T.orders).WHERE(C.price > 100)
<SELECT u'SELECT ((quantity + markup) * price) FROM orders WHERE (price > %s)', (100,)>
```

SQL Builder expressions override basic operations, and wrap them in SQL Builder expressions as well, so Python expressions render as SQL expressions in the generated query — the `C.quantity * C.unit_price + C.fixed_markup` Python expression ends up as `((quantity * unit_price) + fixed_markup)` in the query, while `C.unit_price > 100` ends up as `unit_price > %s` in the query template, with a single query parameter `100`.

Rendering expressions will always add explicit parentheses for the operation evaluation order, even if you don't use parentheses in your Python expression: `C.foo + C.bar * C.baz` will render as `(foo + (bar * baz))`, due to multiplication's order of precedence being higher than addition; `(C.foo + C.bar) * C.baz` will honor your explicit evaluation order and will render as `((foo + bar) * baz)`.

```python
>>> SELECT(C.name).FROM(T.employees).WHERE((C.salary > 1000).AND(C.department == 'HR').AND(C.retired.NOT))
<SELECT u'SELECT name FROM employees WHERE (((salary > %s) AND (department = %s)) AND (NOT retired))', (1000, 'HR')>
```

Since Python does not allow overloading of the boolean operators (`and`, `or`, `not`, etc), those are available as methods on SQL Builder expressions; `C.foo.AND(C.bar)` will render as `(foo AND bar)` in the query, while `(C.salary > C.threshold).NOT` will render as `NOT (salary > threshold)` — note that `.NOT` is a magic attribute, you don't call it as a method.

```python
>>> from sqlbuilder.query import F
>>> SELECT(F.abs(C.change)).FROM(T.stats)
<SELECT u'SELECT abs(change) FROM stats', ()>
```

`F` is a name factory for SQL functions — `F.abs(C.change)` ends up rendered as `abs(change)` in the query. `F` is actually just an alias for the `C` name factory — both just produce SQL identifiers, and calling an SQL identifier generates a function call in the query, so you can actually use `C.abs(C.change)` instead of `F.abs(C.change)`; the `F` name just makes it more explicit that your intention is to refer to a function name instead of a column name.

```python
>>> SELECT(F.count(C.name, DISTINCT=True)).FROM(T.users)
<SELECT u'SELECT count(DISTINCT name) FROM users', ()>
```

Function calls support a `DISTINCT` keyword parameter, which renders as a `DISTINCT` clause for an aggregate function call.

```python
>>> from sqlbuilder.query import A
>>> SELECT(A.id_count(F.count(C.id)), A.subtotal(C.price * C.qty), A.foobar_string('foobar')).FROM(A.table_alias(T.table))
<SELECT u'SELECT count(id) AS id_count, (price * qty) AS subtotal, %s AS foobar_string FROM table AS table_alias', ('foobar',)>
```

`A` is a name factory for aliases — you can alias your columns, expressions, literals and tables by wrapping them in `A.alias_name(...)` calls.

```python
>>> SELECT(C.foo).FROM(A.alias(T.table, columns=(C.foo, C.bar)))
<SELECT u'SELECT foo FROM table AS alias(foo, bar)', ()>
```

Table aliases can also specify aliases for the table column names by providing a tuple in the `columns` parameter.

```python
>>> alias = 'id_count'
>>> SELECT(A(alias, F.count('id'))).FROM(T.table)
<SELECT u'SELECT count(%s) AS id_count FROM table', ('id',)>
```

`A(alias, expression)` is a convenience shorthand for `getattr(A, alias)(expression)`, for situations where you already have the name of the alias stored as a string in a variable.

```python
>>> SELECT(C.foo, C.bar).FROM(T.tab).WHERE(C.cond_foo > C.cond_bar)
<SELECT u'SELECT foo, bar FROM tab WHERE (cond_foo > cond_bar)', ()>

>>> SELECT(C.foo, C.bar).FROM(T.tab).GROUP_BY(C.group_foo, C.group_bar)
<SELECT u'SELECT foo, bar FROM tab GROUP BY group_foo, group_bar', ()>

>>> SELECT(C.foo, C.bar).FROM(T.tab).HAVING(C.cond_foo > C.cond_bar)
<SELECT u'SELECT foo, bar FROM tab HAVING (cond_foo > cond_bar)', ()>

>>> SELECT(C.foo, C.bar).FROM(T.tab).LIMIT(10, 20)
<SELECT u'SELECT foo, bar FROM tab LIMIT %s OFFSET %s', (10, 20)>

>>> SELECT(C.foo, C.bar).FROM(T.tab).LIMIT(10).OFFSET(20)
<SELECT u'SELECT foo, bar FROM tab LIMIT %s OFFSET %s', (10, 20)>

>>> SELECT(C.foo, C.bar).LIMIT(10).FROM(T.table)
<SELECT u'SELECT foo, bar FROM table LIMIT %s', (10,)>
```

You don't need to observe the mandatory SQL clause order; if you call `.LIMIT(10).FROM(T.table)`, SQL Builder will still render the query clauses in the correct order, freeing you to compose the query in an order that is more natural for your logic flow rather than requiring you to follow the SQL syntax requirements.

```python
>>> SELECT(C).FROM(T.foo).CROSS_JOIN(T.bar)
<SELECT u'SELECT * FROM foo CROSS JOIN bar', ()>

>>> SELECT(C).FROM(T.foo).LEFT_JOIN(T.bar, NATURAL=True)
<SELECT u'SELECT * FROM foo NATURAL LEFT OUTER JOIN bar', ()>

>>> SELECT(C).FROM(T.foo).LEFT_JOIN(T.bar, ON=(T.foo().col_foo == T.bar().col_bar))
<SELECT u'SELECT * FROM foo LEFT OUTER JOIN bar ON (foo.col_foo = bar.col_bar)', ()>

>>> SELECT(C).FROM(T.foo).LEFT_JOIN(T.bar, USING=(C.col_foo, C.col_bar))
<SELECT u'SELECT * FROM foo LEFT OUTER JOIN bar USING (col_foo, col_bar)', ()>

>>> SELECT(C).FROM(T.foo).RIGHT_JOIN(T.bar, USING=(C.col_foo, C.col_bar))
<SELECT u'SELECT * FROM foo RIGHT OUTER JOIN bar USING (col_foo, col_bar)', ()>

>>> SELECT(C).FROM(T.foo).FULL_JOIN(T.bar, USING=(C.col_foo, C.col_bar))
<SELECT u'SELECT * FROM foo FULL OUTER JOIN bar USING (col_foo, col_bar)', ()>

>>> SELECT(C).FROM(T.foo).INNER_JOIN(T.bar, USING=(C.col_foo, C.col_bar))
<SELECT u'SELECT * FROM foo INNER JOIN bar USING (col_foo, col_bar)', ()>

>>> SELECT(C).FROM(T.foo).CROSS_JOIN(T.bar)
<SELECT u'SELECT * FROM foo CROSS JOIN bar', ()>

>>> SELECT(C).FROM(T.foo).CROSS_JOIN(T.bar).LEFT_JOIN(T.baz, NATURAL=True)
<SELECT u'SELECT * FROM foo CROSS JOIN bar NATURAL LEFT OUTER JOIN baz)', ()>
```

Various table joins are straightforward.

```python
>>> SELECT(C.name).FROM(T.employees).ORDER_BY(C.department, ASC(C.salary + C.bonus), DESC(C.name))
<SELECT u'SELECT name FROM employees ORDER BY department, (salary + bonus) ASC, name DESC', ()>
```

The `ASC` and `DESC` functions render as ascending and descending order clause expressions; passing plain expressions or values to `.ORDER_BY` will render no `ASC` or `DESC` clause in the query.

```python
>>> SELECT(C.name).FROM(T.employees).ORDER_BY(ASC(C.department).NULLS_FIRST, DESC(C.salary).NULLS_LAST)
<SELECT u'SELECT name FROM employees ORDER BY department ASC NULLS FIRST, salary DESC NULLS LAST', ()>
```

Ordering expressions have `.NULLS_FIRST` and `.NULLS_LAST` magic attributes, that render `NULLS FIRST` and `NULLS LAST` SQL clauses. These are only available on the result of an `ASC` or `DESC` call, so you cannot have a `NULLS FIRST` clause without an explicit direction.

```python
>>> SELECT(C).FROM(T.foo) | SELECT(C).FROM(T.bar)
<SelectSet u'SELECT * FROM foo UNION SELECT * FROM bar', ()>

>>> SELECT(C).FROM(T.foo) - SELECT(C).FROM(T.bar)
<SelectSet u'SELECT * FROM foo EXCEPT SELECT * FROM bar', ()>

>>> SELECT(C).FROM(T.foo) & SELECT(C).FROM(T.bar)
<SelectSet u'SELECT * FROM foo INTERSECT SELECT * FROM bar', ()>

>>> (SELECT(C).FROM(T.foo) & SELECT(C).FROM(T.bar)).ORDER_BY(C.foo)
<SelectSet u'SELECT * FROM foo INTERSECT SELECT * FROM bar ORDER BY foo', ()>
```

Unions, intersections and exclusions are available using `set`-like operations. Note that most DBMS allow some clauses to appear only on the last query in the set, e.g. `LIMIT` or `ORDER BY`; SQL Builder does not enforce such limitations, it is up to you to build your queries to the requirements of your DBMS.

---

_More to come..._
