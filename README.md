SQL Builder
===========

Python module for incremental building of SQL queries


## Basic Example

    >>> from sqlbuilder.query import SELECT, F, V
    >>> SELECT(F.column_foo, (F.column_bar + 23).AS('alias')).FROM(T.table).WHERE(F.counter > 100)
    <u'SELECT column_foo, (column_bar + %s) AS alias FROM table WHERE (counter > %s)', (23, 100)>

## Planned

    >>> SELECT(F.column).FROM(T.table).WHERE(F.condition > V.var_foo).execute(dbconnection, var_foo=123)
    <Cursor ...>

_More to come..._
