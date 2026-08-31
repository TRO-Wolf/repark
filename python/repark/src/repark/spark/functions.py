"""The :mod:`repark.functions` facade — PySpark's ``pyspark.sql.functions`` surface.

Each function returns a :class:`repark.column.Column` backed by a native DataFusion expression.
PySpark scripts import these as ``from repark.functions import col, lit, coalesce`` or,
idiomatically, ``import repark.functions as F`` then ``F.col(...)``.
"""

from __future__ import annotations

import datetime
import enum
import math
from typing import Any

from repark import _native
from repark.errors import PySparkTypeError, PySparkValueError
from repark.spark._idents import quote_column_sql_expr as _quote_column_sql_expr
from repark.spark._idents import sql_string_literal
from repark.spark.column import Column, Scalar
from repark.spark.udtf import UserDefinedTableFunction, udtf


def col(name: str) -> Column:
    """A column reference by name (PySpark ``functions.col``).

    Bare attributes are Spark ``NamedExpression``s: a plain ``.cast(...)`` keeps this name
    in ``DataFrame.select`` (live PySpark 4.1.2). Structural ``sql_expr`` is a
    double-quoted identifier so free-SQL surfaces cannot retarget FROM via hostile names.
    Qualified names (``source.col``) quote each segment.
    """
    return Column(
        _native.PyColumn.column(name),
        spark_display=name,
        projection_name=name,
        stable_name=True,
        has_free_attribute=True,
        sql_expr=_quote_column_sql_expr(name),
    )


def lit(value: Any) -> Column:
    """A literal column from a Python scalar (PySpark ``functions.lit``).

    Supports ``None`` (SQL NULL), ``bool``, ``int``, ``float``, ``str``,
    ``datetime.date`` / ``datetime.datetime`` / ``datetime.time``, ``enum.Enum``
    (uses ``.value``), ``list`` / ``tuple`` (array), and 1-D ``numpy.ndarray``
    (array with Spark element type from dtype — E2). Marked foldable so
    ``df.select(F.sum("x"), F.lit(1))`` is a global aggregate (Spark allows constants
    beside aggregates, not ``[MISSING_GROUP_BY]``.
    """
    if isinstance(value, enum.Enum):
        value = value.value
    # Temporal literals: native PyColumn.literal is scalar-only; lower via SQL DATE/TIMESTAMP/TIME.
    if isinstance(value, datetime.datetime):
        # Naive wall-clock; tz-aware → UTC then strip tz for DF TIMESTAMP literal.
        if value.tzinfo is not None:
            value = value.astimezone(datetime.UTC).replace(tzinfo=None)
        text = value.strftime("%Y-%m-%d %H:%M:%S.%f").rstrip("0").rstrip(".")
        sql = f"TIMESTAMP '{text}'"
        display = text
        return Column(
            _native.PyColumn.sql(sql),
            spark_display=display,
            projection_name=display,
            stable_name=False,
            sql_expr=sql,
            is_foldable=True,
        )
    if isinstance(value, datetime.date):
        sql = f"DATE '{value.isoformat()}'"
        display = value.isoformat()
        return Column(
            _native.PyColumn.sql(sql),
            spark_display=display,
            projection_name=display,
            stable_name=False,
            sql_expr=sql,
            is_foldable=True,
        )
    if isinstance(value, datetime.time):
        text = value.strftime("%H:%M:%S")
        if value.microsecond:
            text = value.strftime("%H:%M:%S.%f").rstrip("0").rstrip(".")
        sql = f"TIME '{text}'"
        display = text
        return Column(
            _native.PyColumn.sql(sql),
            spark_display=display,
            projection_name=display,
            stable_name=False,
            sql_expr=sql,
            is_foldable=True,
        )
    ndarray_column = _lit_numpy_ndarray(value)
    if ndarray_column is not None:
        return ndarray_column
    if isinstance(value, (list, tuple)):
        # Columns inside lit([...]) → COLUMN_IN_LIST (Apache test_lit_list).
        for item in value:
            if isinstance(item, Column):
                raise PySparkValueError(
                    errorClass="COLUMN_IN_LIST",
                    messageParameters={"func_name": "lit"},
                )
        # Mixed-type list → string array (Spark non-ANSI lit; Apache test_lit_list). Nested
        # lists are coerced element-wise. Homogeneous int/float/str lists keep their type.
        coerced: list[Any] = _coerce_lit_list_mixed_to_string(list(value))
        elements = [lit(item) for item in coerced]
        result = _scalar("array", *elements)
        display = result.spark_display_part()
        return Column(
            result._inner,
            spark_display=display,
            projection_name=display,
            stable_name=False,
            sql_expr=result.sql_expr_part(),
            is_foldable=True,
        )
    if not isinstance(value, (type(None), bool, int, float, str)):
        raise PySparkTypeError(
            f"lit() supports None, bool, int, float, str, date, datetime, time, list, tuple, "
            f"ndarray, or Enum; got {type(value).__name__}"
        )
    display = _lit_spark_display(value)
    return Column(
        _native.PyColumn.literal(value),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=_lit_sql_expr(value),
        is_foldable=True,
    )


def _lit_list_item_kind(item: Any) -> str | None:
    """Coercion bucket for one ``lit([...])`` leaf — ``None`` for null."""
    import numbers

    if item is None:
        return None
    if isinstance(item, bool):
        return "bool"
    # numbers.Integral covers int + numpy integer scalars (not bool — checked above).
    if isinstance(item, numbers.Integral):
        return "int"
    if isinstance(item, numbers.Real):
        return "float"
    if isinstance(item, str):
        return "str"
    if isinstance(item, (list, tuple)):
        return "list"
    return type(item).__name__


def _coerce_lit_list_mixed_to_string(values: list[Any]) -> list[Any]:
    """Coerce mixed-type ``lit([...])`` elements (Spark non-ANSI + numeric promotion).

    Homogeneous non-null types (all int, all float, all str, all bool, nested lists of a
    single kind) pass through. Compatible numerics (int+float only) promote to float so
    ``lit([1, 1.0])`` stays a numeric array, not a faked string cast.
    Numpy integer/float scalars count as int/float — ``lit([np.int64(1), 2])``
    must not fake-string. Incompatible mixes — e.g. ``["a", 1, None, 1.0]`` — every non-None
    leaf becomes ``str(...)`` matching live Spark 4.1.2. Nested lists are walked the same way.
    """
    kinds = {_lit_list_item_kind(item) for item in values if item is not None}
    if "list" in kinds:
        # Nested arrays: coerce each sublist independently (may mix at this level too).
        if kinds == {"list"} or kinds <= {"list"}:
            return [
                None if item is None else _coerce_lit_list_mixed_to_string(list(item))
                for item in values
            ]
        # Mix of list + scalar at one level → stringify scalars, walk lists.
        return [
            None
            if item is None
            else (
                _coerce_lit_list_mixed_to_string(list(item))
                if isinstance(item, (list, tuple))
                else str(item)
            )
            for item in values
        ]
    if len(kinds) <= 1:
        # Normalize numpy scalars to Python builtins so ``lit(item)`` accepts them
        if kinds == {"int"}:
            return [None if item is None else int(item) for item in values]
        if kinds == {"float"}:
            return [None if item is None else float(item) for item in values]
        return values
    # int + float only → promote to float (Spark numeric array), never string.
    if kinds <= {"int", "float"}:
        return [
            None if item is None else (float(item) if not isinstance(item, bool) else item)
            for item in values
        ]
    # Mixed scalar kinds involving str/bool/other → string.
    return [None if item is None else str(item) for item in values]


# numpy dtype → Spark array element; object/|S absent (Apache test_ndarray_input).
_NUMPY_DTYPE_TO_SPARK_ELEMENT: dict[str, str] = {
    "int8": "tinyint",
    "int16": "smallint",
    "int32": "int",
    "int64": "bigint",
    "float32": "float",
    "float64": "double",
    "bool": "boolean",
    "bool_": "boolean",
    "str_": "string",
    "string_": "string",
}


def _lit_numpy_ndarray(value: Any) -> Column | None:
    """Lower a 1-D ``numpy.ndarray`` to a typed array Column, or ``None`` if not an ndarray.

    Unsigned integer, object, and bytes (``|S``) dtypes raise
    ``UNSUPPORTED_NUMPY_ARRAY_SCALAR`` (Spark 4.1.2 parity). Requires the optional
    ``numpy`` extra only when an ndarray is actually passed.
    """
    module = getattr(type(value), "__module__", "") or ""
    if not module.startswith("numpy") or type(value).__name__ != "ndarray":
        return None
    import numpy as np

    if not isinstance(value, np.ndarray):
        return None
    if value.ndim != 1:
        raise PySparkTypeError(
            errorClass="UNSUPPORTED_NUMPY_ARRAY_SCALAR",
            messageParameters={"dtype": f"ndarray(ndim={value.ndim})"},
        )
    dtype = value.dtype
    if dtype.kind in {"u", "O", "S"}:
        raise PySparkTypeError(
            errorClass="UNSUPPORTED_NUMPY_ARRAY_SCALAR",
            messageParameters={"dtype": str(dtype)},
        )
    dtype_name = dtype.name  # e.g. int8, float32, bool, str_
    element_type = _NUMPY_DTYPE_TO_SPARK_ELEMENT.get(dtype_name)
    if element_type is None:
        # Unicode string dtypes often surface as <U… rather than str_ (kind U only).
        if dtype.kind == "U":
            element_type = "string"
        elif dtype.kind == "b":
            element_type = "boolean"
        else:
            raise PySparkTypeError(
                errorClass="UNSUPPORTED_NUMPY_ARRAY_SCALAR",
                messageParameters={"dtype": str(dtype)},
            )
    # Materialize Python list; bool_ → bool, numpy scalars → Python via .item().
    python_list: list[Any] = []
    for item in value.tolist():
        if item is None:
            python_list.append(None)
        elif isinstance(item, (bool, int, float, str)):
            python_list.append(item)
        else:
            # numpy scalar leftovers
            python_list.append(item.item() if hasattr(item, "item") else item)
    elements = [lit(item) for item in python_list]
    result = _scalar("array", *elements)
    # Cast to array<element> so dtypes match Spark (int8→tinyint, empty arrays keep type).
    spark_cast_type = {
        "tinyint": "TINYINT",
        "smallint": "SMALLINT",
        "int": "INT",
        "bigint": "BIGINT",
        "float": "FLOAT",
        "double": "DOUBLE",
        "boolean": "BOOLEAN",
        "string": "VARCHAR",
    }.get(element_type, element_type.upper())
    cast_sql = f"CAST({result.sql_expr_part()} AS ARRAY<{spark_cast_type}>)"
    display = f"array<{element_type}>"
    return Column(
        _native.PyColumn.sql(cast_sql),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=cast_sql,
        is_foldable=True,
    )


def _lit_sql_expr(value: Scalar) -> str:
    """SQL literal fragment for embedding a ``lit`` into generated SQL (MERGE, etc.).

    Non-finite floats must not use bare ``nan`` / ``inf`` tokens — those bind as
    identifiers in free-SQL (select-global-agg, cube/rollup, MERGE) rather than float
    constants. Use CAST string forms DataFusion accepts.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        if math.isnan(value):
            return "CAST('NaN' AS DOUBLE)"
        if value == math.inf:
            return "CAST('Infinity' AS DOUBLE)"
        if value == -math.inf:
            return "CAST('-Infinity' AS DOUBLE)"
        return repr(value)
    if isinstance(value, str):
        return sql_string_literal(value)
    return str(value)


def _lit_spark_display(value: Scalar) -> str:
    """PySpark-style literal fragment for display/agg names (not DataFusion's ``Int64(1)``).

    Live PySpark 4.1.2 renders string literals **without** surrounding quotes in both projection
    names (``df.select(F.lit("s")).columns == ['s']``) and aggregate embeds
    (``first(z)``, ``concat(s, z)``). Integer/float/bool/NULL follow Spark coercion.
    """
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        # Spark keeps the double point: lit(2.0) names as "2.0", not "2" (live 4.1.2).
        return repr(value)
    if isinstance(value, str):
        # Unquoted, matching Spark's pretty name (quotes only appear inside the string).
        return value
    return str(value)


def expr(sql: str) -> Column:
    """A column from a SQL expression string (PySpark ``functions.expr``).

    The string is parsed eagerly, so DataFusion built-in functions and literals resolve
    (``expr("make_date(2020, 1, 1)")``, ``expr("1 + 1")``). An expression that references a *column*
    raises here — the DataFrame-bound ``expr`` path that binds column references to the frame
    arrives with the date-function group (the transform that needs it lands there, not here).

    Projection display matches live PySpark 4.1.2 for bare arithmetic fragments: ``1 + 1`` is
    shown as ``(1 + 1)`` (analyzer paren). Already-parenthesized or non-infix SQL is left as given.
    """
    stripped = sql.strip()
    # Spark pretty-names simple infix fragments with surrounding parens (live 4.1.2).
    if stripped and not (stripped.startswith("(") and stripped.endswith(")")):
        if any(token in stripped for token in (" + ", " - ", " * ", " / ", " % ")):
            display = f"({stripped})"
        else:
            display = stripped
    else:
        display = stripped
    return Column(
        _native.PyColumn.sql(sql),
        spark_display=display,
        projection_name=display,
        stable_name=False,
    )


def _partition_transform_of(*columns: Column) -> str | None:
    """Return the first non-None partition-transform marker among ``columns``."""
    for column in columns:
        transform = getattr(column, "_partition_transform", None)
        if transform is not None:
            return transform
    return None


def _thread_origin(*columns: Column) -> dict[str, str | None]:
    """Copy H1 origin tokens from the first origin-bearing argument.

    Wrappers that build a fresh :class:`Column` must pass these through;
    otherwise ``F.abs(right["k"])`` after a semi join silently binds the left
    column.
    """
    for column in columns:
        if column._origin_plan_id is not None:
            return {
                "origin_plan_id": column._origin_plan_id,
                "origin_field": column._origin_field,
            }
    return {}


def coalesce(*columns: Column) -> Column:
    """First non-null across the argument columns (PySpark ``functions.coalesce``)."""
    if not columns:
        from repark.errors import AnalysisException

        raise AnalysisException("coalesce requires at least 1 argument")
    for column in columns:
        column._reject_nested_generator("coalesce")
    parts = ", ".join(column.spark_wrap_display_part() for column in columns)
    sql_parts = ", ".join(column.sql_expr_part() for column in columns)
    display = f"coalesce({parts})"
    is_aggregate = any(column._is_aggregate for column in columns)
    is_foldable = (not is_aggregate) and all(column._is_foldable for column in columns)
    has_free_attribute = any(column._has_free_attribute for column in columns)
    has_ungroupable = any(column._has_ungroupable for column in columns)
    return Column(
        _native.PyColumn.coalesce([column._inner for column in columns]),
        spark_display=display,
        projection_name=display,
        sql_expr=f"coalesce({sql_parts})",
        join_sql_expr=f"coalesce({', '.join(column.join_sql_part() for column in columns)})",
        stable_name=False,
        is_aggregate=is_aggregate,
        is_foldable=is_foldable,
        has_free_attribute=has_free_attribute,
        has_ungroupable=has_ungroupable,
        partition_transform=_partition_transform_of(*columns),
        **_thread_origin(*columns),
    )


def concat(*columns: Column) -> Column:
    """String concatenation of the argument columns (PySpark ``functions.concat``)."""
    if not columns:
        from repark.errors import AnalysisException

        raise AnalysisException("concat requires at least 1 argument")
    for column in columns:
        column._reject_nested_generator("concat")
    parts = ", ".join(column.spark_wrap_display_part() for column in columns)
    sql_parts = ", ".join(column.sql_expr_part() for column in columns)
    display = f"concat({parts})"
    # Spark null propagation: any NULL arg → NULL (not DF empty-string skip).
    null_guard = " OR ".join(f"({column.sql_expr_part()} IS NULL)" for column in columns)
    sql_expr = f"CASE WHEN {null_guard} THEN CAST(NULL AS VARCHAR) ELSE concat({sql_parts}) END"
    is_aggregate = any(column._is_aggregate for column in columns)
    is_foldable = (not is_aggregate) and all(column._is_foldable for column in columns)
    has_free_attribute = any(column._has_free_attribute for column in columns)
    has_ungroupable = any(column._has_ungroupable for column in columns)
    return Column(
        _native.PyColumn.concat([column._inner for column in columns]),
        spark_display=display,
        projection_name=display,
        sql_expr=sql_expr,
        join_sql_expr=f"concat({', '.join(column.join_sql_part() for column in columns)})",
        stable_name=False,
        is_aggregate=is_aggregate,
        is_foldable=is_foldable,
        has_free_attribute=has_free_attribute,
        has_ungroupable=has_ungroupable,
        partition_transform=_partition_transform_of(*columns),
        **_thread_origin(*columns),
    )


def current_timestamp() -> Column:
    """The statement's current timestamp (PySpark ``functions.current_timestamp``).

    Arrow type is ``timestamp[us, tz=UTC]`` — microsecond precision with UTC timezone — matching
    live PySpark 4.1.2 (and Iceberg v2, which rejects nanosecond timestamps). Projection
    display is ``current_timestamp()`` (live Spark; not DataFusion's ``now()``).

    Marked foldable / free of attributes so ``df.select(F.sum("x"), F.current_timestamp())``
    is a global aggregate (Spark allows no-arg non-attribute companions),
    not ``[MISSING_GROUP_BY]``.
    """
    display = "current_timestamp()"
    return Column(
        _native.PyColumn.current_timestamp(),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr="current_timestamp()",
        is_foldable=True,
    )


# PySpark camelCase spelling ``currentTimestamp``; keep both for the import swap.
currentTimestamp = current_timestamp  # noqa: N816 — deliberate PySpark-compatible camelCase alias


def _column_argument(value: Column | str) -> Column:
    """Coerce a date-function's date/timestamp argument (a Column or a column name) to a Column.

    PySpark's date functions accept either a :class:`Column` or a bare column-name string (which
    becomes ``col(name)``); mirror that so scripts pass a column name straight through.
    """
    if isinstance(value, Column):
        return value
    if isinstance(value, str):
        return col(value)
    raise PySparkTypeError(f"expected a Column or column name (str), got {type(value).__name__}")


def _integer_argument(value: Column | int | str) -> Column:
    """Coerce a numeric-count argument (``add_months`` months, ``date_add`` days) to a Column.

    PySpark accepts a Python ``int``, a :class:`Column`, or a column-name ``str`` (SPARK-37738;
    Apache ``test_date_add_function`` / ``test_add_months_function``). An ``int`` is wrapped with
    ``lit``; a string becomes ``col(name)``.
    """
    if isinstance(value, Column):
        return value
    if isinstance(value, str):
        return col(value)
    if isinstance(value, int) and not isinstance(value, bool):
        return lit(value)
    raise PySparkTypeError(
        f"expected a Column, int, or column name (str), got {type(value).__name__}"
    )


#
# These deliberately shadow the Python builtins ``sum``/``min``/``max`` (and add ``count``) exactly
# as ``pyspark.sql.functions`` does — a migrating ``F.sum(...)`` must resolve here, not to the
# builtin. The returned :class:`Column` carries its PySpark default output name in ``_agg_name``


def _aggregate_argument(col: Column | str) -> tuple[Column, str]:
    """Coerce an aggregate argument (a column name or :class:`Column`) to ``(Column, name_part)``.

    The ``name_part`` is the token PySpark embeds in the output name — the column name itself for a
    string argument, or the facade's PySpark-style display for a :class:`Column`
    (``col("x") + 1`` → ``(x + 1)``; never DataFusion's ``x + Int64(1)``).

    String arguments carry a quoted structural ``sql_expr`` so aggregate builders embed safe
    identifiers into free-SQL global-agg select.
    Generator arguments are refused loud (``UNSUPPORTED_GENERATOR``) — wrapping
    ``F.count(F.explode(...))`` / ``F.sum`` / ``collect_*`` would strip ``_generator`` and
    aggregate the array placeholder instead of elements (Spark parity).
    """
    if isinstance(col, str):
        # Build the column reference directly (the `col` parameter shadows the module `col` fn).
        return (
            Column(
                _native.PyColumn.column(col),
                spark_display=col,
                projection_name=col,
                stable_name=True,
                sql_expr=_quote_column_sql_expr(col),
            ),
            col,
        )
    if isinstance(col, Column):
        col._reject_nested_generator("aggregate")
        return col, col.spark_display_part()
    raise PySparkTypeError(
        f"aggregate expects a Column or column name (str), got {type(col).__name__}"
    )


def abs(col: Column | str) -> Column:
    """Absolute value (PySpark ``functions.abs``).

    Implemented facade-side as ``CASE WHEN col < 0 THEN -col ELSE col`` (no new Rust) so compound
    aggregate names can pin ``sum(abs(x))`` bit-for-bit against the live PySpark oracle. The
    ``spark_display`` is forced to ``abs(...)`` regardless of the CASE plan shape.
    """
    column = _column_argument(col)
    # 0 - column for negation (avoids a unary-minus native API).
    negated = lit(0) - column
    result = when(column < 0, negated).otherwise(column)
    display = f"abs({column.spark_wrap_display_part()})"
    return Column(
        result._inner,
        spark_display=display,
        projection_name=display,
        sql_expr=f"abs({column.sql_expr_part()})",
        join_sql_expr=f"abs({column.join_sql_part()})",
        stable_name=False,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable and not column._is_aggregate,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def sum(col: Column | str) -> Column:
    """Sum of a group, skipping NULLs (PySpark ``functions.sum``).

    Integer inputs widen to ``LongType`` (Spark parity); the empty-group sum is NULL.
    Structural ``sql_expr`` uses the argument's quoted SQL fragment so free-SQL global-agg
    select never falls back to DataFusion ``schema_name`` / ``Int64`` display.
    """
    column, part = _aggregate_argument(col)
    agg_name = f"sum({part})"
    return Column(
        column._inner.aggregate("sum", False),
        agg_name=agg_name,
        sql_expr=f"sum({column.sql_expr_part()})",
        join_sql_expr=f"sum({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def count(col: Column | str) -> Column:
    """Row count (PySpark ``functions.count``).

    ``count("*")`` (or ``count(lit(1))``) counts every row; ``count(col)`` counts non-NULL values
    of ``col``. The result is a non-nullable ``LongType``.
    """
    if isinstance(col, str) and col == "*":
        # Structural SQL for free-SQL surfaces (global-agg select, CUBE): never emit
        return Column(
            _native.PyColumn.count_aggregate([_native.PyColumn.literal(1)], False),
            agg_name="count(1)",
            sql_expr="count(*)",
            spark_display="count(1)",
            projection_name="count(1)",
        )
    if isinstance(col, Column):
        star_name = col._projection_name or col._spark_display
        if star_name == "*":
            return count("*")
    column, part = _aggregate_argument(col)
    agg_name = f"count({part})"
    return Column(
        _native.PyColumn.count_aggregate([column._inner], False),
        agg_name=agg_name,
        sql_expr=f"count({column.sql_expr_part()})",
        join_sql_expr=f"count({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        **_thread_origin(column),
    )


def count_distinct(col: Column | str, *cols: Column | str) -> Column:
    """Distinct-tuple count across one or more columns (PySpark ``functions.count_distinct``).

    ``count(DISTINCT a, b)`` counts distinct ``(a, b)`` tuples; a row is excluded when **any** of
    the columns is NULL (live PySpark 4.1.2). The result is a non-nullable ``LongType``; the default
    output name is ``count(DISTINCT a, b)`` (space after each comma). Multi-column form is supported
    end-to-end (engine packs into a null-if-any struct under DataFusion's single-arg
    ``COUNT DISTINCT``).
    """
    columns = [_aggregate_argument(item) for item in (col, *cols)]
    natives = [column._inner for column, _ in columns]
    parts = ", ".join(part for _, part in columns)
    sql_parts = ", ".join(column.sql_expr_part() for column, _ in columns)
    agg_name = f"count(DISTINCT {parts})"
    # DataFusion rejects multi-arg COUNT DISTINCT; pack null-if-any struct so free-SQL
    if len(columns) == 1:
        sql_expr = f"count(DISTINCT {sql_parts})"
    else:
        present = " AND ".join(f"({column.sql_expr_part()} IS NOT NULL)" for column, _ in columns)
        fields = ", ".join(column.sql_expr_part() for column, _ in columns)
        sql_expr = f"count(DISTINCT CASE WHEN {present} THEN struct({fields}) END)"
    return Column(
        _native.PyColumn.count_aggregate(natives, True),
        agg_name=agg_name,
        sql_expr=sql_expr,
        join_sql_expr=(
            f"count(DISTINCT {', '.join(column.join_sql_part() for column, _ in columns)})"
        ),
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=_partition_transform_of(*(column for column, _ in columns)),
        **_thread_origin(*(column for column, _ in columns)),
    )


# PySpark also exposes the camelCase spelling ``countDistinct``; keep both.
countDistinct = count_distinct  # noqa: N816 — deliberate PySpark-compatible camelCase alias


def avg(col: Column | str) -> Column:
    """Mean of a group as ``DoubleType``, skipping NULLs (PySpark ``functions.avg``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"avg({part})"
    return Column(
        column._inner.aggregate("avg", False),
        agg_name=agg_name,
        sql_expr=f"avg({column.sql_expr_part()})",
        join_sql_expr=f"avg({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


# PySpark exposes ``mean`` as an alias of ``avg`` (same output name ``avg(x)``).
mean = avg


def min(col: Column | str) -> Column:
    """Minimum of a group, skipping NULLs; preserves the input type (PySpark ``functions.min``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"min({part})"
    return Column(
        column._inner.aggregate("min", False),
        agg_name=agg_name,
        sql_expr=f"min({column.sql_expr_part()})",
        join_sql_expr=f"min({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def max(col: Column | str) -> Column:
    """Maximum of a group, skipping NULLs; preserves the input type (PySpark ``functions.max``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"max({part})"
    return Column(
        column._inner.aggregate("max", False),
        agg_name=agg_name,
        sql_expr=f"max({column.sql_expr_part()})",
        join_sql_expr=f"max({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def first(col: Column | str, ignorenulls: bool = False) -> Column:
    """First value in a group (PySpark ``functions.first``).

    With ``ignorenulls=True`` the first **non-NULL** value is returned. Without an ordered input
    the choice is nondeterministic (Spark parity), so callers pin a deterministic fixture.
    """
    column, part = _aggregate_argument(col)
    agg_name = f"first({part})"
    # DataFusion SQL: first_value [IGNORE NULLS]; facade default name stays Spark ``first(x)``.
    # Structural IGNORE NULLS keeps the SQL global-agg path (lit/cast/composition) aligned with
    base_sql = f"first_value({column.sql_expr_part()})"
    sql_expr = f"{base_sql} IGNORE NULLS" if ignorenulls else base_sql
    return Column(
        column._inner.aggregate("first", ignorenulls),
        agg_name=agg_name,
        sql_expr=sql_expr,
        join_sql_expr=f"first_value({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def last(col: Column | str, ignorenulls: bool = False) -> Column:
    """Last value in a group (PySpark ``functions.last``).

    With ``ignorenulls=True`` the last **non-NULL** value is returned. Nondeterministic without an
    ordered input (Spark parity).
    """
    column, part = _aggregate_argument(col)
    agg_name = f"last({part})"
    base_sql = f"last_value({column.sql_expr_part()})"
    sql_expr = f"{base_sql} IGNORE NULLS" if ignorenulls else base_sql
    return Column(
        column._inner.aggregate("last", ignorenulls),
        agg_name=agg_name,
        sql_expr=sql_expr,
        join_sql_expr=f"last_value({column.join_sql_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
        **_thread_origin(column),
    )


def collect_list(col: Column | str) -> Column:
    """Collect non-NULL values of a group into an array (PySpark ``functions.collect_list``).

    NULL elements are excluded (live PySpark 4.1.2). An empty group (or a group whose values are
    all NULL) yields an empty array, not NULL. Element order is **nondeterministic** — pin tests
    with sorted contents or single-element groups. Output name ``collect_list(x)``.
    """
    column, part = _aggregate_argument(col)
    agg_name = f"collect_list({part})"
    sql_expr = f"coalesce(array_agg({column.sql_expr_part()}) IGNORE NULLS, make_array())"
    return Column(
        column._inner.aggregate("collect_list", False),
        agg_name=agg_name,
        sql_expr=sql_expr,
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def collect_set(col: Column | str) -> Column:
    """Collect distinct non-NULL values of a group into an array.

    PySpark ``functions.collect_set``. NULL elements are excluded; empty group → empty array.
    Element order is **nondeterministic** (and the set is unique) — pin tests with sorted unique
    contents. Output name ``collect_set(x)``. PySpark 4.1.2 exports only the snake_case spelling
    (no ``collectSet`` camelCase alias).
    """
    column, part = _aggregate_argument(col)
    agg_name = f"collect_set({part})"
    # DataFusion's ``array_agg(DISTINCT x) IGNORE NULLS`` still keeps NULL in the set.
    # ``array_distinct(array_agg(x) IGNORE NULLS)`` excludes nulls and de-dupes; empty
    sql_expr = (
        f"coalesce(array_distinct(array_agg({column.sql_expr_part()}) IGNORE NULLS), make_array())"
    )
    return Column(
        column._inner.aggregate("collect_set", False),
        agg_name=agg_name,
        sql_expr=sql_expr,
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


# ---- window functions ---------------------------------------------------------------------------


def row_number() -> Column:
    """A sequential 1-based row number over a window (PySpark ``functions.row_number``).

    Complete it with :meth:`repark.column.Column.over`::

        F.row_number().over(Window.partitionBy("g").orderBy("ts"))

    The result is ``IntegerType`` (Spark parity — the engine casts DataFusion's ``UInt64``).
    """
    return Column(
        _native.PyColumn.row_number(),
        spark_display="row_number()",
        projection_name="row_number()",
        sql_expr="row_number()",
        is_aggregate=False,
        is_foldable=False,
        has_ungroupable=True,
    )


def rank() -> Column:
    """Ranking with gaps on ties (PySpark ``functions.rank``). Requires ``.over(...)``."""
    return Column(
        _native.PyColumn.rank(),
        spark_display="rank()",
        projection_name="rank()",
        sql_expr="rank()",
        is_aggregate=False,
        is_foldable=False,
        has_ungroupable=True,
    )


def dense_rank() -> Column:
    """Ranking without gaps on ties (PySpark ``functions.dense_rank``). Requires ``.over(...)``."""
    return Column(
        _native.PyColumn.dense_rank(),
        spark_display="dense_rank()",
        projection_name="dense_rank()",
        sql_expr="dense_rank()",
        is_aggregate=False,
        is_foldable=False,
        has_ungroupable=True,
    )


def ntile(n: int) -> Column:
    """Bucket number in ``1..n`` (PySpark ``functions.ntile``). Requires ``.over(...)``."""
    if not isinstance(n, int) or isinstance(n, bool) or n <= 0:
        from repark.errors import IllegalArgumentException

        raise IllegalArgumentException(f"ntile requires a positive integer, got {n!r}")
    return Column(
        _native.PyColumn.ntile(int(n)),
        spark_display=f"ntile({n})",
        projection_name=f"ntile({n})",
        sql_expr=f"ntile({n})",
        is_aggregate=False,
        is_foldable=False,
        has_ungroupable=True,
    )


# ---- date functions -----------------------------------------------------------------------------


def _date_fn(column: Column | str, method_name: str, display_name: str) -> Column:
    """Build a unary date function with Spark projection/agg display ``display_name(child)``."""
    argument = _column_argument(column)
    argument._reject_nested_generator(f"function {display_name}")
    display = f"{display_name}({argument.spark_wrap_display_part()})"
    native = getattr(argument._inner, method_name)()
    return Column(
        native,
        spark_display=display,
        projection_name=display,
        sql_expr=f"{display_name}({argument.sql_expr_part()})",
        join_sql_expr=f"{display_name}({argument.join_sql_part()})",
        stable_name=False,
        is_aggregate=argument._is_aggregate,
        is_foldable=argument._is_foldable and not argument._is_aggregate,
        has_free_attribute=argument._has_free_attribute,
        has_ungroupable=argument._has_ungroupable,
        partition_transform=argument._partition_transform,
        **_thread_origin(argument),
    )


def year(col: Column | str) -> Column:
    """The calendar year of a date/timestamp (PySpark ``functions.year``)."""
    return _date_fn(col, "year", "year")


def month(col: Column | str) -> Column:
    """The month of year (1..12) of a date/timestamp (PySpark ``functions.month``)."""
    return _date_fn(col, "month", "month")


def quarter(col: Column | str) -> Column:
    """The quarter of year (1..4) of a date/timestamp (PySpark ``functions.quarter``)."""
    return _date_fn(col, "quarter", "quarter")


def weekofyear(col: Column | str) -> Column:
    """The ISO-8601 week number (1..53) of a date/timestamp (PySpark ``functions.weekofyear``)."""
    return _date_fn(col, "weekofyear", "weekofyear")


def dayofweek(col: Column | str) -> Column:
    """The day of week, 1=Sunday..7=Saturday (PySpark ``functions.dayofweek``; Spark's indexing)."""
    return _date_fn(col, "dayofweek", "dayofweek")


def weekday(col: Column | str) -> Column:
    """The day of week, 0=Monday..6=Sunday (PySpark ``functions.weekday``; Spark's indexing).

    Distinct from :func:`dayofweek` (1=Sunday..7=Saturday). Recorded against live PySpark 4.1.2:
    ``weekday(DATE '2024-01-08')`` (Monday) = 0; ``weekday(DATE '2024-01-07')`` (Sunday) = 6.
    """
    return _date_fn(col, "weekday", "weekday")


def dayofmonth(col: Column | str) -> Column:
    """The day of month (1..31) of a date/timestamp (PySpark ``functions.dayofmonth``)."""
    return _date_fn(col, "dayofmonth", "dayofmonth")


def dayofyear(col: Column | str) -> Column:
    """The day of year (1..366) of a date/timestamp (PySpark ``functions.dayofyear``)."""
    return _date_fn(col, "dayofyear", "dayofyear")


def last_day(date: Column | str) -> Column:
    """The last day of the month containing ``date`` (PySpark ``functions.last_day``)."""
    return _date_fn(date, "last_day", "last_day")


def add_months(start: Column | str, months: Column | int | str) -> Column:
    """The date ``months`` months after ``start``, end-of-month-preserving (PySpark
    ``functions.add_months``). ``months`` is a Column, column-name str, or ``int``."""
    start_column = _column_argument(start)
    months_column = _integer_argument(months)
    start_column._reject_nested_generator("function add_months")
    months_column._reject_nested_generator("function add_months")
    display = (
        f"add_months({start_column.spark_wrap_display_part()}, "
        f"{months_column.spark_wrap_display_part()})"
    )
    # Sticky free/aggregate so ``select(sum, add_months(d,1))`` is MISSING_GROUP_BY and
    is_aggregate = start_column._is_aggregate or months_column._is_aggregate
    has_free_attribute = start_column._has_free_attribute or months_column._has_free_attribute
    has_ungroupable = start_column._has_ungroupable or months_column._has_ungroupable
    months_sql = months_column.sql_expr_part()
    if months_column._is_foldable:
        months_sql = f"CAST({months_sql} AS INT)"
    return Column(
        start_column._inner.add_months(months_column._inner),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=f"add_months({start_column.sql_expr_part()}, {months_sql})",
        join_sql_expr=(
            f"add_months({start_column.join_sql_part()}, {months_column.join_sql_part()})"
        ),
        is_aggregate=is_aggregate,
        is_foldable=(not is_aggregate) and start_column._is_foldable and months_column._is_foldable,
        has_free_attribute=has_free_attribute,
        has_ungroupable=has_ungroupable,
        partition_transform=_partition_transform_of(start_column, months_column),
        **_thread_origin(start_column, months_column),
    )


def date_add(start: Column | str, days: Column | int | str) -> Column:
    """The date ``days`` days after ``start`` (PySpark ``functions.date_add``). ``days`` may be a
    Column, column-name str, or ``int`` (negative goes backwards)."""
    start_column = _column_argument(start)
    days_column = _integer_argument(days)
    start_column._reject_nested_generator("function date_add")
    days_column._reject_nested_generator("function date_add")
    display = (
        f"date_add({start_column.spark_wrap_display_part()}, "
        f"{days_column.spark_wrap_display_part()})"
    )
    is_aggregate = start_column._is_aggregate or days_column._is_aggregate
    has_free_attribute = start_column._has_free_attribute or days_column._has_free_attribute
    has_ungroupable = start_column._has_ungroupable or days_column._has_ungroupable
    # DataFusion ``date_add`` rejects bare Int64 literals — CAST to INT for foldable counts.
    days_sql = days_column.sql_expr_part()
    if days_column._is_foldable:
        days_sql = f"CAST({days_sql} AS INT)"
    return Column(
        start_column._inner.date_add(days_column._inner),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=f"date_add({start_column.sql_expr_part()}, {days_sql})",
        join_sql_expr=f"date_add({start_column.join_sql_part()}, {days_column.join_sql_part()})",
        is_aggregate=is_aggregate,
        is_foldable=(not is_aggregate) and start_column._is_foldable and days_column._is_foldable,
        has_free_attribute=has_free_attribute,
        has_ungroupable=has_ungroupable,
        partition_transform=_partition_transform_of(start_column, days_column),
        **_thread_origin(start_column, days_column),
    )


def date_format(date: Column | str, format: str) -> Column:
    """Format a date/timestamp with a Java pattern string (PySpark ``functions.date_format``).

    Supported pattern letters: ``y`` (year), ``M`` (month number/name), ``d`` (day), ``q`` / ``Q``
    (quarter), ``E`` (day-of-week name), ``H`` / ``m`` / ``s`` (time), plus single-quoted literals.
    An unsupported letter raises from the engine rather than emitting a wrong string.
    """
    argument = _column_argument(date)
    argument._reject_nested_generator("function date_format")
    # Live PySpark 4.1.2: date_format(d, yyyy-MM) — pattern is unquoted in the name.
    display = f"date_format({argument.spark_wrap_display_part()}, {format})"
    # Escape single quotes in the pattern for structural SQL embeds.
    return Column(
        argument._inner.date_format(format),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=f"date_format({argument.sql_expr_part()}, {sql_string_literal(format)})",
        is_aggregate=argument._is_aggregate,
        is_foldable=argument._is_foldable and not argument._is_aggregate,
        has_free_attribute=argument._has_free_attribute,
        has_ungroupable=argument._has_ungroupable,
        partition_transform=argument._partition_transform,
    )


def trunc(date: Column | str, format: str) -> Column:
    """Truncate a DATE to ``format`` — ``year``/``month``/``week``/``quarter`` (PySpark
    ``functions.trunc``). An invalid format yields NULL (Spark semantics)."""
    argument = _column_argument(date)
    argument._reject_nested_generator("function trunc")
    display = f"trunc({argument.spark_wrap_display_part()}, {format})"
    return Column(
        argument._inner.trunc(format),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=f"trunc({argument.sql_expr_part()}, {sql_string_literal(format)})",
        is_aggregate=argument._is_aggregate,
        is_foldable=argument._is_foldable and not argument._is_aggregate,
        has_free_attribute=argument._has_free_attribute,
        has_ungroupable=argument._has_ungroupable,
        partition_transform=argument._partition_transform,
    )


def date_trunc(format: str, timestamp: Column | str) -> Column:
    """Truncate a TIMESTAMP to ``format`` (PySpark ``functions.date_trunc``).

    Note the PySpark argument order — the ``format`` comes first. Granularities range from ``year``
    down to ``microsecond`` (plus ``week`` / ``quarter``); an invalid format yields NULL.
    """
    argument = _column_argument(timestamp)
    argument._reject_nested_generator("function date_trunc")
    display = f"date_trunc({format}, {argument.spark_wrap_display_part()})"
    return Column(
        argument._inner.date_trunc(format),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=f"date_trunc({sql_string_literal(format)}, {argument.sql_expr_part()})",
        is_aggregate=argument._is_aggregate,
        is_foldable=argument._is_foldable and not argument._is_aggregate,
        has_free_attribute=argument._has_free_attribute,
        has_ungroupable=argument._has_ungroupable,
        partition_transform=argument._partition_transform,
    )


def _partition_transform(transform: str, col: Column | str) -> Column:
    """Build a partition-transform Column valid only inside ``writeTo(...).partitionedBy(...)``.

    PySpark rejects these outside ``partitionedBy``
    (``PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY``). repark marks the Column with
    ``_partition_transform`` so :class:`~repark.dataframe.DataFrameWriterV2` can render
    ``years(col)`` / ``months(col)`` / ``bucket(n, col)`` / … into CTAS ``PARTITIONED BY``; any
    other use raises :class:`~repark.errors.AnalysisException`. The engine's CTAS path builds the
    real Iceberg ``Transform`` (bucket/truncate/year/month/day/hour) and computes each partition
    value from the source column via the iceberg-rust fork, so these transforms work end-to-end
    The engine rejects non-identity transforms.

    The identity argument is double-quoted via :func:`repark._idents.quote_ident` so reserved
    words / hostile text cannot break out of the transform call.
    """
    from repark.spark._idents import quote_ident as _quote_ident

    column = _column_argument(col)
    # Identity name only — transform args are column references, never raw SQL expressions.
    source_name = col if isinstance(col, str) else column.spark_display_part()
    source_sql = _quote_ident(source_name)
    fragment = f"{transform}({source_sql})"
    # Dummy native (never evaluated): partitionedBy reads ``_partition_transform`` only.
    return Column(
        _native.PyColumn.literal(None),
        spark_display=fragment,
        partition_transform=fragment,
    )


def years(col: Column | str) -> Column:
    """Partition transform: years — only for ``partitionedBy`` (PySpark ``functions.years``)."""
    return _partition_transform("years", col)


def months(col: Column | str) -> Column:
    """Partition transform: months — only for ``partitionedBy`` (PySpark ``functions.months``)."""
    return _partition_transform("months", col)


def days(col: Column | str) -> Column:
    """Partition transform: days — only for ``partitionedBy`` (PySpark ``functions.days``)."""
    return _partition_transform("days", col)


def hours(col: Column | str) -> Column:
    """Partition transform: hours — only for ``partitionedBy`` (PySpark ``functions.hours``)."""
    return _partition_transform("hours", col)


def bucket(numBuckets: int | Column, col: Column | str) -> Column:  # noqa: N803 — PySpark arg name
    """Partition transform: bucket — only for ``partitionedBy`` (PySpark ``functions.bucket``).

    Renders ``bucket(<numBuckets>, "col")`` into CTAS ``PARTITIONED BY`` (the identity column arg
    is double-quoted). ``numBuckets`` must be a positive ``int`` (or Column); the engine
    also rejects ``<= 0`` loudly at parse time (Spark/Iceberg analysis-error parity).

    E1: bad ``numBuckets`` type raises ``PySparkTypeError`` with ``NOT_COLUMN_OR_INT`` so Apache
    ``check_error`` class + parameter-key equality PASSes.
    """
    from repark.spark._idents import quote_ident as _quote_ident

    if not isinstance(numBuckets, (int, Column)) or isinstance(numBuckets, bool):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT",
            messageParameters={
                "arg_name": "numBuckets",
                "arg_type": type(numBuckets).__name__,
            },
        )
    if isinstance(numBuckets, Column):
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "bucket(Column, col) as a partition transform is not supported yet "
            "(pass an int numBuckets; disclosed E1)"
        )
    column = _column_argument(col)
    source_name = col if isinstance(col, str) else column.spark_display_part()
    fragment = f"bucket({numBuckets}, {_quote_ident(source_name)})"
    return Column(
        _native.PyColumn.literal(None),
        spark_display=fragment,
        partition_transform=fragment,
    )


column = col
"""PySpark ``functions.column`` — ``builtin.py`` defines it as a bare alias of ``col``."""


# Scalar wrappers over engine calls and Column methods.


def _as_column_arg(argument: Column | str | int | float | bool | None, *, as_lit: bool) -> Column:
    """Coerce a function argument: column name → col, or force lit when ``as_lit``."""
    if isinstance(argument, Column):
        return argument
    if as_lit or not isinstance(argument, str):
        return lit(argument)  # type: ignore[arg-type]
    return col(argument)


def _scalar(
    name: str,
    *args: Column | str | int | float | bool | None,
    lit_indices: frozenset[int] | None = None,
    display: str | None = None,
    foldable: bool | None = None,
    has_ungroupable: bool | None = None,
) -> Column:
    """Build a Column via native ``call_scalar`` with Spark-style display text.

    ``lit_indices`` marks argument positions that must be SQL literals even when they are
    strings (delimiter / pattern / pad char). All other strings are column-name references.

    ``foldable`` overrides inferred foldability. Default inference requires at least one
    argument — vacuous ``all([])`` must not mark nullary calls foldable (``F.rand`` →
    ``random()`` is non-deterministic and non-foldable. Pass
    ``foldable=True`` for known foldable nullaries such as ``current_date``.

    ``has_ungroupable`` overrides sticky ungroupable (OR with child bits when None).
    Generator arguments are refused loud (``UNSUPPORTED_GENERATOR``). Wrapping
    ``F.size(F.explode(...))`` / ``.str`` paths would strip ``_generator`` and skip
    unnest rewrite (Spark parity).
    """
    force_lit = lit_indices or frozenset()
    columns: list[Column] = []
    display_parts: list[str] = []
    sql_parts: list[str] = []
    for index, argument in enumerate(args):
        column = _as_column_arg(argument, as_lit=index in force_lit)
        column._reject_nested_generator(f"function {name}")
        columns.append(column)
        if index in force_lit and isinstance(argument, str):
            display_parts.append(repr(argument))
        else:
            display_parts.append(column.spark_wrap_display_part())
        sql_parts.append(column.sql_expr_part())
    shown = display if display is not None else f"{name}({', '.join(display_parts)})"
    # Nullary: do NOT treat vacuous all([]) as foldable — F.rand→random() is non-foldable
    is_aggregate = any(column._is_aggregate for column in columns)
    if foldable is not None:
        is_foldable = bool(foldable) and not is_aggregate
    else:
        is_foldable = (
            (not is_aggregate) and bool(columns) and all(column._is_foldable for column in columns)
        )
    has_free_attribute = any(column._has_free_attribute for column in columns)
    child_ungroupable = any(column._has_ungroupable for column in columns)
    if has_ungroupable is not None:
        ungroupable_flag = bool(has_ungroupable) or child_ungroupable
    else:
        ungroupable_flag = child_ungroupable
    # WindowSpec so adjacent withColumn(s) can still merge. Other scalars clear it.
    window_spec = None
    if name == "round" and columns:
        window_spec = getattr(columns[0], "_window_spec", None)
    return Column(
        _native.PyColumn.call_scalar(name, [column._inner for column in columns]),
        spark_display=shown,
        projection_name=shown,
        sql_expr=f"{name}({', '.join(sql_parts)})",
        join_sql_expr=f"{name}({', '.join(column.join_sql_part() for column in columns)})",
        stable_name=False,
        is_aggregate=is_aggregate,
        is_foldable=is_foldable,
        has_free_attribute=has_free_attribute,
        has_ungroupable=ungroupable_flag,
        partition_transform=_partition_transform_of(*columns),
        window_spec=window_spec,
        **_thread_origin(*columns),
    )


# Re-export wrappers; late imports break the functions ↔ functions_expr cycle.
from repark.spark.functions_agg import (  # noqa: E402
    bool_and,
    bool_or,
    count_if,
    every,
    first_value,
    last_value,
    some,
    std,
)
from repark.spark.functions_bitwise import (  # noqa: E402
    bit_count,
    bit_get,
    bitmap_bit_position,
    bitmap_bucket_number,
    bitmap_count,
    bitwise_not,
    bitwiseNOT,
    getbit,
    shiftleft,
    shiftright,
    shiftrightunsigned,
)
from repark.spark.functions_collections import (  # noqa: E402
    array_agg,
    array_append,
    array_compact,
    array_prepend,
    array_size,
    arrays_overlap,
    cardinality,
    element_at,
    get,
    map_contains_key,
    map_from_entries,
    named_struct,
    shuffle,
    str_to_map,
)
from repark.spark.functions_datetime import (  # noqa: E402
    curdate,
    current_timezone,
    date_diff,
    date_from_unix_date,
    dateadd,
    datepart,
    day,
    make_date,
    make_dt_interval,
    make_interval,
    now,
    to_unix_timestamp,
    unix_date,
    unix_micros,
    unix_millis,
    unix_seconds,
)
from repark.spark.functions_expr import (  # noqa: E402
    acos,
    acosh,
    approx_count_distinct,
    approx_percentile,
    array,
    array_contains,
    array_distinct,
    array_except,
    array_intersect,
    array_join,
    array_max,
    array_min,
    array_position,
    array_remove,
    array_repeat,
    array_sort,
    array_union,
    arrays_zip,
    asc,
    asc_nulls_first,
    asc_nulls_last,
    ascii,
    asin,
    asinh,
    assert_true,
    atan,
    atan2,
    atanh,
    base64,
    bit_and,
    bit_length,
    bit_or,
    bit_xor,
    btrim,
    cbrt,
    ceil,
    ceiling,
    char,
    char_length,
    character_length,
    chr,
    concat_ws,
    contains,
    corr,
    cos,
    cosh,
    cot,
    covar_pop,
    covar_samp,
    crc32,
    csc,
    current_date,
    currentDate,
    date_part,
    date_sub,
    datediff,
    dayname,
    decode,
    degrees,
    desc,
    desc_nulls_first,
    desc_nulls_last,
    e,
    elt,
    encode,
    endswith,
    equal_null,
    exp,
    explode,
    explode_outer,
    expm1,
    extract,
    find_in_set,
    flatten,
    floor,
    format_number,
    format_string,
    from_csv,
    from_unixtime,
    from_utc_timestamp,
    from_xml,
    greatest,
    grouping,
    hash,
    hour,
    hypot,
    ifnull,
    ilike,
    initcap,
    input_file_name,
    instr,
    is_valid_utf8,
    isnan,
    isnotnull,
    isnull,
    json_tuple,
    kurtosis,
    lcase,
    least,
    left,
    length,
    levenshtein,
    like,
    listagg,
    ln,
    locate,
    log,
    log1p,
    log2,
    log10,
    lower,
    lpad,
    ltrim,
    make_timestamp,
    make_valid_utf8,
    map_entries,
    map_from_arrays,
    map_keys,
    map_values,
    md5,
    median,
    minute,
    mode,
    monotonically_increasing_id,
    monthname,
    months_between,
    nanvl,
    negate,
    negative,
    next_day,
    nullif,
    nullifzero,
    nvl,
    nvl2,
    octet_length,
    overlay,
    percentile_approx,
    pi,
    pmod,
    posexplode,
    posexplode_outer,
    position,
    positive,
    pow,
    power,
    printf,
    quote,
    radians,
    raise_error,
    rand,
    randn,
    random,
    randstr,
    regexp,
    regexp_count,
    regexp_extract,
    regexp_extract_all,
    regexp_instr,
    regexp_like,
    regexp_replace,
    regexp_substr,
    regr_avgx,
    regr_avgy,
    regr_count,
    regr_intercept,
    regr_r2,
    regr_slope,
    regr_sxx,
    regr_sxy,
    regr_syy,
    repeat,
    replace,
    reverse,
    right,
    rlike,
    round,
    rpad,
    rtrim,
    schema_of_csv,
    schema_of_json,
    schema_of_xml,
    sec,
    second,
    sentences,
    sequence,
    sha,
    sha1,
    sha2,
    sign,
    signum,
    sin,
    sinh,
    size,
    skewness,
    slice,
    sort_array,
    soundex,
    spark_partition_id,
    split,
    split_part,
    sqrt,
    startswith,
    stddev,
    stddev_pop,
    stddev_samp,
    string_agg,
    struct,
    substr,
    substring,
    substring_index,
    tan,
    tanh,
    timestamp_micros,
    timestamp_millis,
    timestamp_seconds,
    to_date,
    to_timestamp,
    to_utc_timestamp,
    translate,
    trim,
    try_to_timestamp,
    try_validate_utf8,
    ucase,
    unbase64,
    uniform,
    unix_timestamp,
    upper,
    validate_utf8,
    var_pop,
    var_samp,
    variance,
    when,
    xxhash64,
    zeroifnull,
)
from repark.spark.functions_lambda import (  # noqa: E402
    exists,
)
from repark.spark.functions_math import (  # noqa: E402
    bin,
    factorial,
    hex,
    rint,
    unhex,
    width_bucket,
)
from repark.spark.functions_session import (  # noqa: E402
    broadcast,
    current_catalog,
    current_database,
    current_schema,
    current_user,
    session_user,
    user,
    uuid,
    version,
)
from repark.spark.functions_udf import (  # noqa: E402, F401 — re-export surface
    PandasUDFColumn,
    PandasUDFFunction,
    PandasUDFType,
    PythonUDFColumn,
    UserDefinedFunction,
    _build_pandas_udf,
    _build_python_udf,
    _is_pandas_udf_datatype_like,
    _is_pandas_udf_function_type,
    _is_python_udf_datatype_like,
    _normalize_pandas_udf_function_type,
    _normalize_pandas_udf_return_type_sql,
    _normalize_python_udf_return_type_sql,
    _pandas_udf_arrow_type_for_return,
    _pandas_udf_refuse_fail_open_string_leaves,
    _python_udf_arrow_type_for_return,
    _refuse_udtf_as_scalar_udf,
    pandas_udf,
    udf,
)
from repark.spark.functions_url import (  # noqa: E402
    parse_url,
    try_parse_url,
    try_url_decode,
    url_decode,
    url_encode,
)
from repark.spark.functions_window import (  # noqa: E402
    cume_dist,
    lag,
    lead,
    nth_value,
    percent_rank,
)

__all__ = [
    "PandasUDFType",
    "PythonUDFColumn",
    "UserDefinedFunction",
    "UserDefinedTableFunction",
    "abs",
    "acos",
    "acosh",
    "add_months",
    "approx_count_distinct",
    "approx_percentile",
    "array",
    "array_agg",
    "array_append",
    "array_compact",
    "array_contains",
    "array_distinct",
    "array_except",
    "array_intersect",
    "array_join",
    "array_max",
    "array_min",
    "array_position",
    "array_prepend",
    "array_remove",
    "array_repeat",
    "array_size",
    "array_sort",
    "array_union",
    "arrays_overlap",
    "arrays_zip",
    "asc",
    "asc_nulls_first",
    "asc_nulls_last",
    "ascii",
    "asin",
    "asinh",
    "assert_true",
    "atan",
    "atan2",
    "atanh",
    "avg",
    "base64",
    "bin",
    "bit_and",
    "bit_count",
    "bit_get",
    "bit_length",
    "bit_or",
    "bit_xor",
    "bitmap_bit_position",
    "bitmap_bucket_number",
    "bitmap_count",
    "bitwiseNOT",
    "bitwise_not",
    "bool_and",
    "bool_or",
    "broadcast",
    "btrim",
    "bucket",
    "cardinality",
    "cbrt",
    "ceil",
    "ceiling",
    "char",
    "char_length",
    "character_length",
    "chr",
    "coalesce",
    "col",
    "collect_list",
    "collect_set",
    "column",
    "concat",
    "concat_ws",
    "contains",
    "corr",
    "cos",
    "cosh",
    "cot",
    "count",
    "countDistinct",
    "count_distinct",
    "count_if",
    "covar_pop",
    "covar_samp",
    "crc32",
    "csc",
    "cume_dist",
    "curdate",
    "currentDate",
    "currentTimestamp",
    "current_catalog",
    "current_database",
    "current_date",
    "current_schema",
    "current_timestamp",
    "current_timezone",
    "current_user",
    "date_add",
    "date_diff",
    "date_format",
    "date_from_unix_date",
    "date_part",
    "date_sub",
    "date_trunc",
    "dateadd",
    "datediff",
    "datepart",
    "day",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "days",
    "decode",
    "degrees",
    "dense_rank",
    "desc",
    "desc_nulls_first",
    "desc_nulls_last",
    "e",
    "element_at",
    "elt",
    "encode",
    "endswith",
    "equal_null",
    "every",
    "exists",
    "exp",
    "explode",
    "explode_outer",
    "expm1",
    "expr",
    "extract",
    "factorial",
    "find_in_set",
    "first",
    "first_value",
    "flatten",
    "floor",
    "format_number",
    "format_string",
    "from_csv",
    "from_unixtime",
    "from_utc_timestamp",
    "from_xml",
    "get",
    "getbit",
    "greatest",
    "grouping",
    "hash",
    "hex",
    "hour",
    "hours",
    "hypot",
    "ifnull",
    "ilike",
    "initcap",
    "input_file_name",
    "instr",
    "is_valid_utf8",
    "isnan",
    "isnotnull",
    "isnull",
    "json_tuple",
    "kurtosis",
    "lag",
    "last",
    "last_day",
    "last_value",
    "lcase",
    "lead",
    "least",
    "left",
    "length",
    "levenshtein",
    "like",
    "listagg",
    "lit",
    "ln",
    "locate",
    "log",
    "log1p",
    "log2",
    "log10",
    "lower",
    "lpad",
    "ltrim",
    "make_date",
    "make_dt_interval",
    "make_interval",
    "make_timestamp",
    "make_valid_utf8",
    "map_contains_key",
    "map_entries",
    "map_from_arrays",
    "map_from_entries",
    "map_keys",
    "map_values",
    "max",
    "md5",
    "mean",
    "median",
    "min",
    "minute",
    "mode",
    "monotonically_increasing_id",
    "month",
    "monthname",
    "months",
    "months_between",
    "named_struct",
    "nanvl",
    "negate",
    "negative",
    "next_day",
    "now",
    "nth_value",
    "ntile",
    "nullif",
    "nullifzero",
    "nvl",
    "nvl2",
    "octet_length",
    "overlay",
    "pandas_udf",
    "parse_url",
    "percent_rank",
    "percentile_approx",
    "pi",
    "pmod",
    "posexplode",
    "posexplode_outer",
    "position",
    "positive",
    "pow",
    "power",
    "printf",
    "quarter",
    "quote",
    "radians",
    "raise_error",
    "rand",
    "randn",
    "random",
    "randstr",
    "rank",
    "regexp",
    "regexp_count",
    "regexp_extract",
    "regexp_extract_all",
    "regexp_instr",
    "regexp_like",
    "regexp_replace",
    "regexp_substr",
    "regr_avgx",
    "regr_avgy",
    "regr_count",
    "regr_intercept",
    "regr_r2",
    "regr_slope",
    "regr_sxx",
    "regr_sxy",
    "regr_syy",
    "repeat",
    "replace",
    "reverse",
    "right",
    "rint",
    "rlike",
    "round",
    "row_number",
    "rpad",
    "rtrim",
    "schema_of_csv",
    "schema_of_json",
    "schema_of_xml",
    "sec",
    "second",
    "sentences",
    "sequence",
    "session_user",
    "sha",
    "sha1",
    "sha2",
    "shiftleft",
    "shiftright",
    "shiftrightunsigned",
    "shuffle",
    "sign",
    "signum",
    "sin",
    "sinh",
    "size",
    "skewness",
    "slice",
    "some",
    "sort_array",
    "soundex",
    "spark_partition_id",
    "split",
    "split_part",
    "sqrt",
    "startswith",
    "std",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "str_to_map",
    "string_agg",
    "struct",
    "substr",
    "substring",
    "substring_index",
    "sum",
    "tan",
    "tanh",
    "timestamp_micros",
    "timestamp_millis",
    "timestamp_seconds",
    "to_date",
    "to_timestamp",
    "to_unix_timestamp",
    "to_utc_timestamp",
    "translate",
    "trim",
    "trunc",
    "try_parse_url",
    "try_to_timestamp",
    "try_url_decode",
    "try_validate_utf8",
    "ucase",
    "udf",
    "udtf",
    "unbase64",
    "unhex",
    "uniform",
    "unix_date",
    "unix_micros",
    "unix_millis",
    "unix_seconds",
    "unix_timestamp",
    "upper",
    "url_decode",
    "url_encode",
    "user",
    "uuid",
    "validate_utf8",
    "var_pop",
    "var_samp",
    "variance",
    "version",
    "weekday",
    "weekofyear",
    "when",
    "width_bucket",
    "xxhash64",
    "year",
    "years",
    "zeroifnull",
]
from repark.spark import functions_lambda as _functions_lambda  # noqa: E402
from repark.spark.functions_declared import install_into as _install_declared_refuse  # noqa: E402

_install_declared_refuse(globals(), __all__) or _functions_lambda.install_into(globals(), __all__)
