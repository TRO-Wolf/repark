"""The :mod:`repark.functions` facade — PySpark's ``pyspark.sql.functions`` surface.

Each function returns a :class:`repark.column.Column` backed by a native DataFusion expression.
PySpark scripts import these as ``from repark.functions import col, lit, coalesce`` or,
idiomatically, ``import repark.functions as F`` then ``F.col(...)`` — the one-line import swap from
``pyspark.sql.functions``. This is WG1's in-use set; the date/window functions land in WG2.
"""

from __future__ import annotations

import datetime
import enum
import math
from collections.abc import Callable
from typing import Any

from repark import _native

# === r23 QI1: idents ===
from repark._idents import quote_column_sql_expr as _quote_column_sql_expr
from repark.column import Column, Scalar
from repark.errors import PySparkTypeError, PySparkValueError, UnsupportedOperationException
from repark.udtf import UserDefinedTableFunction, udtf


def col(name: str) -> Column:
    """A column reference by name (PySpark ``functions.col``).

    Bare attributes are Spark ``NamedExpression``s: a plain ``.cast(...)`` keeps this name
    in ``DataFrame.select`` (Group H; live PySpark 4.1.2). Structural ``sql_expr`` is a
    double-quoted identifier so free-SQL surfaces cannot retarget FROM via hostile names
    (octo C3-SEC-001). Qualified names (``source.col``) quote each segment.
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
    beside aggregates — octo C1-Q-002), not ``[MISSING_GROUP_BY]``.
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
    # numpy.ndarray → typed array (E2 hand-off from E1; Apache test_*_ndarray*).
    ndarray_column = _lit_numpy_ndarray(value)
    if ndarray_column is not None:
        return ndarray_column
    # Spark ``lit([1,2])`` builds an array column via engine ``make_array`` (X1 census).
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


def _coerce_lit_list_mixed_to_string(values: list[Any]) -> list[Any]:
    """Coerce mixed-type ``lit([...])`` elements (Spark non-ANSI + numeric promotion).

    Homogeneous non-null types (all int, all float, all str, all bool, nested lists of a
    single kind) pass through. Compatible numerics (int+float only) promote to float so
    ``lit([1, 1.0])`` stays a numeric array — not a faked string cast (octo C1-Q-004).
    Numpy integer/float scalars count as int/float (octo C4-Q-001) — ``lit([np.int64(1), 2])``
    must not fake-string. Incompatible mixes — e.g. ``["a", 1, None, 1.0]`` — every non-None
    leaf becomes ``str(...)`` matching live Spark 4.1.2. Nested lists are walked the same way
    (Apache ``test_lit_list`` jagged mixed arrays — F2).
    """
    import numbers

    def kind(item: Any) -> str | None:
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

    kinds = {kind(item) for item in values if item is not None}
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
        # (octo C5 — C4 kind widen left np.int64 in homogeneous lists).
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


# numpy dtype name → Spark array element simpleString (Apache test_ndarray_input / empty).
# object / |S (bytes) are intentionally absent — Spark 4.1.2 raises
# UNSUPPORTED_NUMPY_ARRAY_SCALAR (octo C6-Q-001); do not map them to string.
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
    # Unsigned / object / bytes: Spark refuses with UNSUPPORTED_NUMPY_ARRAY_SCALAR
    # (C6-Q-001 — no fail-open array<string> for object or |S).
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
    constants (combine octo C6-SAF-002). Use CAST string forms DataFusion accepts.
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
        escaped = value.replace("'", "''")
        return f"'{escaped}'"
    return str(value)


def _lit_spark_display(value: Scalar) -> str:
    """PySpark-style literal fragment for display/agg names (not DataFusion's ``Int64(1)``).

    Live PySpark 4.1.2 renders string literals **without** surrounding quotes in both projection
    names (``df.select(F.lit("s")).columns == ['s']``) and aggregate embeds
    (``first(z)``, ``concat(s, z)``). Integer/float/bool/NULL match the prior Group F matrix.
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
    """First non-None Group I partition-transform marker among ``columns`` (sticky carry)."""
    for column in columns:
        transform = getattr(column, "_partition_transform", None)
        if transform is not None:
            return transform
    return None


def _thread_origin(*columns: Column) -> dict[str, str | None]:
    """Copy H1 origin tokens from the first origin-bearing argument.

    Wrappers that build a fresh :class:`Column` must pass these through;
    otherwise ``F.abs(right["k"])`` after a semi join silently binds the left
    column (Y-5 SAF-001).
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
    # Generators inside coalesce drop ``_generator`` and skip unnest (octo C5-Q-001 / C5-L-001).
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
    # Generators inside concat drop ``_generator`` and skip unnest (octo C5-Q-001 / C5-L-001).
    for column in columns:
        column._reject_nested_generator("concat")
    parts = ", ".join(column.spark_wrap_display_part() for column in columns)
    sql_parts = ", ".join(column.sql_expr_part() for column in columns)
    display = f"concat({parts})"
    # Spark null propagation: any NULL arg → NULL (not DF empty-string skip).
    # MERGE embeds sql_expr, so match the native CASE guard (octo-extra C5-Q-001).
    null_guard = " OR ".join(f"({column.sql_expr_part()} IS NULL)" for column in columns)
    sql_expr = f"CASE WHEN {null_guard} THEN CAST(NULL AS VARCHAR) ELSE concat({sql_parts}) END"
    # Sticky aggregate / free-attr (octo C2-Q-002 / C2-L-002): concat(sum(x), lit) is global-agg.
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
    is a global aggregate (Spark allows no-arg non-attribute companions — octo C3-Q-002),
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


# PySpark also exposes the camelCase spelling ``currentTimestamp``; keep both so the import swap
# just works either way.
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
    ``lit``; a string becomes ``col(name)`` (octo C3).
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


# ---- aggregate functions (Group E) --------------------------------------------------------------
#
# These deliberately shadow the Python builtins ``sum``/``min``/``max`` (and add ``count``) exactly
# as ``pyspark.sql.functions`` does — a migrating ``F.sum(...)`` must resolve here, not to the
# builtin. The returned :class:`Column` carries its PySpark default output name in ``_agg_name``
# (``sum(x)``, ``count(1)``, …), which :meth:`repark.dataframe.GroupedData.agg` applies as the
# aliased output column name. Every name below was verified against real PySpark 4.1.2.


def _aggregate_argument(col: Column | str) -> tuple[Column, str]:
    """Coerce an aggregate argument (a column name or :class:`Column`) to ``(Column, name_part)``.

    The ``name_part`` is the token PySpark embeds in the output name — the column name itself for a
    string argument, or the facade's PySpark-style display for a :class:`Column`
    (``col("x") + 1`` → ``(x + 1)``; never DataFusion's ``x + Int64(1)``).

    String arguments carry a quoted structural ``sql_expr`` so aggregate builders embed safe
    identifiers into free-SQL global-agg select (octo C3-SEC-001 / C3-Q-001).
    Generator arguments are refused loud (``UNSUPPORTED_GENERATOR``) — wrapping
    ``F.count(F.explode(...))`` / ``F.sum`` / ``collect_*`` would strip ``_generator`` and
    aggregate the array placeholder instead of elements (octo C6-Q-001; Spark parity).
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
    # H2: wrap-display collapses ``.alias("v")`` so abs shows ``abs(v)`` not ``abs(… AS v)``.
    display = f"abs({column.spark_wrap_display_part()})"
    # Sticky identity for select global-agg (octo C2-Q-002 / C2-L-002): abs(sum(x)).
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
    select never falls back to DataFusion ``schema_name`` / ``Int64`` display (octo C3-Q-001).
    """
    column, part = _aggregate_argument(col)
    agg_name = f"sum({part})"
    return Column(
        column._inner.aggregate("sum", False),
        agg_name=agg_name,
        sql_expr=f"sum({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def count(col: Column | str) -> Column:
    """Row count (PySpark ``functions.count``).

    ``count("*")`` (or ``count(lit(1))``) counts every row; ``count(col)`` counts non-NULL values
    of ``col``. The result is a non-nullable ``LongType``.
    """
    # Star forms: bare ``"*"``, ``col("*")``, and ``df["*"]`` all mean count-all (X3 census).
    if isinstance(col, str) and col == "*":
        # Structural SQL for free-SQL surfaces (global-agg select, CUBE): never emit
        # native ``count(Int64(1))`` and never rewrite it via substring replace (octo C2-SAF-001).
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
        # Quoted identifier from _aggregate_argument / Column.sql_expr (octo C3-SEC-001).
        sql_expr=f"count({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
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
    # global-agg select matches native/Spark (octo C5-L-001). Single-col stays plain.
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
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=_partition_transform_of(*(column for column, _ in columns)),
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
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
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
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def max(col: Column | str) -> Column:
    """Maximum of a group, skipping NULLs; preserves the input type (PySpark ``functions.max``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"max({part})"
    return Column(
        column._inner.aggregate("max", False),
        agg_name=agg_name,
        sql_expr=f"max({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
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
    # the native aggregate (octo C4-L-001).
    base_sql = f"first_value({column.sql_expr_part()})"
    sql_expr = f"{base_sql} IGNORE NULLS" if ignorenulls else base_sql
    return Column(
        column._inner.aggregate("first", ignorenulls),
        agg_name=agg_name,
        sql_expr=sql_expr,
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
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
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def collect_list(col: Column | str) -> Column:
    """Collect non-NULL values of a group into an array (PySpark ``functions.collect_list``).

    NULL elements are excluded (live PySpark 4.1.2). An empty group (or a group whose values are
    all NULL) yields an empty array, not NULL. Element order is **nondeterministic** — pin tests
    with sorted contents or single-element groups. Output name ``collect_list(x)``.
    """
    column, part = _aggregate_argument(col)
    agg_name = f"collect_list({part})"
    # Match native collect_aggregate: IGNORE NULLS + coalesce empty → [] (octo C4-L-002).
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
    # coalesce restores ``[]`` (octo C4-L-002 / C5-Q-002).
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
    # spark_display set so Column.over can refuse missing ORDER BY (r20 G2 octo C2).
    return Column(
        _native.PyColumn.row_number(),
        spark_display="row_number()",
        projection_name="row_number()",
        sql_expr="row_number()",
        is_aggregate=False,
        is_foldable=False,
        has_ungroupable=True,
    )


# === r20 G2: window/rand/sampleBy ===
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
    # Generators inside year/month/… strip ``_generator`` and skip unnest (octo C7-Q-001).
    argument._reject_nested_generator(f"function {display_name}")
    display = f"{display_name}({argument.spark_wrap_display_part()})"
    native = getattr(argument._inner, method_name)()
    # Sticky aggregate / free-attr for select routing (octo C2-Q-002).
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
    # Generators strip ``_generator`` and skip unnest (octo C7-Q-001; Spark UNSUPPORTED_GENERATOR).
    start_column._reject_nested_generator("function add_months")
    months_column._reject_nested_generator("function add_months")
    display = (
        f"add_months({start_column.spark_wrap_display_part()}, "
        f"{months_column.spark_wrap_display_part()})"
    )
    # Sticky free/aggregate so ``select(sum, add_months(d,1))`` is MISSING_GROUP_BY and
    # ``add_months(max(d),1)`` stays global-agg (octo C4-L-003).
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
    # Generators strip ``_generator`` and skip unnest (octo C7-Q-001; Spark UNSUPPORTED_GENERATOR).
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
    # Generators strip ``_generator`` and skip unnest (octo C7-Q-001; Spark UNSUPPORTED_GENERATOR).
    argument._reject_nested_generator("function date_format")
    # Live PySpark 4.1.2: date_format(d, yyyy-MM) — pattern is unquoted in the name.
    display = f"date_format({argument.spark_wrap_display_part()}, {format})"
    # Escape single quotes in the pattern for structural SQL embeds.
    escaped = format.replace("'", "''")
    return Column(
        argument._inner.date_format(format),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=f"date_format({argument.sql_expr_part()}, '{escaped}')",
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
    # Generators strip ``_generator`` and skip unnest (octo C7-Q-001; Spark UNSUPPORTED_GENERATOR).
    argument._reject_nested_generator("function trunc")
    display = f"trunc({argument.spark_wrap_display_part()}, {format})"
    escaped = format.replace("'", "''")
    return Column(
        argument._inner.trunc(format),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=f"trunc({argument.sql_expr_part()}, '{escaped}')",
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
    # Generators strip ``_generator`` and skip unnest (octo C7-Q-001; Spark UNSUPPORTED_GENERATOR).
    argument._reject_nested_generator("function date_trunc")
    display = f"date_trunc({format}, {argument.spark_wrap_display_part()})"
    escaped = format.replace("'", "''")
    return Column(
        argument._inner.date_trunc(format),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=f"date_trunc('{escaped}', {argument.sql_expr_part()})",
        is_aggregate=argument._is_aggregate,
        is_foldable=argument._is_foldable and not argument._is_aggregate,
        has_free_attribute=argument._has_free_attribute,
        has_ungroupable=argument._has_ungroupable,
        partition_transform=argument._partition_transform,
    )


# ---- partition transforms (Group I — valid ONLY inside DataFrameWriterV2.partitionedBy) ----------


def _partition_transform(transform: str, col: Column | str) -> Column:
    """Build a partition-transform Column valid only inside ``writeTo(...).partitionedBy(...)``.

    PySpark rejects these outside ``partitionedBy``
    (``PARTITION_TRANSFORM_EXPRESSION_NOT_IN_PARTITIONED_BY``). repark marks the Column with
    ``_partition_transform`` so :class:`~repark.dataframe.DataFrameWriterV2` can render
    ``years(col)`` / ``months(col)`` / ``bucket(n, col)`` / … into CTAS ``PARTITIONED BY``; any
    other use raises :class:`~repark.errors.AnalysisException`. The engine's CTAS path builds the
    real Iceberg ``Transform`` (bucket/truncate/year/month/day/hour) and computes each partition
    value from the source column via the iceberg-rust fork, so these transforms work end-to-end
    (Group P) — the former "engine rejects non-identity transforms" disclosure is retired.

    The identity argument is double-quoted via :func:`repark._idents.quote_ident` so reserved
    words / hostile text cannot break out of the transform call (C3-SEC-001 residual of
    C1-SEC-001, which already quoted bare identity partitions).
    """
    from repark._idents import quote_ident as _quote_ident

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
    is double-quoted, C3-SEC-001). ``numBuckets`` must be a positive ``int`` (or Column); the engine
    also rejects ``<= 0`` loudly at parse time (Spark/Iceberg analysis-error parity).

    E1: bad ``numBuckets`` type raises ``PySparkTypeError`` with ``NOT_COLUMN_OR_INT`` so Apache
    ``check_error`` class + parameter-key equality PASSes.
    """
    from repark._idents import quote_ident as _quote_ident

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


# =============================================================================
# U7 — scalar pandas_udf (decorator/export only; bridge lives in dataframe.py)
# =============================================================================


class PandasUDFType:
    """Eval-type tags mirroring ``pyspark.sql.functions.PandasUDFType`` (int values).

    Values match PySpark 4.1.2: SCALAR=200, GROUPED_MAP=201, GROUPED_AGG=202,
    SCALAR_ITER=204. repark implements **SCALAR**, **SCALAR_ITER**, and **GROUPED_AGG**
    (M5/M6). **GROUPED_MAP** remains loud-unsupported. Window form is **GROUPED_AGG**
    + :meth:`PandasUDFColumn.over` over ``Window.partitionBy`` (unbounded whole-partition
    only — M6); ``functionType=WINDOW`` stays a loud refuse (use GROUPED_AGG + ``.over``).
    """

    SCALAR = 200
    GROUPED_MAP = 201
    GROUPED_AGG = 202
    SCALAR_ITER = 204


def _is_pandas_udf_datatype_like(value: Any) -> bool:
    """True when ``value`` is a returnType (str DDL or :class:`~repark.types.DataType`)."""
    if isinstance(value, str):
        return True
    from repark.types import DataType

    return isinstance(value, DataType)


def _is_pandas_udf_function_type(value: Any) -> bool:
    """True when ``value`` looks like a PandasUDFType / eval-type tag (not a returnType)."""
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return value in {
            PandasUDFType.SCALAR,
            PandasUDFType.SCALAR_ITER,
            PandasUDFType.GROUPED_MAP,
            PandasUDFType.GROUPED_AGG,
        }
    if isinstance(value, str):
        # Include WINDOW so positional ``@pandas_udf("long", "WINDOW")`` routes to the
        # FT-first / normalize UOE (M6 seed) instead of the dual-returnType refuse
        # (octo M5 C4 — window pandas_udf HARD OUT honesty).
        return value.upper() in {
            "SCALAR",
            "SCALAR_ITER",
            "GROUPED_MAP",
            "GROUPED_AGG",
            "GROUPED_AGGREGATE",
            "WINDOW",
        }
    return False


def _pandas_udf_refuse_fail_open_string_leaves(data_type: Any, arrow_type: Any) -> None:
    """Refuse DataType leaves that map to Arrow string without being string-like.

    Top-level and nested (array/map/struct-in-array) variant / interval / time markers
    fail-open to ``pa.string()`` via ``repark_type_to_arrow`` — walk leaves so
    ``array<variant>`` / ``map<string,time>`` / ``array<struct<a:variant>>`` cannot
    silently declare string payloads (octo C1-SEC-001 top-level; C2-SEC-001 nested).
    """
    import pyarrow as pa

    from repark.types import (
        ArrayType,
        CharType,
        MapType,
        StringType,
        StructType,
        VarcharType,
    )

    if isinstance(data_type, ArrayType):
        if (
            pa.types.is_list(arrow_type)
            or pa.types.is_large_list(arrow_type)
            or pa.types.is_fixed_size_list(arrow_type)
        ):
            _pandas_udf_refuse_fail_open_string_leaves(data_type.elementType, arrow_type.value_type)
            return
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} does not map to a concrete "
            "Arrow list type in repark v1 (refusing silent string fallback)."
        )
    if isinstance(data_type, MapType):
        if pa.types.is_map(arrow_type):
            _pandas_udf_refuse_fail_open_string_leaves(data_type.keyType, arrow_type.key_type)
            _pandas_udf_refuse_fail_open_string_leaves(data_type.valueType, arrow_type.item_type)
            return
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} does not map to a concrete "
            "Arrow map type in repark v1 (refusing silent string fallback)."
        )
    if isinstance(data_type, StructType):
        if pa.types.is_struct(arrow_type):
            arrow_fields = list(arrow_type)
            if len(arrow_fields) != len(data_type.fields):
                raise PySparkTypeError(
                    f"pandas_udf returnType {data_type.simpleString()!r} struct field count "
                    "mismatch after Arrow mapping."
                )
            for field, arrow_field in zip(data_type.fields, arrow_fields, strict=True):
                _pandas_udf_refuse_fail_open_string_leaves(field.dataType, arrow_field.type)
            return
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} does not map to a concrete "
            "Arrow struct type in repark v1 (refusing silent string fallback)."
        )
    # Leaf: non-string declaration that collapsed to Arrow string.
    if pa.types.is_string(arrow_type) and not isinstance(
        data_type, (StringType, CharType, VarcharType)
    ):
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} does not map to a concrete "
            "Arrow scalar type in repark v1 (refusing silent string fallback). Use a supported "
            "scalar type (boolean/byte/short/int/long/float/double/string/binary/date/"
            "timestamp/decimal/array/…)."
        )


def _pandas_udf_arrow_type_for_return(data_type: Any) -> Any:
    """Map a scalar return :class:`~repark.types.DataType` to Arrow, refusing fail-open string.

    ``repark_type_to_arrow`` / ``_sql_type_to_arrow`` map unknown markers (variant / interval /
    time / …) to ``pa.string()``. Scalar pandas_udf must not silently declare string when the
    user asked for another type (octo C1-SEC-001 top-level; C2-SEC-001 nested leaves).
    """
    from repark.session import _data_type_to_sql_type, _sql_type_to_arrow
    from repark.types import (
        DataType,
        StructType,
    )

    if not isinstance(data_type, DataType):
        raise PySparkTypeError(
            "pandas_udf returnType must be a DataType or DDL type string, "
            f"got {type(data_type).__name__}"
        )
    if isinstance(data_type, StructType):
        raise UnsupportedOperationException(
            "pandas_udf StructType / struct returnType is not supported in repark v1 "
            "(scalar only). Grouped-map pandas_udf is an M5-class seed."
        )
    try:
        sql_type = _data_type_to_sql_type(data_type)
    except Exception as error:
        raise PySparkTypeError(
            f"pandas_udf returnType {data_type.simpleString()!r} is not a supported "
            f"scalar type: {error}"
        ) from error
    arrow_type = _sql_type_to_arrow(sql_type)
    # Top-level and nested leaves (array/map/struct-in-array of variant|interval|time).
    _pandas_udf_refuse_fail_open_string_leaves(data_type, arrow_type)
    return arrow_type


def _normalize_pandas_udf_return_type_sql(return_type: Any) -> str:
    """Lower ``returnType`` to a logical DDL fragment that preserves Spark type identity.

    Stores :meth:`~repark.types.DataType.simpleString` (not ``_data_type_to_sql_type``), so
    ``timestamp_ntz`` / ``varchar(n)`` / ``char(n)`` survive round-trip through the bridge
    ``DataType.fromDDL`` → :attr:`DataFrame.schema` (octo C4-Q-001). Engine cast tokens
    (``TIMESTAMP`` / ``STRING``) collapse those distinctions and must not be stored.

    Parses string DDL fully so field-list forms (``a int, b string`` / ``a: int``) are
    detected as :class:`~repark.types.StructType` and refused (octo C1-SEC-002), not only
    ``struct…`` prefixes. Unsupported markers that would fail-open to Arrow string are
    refused (octo C1-SEC-001). Arrow physical mapping still uses ``_data_type_to_sql_type``
    inside :func:`_pandas_udf_arrow_type_for_return` at validation / bridge time.
    """
    from repark.types import DataType, StructType

    if isinstance(return_type, str):
        text = return_type.strip()
        if not text:
            raise PySparkTypeError("pandas_udf returnType must be a non-empty type string")
        # Prefix refuse keeps the historical M5 message for explicit struct spellings.
        if text.lower().startswith("struct"):
            raise UnsupportedOperationException(
                "pandas_udf StructType / struct returnType is not supported in repark v1 "
                "(scalar only). Grouped-map pandas_udf is an M5-class seed."
            )
        try:
            parsed = DataType.fromDDL(text)
        except Exception as error:
            raise PySparkTypeError(
                f"pandas_udf returnType {text!r} is not a valid type: {error}"
            ) from error
        # Field-list DDL (``a int, b string`` / ``a: int``) parses as StructType without a
        # ``struct`` prefix — refuse the same as StructType objects (octo C1-SEC-002).
        if isinstance(parsed, StructType):
            raise UnsupportedOperationException(
                "pandas_udf StructType / struct returnType is not supported in repark v1 "
                "(scalar only). Grouped-map pandas_udf is an M5-class seed."
            )
        _pandas_udf_arrow_type_for_return(parsed)
        # logical simpleString — not engine SQL (TIMESTAMP/STRING collapse NTZ/varchar/char).
        return parsed.simpleString()
    if isinstance(return_type, StructType):
        raise UnsupportedOperationException(
            "pandas_udf StructType returnType is not supported in repark v1 "
            "(scalar only). Grouped-map pandas_udf is an M5-class seed."
        )
    if isinstance(return_type, DataType):
        _pandas_udf_arrow_type_for_return(return_type)
        return return_type.simpleString()
    raise PySparkTypeError(
        "pandas_udf returnType must be a DataType or DDL type string, "
        f"got {type(return_type).__name__}"
    )


def _normalize_pandas_udf_function_type(function_type: Any) -> int:
    """Accept SCALAR / SCALAR_ITER / GROUPED_AGG; GROUPED_MAP + window are loud seeds."""
    if function_type is None:
        return PandasUDFType.SCALAR
    if isinstance(function_type, str):
        key = function_type.upper()
        if key == "SCALAR":
            return PandasUDFType.SCALAR
        if key == "SCALAR_ITER":
            return PandasUDFType.SCALAR_ITER
        if key in {"GROUPED_AGG", "GROUPED_AGGREGATE"}:
            return PandasUDFType.GROUPED_AGG
        if key in {"GROUPED_MAP", "WINDOW"}:
            raise UnsupportedOperationException(
                f"pandas_udf functionType={function_type!r} is not supported in repark v1 "
                "(GROUPED_MAP / window pandas_udf are M6-class seeds). "
                "Supported: SCALAR, SCALAR_ITER, GROUPED_AGG."
            )
        raise PySparkTypeError(f"unknown pandas_udf functionType {function_type!r}")
    if isinstance(function_type, int) and not isinstance(function_type, bool):
        if function_type == PandasUDFType.SCALAR:
            return PandasUDFType.SCALAR
        if function_type == PandasUDFType.SCALAR_ITER:
            return PandasUDFType.SCALAR_ITER
        if function_type == PandasUDFType.GROUPED_AGG:
            return PandasUDFType.GROUPED_AGG
        if function_type == PandasUDFType.GROUPED_MAP:
            raise UnsupportedOperationException(
                f"pandas_udf functionType={function_type!r} is not supported in repark v1 "
                "(GROUPED_MAP / window pandas_udf are M6-class seeds). "
                "Supported: SCALAR, SCALAR_ITER, GROUPED_AGG."
            )
        raise PySparkTypeError(f"unknown pandas_udf functionType {function_type!r}")
    raise PySparkTypeError(
        f"pandas_udf functionType must be int or str, got {type(function_type).__name__}"
    )


class PandasUDFColumn:
    """Marker for a ``pandas_udf`` projection / agg (not a SQL-plan :class:`Column`).

    Produced by calling a :func:`pandas_udf`-decorated function with column arguments.

    * **SCALAR / SCALAR_ITER** — top-level ``select`` / ``withColumn`` only (U7/M5 bridge).
    * **GROUPED_AGG** — ``groupBy(...).agg(...)`` (M5 pure / M6 mixed with builtins).
    * **Windowed GROUPED_AGG** — ``.over(Window.partitionBy(...))`` unbounded whole-partition
      (M6); M7 adds ``orderBy`` with default frame
      ``ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW``, plus duck-typed
      ``_frame_start`` / ``_frame_end`` when G2 lands ``rowsBetween`` plumbing.

    Mid-expression composition (``udf_col + 1``, ``udf_col > 0`` in filter, nesting under
    ``coalesce``) is refused — the result is a mapInArrow / applyInPandas bridge-node
    output, same composition limit class as ``mapInArrow`` (U7 v1).
    """

    __slots__ = (
        "_alias_name",
        "_function_name",
        "_function_type",
        "_inputs",
        "_return_type_sql",
        "_user_func",
        "_window_spec",
    )

    def __init__(
        self,
        user_func: Callable[..., Any],
        return_type_sql: str,
        inputs: list[Column],
        function_name: str,
        *,
        alias_name: str | None = None,
        function_type: int = PandasUDFType.SCALAR,
        window_spec: Any | None = None,
    ) -> None:
        """Bind the user function, declared return type, eval type, and input Columns.

        ``return_type_sql`` is re-normalized here so a hostile public constructor call
        (or a hand-built marker) cannot skip decorator validation and fail-open to
        Arrow string via ``_sql_type_to_arrow`` (octo C3-SEC-001).
        """
        self._user_func = user_func
        # Revalidate every construction path — not only ``@pandas_udf`` (octo C3-SEC-001).
        self._return_type_sql = _normalize_pandas_udf_return_type_sql(return_type_sql)
        self._inputs = list(inputs)
        self._function_name = function_name
        self._alias_name = alias_name
        self._function_type = _normalize_pandas_udf_function_type(function_type)
        self._window_spec = window_spec

    def alias(self, name: str) -> PandasUDFColumn:
        """Set the output column name (PySpark ``Column.alias`` parity for UDF results)."""
        if not isinstance(name, str) or name.strip() == "":
            raise PySparkTypeError("pandas_udf alias name must be a non-empty str")
        return PandasUDFColumn(
            self._user_func,
            self._return_type_sql,
            self._inputs,
            self._function_name,
            alias_name=name,
            function_type=self._function_type,
            window_spec=self._window_spec,
        )

    def over(self, window: Any) -> PandasUDFColumn:
        """Attach a window for GROUPED_AGG (M6 unbounded / M7 ordered frames).

        Accepted :class:`~repark.window.WindowSpec` forms:

        * ``Window.partitionBy(...)`` only — unbounded whole-partition (M6).
        * ``Window.partitionBy(...).orderBy(...)`` — default frame
          ``ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`` (M7 engine frames).
        * Same with duck-typed ``_frame_start`` / ``_frame_end`` int offsets when G2
          lands ``rowsBetween`` on :class:`~repark.window.WindowSpec` (M7; G2 owns
          the facade methods — this path does not edit ``window.py``).

        Requires ``functionType=GROUPED_AGG``. Scalar / SCALAR_ITER refuse.
        """
        from repark.errors import AnalysisException
        from repark.window import WindowSpec

        # === r20 M7: pandas_udf-over-frames (not G2 window.py facade) ===
        if not isinstance(window, WindowSpec):
            raise PySparkTypeError(
                f"pandas_udf.over expects a WindowSpec, got {type(window).__name__}"
            )
        if int(self._function_type) != PandasUDFType.GROUPED_AGG:
            raise AnalysisException(
                "pandas_udf.over requires functionType=GROUPED_AGG "
                f"(got functionType={self._function_type!r} for {self._function_name!r}); "
                "SCALAR / SCALAR_ITER are not window forms"
            )
        partition_columns = list(getattr(window, "_partition_columns", []) or [])
        if not partition_columns:
            raise UnsupportedOperationException(
                "windowed pandas_udf requires Window.partitionBy(...) "
                "(global/unpartitioned window is not supported; use groupBy().agg for "
                "global GROUPED_AGG)"
            )
        order_columns = list(getattr(window, "_order_columns", []) or [])
        frame_units = getattr(window, "_frame_units", None)
        if frame_units is not None and str(frame_units).lower() not in {"rows", "row"}:
            raise UnsupportedOperationException(
                "windowed pandas_udf supports only ROWS frames "
                f"(got frame_units={frame_units!r}); RANGE frames are not supported"
            )
        # Frame bounds (G2 rowsBetween): WindowSpec always DECLARES _frame_start/_frame_end
        # (None until rowsBetween/rangeBetween sets normalized ints) — presence means
        # value-is-not-None, never hasattr. When orderBy is present and bounds are absent →
        # Spark default UNBOUNDED PRECEDING … CURRENT ROW (start=None, end=0).
        has_frame_attrs = (
            getattr(window, "_frame_start", None) is not None
            or getattr(window, "_frame_end", None) is not None
        )
        if has_frame_attrs and not order_columns:
            raise UnsupportedOperationException(
                "windowed pandas_udf rowsBetween/range frame requires orderBy "
                "(Spark ordered-frame semantics)"
            )
        return PandasUDFColumn(
            self._user_func,
            self._return_type_sql,
            self._inputs,
            self._function_name,
            alias_name=self._alias_name,
            function_type=self._function_type,
            window_spec=window,
        )

    def default_name(self) -> str:
        """Spark-style default projection name ``func(arg, …)`` when no ``.alias`` is set."""
        arg_parts: list[str] = []
        for column in self._inputs:
            if column._projection_name is not None and column._stable_name:
                arg_parts.append(column._projection_name)
            else:
                arg_parts.append(column.spark_display_part())
        return f"{self._function_name}({', '.join(arg_parts)})"

    def output_name(self) -> str:
        """Resolved output field name (alias wins over :meth:`default_name`)."""
        if self._alias_name is not None:
            return self._alias_name
        return self.default_name()

    def _refuse_composition(self, surface: str) -> None:
        """Loud composition limit (U7 v1 — not a SQL Column expression)."""
        raise UnsupportedOperationException(
            f"pandas_udf result cannot be used in {surface} in repark v1 "
            "(facade projection-rewrite bridge only; not a Column expression in the SQL plan). "
            "Materialize via select/withColumn, then apply further expressions on that column. "
            "Mid-expression embedding is an M5-class seed."
        )

    def __add__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (+)")

    def __radd__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (+)")

    def __sub__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (-)")

    def __rsub__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (-)")

    def __mul__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (*)")

    def __rmul__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (*)")

    def __truediv__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (/)")

    def __rtruediv__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (/)")

    def __mod__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (%)")

    def __rmod__(self, _other: Any) -> None:
        # Column-parity reflected mod — UOE not TypeError (octo C5-Q-002).
        self._refuse_composition("arithmetic (%)")

    def __pow__(self, _other: Any) -> None:
        # Column-parity power — UOE not TypeError (octo C5-Q-002).
        self._refuse_composition("arithmetic (**)")

    def __rpow__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (**)")

    def __neg__(self) -> None:
        # Unary minus must refuse UOE, not TypeError (octo C5-Q-002).
        self._refuse_composition("unary (-)")

    def __eq__(self, _other: Any) -> bool:  # type: ignore[override]
        self._refuse_composition("comparison (==)")
        return False

    def __ne__(self, _other: Any) -> bool:  # type: ignore[override]
        self._refuse_composition("comparison (!=)")
        return False

    def __lt__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (<)")
        return False

    def __le__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (<=)")
        return False

    def __gt__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (>)")
        return False

    def __ge__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (>=)")
        return False

    def __and__(self, _other: Any) -> None:
        self._refuse_composition("logical (&)")

    def __rand__(self, _other: Any) -> None:
        # Column-parity reflected and — UOE not TypeError (octo C5-Q-002).
        self._refuse_composition("logical (&)")

    def __or__(self, _other: Any) -> None:
        self._refuse_composition("logical (|)")

    def __ror__(self, _other: Any) -> None:
        self._refuse_composition("logical (|)")

    def __invert__(self) -> None:
        self._refuse_composition("logical (~)")

    def cast(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("cast")

    # ``over`` is implemented above for M6 unbounded windowed GROUPED_AGG (not refused).

    # Column-parity methods that would otherwise AttributeError (octo C7-Q-002 /
    # C5 residual). All refuse UOE M5 seed — same class as arithmetic/cast.

    def is_null(self) -> None:
        self._refuse_composition("isNull")

    isNull = is_null  # noqa: N815 — PySpark camelCase alias

    def is_not_null(self) -> None:
        self._refuse_composition("isNotNull")

    isNotNull = is_not_null  # noqa: N815 — PySpark camelCase alias

    def between(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("between")

    def eqNullSafe(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        self._refuse_composition("eqNullSafe")

    def when(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("when")

    def otherwise(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("otherwise")

    def asc(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("asc")

    def desc(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("desc")

    def contains(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (contains)")

    def startswith(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (startswith)")

    def endswith(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (endswith)")

    def like(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (like)")

    def ilike(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (ilike)")

    def rlike(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (rlike)")

    def bitwiseAND(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        self._refuse_composition("bitwiseAND")

    def bitwiseOR(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        self._refuse_composition("bitwiseOR")

    def bitwiseXOR(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        self._refuse_composition("bitwiseXOR")

    def __contains__(self, _item: object) -> bool:
        # UOE (composition refuse), not AttributeError / Column's PySparkValueError.
        self._refuse_composition("__contains__ / in")
        return False

    def __bool__(self) -> bool:
        """Raise — a pandas_udf marker has no truth value (parity with :class:`Column.__bool__`).

        Without this guard, Python ``and`` / ``or`` / ``not`` / ``if`` treat the marker as
        always-truthy and silently drop composition (octo C1-Q-001).
        """
        raise PySparkValueError(
            "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' when building DataFrame boolean expressions."
        )

    __nonzero__ = __bool__


class PandasUDFFunction:
    """Callable from :func:`pandas_udf` — call with columns to build a projection/agg marker."""

    __slots__ = ("__name__", "_function_type", "_return_type_sql", "_user_func")

    def __init__(
        self,
        user_func: Callable[..., Any],
        return_type_sql: str,
        function_type: int = PandasUDFType.SCALAR,
    ) -> None:
        """Wrap a user function with its declared return type and eval type."""
        self._user_func = user_func
        self._return_type_sql = return_type_sql
        self._function_type = function_type
        self.__name__ = getattr(user_func, "__name__", "pandas_udf")

    def __call__(self, *args: Column | str) -> PandasUDFColumn:
        """Bind input columns; returns a :class:`PandasUDFColumn` for select/withColumn/agg."""
        if not args:
            raise PySparkTypeError(
                "pandas_udf requires at least one column argument (zero-arg form is unsupported)"
            )
        inputs: list[Column] = []
        for argument in args:
            if isinstance(argument, Column):
                inputs.append(argument)
            elif isinstance(argument, str):
                inputs.append(col(argument))
            else:
                raise PySparkTypeError(
                    "pandas_udf arguments must be Column or column-name str, "
                    f"got {type(argument).__name__}"
                )
        return PandasUDFColumn(
            self._user_func,
            self._return_type_sql,
            inputs,
            self.__name__,
            function_type=self._function_type,
        )


def _build_pandas_udf(
    user_func: Callable[..., Any],
    return_type: Any,
    function_type: Any,
) -> PandasUDFFunction:
    """Validate eval type + return type and wrap ``user_func`` as a :class:`PandasUDFFunction`."""
    if not callable(user_func):
        raise PySparkTypeError(f"pandas_udf func must be callable, got {type(user_func).__name__}")
    normalized_ft = _normalize_pandas_udf_function_type(function_type)
    return_type_sql = _normalize_pandas_udf_return_type_sql(return_type)
    return PandasUDFFunction(user_func, return_type_sql, function_type=normalized_ft)


def pandas_udf(
    f: Any = None,
    returnType: Any = None,  # noqa: N803 — PySpark camelCase
    functionType: Any = None,  # noqa: N803 — PySpark camelCase
) -> Any:
    """Vectorized pandas UDF decorator (PySpark ``functions.pandas_udf``).

    **Supported (U7 + M5):**

    * **SCALAR** (default) — ``Series → Series`` in ``select`` / ``withColumn``.
    * **SCALAR_ITER** — ``Iterator[Series] → Iterator[Series]`` (or multi-arg
      ``Iterator[tuple[Series, …]]``) via the same U7 bridge with a batch-iterator adapter.
    * **GROUPED_AGG** — ``Series → scalar`` in ``groupBy(...).agg(...)`` (pure pandas_udf
      form only; mixed UDF+builtin is a loud M6 seed).

    Usage::

        @pandas_udf("long")
        def double_x(series: pd.Series) -> pd.Series:
            return series * 2

        df.select(double_x(df.x).alias("y"))
        df.withColumn("y", double_x("x"))

        @pandas_udf("long", PandasUDFType.SCALAR_ITER)
        def double_iter(batches):
            for series in batches:
                yield series * 2

        @pandas_udf("double", PandasUDFType.GROUPED_AGG)
        def mean_udf(series: pd.Series) -> float:
            return float(series.mean())

        df.groupBy("k").agg(mean_udf("v").alias("m"))

    SCALAR / SCALAR_ITER implementation is a **facade projection rewrite** over the deferred
    mapInArrow-style bridge (see :meth:`repark.dataframe.DataFrame._select_with_pandas_udfs`)
    — the UDF result is **not** a :class:`~repark.column.Column` expression in the SQL plan.
    Composition mid-expression is refused; materialize via select/withColumn first.

    Multi-UDF SCALAR ``select`` lists run in **one** mapInArrow pass per batch. Requires the
    optional ``pandas`` extra at execution time (import is deferred to the bridge).

    **OUT (loud):** ``GROUPED_MAP`` and window pandas_udf —
    :class:`~repark.errors.UnsupportedOperationException` naming M6-class seed.
    Apache ``test_pandas_udf*`` census claims are out of scope for this unit.
    """
    # Direct: pandas_udf(fn, returnType[, functionType])
    if f is not None and callable(f) and not _is_pandas_udf_datatype_like(f):
        if returnType is None:
            raise PySparkTypeError("pandas_udf(func, returnType) requires returnType")
        return _build_pandas_udf(f, returnType, functionType)

    # @pandas_udf("long", PandasUDFType.SCALAR) — second positional is functionType
    if (
        f is not None
        and returnType is not None
        and functionType is None
        and _is_pandas_udf_datatype_like(f)
        and _is_pandas_udf_function_type(returnType)
    ):

        def _decorator_with_ft(func: Callable[..., Any]) -> PandasUDFFunction:
            return _build_pandas_udf(func, f, returnType)

        return _decorator_with_ft

    # @pandas_udf("long", "double") — two datatype positionals is not a legal form; the old
    # keyword fall-through silently took the second as returnType and dropped the first
    # (octo C1-Q-002). Second positional must be functionType when present.
    # Exclude functionType-like first positionals (string "SCALAR" / "GROUPED_AGG" / …): those
    # are dual-datatype-looking only because every str is datatype-like, but they must reach
    # the C7 functionType-first route (octo C8-Q-001) — not this refuse.
    if (
        f is not None
        and returnType is not None
        and functionType is None
        and _is_pandas_udf_datatype_like(f)
        and not _is_pandas_udf_function_type(f)
        and _is_pandas_udf_datatype_like(returnType)
        and not _is_pandas_udf_function_type(returnType)
    ):
        raise PySparkTypeError(
            "pandas_udf decorator second positional argument must be functionType "
            f"(SCALAR / PandasUDFType.*), not a second returnType; got {returnType!r}. "
            "Use @pandas_udf('long') or @pandas_udf('long', PandasUDFType.SCALAR)."
        )

    # @pandas_udf(PandasUDFType.GROUPED_AGG, "long") / @pandas_udf(201, returnType="long") —
    # first positional is a functionType tag and the second/kw is returnType. Old keyword
    # fall-through ignored ``f`` and built SCALAR (fail-open). Route through normalize so
    # non-SCALAR raises UOE M5 seed (octo C7-L-001). String tags ("GROUPED_AGG", "SCALAR", …)
    # are also functionType-first (octo C8-Q-001) — dual-datatype refuse must not steal them.
    if (
        f is not None
        and returnType is not None
        and functionType is None
        and _is_pandas_udf_function_type(f)
        and _is_pandas_udf_datatype_like(returnType)
    ):

        def _decorator_ft_first(func: Callable[..., Any]) -> PandasUDFFunction:
            return _build_pandas_udf(func, returnType, f)

        return _decorator_ft_first

    # @pandas_udf("long") / @pandas_udf(LongType())
    if f is not None and returnType is None:
        if not _is_pandas_udf_datatype_like(f):
            raise PySparkTypeError(
                "pandas_udf decorator expects a returnType (DataType or str) as the first "
                f"argument, got {type(f).__name__}"
            )

        def _decorator(func: Callable[..., Any]) -> PandasUDFFunction:
            return _build_pandas_udf(func, f, functionType)

        return _decorator

    # @pandas_udf(returnType=..., functionType=...) — keyword / returnType-only form.
    # When the first positional is also a datatype (not a function), refuse rather than
    # silently ignoring it (octo C1-Q-002 companion of the dual-datatype case above).
    if returnType is not None:
        if f is not None and _is_pandas_udf_datatype_like(f) and not callable(f):
            raise PySparkTypeError(
                "pandas_udf received two returnType-like values; use a single returnType "
                f"(first={f!r}, returnType={returnType!r})"
            )

        def _decorator_kw(func: Callable[..., Any]) -> PandasUDFFunction:
            return _build_pandas_udf(func, returnType, functionType)

        return _decorator_kw

    raise PySparkTypeError(
        "pandas_udf requires returnType (e.g. @pandas_udf('long') or "
        "pandas_udf(fn, returnType='long'))"
    )


# =============================================================================
# U8 — scalar Python udf (decorator/export; bridge lives in dataframe.py)
# =============================================================================
#
# Per-row Python by design (same shape as PySpark's classic scalar UDF): each
# batch is Arrow → Python scalars row-by-row → user func → Arrow re-ingest.
# That is O(rows) Python calls and slower than :func:`pandas_udf` (vectorized
# Series path). Document the cost honestly; do not pretend it is vectorized.


def _is_python_udf_datatype_like(value: Any) -> bool:
    """True when ``value`` is a returnType (str DDL, repark/pyspark DataType, or duck-typed).

    U9: accept duck-typed DataType objects (``simpleString`` / ``jsonValue``) so harness
    imports that bind Apache ``StringType()`` instances still validate as returnType.
    """
    if isinstance(value, str):
        return True
    from repark.types import DataType

    if isinstance(value, DataType):
        return True
    # Duck-typed Spark DataType instance (not the class itself).
    if isinstance(value, type):
        return False
    simple = getattr(value, "simpleString", None)
    return callable(simple)


def _python_udf_arrow_type_for_return(data_type: Any) -> Any:
    """Map a Spark :class:`~repark.types.DataType` to a concrete Arrow type for re-ingest.

    Reuses the same fail-open string refuse as :func:`_pandas_udf_arrow_type_for_return`
    (variant / interval / time must not silently declare string).
    """
    from repark.session import _data_type_to_sql_type, _sql_type_to_arrow
    from repark.types import DataType

    if not isinstance(data_type, DataType):
        raise PySparkTypeError(
            f"udf returnType must be a DataType or DDL type string, got {type(data_type).__name__}"
        )
    try:
        sql_type = _data_type_to_sql_type(data_type)
    except Exception as error:
        raise PySparkTypeError(
            f"udf returnType {data_type.simpleString()!r} is not a supported scalar type: {error}"
        ) from error
    try:
        arrow_type = _sql_type_to_arrow(sql_type)
    except Exception as error:
        raise PySparkTypeError(
            f"udf returnType {data_type.simpleString()!r} is not a supported scalar type: {error}"
        ) from error
    # Shared refuse for variant/interval/time string fail-open (nested leaves too).
    _pandas_udf_refuse_fail_open_string_leaves(data_type, arrow_type)
    return arrow_type


def _normalize_python_udf_return_type_sql(return_type: Any) -> str:
    """Lower ``returnType`` to a logical DDL fragment (``DataType.simpleString``).

    Default when omitted is Spark's ``string``. Struct / field-list DDL is allowed for
    classic scalar UDFs (unlike :func:`pandas_udf` scalar which is Series-shaped).
    U9: duck-typed DataType instances (harness / pyspark types) via ``simpleString()``.
    """
    from repark.types import DataType, StringType

    if return_type is None:
        return_type = StringType()
    if isinstance(return_type, str):
        text = return_type.strip()
        if not text:
            raise PySparkTypeError("udf returnType must be a non-empty type string")
        try:
            parsed = DataType.fromDDL(text)
        except Exception as error:
            raise PySparkTypeError(
                f"udf returnType {text!r} is not a valid type: {error}"
            ) from error
        _python_udf_arrow_type_for_return(parsed)
        return parsed.simpleString()
    if isinstance(return_type, DataType):
        _python_udf_arrow_type_for_return(return_type)
        return return_type.simpleString()
    # Duck-typed DataType (e.g. pyspark.sql.types.LongType instance).
    simple = getattr(return_type, "simpleString", None)
    if callable(simple) and not isinstance(return_type, type):
        try:
            text = str(simple()).strip()
        except Exception as error:
            raise PySparkTypeError(
                f"udf returnType {type(return_type).__name__} simpleString() failed: {error}"
            ) from error
        if not text:
            raise PySparkTypeError("udf returnType simpleString() must be non-empty")
        try:
            parsed = DataType.fromDDL(text)
        except Exception as error:
            raise PySparkTypeError(
                f"udf returnType {text!r} is not a valid type: {error}"
            ) from error
        _python_udf_arrow_type_for_return(parsed)
        return parsed.simpleString()
    raise PySparkTypeError(
        f"udf returnType must be a DataType or DDL type string, got {type(return_type).__name__}"
    )


class PythonUDFColumn:
    """Marker for a classic scalar ``udf`` projection (not a SQL-plan :class:`Column`).

    Produced by calling a :func:`udf`-decorated / :class:`UserDefinedFunction` with
    column arguments. Top-level ``select`` / ``withColumn`` only — mid-expression
    composition is refused (same class as :class:`PandasUDFColumn`).
    """

    __slots__ = (
        "_alias_name",
        "_function_name",
        "_inputs",
        "_return_type_sql",
        "_user_func",
    )

    def __init__(
        self,
        user_func: Callable[..., Any],
        return_type_sql: str,
        inputs: list[Column],
        function_name: str,
        *,
        alias_name: str | None = None,
    ) -> None:
        """Bind the user function, declared return type, and input Columns."""
        self._user_func = user_func
        # Revalidate every construction path (hostile constructor / post-build mutation).
        self._return_type_sql = _normalize_python_udf_return_type_sql(return_type_sql)
        self._inputs = list(inputs)
        self._function_name = function_name
        self._alias_name = alias_name

    def alias(self, name: str) -> PythonUDFColumn:
        """Set the output column name (PySpark ``Column.alias`` parity for UDF results)."""
        if not isinstance(name, str) or name.strip() == "":
            raise PySparkTypeError("udf alias name must be a non-empty str")
        return PythonUDFColumn(
            self._user_func,
            self._return_type_sql,
            self._inputs,
            self._function_name,
            alias_name=name,
        )

    def default_name(self) -> str:
        """Spark-style default projection name ``func(arg, …)`` when no ``.alias`` is set."""
        arg_parts: list[str] = []
        for column in self._inputs:
            if column._projection_name is not None and column._stable_name:
                arg_parts.append(column._projection_name)
            else:
                arg_parts.append(column.spark_display_part())
        return f"{self._function_name}({', '.join(arg_parts)})"

    def output_name(self) -> str:
        """Resolved output field name (alias wins over :meth:`default_name`)."""
        if self._alias_name is not None:
            return self._alias_name
        return self.default_name()

    def _refuse_composition(self, surface: str) -> None:
        """Loud composition limit (U8 v1 — not a SQL Column expression)."""
        raise UnsupportedOperationException(
            f"udf result cannot be used in {surface} in repark v1 "
            "(facade projection-rewrite bridge only; not a Column expression in the SQL plan). "
            "Materialize via select/withColumn, then apply further expressions on that column. "
            "Mid-expression embedding is a follow-on seed."
        )

    def __add__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (+)")

    def __radd__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (+)")

    def __sub__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (-)")

    def __rsub__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (-)")

    def __mul__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (*)")

    def __rmul__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (*)")

    def __truediv__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (/)")

    def __rtruediv__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (/)")

    def __mod__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (%)")

    def __rmod__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (%)")

    def __pow__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (**)")

    def __rpow__(self, _other: Any) -> None:
        self._refuse_composition("arithmetic (**)")

    def __neg__(self) -> None:
        self._refuse_composition("unary (-)")

    def __eq__(self, _other: Any) -> bool:  # type: ignore[override]
        self._refuse_composition("comparison (==)")
        return False

    def __ne__(self, _other: Any) -> bool:  # type: ignore[override]
        self._refuse_composition("comparison (!=)")
        return False

    def __lt__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (<)")
        return False

    def __le__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (<=)")
        return False

    def __gt__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (>)")
        return False

    def __ge__(self, _other: Any) -> bool:
        self._refuse_composition("comparison (>=)")
        return False

    def __and__(self, _other: Any) -> None:
        self._refuse_composition("logical (&)")

    def __rand__(self, _other: Any) -> None:
        self._refuse_composition("logical (&)")

    def __or__(self, _other: Any) -> None:
        self._refuse_composition("logical (|)")

    def __ror__(self, _other: Any) -> None:
        self._refuse_composition("logical (|)")

    def __invert__(self) -> None:
        self._refuse_composition("logical (~)")

    def cast(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("cast")

    def over(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("window .over")

    def is_null(self) -> None:
        self._refuse_composition("isNull")

    isNull = is_null  # noqa: N815 — PySpark camelCase alias

    def is_not_null(self) -> None:
        self._refuse_composition("isNotNull")

    isNotNull = is_not_null  # noqa: N815 — PySpark camelCase alias

    def between(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("between")

    def eqNullSafe(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        self._refuse_composition("eqNullSafe")

    def when(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("when")

    def otherwise(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("otherwise")

    def asc(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("asc")

    def desc(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("desc")

    def contains(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (contains)")

    def startswith(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (startswith)")

    def endswith(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (endswith)")

    def like(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (like)")

    def ilike(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (ilike)")

    def rlike(self, *_args: Any, **_kwargs: Any) -> None:
        self._refuse_composition("string predicate (rlike)")

    def bitwiseAND(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        self._refuse_composition("bitwiseAND")

    def bitwiseOR(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        self._refuse_composition("bitwiseOR")

    def bitwiseXOR(self, *_args: Any, **_kwargs: Any) -> None:  # noqa: N802 — PySpark camelCase
        self._refuse_composition("bitwiseXOR")

    def __contains__(self, _item: object) -> bool:
        self._refuse_composition("__contains__ / in")
        return False

    def __bool__(self) -> bool:
        """Raise — a udf marker has no truth value (parity with :class:`Column.__bool__`)."""
        raise PySparkValueError(
            "Cannot convert column into bool: please use '&' for 'and', '|' for 'or', "
            "'~' for 'not' when building DataFrame boolean expressions."
        )

    __nonzero__ = __bool__


class UserDefinedFunction:
    """Callable from :func:`udf` / :meth:`UDFRegistration.register` (PySpark name).

    Call with column arguments to build a :class:`PythonUDFColumn` for select/withColumn.
    **Cost:** each row invokes the Python function once (per-row scalar UDF — not vectorized).
    Prefer :func:`pandas_udf` for Series-batch throughput.

    ``deterministic`` defaults to ``True`` (Spark parity); :meth:`asNondeterministic`
    flips it to ``False`` (accepted flag; repark has no Spark codegen path that
    consults it for fold/cache — flag is surface-honest only; r23 C6 census).
    """

    # === r23 C6: census-catalog-udf ===
    __slots__ = ("__name__", "_deterministic", "_return_type_sql", "_user_func")

    def __init__(
        self,
        user_func: Callable[..., Any],
        return_type_sql: str,
        *,
        name: str | None = None,
        deterministic: bool = True,
    ) -> None:
        """Wrap a user function with its declared return type."""
        if not callable(user_func):
            raise PySparkTypeError(f"udf func must be callable, got {type(user_func).__name__}")
        # Defense in depth — direct ``UserDefinedFunction(udtf_obj, …)`` must not half-wire.
        _refuse_udtf_as_scalar_udf(user_func, surface="UserDefinedFunction")
        self._user_func = user_func
        self._return_type_sql = _normalize_python_udf_return_type_sql(return_type_sql)
        self.__name__ = name if name is not None else getattr(user_func, "__name__", "udf")
        self._deterministic = bool(deterministic)

    @property
    def deterministic(self) -> bool:
        """Whether the UDF is marked deterministic (Spark ``UserDefinedFunction.deterministic``)."""
        return self._deterministic

    def __call__(self, *args: Column | str) -> PythonUDFColumn:
        """Bind input columns; returns a :class:`PythonUDFColumn` for select/withColumn."""
        if not args:
            raise PySparkTypeError(
                "udf requires at least one column argument (zero-arg form is unsupported)"
            )
        inputs: list[Column] = []
        for argument in args:
            if isinstance(argument, Column):
                inputs.append(argument)
            elif isinstance(argument, str):
                inputs.append(col(argument))
            else:
                raise PySparkTypeError(
                    "udf arguments must be Column or column-name str, "
                    f"got {type(argument).__name__}"
                )
        return PythonUDFColumn(
            self._user_func,
            self._return_type_sql,
            inputs,
            self.__name__,
        )

    def asNondeterministic(self) -> UserDefinedFunction:  # noqa: N802 — PySpark camelCase
        """Mark the UDF nondeterministic (Spark parity flag; no codegen path in repark)."""
        self._deterministic = False
        return self


def _refuse_udtf_as_scalar_udf(user_func: Any, *, surface: str) -> None:
    """Refuse wrapping a table UDTF as a classic scalar UDF (r22 U11 half-wired guard).

    ``UserDefinedTableFunction`` is callable (scalar-arg call produces a DataFrame in
    U12), so without this gate ``F.udf(udtf_obj)`` / ``spark.udf.register(name, udtf_obj)``
    would half-wire a table function as a scalar UDF.
    """
    if isinstance(user_func, UserDefinedTableFunction):
        raise PySparkTypeError(
            f"{surface} does not accept UserDefinedTableFunction (table UDTF). "
            "Use spark.udtf.register / @udtf for table functions (U12 scalar-arg "
            "core via mapInArrow), or pass a scalar Python callable to F.udf / "
            "spark.udf.register."
        )


def _build_python_udf(
    user_func: Callable[..., Any],
    return_type: Any,
    *,
    name: str | None = None,
) -> UserDefinedFunction:
    """Construct a :class:`UserDefinedFunction` with validated return type."""
    if not callable(user_func):
        raise PySparkTypeError(f"udf func must be callable, got {type(user_func).__name__}")
    _refuse_udtf_as_scalar_udf(user_func, surface="F.udf / spark.udf.register")
    return_type_sql = _normalize_python_udf_return_type_sql(return_type)
    return UserDefinedFunction(user_func, return_type_sql, name=name)


def udf(
    f: Callable[..., Any] | Any | None = None,
    returnType: Any = None,  # noqa: N803 — PySpark camelCase
    *,
    useArrow: bool | None = None,  # noqa: N803 — PySpark camelCase (accepted, ignored)
) -> UserDefinedFunction | Callable[[Callable[..., Any]], UserDefinedFunction]:
    """Classic scalar Python UDF decorator (PySpark ``functions.udf``).

    **Per-row cost (honest):** each input row is a separate Python call. Arrow batches
    stream through the mapInArrow bridge; inside each batch the facade walks rows and
    invokes ``f`` once per row. Prefer :func:`pandas_udf` for vectorized Series→Series
    throughput on the same bridge.

    Forms::

        @udf("long")
        def double(x: int | None) -> int | None:
            return None if x is None else x * 2

        @udf(returnType=LongType())
        def double2(x: int | None) -> int | None:
            return None if x is None else x * 2

        double = udf(lambda x: x * 2 if x is not None else None, "long")

        df.select(double("a"))
        df.withColumn("b", double(col("a")))

    Null semantics: SQL NULL arrives as Python ``None``; return ``None`` for NULL.
    ``returnType`` defaults to ``string`` when omitted (Spark contract).
    ``useArrow`` is accepted for PySpark signature parity and ignored (repark always
    uses the Arrow mapInArrow bridge).
    """
    from repark.types import StringType

    _ = useArrow  # PySpark parity; repark bridge is always Arrow

    # Direct: udf(fn, returnType)
    if f is not None and callable(f) and not _is_python_udf_datatype_like(f):
        resolved = returnType if returnType is not None else StringType()
        return _build_python_udf(f, resolved)

    # @udf("long") / @udf(LongType()) — first positional is returnType
    if f is not None and _is_python_udf_datatype_like(f) and returnType is None:

        def _decorator_type(func: Callable[..., Any]) -> UserDefinedFunction:
            return _build_python_udf(func, f)

        return _decorator_type

    # @udf / @udf() — default StringType
    if f is None and returnType is None:

        def _decorator_default(func: Callable[..., Any]) -> UserDefinedFunction:
            return _build_python_udf(func, StringType())

        return _decorator_default

    # @udf(returnType=...) / udf(returnType=...)
    if f is None and returnType is not None:

        def _decorator_kw(func: Callable[..., Any]) -> UserDefinedFunction:
            return _build_python_udf(func, returnType)

        return _decorator_kw

    # udf(fn, returnType=...) already handled; dual-datatype positionals refuse
    if (
        f is not None
        and _is_python_udf_datatype_like(f)
        and returnType is not None
        and _is_python_udf_datatype_like(returnType)
    ):
        raise PySparkTypeError(
            "udf decorator second positional argument must not be a second returnType; "
            "use @udf('long') or udf(fn, returnType='long')."
        )

    raise PySparkTypeError(
        "udf requires a callable and optional returnType "
        "(e.g. @udf('long') or udf(fn, returnType='long') or @udf(returnType='long'))"
    )


# =============================================================================
# r22 U11 — UDTF (functions.udtf re-export; full refuse lives in repark.udtf)
# =============================================================================
#
# PySpark places ``udtf`` on ``pyspark.sql.functions``. Construction validates
# handlers with Spark INVALID_UDTF_* classes; call/register refuse loud (Q16).
# Imports: top-of-module ``from repark.udtf import UserDefinedTableFunction, udtf``.


__all__ = [
    "PandasUDFType",
    "PythonUDFColumn",
    "UserDefinedFunction",
    "UserDefinedTableFunction",
    "abs",
    "acos",
    "acosh",
    "add_months",
    "approx_percentile",
    "array",
    "array_contains",
    "array_distinct",
    "array_except",
    "array_intersect",
    "array_join",
    "array_max",
    "array_min",
    "array_position",
    "array_remove",
    "array_repeat",
    "array_sort",
    "array_union",
    "arrays_zip",
    "ascii",
    "asin",
    "asinh",
    "atan",
    "atan2",
    "atanh",
    "avg",
    "base64",
    "bit_and",
    "bit_or",
    "bit_xor",
    "bucket",
    "ceil",
    "ceiling",
    "chr",
    "coalesce",
    "col",
    "collect_list",
    "collect_set",
    "concat",
    "concat_ws",
    "corr",
    "cos",
    "cosh",
    "cot",
    "count",
    "countDistinct",
    "count_distinct",
    "covar_pop",
    "covar_samp",
    "crc32",
    "csc",
    "currentDate",
    "currentTimestamp",
    "current_date",
    "current_timestamp",
    "date_add",
    "date_format",
    "date_part",
    "date_sub",
    "date_trunc",
    "datediff",
    "dayname",
    "dayofmonth",
    "dayofweek",
    "dayofyear",
    "days",
    "decode",
    "dense_rank",
    "elt",
    "encode",
    "exp",
    "explode",
    "explode_outer",
    "expr",
    "extract",
    "find_in_set",
    "first",
    "flatten",
    "floor",
    "format_number",
    "format_string",
    "from_csv",
    "from_unixtime",
    "from_utc_timestamp",
    "from_xml",
    "greatest",
    "hash",
    "hour",
    "hours",
    "hypot",
    "initcap",
    "input_file_name",
    "instr",
    "isnan",
    "isnull",
    "json_tuple",
    "kurtosis",
    "last",
    "last_day",
    "least",
    "length",
    "levenshtein",
    "lit",
    "locate",
    "log",
    "log10",
    "lower",
    "lpad",
    "ltrim",
    "make_timestamp",
    "map_entries",
    "map_from_arrays",
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
    "nanvl",
    "next_day",
    "ntile",
    "overlay",
    "pandas_udf",
    "percentile_approx",
    "posexplode",
    "posexplode_outer",
    "position",
    "pow",
    "power",
    "quarter",
    "raise_error",
    "rand",
    "randn",
    "random",
    "rank",
    "regexp_extract",
    "regexp_replace",
    "repeat",
    "reverse",
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
    "sha1",
    "sha2",
    "signum",
    "sin",
    "sinh",
    "size",
    "skewness",
    "slice",
    "sort_array",
    "soundex",
    "spark_partition_id",
    "split",
    "sqrt",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "struct",
    "substring_index",
    "sum",
    "tan",
    "tanh",
    "timestamp_micros",
    "timestamp_millis",
    "timestamp_seconds",
    "to_date",
    "to_timestamp",
    "to_utc_timestamp",
    "translate",
    "trim",
    "trunc",
    "try_to_timestamp",
    "udf",
    "udtf",
    "unbase64",
    "unix_timestamp",
    "upper",
    "var_pop",
    "var_samp",
    "variance",
    "weekday",
    "weekofyear",
    "when",
    "xxhash64",
    "year",
    "years",
]


def when(condition: Column, value: Column | Scalar) -> Column:
    """Start a searched ``CASE`` (PySpark ``functions.when``).

    Chain further ``.when(...)`` arms and finish with ``.otherwise(...)``::

        F.when(F.col("a") > 0, 1).when(F.col("a") < 0, -1).otherwise(0)

    Without ``otherwise``, non-matching rows yield NULL.
    """
    # Apache ``test_when``: bare str condition → NOT_COLUMN (not AttributeError on str).
    if not isinstance(condition, Column):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN",
            messageParameters={
                "arg_name": "condition",
                "arg_type": type(condition).__name__,
            },
        )
    return Column._from_when_pairs([(condition, Column._to_column(value))], otherwise=None)


# ---- R-FN-BATCH1: top-N scalar wrappers over engine call_scalar / Column methods ---------------


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
    ``random()`` is non-deterministic / non-foldable — octo C7-L-001). Pass
    ``foldable=True`` for known foldable nullaries such as ``current_date``.

    ``has_ungroupable`` overrides sticky ungroupable (OR with child bits when None).
    Generator arguments are refused loud (``UNSUPPORTED_GENERATOR``) — wrapping
    ``F.size(F.explode(...))`` / ``.str`` paths would strip ``_generator`` and skip
    unnest rewrite (octo C5-Q-001 / C5-L-001; Spark parity).
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
            # H2: collapse aliased children so ``round(col.alias("v"), 2)`` → ``round(v, 2)``.
            display_parts.append(column.spark_wrap_display_part())
        sql_parts.append(column.sql_expr_part())
    shown = display if display is not None else f"{name}({', '.join(display_parts)})"
    # Sticky aggregate / free-attr (octo C2-Q-002 / C2-L-002): round(sum(x)), abs via CASE, …
    # Sticky ungroupable (octo C7-L-002): abs(window) / coalesce via other builders.
    # Nullary: do NOT treat vacuous all([]) as foldable — F.rand→random() is non-foldable
    # (octo C7-L-001). Known foldable nullaries (current_date) pass foldable=True.
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
    # === r23b N2: plan-collapse ===
    # ``.round()`` / ``F.round`` on a windowed column is same-layer wrap (Q15) — keep the
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


def lower(col: Column | str) -> Column:
    """Lowercase string (PySpark ``functions.lower``)."""
    return _scalar("lower", col)


def upper(col: Column | str) -> Column:
    """Uppercase string (PySpark ``functions.upper``)."""
    return _scalar("upper", col)


def trim(col: Column | str) -> Column:
    """Trim both sides (PySpark ``functions.trim``)."""
    return _scalar("trim", col)


def ltrim(col: Column | str) -> Column:
    """Trim leading whitespace (PySpark ``functions.ltrim``)."""
    return _scalar("ltrim", col)


def rtrim(col: Column | str) -> Column:
    """Trim trailing whitespace (PySpark ``functions.rtrim``)."""
    return _scalar("rtrim", col)


def length(col: Column | str) -> Column:
    """Character length (PySpark ``functions.length``)."""
    return _scalar("length", col)


def initcap(col: Column | str) -> Column:
    """Title-case words (PySpark ``functions.initcap``)."""
    return _scalar("initcap", col)


def lpad(col: Column | str, len: int, pad: str = " ") -> Column:
    """Left-pad a string (PySpark ``functions.lpad``)."""
    return _scalar("lpad", col, len, pad, lit_indices=frozenset({1, 2}))


def rpad(col: Column | str, len: int, pad: str = " ") -> Column:
    """Right-pad a string (PySpark ``functions.rpad``)."""
    return _scalar("rpad", col, len, pad, lit_indices=frozenset({1, 2}))


def instr(str: Column | str, substr: str) -> Column:
    """1-based index of substring (PySpark ``functions.instr``)."""
    return _scalar("instr", str, substr, lit_indices=frozenset({1}))


def concat_ws(sep: str, *cols: Column | str) -> Column:
    """Join strings with a separator (PySpark ``functions.concat_ws``)."""
    return _scalar("concat_ws", sep, *cols, lit_indices=frozenset({0}))


def regexp_replace(
    str: Column | str,
    pattern: Column | str,
    replacement: Column | str,
) -> Column:
    """Replace regex matches (PySpark ``functions.regexp_replace``).

    Engine lowers with the global ``g`` flag (Spark replaces every match; DataFusion defaults
    to first-match only — F2 / Apache ``test_regexp_replace``). ``pattern`` / ``replacement``
    may be Column or str (literal strings stay forced-lit via ``lit_indices``).
    """
    lit_indices: set[int] = set()
    if not isinstance(pattern, Column):
        lit_indices.add(1)
    if not isinstance(replacement, Column):
        lit_indices.add(2)
    return _scalar(
        "regexp_replace",
        str,
        pattern,
        replacement,
        lit_indices=frozenset(lit_indices),
    )


def sqrt(col: Column | str) -> Column:
    """Square root (PySpark ``functions.sqrt``)."""
    return _scalar("sqrt", col)


def cos(col: Column | str) -> Column:
    """Cosine (PySpark ``functions.cos``)."""
    return _scalar("cos", col)


def sin(col: Column | str) -> Column:
    """Sine (PySpark ``functions.sin``)."""
    return _scalar("sin", col)


def tan(col: Column | str) -> Column:
    """Tangent (PySpark ``functions.tan``)."""
    return _scalar("tan", col)


def cosh(col: Column | str) -> Column:
    """Hyperbolic cosine (PySpark ``functions.cosh``)."""
    return _scalar("cosh", col)


def sinh(col: Column | str) -> Column:
    """Hyperbolic sine (PySpark ``functions.sinh``)."""
    return _scalar("sinh", col)


def tanh(col: Column | str) -> Column:
    """Hyperbolic tangent (PySpark ``functions.tanh``)."""
    return _scalar("tanh", col)


def acos(col: Column | str) -> Column:
    """Inverse cosine (PySpark ``functions.acos``)."""
    return _scalar("acos", col)


def asin(col: Column | str) -> Column:
    """Inverse sine (PySpark ``functions.asin``)."""
    return _scalar("asin", col)


def atan(col: Column | str) -> Column:
    """Inverse tangent (PySpark ``functions.atan``)."""
    return _scalar("atan", col)


def atan2(col1: Column | str | float | int, col2: Column | str | float | int) -> Column:
    """Two-argument inverse tangent (PySpark ``functions.atan2``)."""
    return _scalar("atan2", col1, col2)


def acosh(col: Column | str) -> Column:
    """Inverse hyperbolic cosine (PySpark ``functions.acosh``)."""
    return _scalar("acosh", col)


def asinh(col: Column | str) -> Column:
    """Inverse hyperbolic sine (PySpark ``functions.asinh``)."""
    return _scalar("asinh", col)


def atanh(col: Column | str) -> Column:
    """Inverse hyperbolic tangent (PySpark ``functions.atanh``)."""
    return _scalar("atanh", col)


def cot(col: Column | str) -> Column:
    """Cotangent (PySpark ``functions.cot``)."""
    return _scalar("cot", col)


def sec(col: Column | str) -> Column:
    """Secant ``1/cos`` (PySpark ``functions.sec``)."""
    return _scalar("sec", col)


def csc(col: Column | str) -> Column:
    """Cosecant ``1/sin`` (PySpark ``functions.csc``)."""
    return _scalar("csc", col)


def hypot(col1: Column | str | float | int, col2: Column | str | float | int) -> Column:
    """Euclidean norm ``sqrt(a² + b²)`` (PySpark ``functions.hypot``)."""
    return _scalar("hypot", col1, col2)


def dayname(col: Column | str) -> Column:
    """Abbreviated weekday name (PySpark ``functions.dayname`` → ``date_format(..., 'EEE')``)."""
    column = _column_argument(col)
    result = date_format(column, "EEE")
    display = f"dayname({column.spark_wrap_display_part()})"
    return Column(
        result._inner,
        spark_display=display,
        projection_name=display,
        sql_expr=result.sql_expr_part(),
        stable_name=False,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable and not column._is_aggregate,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        partition_transform=column._partition_transform,
    )


def monthname(col: Column | str) -> Column:
    """Abbreviated month name (PySpark ``functions.monthname`` → ``date_format(..., 'MMM')``)."""
    column = _column_argument(col)
    result = date_format(column, "MMM")
    display = f"monthname({column.spark_wrap_display_part()})"
    return Column(
        result._inner,
        spark_display=display,
        projection_name=display,
        sql_expr=result.sql_expr_part(),
        stable_name=False,
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable and not column._is_aggregate,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        partition_transform=column._partition_transform,
    )


def floor(col: Column | str) -> Column:
    """Floor (PySpark ``functions.floor``)."""
    return _scalar("floor", col)


def ceil(col: Column | str) -> Column:
    """Ceiling (PySpark ``functions.ceil``)."""
    return _scalar("ceil", col)


ceiling = ceil


def signum(col: Column | str) -> Column:
    """Sign as -1/0/1 (PySpark ``functions.signum``)."""
    return _scalar("signum", col)


def exp(col: Column | str) -> Column:
    """Natural exponential (PySpark ``functions.exp``)."""
    return _scalar("exp", col)


def pow(col1: Column | str | float | int, col2: Column | str | float | int) -> Column:
    """Power (PySpark ``functions.pow``)."""
    left = (
        col1 if isinstance(col1, Column) else lit(col1) if not isinstance(col1, str) else col(col1)
    )
    right = (
        col2 if isinstance(col2, Column) else lit(col2) if not isinstance(col2, str) else col(col2)
    )
    return _scalar("pow", left, right)


def power(col1: Column | str | float | int, col2: Column | str | float | int) -> Column:
    """Alias of :func:`pow` (PySpark ``functions.power``)."""
    return pow(col1, col2)


def round(col: Column | str, scale: int = 0) -> Column:
    """Round to ``scale`` decimals (PySpark ``functions.round``)."""
    return _scalar("round", col, scale, lit_indices=frozenset({1}))


def log(col: Column | str) -> Column:
    """Natural logarithm (PySpark ``functions.log`` — base *e*, not log10)."""
    return _scalar("log", col)


def log10(col: Column | str) -> Column:
    """Base-10 logarithm (PySpark ``functions.log10``)."""
    return _scalar("log10", col)


def md5(col: Column | str) -> Column:
    """MD5 hex digest (PySpark ``functions.md5``)."""
    return _scalar("md5", col)


def isnan(col: Column | str) -> Column:
    """True when value is NaN (PySpark ``functions.isnan``)."""
    return _scalar("isnan", col)


def isnull(col: Column | str) -> Column:
    """True when value is NULL (PySpark ``functions.isnull``).

    Sticky free/aggregate identity matches :meth:`Column.is_null` so
    ``select(sum(x), isnull(id))`` is ``[MISSING_GROUP_BY]`` and ``isnull(sum(x))`` stays
    global-agg (octo C4-L-003). Structural SQL is ``IS NULL`` (DataFusion has no ``isnull``
    scalar).
    """
    column = _column_argument(col)
    null_column = column.is_null()
    display = f"isnull({column.spark_wrap_display_part()})"
    return Column(
        null_column._inner,
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=f"({column.sql_expr_part()} IS NULL)",
        is_aggregate=column._is_aggregate,
        is_foldable=column._is_foldable and not column._is_aggregate,
        has_free_attribute=column._has_free_attribute,
        has_ungroupable=column._has_ungroupable,
        partition_transform=column._partition_transform,
    )


def nanvl(col1: Column | str, col2: Column | str | float | int) -> Column:
    """Replace NaN with ``col2`` (PySpark ``functions.nanvl``)."""
    return _scalar("nanvl", col1, col2)


def greatest(*cols: Column | str) -> Column:
    """Row-wise maximum (PySpark ``functions.greatest``).

    E1: requires ≥2 columns — ``WRONG_NUM_COLUMNS`` with keys ``func_name`` / ``num_cols``.
    """
    if len(cols) < 2:
        raise PySparkValueError(
            errorClass="WRONG_NUM_COLUMNS",
            messageParameters={"func_name": "greatest", "num_cols": "2"},
        )
    return _scalar("greatest", *cols)


def least(*cols: Column | str) -> Column:
    """Row-wise minimum (PySpark ``functions.least``).

    E1: requires ≥2 columns — same ``WRONG_NUM_COLUMNS`` bar as :func:`greatest`.
    """
    if len(cols) < 2:
        raise PySparkValueError(
            errorClass="WRONG_NUM_COLUMNS",
            messageParameters={"func_name": "least", "num_cols": "2"},
        )
    return _scalar("least", *cols)


# ---- E1 error-class pre-check stubs (type/arg validation only; happy path loud) ----------------


def _require_column_or_str(value: object, arg_name: str) -> None:
    """Raise ``NOT_COLUMN_OR_STR`` when ``value`` is neither Column nor str (E1 check_error bar)."""
    if not isinstance(value, (str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={
                "arg_name": arg_name,
                "arg_type": type(value).__name__,
            },
        )


def from_csv(
    col: Column | str,
    schema: Column | str,
    options: dict[str, str] | None = None,
) -> Column:
    """Parse a CSV string column (PySpark ``functions.from_csv``).

    E1: type pre-check only (``NOT_COLUMN_OR_STR`` on ``schema``). Happy path is loud-
    unsupported until a CSV parse kernel lands.
    """
    _ = options
    _require_column_or_str(col, "col")
    _require_column_or_str(schema, "schema")
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.from_csv is not supported yet (CSV parse kernel deferred; disclosed E1)"
    )


def from_xml(
    col: Column | str,
    schema: object,
    options: dict[str, str] | None = None,
) -> Column:
    """Parse an XML string column (PySpark ``functions.from_xml``).

    E1: type pre-check — ``schema`` must be StructType, Column, or str
    (``NOT_COLUMN_OR_STR_OR_STRUCT``).
    """
    from repark.types import StructType

    _ = options
    _require_column_or_str(col, "col")
    if not isinstance(schema, (StructType, str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR_OR_STRUCT",
            messageParameters={
                "arg_name": "schema",
                "arg_type": type(schema).__name__,
            },
        )
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.from_xml is not supported yet (XML parse kernel deferred; disclosed E1)"
    )


def schema_of_csv(csv: Column | str, options: dict[str, str] | None = None) -> Column:
    """Infer CSV schema as DDL (PySpark ``functions.schema_of_csv``). E1 type pre-check only."""
    _ = options
    _require_column_or_str(csv, "csv")
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.schema_of_csv is not supported yet (disclosed E1)"
    )


def schema_of_json(json: Column | str, options: dict[str, str] | None = None) -> Column:
    """Infer JSON schema as DDL (PySpark ``functions.schema_of_json``). E1 type pre-check only."""
    _ = options
    _require_column_or_str(json, "json")
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.schema_of_json is not supported yet (disclosed E1)"
    )


def schema_of_xml(xml: Column | str, options: dict[str, str] | None = None) -> Column:
    """Infer XML schema as DDL (PySpark ``functions.schema_of_xml``). E1 type pre-check only."""
    _ = options
    _require_column_or_str(xml, "xml")
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.schema_of_xml is not supported yet (disclosed E1)"
    )


def json_tuple(col: Column | str, *fields: str) -> Column:
    """Extract JSON fields as a row (PySpark ``functions.json_tuple``).

    E1: empty ``fields`` raises ``CANNOT_BE_EMPTY`` (Apache ``test_json_tuple_empty_fields``
    pins the message text via assertRaisesRegex).
    """
    if len(fields) == 0:
        # Apache test_json_tuple_empty_fields asserts message text via assertRaisesRegex
        # (oracle: "At least one field must be specified") — faithful template, G5.
        raise PySparkValueError(
            "At least one field must be specified",
            errorClass="CANNOT_BE_EMPTY",
            messageParameters={"item": "field"},
        )
    _require_column_or_str(col, "col")
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.json_tuple is not supported yet (JSON tuple kernel deferred; disclosed E1)"
    )


def raise_error(errMsg: Column | str) -> Column:  # noqa: N803 — PySpark arg name
    """Throw at evaluation with the given message (PySpark ``functions.raise_error``).

    E1: type pre-check (``NOT_COLUMN_OR_STR``). Happy path needs an engine raise kernel —
    residual ``test_raise_error`` until that lands.
    """
    if not isinstance(errMsg, (str, Column)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_STR",
            messageParameters={
                "arg_name": "errMsg",
                "arg_type": type(errMsg).__name__,
            },
        )
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.raise_error evaluation is not supported yet (engine raise kernel deferred; "
        "disclosed E1)"
    )


def current_date() -> Column:
    """Current date (PySpark ``functions.current_date``).

    Explicitly foldable so ``select(sum(x), current_date())`` is global-agg (C3-Q-002).
    Must not rely on vacuous ``all([])`` in ``_scalar`` (that path is non-foldable after
    octo C7-L-001).
    """
    return _scalar("current_date", foldable=True)


currentDate = current_date  # noqa: N816


def to_date(col: Column | str, format: str | None = None) -> Column:
    """Parse/cast to date (PySpark ``functions.to_date``).

    A bare ``str`` is a **column name** (Spark ColumnOrName), not a date literal. Pass
    ``lit("2020-01-02")`` for a string literal. ``format=`` is refused until Java-pattern
    parity is wired (engine uses Chrono ``%Y`` not Spark ``yyyy`` — octo C3-Q-002).
    """
    if format is not None:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "to_date(format=...) is not supported yet "
            "(engine Chrono patterns ≠ Spark Java patterns; use format-less to_date or SQL)"
        )
    return _scalar("to_date", col)


def to_timestamp(col: Column | str, format: str | None = None) -> Column:
    """Parse/cast to timestamp (PySpark ``functions.to_timestamp``).

    A bare ``str`` is a **column name**. ``format=`` refused (same Chrono/Java gap as to_date).
    """
    if format is not None:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "to_timestamp(format=...) is not supported yet "
            "(engine Chrono patterns ≠ Spark Java patterns; use format-less to_timestamp or SQL)"
        )
    return _scalar("to_timestamp", col)


def from_unixtime(col: Column | str | int) -> Column:
    """Epoch seconds → **string** timestamp (PySpark ``functions.from_unixtime``)."""
    if isinstance(col, int) and not isinstance(col, bool):
        return _scalar("from_unixtime", lit(col))
    return _scalar("from_unixtime", col)


def date_sub(start: Column | str, days: Column | int | str) -> Column:
    """Subtract days (PySpark ``functions.date_sub``) via ``date_add(..., -days)``."""
    if isinstance(days, bool):
        raise PySparkTypeError("date_sub days must be int, Column, or column name (str)")
    if isinstance(days, int):
        return date_add(start, -days)
    # Column or column-name str (SPARK-37738 / octo C3).
    return date_add(start, lit(0) - _integer_argument(days))


# Explicitly unsupported this unit (engine gap) — loud helpers for dogfood discoverability.
def split(str: Column | str, pattern: str, limit: int = -1) -> Column:
    """Unsupported: engine has no Spark ``split`` (use SQL when available)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.split is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def regexp_extract(str: Column | str, pattern: str, idx: int) -> Column:
    """Unsupported: engine has no ``regexp_extract``."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.regexp_extract is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def datediff(end: Column | str, start: Column | str) -> Column:
    """Unsupported: engine has no ``datediff``."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.datediff is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def months_between(date1: Column | str, date2: Column | str, roundOff: bool = True) -> Column:  # noqa: N803
    """Unsupported: engine has no ``months_between``."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.months_between is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def unix_timestamp(
    timestamp: Column | str | None = None,
    format: str | None = None,
) -> Column:
    """Unsupported: engine has no ``unix_timestamp``."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.unix_timestamp is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def hash(*cols: Column | str) -> Column:
    """Unsupported: engine has no Spark ``hash`` (xxhash-style)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.hash is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def struct(*cols: Column | str) -> Column:
    """Build a struct column (PySpark ``functions.struct``).

    Lowers to the engine ``struct`` expression over the child columns so ``df.select`` can
    bind field references (free-SQL ``named_struct`` cannot — no FROM schema at construct
    time). Field names follow argument names; structural ``sql_expr`` still emits
    ``named_struct`` for free-SQL surfaces.
    """
    if not cols:
        raise PySparkTypeError("struct() requires at least one column")
    columns: list[Column] = []
    named_parts: list[str] = []
    display_parts: list[str] = []
    free = False
    for index, item in enumerate(cols):
        if isinstance(item, str):
            column = col(item)
            field_name = item
            free = True
        elif isinstance(item, Column):
            column = item
            field_name = item._projection_name or item._spark_display or f"col{index}"
            free = free or bool(item._has_free_attribute)
        else:
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_STR",
                messageParameters={
                    "arg_name": "cols",
                    "arg_type": type(item).__name__,
                },
            )
        columns.append(column)
        safe_name = str(field_name).replace("'", "''")
        named_parts.append(f"'{safe_name}', {column.sql_expr_part()}")
        display_parts.append(str(field_name))
    sql = f"named_struct({', '.join(named_parts)})"
    display = f"struct({', '.join(display_parts)})"
    # Alias children to field names so DF ``struct`` keeps Spark field names (c0/c1 otherwise).
    named_natives = [
        column._inner.alias(str(name)) for column, name in zip(columns, display_parts, strict=True)
    ]
    return Column(
        _native.PyColumn.make_struct(named_natives),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=sql,
        has_free_attribute=free,
    )


def array(*cols: Column | str | int | float | bool | None) -> Column:
    """Build an array column (PySpark ``functions.array``; X1 → engine ``make_array``)."""
    return _scalar("array", *cols)


def array_contains(col: Column | str, value: Column | str | int | float) -> Column:
    """True when the array column contains ``value`` (PySpark ``functions.array_contains``).

    # === r21 T7: census-r6 ===
    Lowers via engine ``array_has`` (DataFusion name for Spark ``array_contains``).
    Literal ``value`` is forced so string needles are not misread as column names.
    """
    lit_indices = frozenset({1}) if not isinstance(value, Column) else None
    left = _as_column_arg(col, as_lit=False)
    right = _as_column_arg(value, as_lit=not isinstance(value, Column))  # type: ignore[arg-type]
    shown = f"array_contains({left.spark_wrap_display_part()}, {right.spark_wrap_display_part()})"
    return _scalar("array_has", col, value, lit_indices=lit_indices, display=shown)


def format_string(format: str, *cols: Column | str) -> Column:
    """Unsupported: ``format_string`` / printf not wired."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.format_string is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


# ---- R-FN-BATCH2: strings / regexp / collection wrappers --------------------------------------


def reverse(col: Column | str) -> Column:
    """Reverse a string (PySpark ``functions.reverse``)."""
    return _scalar("reverse", col)


def repeat(col: Column | str, n: Column | int) -> Column:
    """Repeat a string ``n`` times (PySpark ``functions.repeat``)."""
    return _scalar("repeat", col, n, lit_indices=frozenset({1}) if isinstance(n, int) else None)


def translate(src: Column | str, matching: str, replace: str) -> Column:
    """Character-wise translate (PySpark ``functions.translate``)."""
    return _scalar("translate", src, matching, replace, lit_indices=frozenset({1, 2}))


def substring_index(str: Column | str, delim: str, count: int) -> Column:
    """Substring before ``count`` occurrences of ``delim`` (PySpark ``substring_index``)."""
    return _scalar("substring_index", str, delim, count, lit_indices=frozenset({1, 2}))


def levenshtein(left: Column | str, right: Column | str) -> Column:
    """Levenshtein edit distance (PySpark ``functions.levenshtein``)."""
    return _scalar("levenshtein", left, right)


def ascii(col: Column | str) -> Column:
    """Unicode code point of the first character (PySpark ``functions.ascii``)."""
    return _scalar("ascii", col)


def chr(col: Column | str | int) -> Column:
    """Unicode code point → character (PySpark ``functions.chr``)."""
    if isinstance(col, int) and not isinstance(col, bool):
        return _scalar("chr", lit(col))
    return _scalar("chr", col)


def overlay(
    src: Column | str,
    replace: Column | str,
    pos: Column | int,
    len: Column | int | None = None,
) -> Column:
    """Overlay ``replace`` onto ``src`` at ``pos`` (PySpark ``functions.overlay``).

    When ``len`` is omitted (or the Spark default ``-1``), display shows ``-1`` and the
    engine uses the 3-arg form (replace-length semantics). DataFusion's 4-arg ``len=-1``
    replaces the *remainder* of the string — not Spark — so literal ``-1`` must not be
    forwarded as a 4th arg (F2 / octo C1-Q-002).
    """
    # Apache test_overlay: float pos/len → NOT_COLUMN_OR_INT_OR_STR (octo C2-Q-002).
    if not isinstance(pos, (Column, int, str)) or isinstance(pos, bool):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={"arg_name": "pos", "arg_type": type(pos).__name__},
        )
    if len is not None and (not isinstance(len, (Column, int, str)) or isinstance(len, bool)):
        raise PySparkTypeError(
            errorClass="NOT_COLUMN_OR_INT_OR_STR",
            messageParameters={"arg_name": "len", "arg_type": type(len).__name__},
        )
    # Spark default len = -1 means "use length of replace" (same as omit / 3-arg).
    if len is None or (isinstance(len, int) and not isinstance(len, bool) and len == -1):
        result = _scalar("overlay", src, replace, pos)
        display = (
            f"overlay({_as_column_arg(src, as_lit=False).spark_display_part()}, "
            f"{_as_column_arg(replace, as_lit=False).spark_display_part()}, "
            f"{_as_column_arg(pos, as_lit=isinstance(pos, int)).spark_display_part()}, -1)"
        )
        return Column(
            result._inner,
            spark_display=display,
            projection_name=display,
            stable_name=False,
            sql_expr=result.sql_expr_part(),
            is_foldable=result._is_foldable,
            is_aggregate=result._is_aggregate,
            has_free_attribute=result._has_free_attribute,
            has_ungroupable=result._has_ungroupable,
        )
    return _scalar("overlay", src, replace, pos, len)


def find_in_set(str: Column | str, str_array: Column | str) -> Column:
    """1-based index of ``str`` in comma-separated ``str_array`` (PySpark ``find_in_set``)."""
    return _scalar("find_in_set", str, str_array)


def locate(substr: str, str: Column | str, pos: int | None = None) -> Column:
    """1-based index of ``substr`` in ``str`` (PySpark ``functions.locate``).

    ``pos`` start offset is not supported yet (engine strpos is full-string) — raise if set.
    """
    if pos is not None and pos != 1:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "functions.locate(pos=...) start offset is not supported yet "
            "(engine strpos has no start; disclosed R-FN-BATCH2)"
        )
    return _scalar("locate", lit(substr), str)


def position(substr: Column | str, str: Column | str | None = None) -> Column:
    """Position of substring (PySpark ``functions.position``)."""
    if str is None:
        from repark.errors import PySparkTypeError

        raise PySparkTypeError("position requires (substr, str)")
    return _scalar("position", substr, str)


def base64(col: Column | str) -> Column:
    """Base64-encode a string/binary (PySpark ``functions.base64`` → encode base64)."""
    return _scalar("base64", col)


def unbase64(col: Column | str) -> Column:
    """Base64-decode (PySpark ``functions.unbase64`` → decode base64)."""
    return _scalar("unbase64", col)


def encode(col: Column | str, charset: str) -> Column:
    """Encode string with charset (PySpark ``functions.encode``).

    Engine supports ``base64`` and ``hex`` (not full Java charset set) — other names
    raise from the engine.
    """
    return _scalar("encode", col, charset, lit_indices=frozenset({1}))


def decode(col: Column | str, charset: str) -> Column:
    """Decode binary with charset (PySpark ``functions.decode``). Engine: base64/hex."""
    return _scalar("decode", col, charset, lit_indices=frozenset({1}))


def size(col: Column | str) -> Column:
    """Array/map cardinality (PySpark ``functions.size`` → engine ``cardinality``)."""
    return _scalar("size", col)


def array_distinct(col: Column | str) -> Column:
    """Distinct elements of an array (PySpark ``functions.array_distinct``)."""
    return _scalar("array_distinct", col)


def array_except(col1: Column | str, col2: Column | str) -> Column:
    """Array set difference (PySpark ``functions.array_except``)."""
    return _scalar("array_except", col1, col2)


def array_intersect(col1: Column | str, col2: Column | str) -> Column:
    """Array set intersection (PySpark ``functions.array_intersect``)."""
    return _scalar("array_intersect", col1, col2)


def array_union(col1: Column | str, col2: Column | str) -> Column:
    """Array set union (PySpark ``functions.array_union``)."""
    return _scalar("array_union", col1, col2)


def array_join(col: Column | str, delimiter: str, null_replacement: str | None = None) -> Column:
    """Join array elements with delimiter (PySpark ``functions.array_join``)."""
    if null_replacement is not None:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "functions.array_join(null_replacement=...) is not supported yet "
            "(engine array_to_string is 2-arg; disclosed R-FN-BATCH2)"
        )
    return _scalar("array_join", col, delimiter, lit_indices=frozenset({1}))


def array_max(col: Column | str) -> Column:
    """Max element of an array (PySpark ``functions.array_max``)."""
    return _scalar("array_max", col)


def array_min(col: Column | str) -> Column:
    """Min element of an array (PySpark ``functions.array_min``)."""
    return _scalar("array_min", col)


def array_position(col: Column | str, value: Column | str | int | float) -> Column:
    """1-based index of value in array (PySpark ``functions.array_position``)."""
    return _scalar("array_position", col, value)


def array_remove(col: Column | str, element: Column | str | int | float) -> Column:
    """Remove all occurrences of element (PySpark ``functions.array_remove``)."""
    return _scalar("array_remove", col, element)


def array_repeat(element: Column | str | int | float, count: Column | int) -> Column:
    """Repeat element into an array (PySpark ``functions.array_repeat``)."""
    return _scalar("array_repeat", element, count)


def array_sort(col: Column | str, asc: bool | None = None) -> Column:
    """Sort array ascending by default (PySpark ``functions.array_sort``)."""
    if asc is None or asc is True:
        return _scalar("array_sort", col)
    return _scalar("array_sort", col, lit("DESC"))


def sort_array(col: Column | str, asc: bool = True) -> Column:
    """Alias of :func:`array_sort` (PySpark ``functions.sort_array``)."""
    return array_sort(col, asc=asc)


def slice(x: Column | str, start: Column | int, length: Column | int) -> Column:
    """Spark ``slice(array, start, length)`` (1-based, length count)."""
    return _scalar("slice", x, start, length)


def flatten(col: Column | str) -> Column:
    """Flatten one level of nested arrays (PySpark ``functions.flatten``)."""
    return _scalar("flatten", col)


def map_keys(col: Column | str) -> Column:
    """Keys of a map (PySpark ``functions.map_keys``)."""
    return _scalar("map_keys", col)


def map_values(col: Column | str) -> Column:
    """Values of a map (PySpark ``functions.map_values``)."""
    return _scalar("map_values", col)


def map_entries(col: Column | str) -> Column:
    """Entries of a map as array of structs (PySpark ``functions.map_entries``)."""
    return _scalar("map_entries", col)


def sequence(start: Column | int, stop: Column | int, step: Column | int | None = None) -> Column:
    """Generate sequence (PySpark ``functions.sequence`` → engine ``generate_series``)."""
    if step is None:
        return _scalar("sequence", start, stop)
    return _scalar("sequence", start, stop, step)


def elt(n: Column | int, *inputs: Column | str) -> Column:
    """1-based pick among inputs (PySpark ``functions.elt``)."""
    if not inputs:
        from repark.errors import AnalysisException

        raise AnalysisException("elt requires at least one value after the index")
    return _scalar("elt", n, *inputs)


def soundex(col: Column | str) -> Column:
    """Unsupported: engine has no ``soundex`` (R-FN-BATCH2 census)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.soundex is not supported yet (engine gap; disclosed R-FN-BATCH2)"
    )


def sentences(col: Column | str, language: Column | str | None = None) -> Column:
    """Unsupported: engine has no ``sentences`` (R-FN-BATCH2 census)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.sentences is not supported yet (engine gap; disclosed R-FN-BATCH2)"
    )


def arrays_zip(*cols: Column | str) -> Column:
    """Unsupported: engine has no ``arrays_zip`` (R-FN-BATCH2 census)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.arrays_zip is not supported yet (engine gap; disclosed R-FN-BATCH2)"
    )


def map_from_arrays(col1: Column | str, col2: Column | str) -> Column:
    """Unsupported as Column builder (SQL ``map_from_arrays`` may work; R-FN-BATCH2)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.map_from_arrays Column builder not supported yet "
        "(use SQL map_from_arrays; disclosed R-FN-BATCH2)"
    )


# ---- R-FN-BATCH3: datetime / interval / formatting --------------------------------------------


def next_day(date: Column | str, dayOfWeek: str) -> Column:  # noqa: N803 — PySpark name
    """Next day-of-week on or after ``date`` (PySpark ``functions.next_day``)."""
    return _scalar("next_day", date, dayOfWeek, lit_indices=frozenset({1}))


def hour(col: Column | str) -> Column:
    """Hour of day 0..23 (PySpark ``functions.hour``)."""
    return _scalar("hour", col)


def minute(col: Column | str) -> Column:
    """Minute 0..59 (PySpark ``functions.minute``)."""
    return _scalar("minute", col)


def second(col: Column | str) -> Column:
    """Second 0..59 (PySpark ``functions.second``)."""
    return _scalar("second", col)


def date_part(field: str, source: Column | str) -> Column:
    """Extract calendar field (PySpark ``functions.date_part``)."""
    return _scalar("date_part", field, source, lit_indices=frozenset({0}))


def extract(field: str, source: Column | str) -> Column:
    """Alias of :func:`date_part` (PySpark ``functions.extract``)."""
    return date_part(field, source)


def timestamp_seconds(col: Column | str | int) -> Column:
    """Epoch seconds → timestamp (PySpark ``functions.timestamp_seconds``)."""
    if isinstance(col, int) and not isinstance(col, bool):
        return _scalar("timestamp_seconds", lit(col))
    return _scalar("timestamp_seconds", col)


def timestamp_millis(col: Column | str | int) -> Column:
    """Epoch millis → timestamp (PySpark ``functions.timestamp_millis``)."""
    if isinstance(col, int) and not isinstance(col, bool):
        return _scalar("timestamp_millis", lit(col))
    return _scalar("timestamp_millis", col)


def timestamp_micros(col: Column | str | int) -> Column:
    """Epoch micros → timestamp (PySpark ``functions.timestamp_micros``)."""
    if isinstance(col, int) and not isinstance(col, bool):
        return _scalar("timestamp_micros", lit(col))
    return _scalar("timestamp_micros", col)


def format_number(col: Column | str, d: int) -> Column:
    """Unsupported: Spark ``format_number`` not wired (R-FN-BATCH3 census)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.format_number is not supported yet (engine gap; disclosed R-FN-BATCH3)"
    )


def try_to_timestamp(col: Column | str, format: str | None = None) -> Column:
    """Unsupported: ``try_to_timestamp`` not wired (R-FN-BATCH3 census)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.try_to_timestamp is not supported yet (engine gap; disclosed R-FN-BATCH3)"
    )


def to_utc_timestamp(timestamp: Column | str, tz: str) -> Column:
    """Unsupported: timezone conversion not wired (R-FN-BATCH3 census)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.to_utc_timestamp is not supported yet (engine gap; disclosed R-FN-BATCH3)"
    )


def from_utc_timestamp(timestamp: Column | str, tz: str) -> Column:
    """Unsupported: timezone conversion not wired (R-FN-BATCH3 census)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.from_utc_timestamp is not supported yet (engine gap; disclosed R-FN-BATCH3)"
    )


def make_timestamp(
    years: Column | int,
    months: Column | int,
    days: Column | int,
    hours: Column | int,
    mins: Column | int,
    secs: Column | float | int,
    timezone: str | None = None,
) -> Column:
    """Unsupported: ``make_timestamp`` not wired (R-FN-BATCH3 census; use make_date + SQL)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.make_timestamp is not supported yet (engine gap; disclosed R-FN-BATCH3)"
    )


# ---- R-FN-BATCH4: aggregates / stats / hashes / ids -------------------------------------------


def stddev(col: Column | str) -> Column:
    """Sample standard deviation (PySpark ``functions.stddev`` / ``stddev_samp``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"stddev({part})"
    return Column(
        column._inner.aggregate("stddev", False),
        agg_name=agg_name,
        # Structural quoted sql_expr for free-SQL global-agg (octo C4-Q-002 / C4-SEC-001).
        sql_expr=f"stddev({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def stddev_samp(col: Column | str) -> Column:
    """Alias of :func:`stddev`."""
    return stddev(col)


def stddev_pop(col: Column | str) -> Column:
    """Population standard deviation (PySpark ``functions.stddev_pop``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"stddev_pop({part})"
    return Column(
        column._inner.aggregate("stddev_pop", False),
        agg_name=agg_name,
        sql_expr=f"stddev_pop({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def variance(col: Column | str) -> Column:
    """Sample variance (PySpark ``functions.variance`` / ``var_samp``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"variance({part})"
    # DataFusion SQL name is ``var_samp`` (``variance`` is not registered — octo C4-Q-002).
    return Column(
        column._inner.aggregate("variance", False),
        agg_name=agg_name,
        sql_expr=f"var_samp({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def var_samp(col: Column | str) -> Column:
    """Alias of :func:`variance`."""
    return variance(col)


def var_pop(col: Column | str) -> Column:
    """Population variance (PySpark ``functions.var_pop``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"var_pop({part})"
    return Column(
        column._inner.aggregate("var_pop", False),
        agg_name=agg_name,
        sql_expr=f"var_pop({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def median(col: Column | str) -> Column:
    """Exact median aggregate (PySpark ``functions.median``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"median({part})"
    return Column(
        column._inner.aggregate("median", False),
        agg_name=agg_name,
        sql_expr=f"median({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def corr(col1: Column | str, col2: Column | str) -> Column:
    """Pearson correlation (PySpark ``functions.corr``)."""
    left, left_part = _aggregate_argument(col1)
    right, right_part = _aggregate_argument(col2)
    agg_name = f"corr({left_part}, {right_part})"
    return Column(
        left._inner.aggregate_binary("corr", right._inner),
        agg_name=agg_name,
        sql_expr=f"corr({left.sql_expr_part()}, {right.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=_partition_transform_of(left, right),
    )


def covar_pop(col1: Column | str, col2: Column | str) -> Column:
    """Population covariance (PySpark ``functions.covar_pop``)."""
    left, left_part = _aggregate_argument(col1)
    right, right_part = _aggregate_argument(col2)
    agg_name = f"covar_pop({left_part}, {right_part})"
    return Column(
        left._inner.aggregate_binary("covar_pop", right._inner),
        agg_name=agg_name,
        sql_expr=f"covar_pop({left.sql_expr_part()}, {right.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=_partition_transform_of(left, right),
    )


def covar_samp(col1: Column | str, col2: Column | str) -> Column:
    """Sample covariance (PySpark ``functions.covar_samp``)."""
    left, left_part = _aggregate_argument(col1)
    right, right_part = _aggregate_argument(col2)
    agg_name = f"covar_samp({left_part}, {right_part})"
    return Column(
        left._inner.aggregate_binary("covar_samp", right._inner),
        agg_name=agg_name,
        sql_expr=f"covar_samp({left.sql_expr_part()}, {right.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=_partition_transform_of(left, right),
    )


def bit_and(col: Column | str) -> Column:
    """Bitwise AND aggregate (PySpark ``functions.bit_and``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"bit_and({part})"
    return Column(
        column._inner.aggregate("bit_and", False),
        agg_name=agg_name,
        sql_expr=f"bit_and({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def bit_or(col: Column | str) -> Column:
    """Bitwise OR aggregate (PySpark ``functions.bit_or``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"bit_or({part})"
    return Column(
        column._inner.aggregate("bit_or", False),
        agg_name=agg_name,
        sql_expr=f"bit_or({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def bit_xor(col: Column | str) -> Column:
    """Bitwise XOR aggregate (PySpark ``functions.bit_xor``)."""
    column, part = _aggregate_argument(col)
    agg_name = f"bit_xor({part})"
    return Column(
        column._inner.aggregate("bit_xor", False),
        agg_name=agg_name,
        sql_expr=f"bit_xor({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def sha2(col: Column | str, numBits: int) -> Column:  # noqa: N803
    """SHA-2 hash; only 256-bit supported via engine ``sha256`` (R-FN-BATCH4)."""
    if numBits != 256:
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            f"functions.sha2(numBits={numBits}) only 256 is supported "
            "(engine sha256; disclosed R-FN-BATCH4)"
        )
    return _scalar("sha256", col)


def sha1(col: Column | str) -> Column:
    """Unsupported: engine has no ``sha1`` (use sha2(..., 256); R-FN-BATCH4)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.sha1 is not supported yet (engine gap; disclosed R-FN-BATCH4)"
    )


def crc32(col: Column | str) -> Column:
    """Unsupported: engine has no ``crc32`` (R-FN-BATCH4)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.crc32 is not supported yet (engine gap; disclosed R-FN-BATCH4)"
    )


def xxhash64(col: Column | str) -> Column:
    """Unsupported: engine has no ``xxhash64`` (R-FN-BATCH4)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.xxhash64 is not supported yet (engine gap; disclosed R-FN-BATCH4)"
    )


def rand(seed: int | None = None) -> Column:
    """Uniform random [0, 1) (PySpark ``functions.rand``).

    **r20 G2:** seeded via Spark XORShiftRandom (``seed + partitionIndex``; repark
    partitionIndex=0). Same seed ⇒ same values **per partition layout** (single-batch
    MemTable layout matches Spark single-partition tasks).

    **Seed contract (honest divergence):** an **omitted** seed lowers to engine seed ``0``
    (stable XORShift sequence) so range-only callers and sampleBy default stay deterministic.
    Live Spark unseeded ``rand()`` draws a fresh non-deterministic seed per query — do **not**
    treat repark unseeded ``rand()`` as Spark-random; pass an explicit seed for parity with
    Spark seeded calls. Multi-batch layouts restart the sequence per batch (disclosed residual).

    Non-foldable and ungroupable (Spark ``Rand``): ``select(sum(x), rand())`` and nested
    ``sum(x)+rand()`` raise ``[MISSING_GROUP_BY]`` — not pure_global (octo C7-L-001).
    """
    # === r20 G2: window/rand/sampleBy ===
    if seed is None:
        return _scalar("rand", has_ungroupable=True)
    if isinstance(seed, bool) or not isinstance(seed, int):
        from repark.errors import PySparkTypeError

        raise PySparkTypeError(
            errorClass="NOT_INT",
            messageParameters={"arg_name": "seed", "arg_type": type(seed).__name__},
        )
    return _scalar("rand", int(seed), lit_indices=frozenset({0}), has_ungroupable=True)


def randn(seed: int | None = None) -> Column:
    """Standard normal (PySpark ``functions.randn``).

    **r20 G2:** Spark XORShift + java.util.Random polar Gaussian. Seed contract same as
    :func:`rand` (per partition layout; partitionIndex=0; unseeded → seed ``0``, not Spark's
    non-deterministic unseeded path — see :func:`rand`).
    """
    # === r20 G2: window/rand/sampleBy ===
    if seed is None:
        return _scalar("randn", has_ungroupable=True)
    if isinstance(seed, bool) or not isinstance(seed, int):
        from repark.errors import PySparkTypeError

        raise PySparkTypeError(
            errorClass="NOT_INT",
            messageParameters={"arg_name": "seed", "arg_type": type(seed).__name__},
        )
    return _scalar("randn", int(seed), lit_indices=frozenset({0}), has_ungroupable=True)


def random(seed: int | None = None) -> Column:
    """Alias of :func:`rand` (SQL ``random`` spelling)."""
    return rand(seed)


def skewness(col: Column | str) -> Column:
    """Unsupported: engine has no ``skewness`` (R-FN-BATCH4)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.skewness is not supported yet (engine gap; disclosed R-FN-BATCH4)"
    )


def kurtosis(col: Column | str) -> Column:
    """Unsupported: engine has no ``kurtosis`` (R-FN-BATCH4)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.kurtosis is not supported yet (engine gap; disclosed R-FN-BATCH4)"
    )


def percentile_approx(
    col: Column | str,
    percentage: float | list[float] | tuple[float, ...],
    accuracy: int | None = None,
) -> Column:
    """Approximate percentile (PySpark ``functions.percentile_approx``).

    Lowers to engine ``approx_percentile_cont`` (DataFusion t-digest). Spark uses
    Greenwald-Khanna QuantileSummaries — values may differ within approximation bounds;
    oracles pin bounds-windows, never cross-engine exact equality.

    ``accuracy`` is **accepted and ignored** (t-digest has no Greenwald-Khanna accuracy
    knob). Array/list/tuple of percentages is not shipped yet (engine returns a scalar
    per call) — loud STOP seed.
    """
    if isinstance(percentage, (list, tuple)):
        from repark.errors import UnsupportedOperationException

        raise UnsupportedOperationException(
            "functions.percentile_approx(array_of_percentages) is not supported yet "
            "(engine approx_percentile_cont is scalar-only; named seed: "
            "percentile_approx_array_percentages)"
        )
    # bool is a subclass of int — reject before the numeric branch (octo F-Q1-008).
    if isinstance(percentage, bool) or not isinstance(percentage, (int, float)):
        from repark.errors import PySparkTypeError

        raise PySparkTypeError(
            f"percentile_approx percentage must be float or sequence of float, "
            f"got {type(percentage).__name__}"
        )
    pct = float(percentage)
    if not 0.0 <= pct <= 1.0:
        from repark.errors import IllegalArgumentException

        raise IllegalArgumentException(f"percentile_approx percentage must be in [0, 1], got {pct}")
    # accuracy: facade accepts-and-ignores (Spark GK relative-error knob has no t-digest
    # equivalent). Free-SQL `percentile_approx(col, p, n)` is a *different* path: DataFusion
    # treats the optional third arg as t-digest **centroids**, not Spark accuracy
    # (octo F-Q1-001; dual-path also noted on this module's map entry).
    del accuracy
    column, part = _aggregate_argument(col)
    # Spark display name keeps the user-facing Spark name; SQL path uses the engine name
    # (aliases percentile_approx / approx_percentile are registered for free-SQL too).
    agg_name = f"percentile_approx({part}, {pct})"
    return Column(
        column._inner.approx_percentile_cont(pct),
        agg_name=agg_name,
        sql_expr=f"approx_percentile_cont({column.sql_expr_part()}, {pct})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=column._partition_transform,
    )


def approx_percentile(
    col: Column | str,
    percentage: float | list[float] | tuple[float, ...],
    accuracy: int | None = None,
) -> Column:
    """Alias of :func:`percentile_approx` (PySpark ``functions.approx_percentile``)."""
    return percentile_approx(col, percentage, accuracy)


def mode(col: Column | str) -> Column:
    """Unsupported: engine has no ``mode`` (R-FN-BATCH4)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.mode is not supported yet (engine gap; disclosed R-FN-BATCH4)"
    )


def monotonically_increasing_id() -> Column:
    """Unsupported: single-node id generator not wired (R-FN-BATCH4 disclosed)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.monotonically_increasing_id is not supported yet "
        "(single-node semantics disclosed; R-FN-BATCH4)"
    )


def spark_partition_id() -> Column:
    """Unsupported: single-node partition id (always 0 if implemented; R-FN-BATCH4 loud)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.spark_partition_id is not supported yet (single-node disclosed; R-FN-BATCH4)"
    )


def input_file_name() -> Column:
    """Unsupported: input_file_name not wired (R-FN-BATCH4 disclosed)."""
    from repark.errors import UnsupportedOperationException

    raise UnsupportedOperationException(
        "functions.input_file_name is not supported yet (disclosed R-FN-BATCH4)"
    )


def explode(column: Column | str) -> Column:
    """Generator: one row per array element; drop null/empty arrays (PySpark ``explode``).

    Lowered at :meth:`~repark.dataframe.DataFrame.select` time via a guarded DataFusion
    ``unnest`` (R-EXPLODE-REWRITE): pre-filter null/empty so the engine null-list gap is
    avoided. Only one generator per select list.

    A bare ``str`` is a **column name** (Spark ColumnOrName), not a string literal
    (octo C1-Q-001). Pre-aliased inputs strip trailing ``AS name`` so unnest never
    embeds illegal alias SQL (octo C1-Q-005). Nested generators
    (``explode(explode_outer(...))``) refuse loud — overwriting kind would silently drop
    null/empty rows (octo C5-L-002 / C5-Q-001). Sticky aggregate arguments
    (``explode(collect_list(x))`` / ``explode(array_repeat(sum(x), 1))``) refuse with
    ``[MISSING_GROUP_BY]`` — building a generator Column would strip ``_is_aggregate`` and
    let ``select`` enter unnest mid-project instead of the F1xF3 gate (combine octo C4-Q-001).
    """
    array_column = _column_argument(column)
    array_column._reject_nested_generator("explode")
    _reject_aggregate_generator_argument(array_column, "explode")
    array_sql = array_column.sql_expr_without_alias()
    return Column(
        array_column._inner,  # placeholder; select rewrites via SQL
        spark_display=f"explode({array_sql})",
        sql_expr=array_sql,
        projection_name="col",  # Spark default name for explode
        generator="explode",
    )


def explode_outer(column: Column | str) -> Column:
    """Generator: one row per element; null/empty arrays yield one null row (``explode_outer``).

    Lowered via ``unnest(CASE WHEN null/empty THEN array(NULL) ELSE col END)`` so DataFusion's
    null-list ``unnest`` gap is avoided without forking the engine (R-EXPLODE-REWRITE).

    A bare ``str`` is a column name, not a literal (octo C1-Q-001). Pre-aliased inputs
    strip trailing ``AS name`` (octo C1-Q-005). Nested generators refuse loud (octo C5-L-002).
    Sticky aggregate arguments refuse ``[MISSING_GROUP_BY]`` (combine octo C4-Q-001).
    """
    array_column = _column_argument(column)
    array_column._reject_nested_generator("explode_outer")
    _reject_aggregate_generator_argument(array_column, "explode_outer")
    array_sql = array_column.sql_expr_without_alias()
    return Column(
        array_column._inner,
        spark_display=f"explode_outer({array_sql})",
        sql_expr=array_sql,
        projection_name="col",
        generator="explode_outer",
    )


def _reject_aggregate_generator_argument(array_column: Column, operation: str) -> None:
    """Refuse explode* on sticky-aggregate args so select cannot unnest past MISSING_GROUP_BY.

    Combine octo C4-Q-001: ``explode`` / ``explode_outer`` previously built a generator
    Column without ``_is_aggregate``, so ``select(explode(collect_list(x)))`` bypassed the
    F1xF3 sibling gate (``select(explode, sum)``) and entered unnest mid-project.
    """
    if not array_column._is_aggregate:
        return
    from repark.errors import AnalysisException

    raise AnalysisException(
        "[MISSING_GROUP_BY] The query does not include a GROUP BY clause. "
        "Add GROUP BY or turn it into the window functions using OVER clauses. "
        f"(generator {operation} cannot wrap an aggregate expression)"
    )


def posexplode(column: Column | str) -> Column:
    """Generator with ordinal (PySpark ``posexplode``) — STOP: no DF unnest-with-ordinality.

    Raises :class:`~repark.errors.UnsupportedOperationException`. ``explode`` /
    ``explode_outer`` are delivered separately (partial WIN per R-EXPLODE-REWRITE).
    """
    from repark.errors import UnsupportedOperationException

    _ = column
    # r24 A3 octo C1-Q-001: do not embed a DataFusion major in the user-facing
    # message — the number rots (audit QUAL-06 cited the former "52.x" claim while
    # the wheel pins 54.1). Capability fact only.
    raise UnsupportedOperationException(
        "posexplode is not supported yet (no first-class unnest-with-ordinality; "
        "explode/explode_outer are available via guarded unnest rewrite)"
    )


def posexplode_outer(column: Column | str) -> Column:
    """``posexplode_outer`` — same STOP as :func:`posexplode`."""
    from repark.errors import UnsupportedOperationException

    _ = column
    raise UnsupportedOperationException(
        "posexplode_outer is not supported yet (see posexplode; use explode_outer without ordinal)"
    )
