"""Scalar / string / math / collection / remaining-agg wrappers (FN-SPLIT).

Move-only origin: every name here previously lived in ``repark.spark.functions``.
Public names are re-exported from ``functions.py``. Helpers ``_scalar`` /
``_as_column_arg`` stay imported from ``functions`` (they serve these wrappers).

FN-A (2026-08-15): ordering / null / math names land in this module.

FN-B (2026-08-15): string-function names land in this module.

FN-GT1 (2026-08-17): leftover string / utf8 thin-wires land in this module
(``split_part`` / ``regexp_count`` / ``regexp_instr`` / ``bit_length`` /
``octet_length`` / ``is_valid_utf8`` / ``make_valid_utf8``).
"""

from __future__ import annotations

import re

from repark import _native
from repark.errors import (
    AnalysisException,
    PySparkTypeError,
    PySparkValueError,
    UnsupportedOperationException,
)
from repark.spark._idents import sql_string_literal
from repark.spark.column import Column, Scalar
from repark.spark.functions import (
    _aggregate_argument,
    _as_column_arg,
    _column_argument,
    _integer_argument,
    _partition_transform_of,
    _scalar,
    coalesce,
    col,
    concat,
    date_add,
    date_format,
    lit,
)
from repark.spark.types import StructType


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

    raise UnsupportedOperationException(
        "functions.schema_of_csv is not supported yet (disclosed E1)"
    )


def schema_of_json(json: Column | str, options: dict[str, str] | None = None) -> Column:
    """Infer JSON schema as DDL (PySpark ``functions.schema_of_json``). E1 type pre-check only."""
    _ = options
    _require_column_or_str(json, "json")

    raise UnsupportedOperationException(
        "functions.schema_of_json is not supported yet (disclosed E1)"
    )


def schema_of_xml(xml: Column | str, options: dict[str, str] | None = None) -> Column:
    """Infer XML schema as DDL (PySpark ``functions.schema_of_xml``). E1 type pre-check only."""
    _ = options
    _require_column_or_str(xml, "xml")

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

    raise UnsupportedOperationException(
        "functions.split is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def regexp_extract(str: Column | str, pattern: str, idx: int) -> Column:
    """Unsupported: engine has no ``regexp_extract``."""

    raise UnsupportedOperationException(
        "functions.regexp_extract is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def datediff(end: Column | str, start: Column | str) -> Column:
    """Days from ``start`` to ``end`` (PySpark ``functions.datediff``).

    Spark's older spelling of :func:`date_diff`; PySpark 4.1.2 declares both with the same
    ``(end, start)`` order over the same Catalyst expression, so they share one engine arm.
    """
    return _scalar("datediff", end, start)


def months_between(date1: Column | str, date2: Column | str, roundOff: bool = True) -> Column:  # noqa: N803
    """Unsupported: engine has no ``months_between``."""

    raise UnsupportedOperationException(
        "functions.months_between is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def unix_timestamp(
    timestamp: Column | str | None = None,
    format: str | None = None,
) -> Column:
    """Unsupported: engine has no ``unix_timestamp``."""

    raise UnsupportedOperationException(
        "functions.unix_timestamp is not supported yet (engine gap; disclosed R-FN-BATCH1)"
    )


def hash(*cols: Column | str) -> Column:
    """Unsupported: engine has no Spark ``hash`` (xxhash-style)."""

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
        named_parts.append(f"{sql_string_literal(str(field_name))}, {column.sql_expr_part()}")
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
    """printf-style formatting (PySpark ``functions.format_string``).

    ``format`` is always a literal; the remaining arguments follow ``ColumnOrName``.
    """
    return _scalar("format_string", format, *cols, lit_indices=frozenset({0}))


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


def validate_utf8(str: Column | str) -> Column:
    """The input when it is valid UTF-8, an error otherwise (PySpark ``functions.validate_utf8``).

    An Arrow string column cannot hold invalid UTF-8, so this only ever fails on **binary** input.
    Spark's own strings are byte arrays that can carry invalid sequences, so a Spark program can
    reach this on a STRING column where repark cannot — a difference in value representation, not
    a behaviour choice.
    """
    return _scalar("validate_utf8", str)


def try_validate_utf8(str: Column | str) -> Column:
    """The input when valid UTF-8, NULL otherwise (PySpark ``functions.try_validate_utf8``)."""
    return _scalar("try_validate_utf8", str)


def assert_true(col: Column | str, errMsg: Column | str | None = None) -> Column:  # noqa: N803
    """NULL when ``col`` is true, an error otherwise (PySpark ``functions.assert_true``).

    NULL is **not** true: like Spark, a NULL condition raises rather than passing.
    """
    if errMsg is None:
        return _scalar("assert_true", col)
    return _scalar("assert_true", col, errMsg, lit_indices=frozenset({1}))


def randstr(length: int | Column, seed: int | Column | None = None) -> Column:
    """A random string of ``length`` characters from 0-9, a-z, A-Z (PySpark ``functions.randstr``).

    ``length`` must be a **constant** — Spark requires a literal SMALLINT/INT, and a column
    argument is refused loudly rather than silently reading the first row.
    """
    if seed is None:
        return _scalar("randstr", length, lit_indices=frozenset({0}))
    return _scalar("randstr", length, seed, lit_indices=frozenset({0, 1}))


def uniform(
    min: int | float | Column,
    max: int | float | Column,
    seed: int | Column | None = None,
) -> Column:
    """A random value in ``[min, max)`` (PySpark ``functions.uniform``).

    Both bounds must be constant. **The result type follows them**: two integers give an integer,
    anything else gives a double — Spark's documented rule, and a silent type change if got wrong.
    """
    if seed is None:
        return _scalar("uniform", min, max, lit_indices=frozenset({0, 1}))
    return _scalar("uniform", min, max, seed, lit_indices=frozenset({0, 1, 2}))


def regexp_extract_all(
    str: Column | str,
    regexp: Column | str,
    idx: int | Column | None = None,
) -> Column:
    """Every match's ``idx``-th group, as an array (PySpark ``functions.regexp_extract_all``).

    ``regexp`` is ``ColumnOrName``: a bare ``str`` is a **column name**, matching
    :func:`regexp_count` and PySpark itself. Pass ``F.lit(...)`` for a pattern literal.

    ``idx`` defaults to **1** (the first capture group), which is Spark's default and not the whole
    match — pass ``idx=0`` for that. A pattern with no capture group therefore RAISES on the
    two-argument form, as it does in Spark.

    No match yields an EMPTY array, not NULL — NULL is reserved for a NULL input, a distinction
    ``regexp_extract``'s empty-string convention cannot make.
    """
    if idx is None:
        return _scalar("regexp_extract_all", str, regexp)
    # A plain-string idx is a literal group index, not a column name — the same contract
    # `regexp_instr` carries. F-FNP6A-1 correctly removed position 1 (a bare `regexp` IS a column
    # name) but took position 2 with it; SEM-3 narrows rather than restores.
    return _scalar(
        "regexp_extract_all",
        str,
        regexp,
        idx,
        lit_indices=None if isinstance(idx, Column) else frozenset({2}),
    )


def regexp_substr(str: Column | str, regexp: Column | str) -> Column:
    """The first match, or NULL (PySpark ``functions.regexp_substr``).

    ``regexp`` is ``ColumnOrName``: a bare ``str`` is a **column name**.

    NULL covers **two** cases, not one: no match at all, and a first match that is **zero-width**.
    Spark takes the first match and nulls it when empty rather than looking for a later non-empty
    one, so ``regexp_substr('a1b2', '[0-9]*')`` is NULL even though ``'1'`` matches at position 1.

    Deliberately unlike ``regexp_extract``, which returns an empty string on no match; Spark keeps
    the two conventions apart.
    """
    return _scalar("regexp_substr", str, regexp)


def soundex(col: Column | str) -> Column:
    """Four-character Soundex code (PySpark ``functions.soundex``)."""
    return _scalar("soundex", col)


def sentences(col: Column | str, language: Column | str | None = None) -> Column:
    """Unsupported: engine has no ``sentences`` (R-FN-BATCH2 census)."""

    raise UnsupportedOperationException(
        "functions.sentences is not supported yet (engine gap; disclosed R-FN-BATCH2)"
    )


def arrays_zip(*cols: Column | str) -> Column:
    """Unsupported: engine has no ``arrays_zip`` (R-FN-BATCH2 census)."""

    raise UnsupportedOperationException(
        "functions.arrays_zip is not supported yet (engine gap; disclosed R-FN-BATCH2)"
    )


def map_from_arrays(col1: Column | str, col2: Column | str) -> Column:
    """Map from an array of keys and an array of values (PySpark ``functions.map_from_arrays``)."""
    return _scalar("map_from_arrays", col1, col2)


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


def date_part(field: Column | str, source: Column | str) -> Column:
    """Extract calendar field (PySpark ``functions.date_part``).

    ``field`` is de-facto ``ColumnOrName``: a bare ``str`` is a **column name**
    (live PySpark 4.1.2). Pass ``F.lit('YEAR')`` for a literal field.
    """
    return _scalar("date_part", field, source)


def extract(field: Column | str, source: Column | str) -> Column:
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

    raise UnsupportedOperationException(
        "functions.format_number is not supported yet (engine gap; disclosed R-FN-BATCH3)"
    )


def try_to_timestamp(col: Column | str, format: str | None = None) -> Column:
    """Unsupported: ``try_to_timestamp`` not wired (R-FN-BATCH3 census)."""

    raise UnsupportedOperationException(
        "functions.try_to_timestamp is not supported yet (engine gap; disclosed R-FN-BATCH3)"
    )


def to_utc_timestamp(timestamp: Column | str, tz: str) -> Column:
    """Read a wall clock in ``tz`` as a UTC instant (PySpark ``functions.to_utc_timestamp``)."""
    return _scalar("to_utc_timestamp", timestamp, tz, lit_indices=frozenset({1}))


def from_utc_timestamp(timestamp: Column | str, tz: str) -> Column:
    """Render a UTC instant in ``tz`` (PySpark ``functions.from_utc_timestamp``)."""
    return _scalar("from_utc_timestamp", timestamp, tz, lit_indices=frozenset({1}))


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


def grouping(col: Column | str) -> Column:
    """1 when the row is aggregated over ``col`` in a CUBE/ROLLUP/grouping-set, else 0.

    Only meaningful under a grouping-set query; outside one every row is ungrouped, so the answer
    is always 0.
    """
    column, part = _aggregate_argument(col)
    agg_name = f"grouping({part})"
    return Column(
        column._inner.aggregate("grouping", False),
        agg_name=agg_name,
        sql_expr=f"grouping({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
    )


def approx_count_distinct(col: Column | str, rsd: float | None = None) -> Column:
    """Approximate distinct count (PySpark ``functions.approx_count_distinct``).

    ``rsd`` (the target relative standard deviation) is **accepted and ignored**: Spark's estimator
    is HyperLogLog++ and DataFusion's is HyperLogLog, so the accuracy knob has no counterpart to
    tune. The counts are close but not identical to Spark's — a value divergence, disclosed rather
    than papered over. Same treatment as ``percentile_approx``'s accuracy argument.
    """
    del rsd
    column, part = _aggregate_argument(col)
    agg_name = f"approx_count_distinct({part})"
    return Column(
        column._inner.aggregate("approx_count_distinct", False),
        agg_name=agg_name,
        sql_expr=f"approx_count_distinct({column.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
    )


def listagg(col: Column | str, delimiter: str = "") -> Column:
    """Concatenate values with ``delimiter`` (PySpark ``functions.listagg``)."""
    return _binary_aggregate("listagg", col, lit(delimiter))


def string_agg(col: Column | str, delimiter: str = "") -> Column:
    """Concatenate values with ``delimiter`` (PySpark ``functions.string_agg``; same as listagg)."""
    return _binary_aggregate("string_agg", col, lit(delimiter))


def _binary_aggregate(name: str, col1: Column | str, col2: Column | str) -> Column:
    """A two-column aggregate: coerce both arguments, name the output the way PySpark does.

    Every two-argument aggregate on this surface is this same shape, so it lives once. The
    ``agg_name`` is what PySpark puts in the projection (``corr(x, y)``), and ``sql_expr`` carries
    the quoted structural spelling free-SQL global-agg needs (octo C3-SEC-001).
    """
    left, left_part = _aggregate_argument(col1)
    right, right_part = _aggregate_argument(col2)
    agg_name = f"{name}({left_part}, {right_part})"
    return Column(
        left._inner.aggregate_binary(name, right._inner),
        agg_name=agg_name,
        sql_expr=f"{name}({left.sql_expr_part()}, {right.sql_expr_part()})",
        spark_display=agg_name,
        projection_name=agg_name,
        partition_transform=_partition_transform_of(left, right),
    )


def corr(col1: Column | str, col2: Column | str) -> Column:
    """Pearson correlation (PySpark ``functions.corr``)."""
    return _binary_aggregate("corr", col1, col2)


def regr_avgx(y: Column | str, x: Column | str) -> Column:
    """Average of the independent column over non-null pairs (PySpark ``functions.regr_avgx``).

    Spark's argument order is ``(dependent, independent)``.
    """
    return _binary_aggregate("regr_avgx", y, x)


def regr_avgy(y: Column | str, x: Column | str) -> Column:
    """Average of the dependent column over non-null pairs (PySpark ``functions.regr_avgy``).

    Spark's argument order is ``(dependent, independent)``.
    """
    return _binary_aggregate("regr_avgy", y, x)


def regr_count(y: Column | str, x: Column | str) -> Column:
    """Count of non-null ``(y, x)`` pairs (PySpark ``functions.regr_count``).

    Spark's argument order is ``(dependent, independent)``.
    """
    return _binary_aggregate("regr_count", y, x)


def regr_intercept(y: Column | str, x: Column | str) -> Column:
    """Intercept of the least-squares fit (PySpark ``functions.regr_intercept``).

    Spark's argument order is ``(dependent, independent)``.
    """
    return _binary_aggregate("regr_intercept", y, x)


def regr_r2(y: Column | str, x: Column | str) -> Column:
    """Coefficient of determination of the least-squares fit (PySpark ``functions.regr_r2``).

    Spark's argument order is ``(dependent, independent)``.
    """
    return _binary_aggregate("regr_r2", y, x)


def regr_slope(y: Column | str, x: Column | str) -> Column:
    """Slope of the least-squares fit (PySpark ``functions.regr_slope``).

    Spark's argument order is ``(dependent, independent)``.
    """
    return _binary_aggregate("regr_slope", y, x)


def regr_sxx(y: Column | str, x: Column | str) -> Column:
    """Sum of squares of the independent column (PySpark ``functions.regr_sxx``).

    Spark's argument order is ``(dependent, independent)``.
    """
    return _binary_aggregate("regr_sxx", y, x)


def regr_sxy(y: Column | str, x: Column | str) -> Column:
    """Sum of products of the paired columns (PySpark ``functions.regr_sxy``).

    Spark's argument order is ``(dependent, independent)``.
    """
    return _binary_aggregate("regr_sxy", y, x)


def regr_syy(y: Column | str, x: Column | str) -> Column:
    """Sum of squares of the dependent column (PySpark ``functions.regr_syy``).

    Spark's argument order is ``(dependent, independent)``.
    """
    return _binary_aggregate("regr_syy", y, x)


def covar_pop(col1: Column | str, col2: Column | str) -> Column:
    """Population covariance (PySpark ``functions.covar_pop``)."""
    return _binary_aggregate("covar_pop", col1, col2)


def covar_samp(col1: Column | str, col2: Column | str) -> Column:
    """Sample covariance (PySpark ``functions.covar_samp``)."""
    return _binary_aggregate("covar_samp", col1, col2)


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
    """SHA-1 as a lowercase hex string (PySpark ``functions.sha1``)."""
    return _scalar("sha1", col)


def sha(col: Column | str) -> Column:
    """SHA-1 as a hex string (PySpark ``functions.sha``; Spark's older spelling of ``sha1``)."""
    return _scalar("sha", col)


def crc32(col: Column | str) -> Column:
    """CRC-32 checksum as a bigint (PySpark ``functions.crc32``)."""
    return _scalar("crc32", col)


def xxhash64(*cols: Column | str) -> Column:
    """64-bit xxHash of the arguments (PySpark ``functions.xxhash64``).

    **Variadic**, like PySpark's — this function exists mainly to hash a composite key across
    several columns, and the one-column form is the uncommon case. The Rust builder and the
    dispatch arm were already variadic; only this signature was not (F-CSP-5 / F-CFS-9).

    The signature accepts zero arguments and Spark does not: it raises `WRONG_NUM_ARGS`, through
    the facade and through SQL alike (measured, LRS-2). Refused here so the message names this
    function rather than the internal dispatcher the user never called.
    """
    if not cols:
        raise AnalysisException(
            "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION] The `xxhash64` requires > 0 parameters "
            "but the actual number is 0."
        )
    return _scalar("xxhash64", *cols)


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

    raise UnsupportedOperationException(
        "functions.skewness is not supported yet (engine gap; disclosed R-FN-BATCH4)"
    )


def kurtosis(col: Column | str) -> Column:
    """Unsupported: engine has no ``kurtosis`` (R-FN-BATCH4)."""

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

    raise UnsupportedOperationException(
        "functions.mode is not supported yet (engine gap; disclosed R-FN-BATCH4)"
    )


def monotonically_increasing_id() -> Column:
    """Unsupported: single-node id generator not wired (R-FN-BATCH4 disclosed)."""

    raise UnsupportedOperationException(
        "functions.monotonically_increasing_id is not supported yet "
        "(single-node semantics disclosed; R-FN-BATCH4)"
    )


def spark_partition_id() -> Column:
    """Unsupported: single-node partition id (always 0 if implemented; R-FN-BATCH4 loud)."""

    raise UnsupportedOperationException(
        "functions.spark_partition_id is not supported yet (single-node disclosed; R-FN-BATCH4)"
    )


def input_file_name() -> Column:
    """Unsupported: input_file_name not wired (R-FN-BATCH4 disclosed)."""

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

    Lowered via ``unnest(CASE WHEN null/empty THEN make_array(CAST(NULL AS <element>))
    ELSE col END)``; void elements use untyped ``make_array(NULL)``. Avoids DataFusion's
    null-list ``unnest`` gap without forking the engine (R-EXPLODE-REWRITE / DF-2).

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

    _ = column
    raise UnsupportedOperationException(
        "posexplode_outer is not supported yet (see posexplode; use explode_outer without ordinal)"
    )


# ---- FN-A: ordering / null / math -------------------------------------------------------------


def sign(col: Column | str) -> Column:
    """Sign as -1/0/1 (PySpark ``functions.sign``; alias of ``signum``)."""
    return _scalar("sign", col)


def ifnull(col: Column | str, alt: Column | str) -> Column:
    """Replace NULL with ``alt`` (PySpark ``functions.ifnull``; 2-arg ``coalesce``)."""
    return coalesce(_as_column_arg(col, as_lit=False), _as_column_arg(alt, as_lit=False))


def nvl(col1: Column | str, col2: Column | str) -> Column:
    """Replace NULL with ``col2`` (PySpark ``functions.nvl``; 2-arg ``coalesce``)."""
    return coalesce(_as_column_arg(col1, as_lit=False), _as_column_arg(col2, as_lit=False))


def asc(col: Column | str) -> Column:
    """Ascending sort marker, nulls first (PySpark ``functions.asc``)."""
    return _as_column_arg(col, as_lit=False).asc()


def desc(col: Column | str) -> Column:
    """Descending sort marker, nulls last (PySpark ``functions.desc``)."""
    return _as_column_arg(col, as_lit=False).desc()


def asc_nulls_first(col: Column | str) -> Column:
    """Ascending sort, nulls first (PySpark ``functions.asc_nulls_first``; same as ``asc``)."""
    return _as_column_arg(col, as_lit=False).asc_nulls_first()


def asc_nulls_last(col: Column | str) -> Column:
    """Ascending sort, nulls LAST (PySpark ``functions.asc_nulls_last``)."""
    return _as_column_arg(col, as_lit=False).asc_nulls_last()


def desc_nulls_first(col: Column | str) -> Column:
    """Descending sort, nulls FIRST (PySpark ``functions.desc_nulls_first``)."""
    return _as_column_arg(col, as_lit=False).desc_nulls_first()


def desc_nulls_last(col: Column | str) -> Column:
    """Descending sort, nulls last (PySpark ``functions.desc_nulls_last``; same as ``desc``)."""
    return _as_column_arg(col, as_lit=False).desc_nulls_last()


def e() -> Column:
    """Euler's number (PySpark ``functions.e``).

    Foldable. Implemented as ``exp(1)`` so the Spark-door SQL global-agg path
    (``select(sum(x), e())``) stays DOUBLE — a bare ``lit(math.e)`` re-embeds as
    a decimal literal (``2.718…``) and comes back ``decimal128``.
    """
    return Column(
        exp(lit(1))._inner,
        spark_display="E()",
        projection_name="E()",
        sql_expr="exp(1)",
        is_foldable=True,
    )


def pi() -> Column:
    """π (PySpark ``functions.pi``). Foldable DataFusion ``pi()``."""
    return Column(
        _native.PyColumn.sql("pi()"),
        spark_display="pi()",
        projection_name="pi()",
        sql_expr="pi()",
        is_foldable=True,
    )


def negative(col: Column | str) -> Column:
    """Unary minus (PySpark ``functions.negative``)."""
    return -_as_column_arg(col, as_lit=False)


negate = negative
"""PySpark ``functions.negate`` — the Spark spelling of ``negative`` (``builtin.py`` aliases it)."""


def positive(col: Column | str) -> Column:
    """Unary plus — identity column (PySpark ``functions.positive``)."""
    return _as_column_arg(col, as_lit=False)


def pmod(dividend: Column | str, divisor: Column | str) -> Column:
    """Positive modulo (PySpark ``functions.pmod``).

    ``((dividend % divisor) + divisor) % divisor`` matches Spark's sign-of-divisor
    remainder (``pmod(-10, 3) == 2``). ``call_scalar("pmod")`` is not wired.
    """
    left = _as_column_arg(dividend, as_lit=False)
    right = _as_column_arg(divisor, as_lit=False)
    return ((left % right) + right) % right


def expm1(col: Column | str) -> Column:
    """``exp(col) - 1`` (PySpark ``functions.expm1``)."""
    return exp(col) - lit(1)


def ln(col: Column | str) -> Column:
    """Natural logarithm (PySpark ``functions.ln``; alias of ``log``)."""
    return _scalar("ln", col)


def log2(col: Column | str) -> Column:
    """Base-2 logarithm (PySpark ``functions.log2``).

    ``call_scalar`` has no ``log2`` arm; SHIM as ``log(col) / log(2)``.
    """
    return log(col) / log(lit(2))


def log1p(col: Column | str) -> Column:
    """``log(1 + col)`` (PySpark ``functions.log1p``)."""
    return log(lit(1) + _as_column_arg(col, as_lit=False))


def degrees(col: Column | str) -> Column:
    """Radians to degrees (PySpark ``functions.degrees``)."""
    return _as_column_arg(col, as_lit=False) * lit(180) / pi()


def radians(col: Column | str) -> Column:
    """Degrees to radians (PySpark ``functions.radians``)."""
    return _as_column_arg(col, as_lit=False) * pi() / lit(180)


def nvl2(col1: Column | str, col2: Column | str, col3: Column | str) -> Column:
    """If ``col1`` is not null return ``col2`` else ``col3`` (PySpark ``functions.nvl2``)."""
    return when(~isnull(col1), _as_column_arg(col2, as_lit=False)).otherwise(
        _as_column_arg(col3, as_lit=False)
    )


def nullif(col1: Column | str, col2: Column | str) -> Column:
    """NULL if the arguments compare equal, else ``col1`` (PySpark ``functions.nullif``)."""
    left = _as_column_arg(col1, as_lit=False)
    right = _as_column_arg(col2, as_lit=False)
    return when(left == right, lit(None)).otherwise(left)


def equal_null(col1: Column | str, col2: Column | str) -> Column:
    """Null-safe equality (PySpark ``functions.equal_null``; ``Column.eqNullSafe``)."""
    return _as_column_arg(col1, as_lit=False).eqNullSafe(_as_column_arg(col2, as_lit=False))


def zeroifnull(col: Column | str) -> Column:
    """Replace NULL with 0 (PySpark ``functions.zeroifnull``)."""
    return coalesce(_as_column_arg(col, as_lit=False), lit(0))


def nullifzero(col: Column | str) -> Column:
    """NULL when the value is 0 (PySpark ``functions.nullifzero``)."""
    return nullif(col, lit(0))


def isnotnull(col: Column | str) -> Column:
    """True when the value is not NULL (PySpark ``functions.isnotnull``)."""
    return ~isnull(col)


def cbrt(col: Column | str) -> Column:
    """Cube root (PySpark ``functions.cbrt``).

    ``pow(col, 1/3)`` is NaN on negatives (IEEE); Spark returns the real root.
    Negatives use ``-pow(-col, 1/3)`` so the named hazard is not a lie.
    """
    column = _as_column_arg(col, as_lit=False)
    third = lit(1.0 / 3.0)
    return when(column < lit(0), -pow(-column, third)).otherwise(pow(column, third))


# ---- FN-B: strings ----------------------------------------------------------------------------


def lcase(col: Column | str) -> Column:
    """Lowercase string (PySpark ``functions.lcase``; alias of ``lower``)."""
    return lower(col)


def ucase(col: Column | str) -> Column:
    """Uppercase string (PySpark ``functions.ucase``; alias of ``upper``)."""
    return upper(col)


def char(col: Column | str | int) -> Column:
    """Unicode code point → character (PySpark ``functions.char``; alias of ``chr``)."""
    return chr(col)


def char_length(col: Column | str) -> Column:
    """Character length (PySpark ``functions.char_length``; alias of ``length``)."""
    return length(col)


def character_length(col: Column | str) -> Column:
    """Character length (PySpark ``functions.character_length``; alias of ``length``)."""
    return _scalar("character_length", col)


def substring(str: Column | str, pos: Column | int, len: Column | int) -> Column:
    """1-based substring (PySpark ``functions.substring``)."""
    lit_indices: set[int] = set()
    if isinstance(pos, int) and not isinstance(pos, bool):
        lit_indices.add(1)
    if isinstance(len, int) and not isinstance(len, bool):
        lit_indices.add(2)
    return _scalar("substring", str, pos, len, lit_indices=frozenset(lit_indices) or None)


def substr(str: Column | str, pos: Column | int, len: Column | int) -> Column:
    """1-based substring (PySpark ``functions.substr``; alias of ``substring``)."""
    return substring(str, pos, len)


def left(str: Column | str, len: Column | int) -> Column:
    """Leftmost ``len`` characters (PySpark ``functions.left``)."""
    return substring(str, 1, len)


def right(str: Column | str, len: Column | int) -> Column:
    """Rightmost ``len`` characters (PySpark ``functions.right``)."""
    source = _as_column_arg(str, as_lit=False)
    count = _as_column_arg(len, as_lit=isinstance(len, int) and not isinstance(len, bool))
    start = greatest(length(source) - count + lit(1), lit(1))
    return substring(source, start, count)


def contains(col: Column | str, value: Column | str) -> Column:
    """Substring containment (PySpark ``functions.contains``).

    ``value`` is ``ColumnOrName``: a bare ``str`` is a **column name**.
    """
    return _scalar("contains", col, value)


def like(col: Column | str, pattern: Column | str) -> Column:
    """SQL LIKE (PySpark ``functions.like``).

    ``pattern`` is ``ColumnOrName``: a bare ``str`` is a **column name**.
    """
    return _scalar("like", col, pattern)


def ilike(col: Column | str, pattern: Column | str) -> Column:
    """Case-insensitive SQL LIKE (PySpark ``functions.ilike``).

    ``pattern`` is ``ColumnOrName``: a bare ``str`` is a **column name**.
    """
    return _scalar("ilike", col, pattern)


def regexp_like(col: Column | str, pattern: Column | str) -> Column:
    """Regular-expression match (PySpark ``functions.regexp_like``).

    ``pattern`` is ``ColumnOrName``: a bare ``str`` is a **column name**.
    """
    return _scalar("regexp_like", col, pattern)


def rlike(col: Column | str, pattern: Column | str) -> Column:
    """Regular-expression match (PySpark ``functions.rlike``; alias of ``regexp_like``)."""
    return regexp_like(col, pattern)


def regexp(col: Column | str, pattern: Column | str) -> Column:
    """Regular-expression match (PySpark ``functions.regexp``; alias of ``regexp_like``)."""
    return regexp_like(col, pattern)


def btrim(col: Column | str, trim: Column | str | None = None) -> Column:
    """Trim both sides (PySpark ``functions.btrim``).

    ``trim`` is ``ColumnOrName``: a bare ``str`` is a **column name**.
    """
    if trim is None:
        return _scalar("btrim", col)
    return _scalar("btrim", col, trim)


def startswith(col: Column | str, prefix: Column | str) -> Column:
    """Prefix test (PySpark ``functions.startswith``).

    ``prefix`` is ``ColumnOrName``: a bare ``str`` is a **column name**.
    """
    return _scalar("starts_with", col, prefix)


def endswith(col: Column | str, suffix: Column | str) -> Column:
    """Suffix test (PySpark ``functions.endswith``).

    ``suffix`` is ``ColumnOrName``: a bare ``str`` is a **column name**.
    """
    return _scalar("ends_with", col, suffix)


def printf(format: str, *cols: Column | str) -> Column:
    """Printf-style format (PySpark ``functions.printf``; alias of ``format_string``)."""
    return format_string(format, *cols)


def replace(src: Column | str, search: str, replacement: Column | str) -> Column:
    """Literal string replace (PySpark ``functions.replace``).

    DataFusion ``replace`` is not on ``call_scalar``. Lowered via ``regexp_replace``
    after escaping ``search`` so ``.`` / ``*`` are literals, not regex — pin vs
    ``regexp_replace`` (SEMANTIC-HAZARD). Column ``search`` is not accepted (cannot
    escape per row without a kernel).
    """
    escaped_search = re.escape(search)
    if isinstance(replacement, str):
        escaped_replacement = replacement.replace("\\", "\\\\").replace("$", "\\$")
        return regexp_replace(src, escaped_search, escaped_replacement)
    return regexp_replace(src, escaped_search, replacement)


def quote(col: Column | str) -> Column:
    """SQL single-quoted literal of a string column (PySpark ``functions.quote``)."""
    column = _as_column_arg(col, as_lit=False)
    escaped = regexp_replace(column, "'", "''")
    return concat(lit("'"), escaped, lit("'"))


def split_part(
    src: Column | str,
    delimiter: Column | str,
    partNum: Column | str | int,  # noqa: N803
) -> Column:
    """Nth field after splitting on a delimiter (PySpark ``functions.split_part``).

    All three arguments are ``ColumnOrName``: a bare ``str`` is a **column name**.

    Parameters
    ----------
    src : Column or str
        Input string.
    delimiter : Column or str
        Field separator.
    partNum : Column or str or int
        1-based field index (negative counts from the end).

    Returns
    -------
    Column
        The selected field, or empty string when out of range.

    Examples
    --------
    ``F.split_part(F.lit('a.b.c'), F.lit('.'), F.lit(2))`` is ``'b'``.
    """
    return _scalar("split_part", src, delimiter, partNum)


def regexp_count(str: Column | str, regexp: Column | str) -> Column:
    """Count regex matches (PySpark ``functions.regexp_count``).

    ``regexp`` is ``ColumnOrName``: a bare ``str`` is a **column name**.

    Parameters
    ----------
    str : Column or str
        Input string.
    regexp : Column or str
        Java/Spark regular expression.

    Returns
    -------
    Column
        Match count. NULL when either input is NULL (Spark 4.1.2).

    Examples
    --------
    ``F.regexp_count(F.lit('ababab'), F.lit('ab'))`` is ``3``.
    """
    return _scalar("regexp_count", str, regexp)


def regexp_instr(
    str: Column | str,
    regexp: Column | str,
    idx: Column | int | None = None,
) -> Column:
    """1-based index of the first regex match (PySpark ``functions.regexp_instr``).

    ``regexp`` is ``ColumnOrName``. ``idx`` is optional ``int`` or ``Column``
    (a bare ``str`` is force-lit, then CAST to INT — Spark 4.1.2). Live Spark
    ``RegExpInStr.nullSafeEval`` **ignores** the idx value and returns the
    start of the whole match; a NULL idx still yields NULL. Omitted idx is
    ``0`` (PySpark projects ``regexp_instr(s, re, 0)``).

    Parameters
    ----------
    str : Column or str
        Input string.
    regexp : Column or str
        Java/Spark regular expression.
    idx : Column or int, optional
        Spark's group-index slot. NULL-propagates; the position is always the
        first-match start (1-based), or ``0`` when there is no match.

    Returns
    -------
    Column
        1-based start index, or ``0`` when there is no match.

    Examples
    --------
    ``F.regexp_instr(F.lit('abcde'), F.lit('c'))`` is ``3``.
    """
    if idx is None:
        idx = 0
    return _scalar(
        "regexp_instr",
        str,
        regexp,
        idx,
        lit_indices=frozenset({} if isinstance(idx, Column) else {2}),
    )


def bit_length(col: Column | str) -> Column:
    """Bit length of a string (PySpark ``functions.bit_length``).

    Spark stringifies non-binary inputs (``bit_length(12)`` is ``16``) and
    counts binary payloads as raw bytes. The engine kernel is that coercion
    (G5); the wrapper stays a thin wire.

    Parameters
    ----------
    col : Column or str
        String, binary, or stringify-able column.

    Returns
    -------
    Column
        ``8 * octet_length`` as Spark INT.

    Examples
    --------
    ``F.bit_length(F.lit('ab'))`` is ``16``.
    """
    return _scalar("bit_length", col)


def octet_length(col: Column | str) -> Column:
    """Byte length of a string (PySpark ``functions.octet_length``).

    Same Spark stringify-non-binary / pass-through-binary rule as
    :func:`bit_length` (G5).

    Parameters
    ----------
    col : Column or str
        String, binary, or stringify-able column.

    Returns
    -------
    Column
        UTF-8 / binary byte count as Spark INT.

    Examples
    --------
    ``F.octet_length(F.lit('ab'))`` is ``2``.
    """
    return _scalar("octet_length", col)


def is_valid_utf8(col: Column | str) -> Column:
    """Whether the value is valid UTF-8 (PySpark ``functions.is_valid_utf8``).

    Parameters
    ----------
    col : Column or str
        String or binary column.

    Returns
    -------
    Column
        Boolean.

    Examples
    --------
    ``F.is_valid_utf8(F.lit('ok'))`` is ``True``.
    """
    return _scalar("is_valid_utf8", col)


def make_valid_utf8(col: Column | str) -> Column:
    """Replace invalid UTF-8 with U+FFFD (PySpark ``functions.make_valid_utf8``).

    Parameters
    ----------
    col : Column or str
        String or binary column.

    Returns
    -------
    Column
        A valid UTF-8 string.

    Examples
    --------
    ``F.make_valid_utf8(F.lit('ok'))`` is ``'ok'``.
    """
    return _scalar("make_valid_utf8", col)
