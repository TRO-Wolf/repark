"""Collection facade wrappers (FN-E).

Aliases and shims over existing ``functions`` / ``functions_expr`` helpers.
Public names are re-exported from ``functions.py``.

Higher-order / JSON / generator names stay deferred (lambda module empty;
``call_scalar`` has no arms). ``concat`` is string-only (casts Utf8) — array
append/prepend cannot use it.
"""

from __future__ import annotations

from repark import _native
from repark.errors import PySparkTypeError, PySparkValueError
from repark.spark.column import Column
from repark.spark.functions import (
    _as_column_arg,
    _scalar,
    collect_list,
    lit,
)
from repark.spark.functions_expr import (
    array,
    array_contains,
    array_except,
    array_intersect,
    flatten,
    isnull,
    map_keys,
    size,
    when,
)


def cardinality(col: Column | str) -> Column:
    """Array/map cardinality (PySpark ``functions.cardinality``; alias of ``size``)."""
    return _scalar("cardinality", col)


def array_size(col: Column | str) -> Column:
    """Array/map cardinality (PySpark ``functions.array_size``; alias of ``size``)."""
    return size(col)


def array_agg(col: Column | str) -> Column:
    """Collect non-NULL values into an array (PySpark ``array_agg``; alias of ``collect_list``)."""
    return collect_list(col)


def named_struct(*cols: Column | str | int | float | bool | None) -> Column:
    """Named struct from even-length name/value pairs (PySpark ``functions.named_struct``).

    ``call_scalar`` has no ``named_struct`` arm. SHIM via the same ``make_struct``
    path as :func:`repark.spark.functions_expr.struct` (DataFusion ``named_struct``).
    Field names must be Python strings or foldable string columns.
    """
    if len(cols) < 2 or len(cols) % 2 != 0:
        raise PySparkValueError(
            "named_struct requires an even number of arguments (name, value, ...)"
        )
    columns: list[Column] = []
    named_parts: list[str] = []
    display_parts: list[str] = []
    field_names: list[str] = []
    free = False
    for index in range(0, len(cols), 2):
        name_argument = cols[index]
        value_argument = cols[index + 1]
        if isinstance(name_argument, str):
            field_name = name_argument
        elif isinstance(name_argument, Column):
            if not name_argument._is_foldable:
                raise PySparkValueError("named_struct field names must be foldable string literals")
            field_name = name_argument.spark_display_part()
        else:
            raise PySparkTypeError(
                errorClass="NOT_COLUMN_OR_STR",
                messageParameters={
                    "arg_name": "name",
                    "arg_type": type(name_argument).__name__,
                },
            )
        value = _as_column_arg(
            value_argument,
            as_lit=not isinstance(value_argument, (Column, str)),
        )
        free = free or bool(value._has_free_attribute) or isinstance(value_argument, str)
        columns.append(value)
        field_names.append(field_name)
        safe_name = str(field_name).replace("'", "''")
        named_parts.append(f"'{safe_name}', {value.sql_expr_part()}")
        display_parts.append(str(field_name))
    sql = f"named_struct({', '.join(named_parts)})"
    display = f"named_struct({', '.join(display_parts)})"
    named_natives = [
        column._inner.alias(str(name)) for column, name in zip(columns, field_names, strict=True)
    ]
    return Column(
        _native.PyColumn.make_struct(named_natives),
        spark_display=display,
        projection_name=display,
        stable_name=False,
        sql_expr=sql,
        has_free_attribute=free,
    )


def map_contains_key(col: Column | str, key: Column | str | int | float) -> Column:
    """True when the map contains ``key`` (PySpark ``functions.map_contains_key``).

    SHIM: ``array_contains(map_keys(m), k)``.
    """
    return array_contains(map_keys(col), key)


def _glue_element(array_col: Column, element: Column, *, prepend: bool) -> Column:
    """Concatenate one wrapped element onto ``array_col`` (NULL array → NULL).

    ``F.concat`` is string-only (Utf8 cast). ``flatten(array(arr, array(x)))`` is
    the honest array glue. A bare flatten of a NULL array would yield ``[x]``.
    """
    wrapped = array(element)
    pieces = (wrapped, array_col) if prepend else (array_col, wrapped)
    built = flatten(array(*pieces))
    return when(isnull(array_col), lit(None)).otherwise(built)


def array_append(
    col: Column | str,
    value: Column | str | int | float | bool | None,
) -> Column:
    """Append ``value`` to an array (PySpark ``functions.array_append``)."""
    array_col = _as_column_arg(col, as_lit=False)
    element = value if isinstance(value, Column) else lit(value)
    return _glue_element(array_col, element, prepend=False)


def array_prepend(
    col: Column | str,
    value: Column | str | int | float | bool | None,
) -> Column:
    """Prepend ``value`` to an array (PySpark ``functions.array_prepend``)."""
    array_col = _as_column_arg(col, as_lit=False)
    element = value if isinstance(value, Column) else lit(value)
    return _glue_element(array_col, element, prepend=True)


def arrays_overlap(a1: Column | str, a2: Column | str) -> Column:
    """True when the arrays share a non-NULL element (PySpark ``functions.arrays_overlap``).

    SHIM: ``size(array_except(array_intersect(a, b), array(NULL))) > 0``.
    Null-only intersection is not overlap (Spark). A NULL array yields NULL.
    """
    intersection = array_except(array_intersect(a1, a2), array(lit(None)))
    return size(intersection) > 0


def get(
    col: Column | str,
    index: Column | str | int | float | bool | None,
) -> Column:
    """0-based array element or map value (PySpark ``functions.get``).

    SEMANTIC-HAZARD vs SQL ``element_at`` (1-based; index 0 raises
    ``INVALID_INDEX_OF_ZERO``). ``call_scalar`` has no ``element_at`` arm, so
    the 1-based spelling is not a facade name — pin the base contrast here.
    """
    container = _as_column_arg(col, as_lit=False)
    key = index if isinstance(index, Column) else lit(index)
    return _scalar("getitem", container, key)
