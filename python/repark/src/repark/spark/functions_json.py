"""JSON facade wrappers (FNP-10), and the installer for the FNP-9/10 surface.

The JSON functions are defined here; the collection constructors this unit added live in
their family home, ``functions_collections``, and are re-exported through :func:`install_into`
so ``functions.py`` gains the whole surface without growing past its size baseline.
"""

from __future__ import annotations

from typing import Any, NoReturn

from repark.errors import UnsupportedOperationException
from repark.spark.column import Column
from repark.spark.functions import _scalar, lit
from repark.spark.functions_collections import array_insert, create_map, map_concat
from repark.spark.types import DataType

REFUSED_MESSAGES: dict[str, str] = {
    "inline": (
        "inline is not supported yet: it is a multi-column generator, and the facade select "
        "path carries one generator column that yields exactly one output column. See "
        "docs/spark-sql-iceberg-parity.md (FNP9-GENERATORS-1)."
    ),
    "inline_outer": (
        "inline_outer is not supported yet: it is inline with a NULL row for an empty or NULL "
        "array, and shares inline's multi-column generator gap. See "
        "docs/spark-sql-iceberg-parity.md (FNP9-GENERATORS-1)."
    ),
    "stack": (
        "stack is not supported yet: it reshapes n values into n/k rows of k columns, which is "
        "a multi-column generator the facade select path cannot express. See "
        "docs/spark-sql-iceberg-parity.md (FNP9-GENERATORS-1)."
    ),
    "call_udf": (
        "call_udf is not supported yet: it resolves a registered function by name, and a "
        "facade Column is built without a session, so there is no function registry to look "
        "the name up in. Call the registered function through spark.sql instead. See "
        "docs/spark-sql-iceberg-parity.md (FNP9-BYNAME-1)."
    ),
    "call_function": (
        "call_function is not supported yet: it resolves any function by name, and a facade "
        "Column is built without a session, so there is no function registry to look the name "
        "up in. Call the function through spark.sql instead. See "
        "docs/spark-sql-iceberg-parity.md (FNP9-BYNAME-1)."
    ),
    "sequence": (
        "sequence is not supported yet: Spark's generates DATE and TIMESTAMP ranges by INTERVAL "
        "as well as integer ranges, and the temporal arm belongs to the FNP-11 time family. An "
        "integer-only sequence would answer the common case and diverge silently on the rest. "
        "See docs/spark-sql-iceberg-parity.md (FNP9-SEQUENCE-1)."
    ),
}

FNP9_NAMES: tuple[str, ...] = (
    "array_insert",
    "call_function",
    "call_udf",
    "create_map",
    "from_json",
    "get_json_object",
    "inline",
    "inline_outer",
    "json_array_length",
    "json_object_keys",
    "map_concat",
    "sequence",
    "stack",
    "to_json",
)


def _refuse(name: str) -> NoReturn:
    raise UnsupportedOperationException(REFUSED_MESSAGES[name])


def get_json_object(col: Column | str, path: str) -> Column:
    """Extract one JSONPath value as a string (PySpark ``functions.get_json_object``).

    Args:
        col: the JSON string column, or a column name.
        path: the path, which must start with ``$``.

    Returns:
        A ``STRING`` column; NULL for a missing path, a malformed document, or a JSON null.
    """
    return _scalar("get_json_object", col, path, lit_indices=frozenset({1}))


def json_array_length(col: Column | str) -> Column:
    """Length of a JSON array (PySpark ``functions.json_array_length``).

    Args:
        col: the JSON string column, or a column name.

    Returns:
        An ``INT`` column; NULL when the document is not an array or is malformed.
    """
    return _scalar("json_array_length", col)


def json_object_keys(col: Column | str) -> Column:
    """Keys of a JSON object in document order (PySpark ``functions.json_object_keys``).

    Args:
        col: the JSON string column, or a column name.

    Returns:
        An ``ARRAY<STRING>`` column; NULL when the document is not an object or is malformed.
    """
    return _scalar("json_object_keys", col)


def to_json(col: Column | str, options: dict[str, str] | None = None) -> Column:
    """Render a STRUCT, ARRAY, or MAP column as JSON (PySpark ``functions.to_json``).

    A NULL struct field is omitted; a NULL map value is written as ``null``. Both are Spark's,
    measured on 4.1.2.

    Args:
        col: the column to render, or a column name.
        options: accepted and ignored, as Spark ignores an option it does not know.

    Returns:
        A ``STRING`` column; NULL where the input value is NULL.
    """
    _ = options
    return _scalar("to_json", col)


def from_json(
    col: Column | str,
    schema: DataType | str | Column,
    options: dict[str, str] | None = None,
) -> Column:
    """Parse a JSON string column against a schema (PySpark ``functions.from_json``).

    PERMISSIVE is the default mode: a missing field, a JSON null, and a value of the wrong shape
    are all NULL, and a malformed document yields an all-NULL result rather than an error.

    Args:
        col: the JSON string column, or a column name.
        schema: a DDL string or a :class:`DataType`. A Column is refused because the result type
            must be known when the expression is built, and the facade cannot fold one.
        options: only ``mode`` and ``columnNameOfCorruptRecord`` are honoured; any other key is
            refused rather than silently ignored.

    Returns:
        A column of the schema's type.

    Raises:
        UnsupportedOperationException: ``schema`` is a Column.
    """
    if isinstance(schema, Column):
        raise UnsupportedOperationException(
            "from_json does not accept a Column schema: repark resolves the result type when "
            "the expression is built, so the schema must be a DDL string or a DataType. Spark "
            "folds schema_of_json first. See docs/spark-sql-iceberg-parity.md "
            "(FNP10-JSON-SCHEMA-COLUMN-1)."
        )
    text = schema if isinstance(schema, str) else schema.simpleString()
    if options:
        pairs: list[Column | str] = []
        for key, value in options.items():
            pairs.extend([lit(key), lit(value)])
        return _scalar("from_json", col, lit(text), create_map(*pairs))
    return _scalar("from_json", col, lit(text))


def inline(*args: Any, **kwargs: Any) -> Column:
    """Multi-column generator over an array of structs; refused.

    pins: fnp-9-collections-json/C-007
    """
    _refuse("inline")


def inline_outer(*args: Any, **kwargs: Any) -> Column:
    """``inline`` keeping NULL and empty arrays; refused. pins: fnp-9-collections-json/C-007"""
    _refuse("inline_outer")


def stack(*args: Any, **kwargs: Any) -> Column:
    """Reshape n values into rows of k columns; refused. pins: fnp-9-collections-json/C-007"""
    _refuse("stack")


def call_udf(*args: Any, **kwargs: Any) -> Column:
    """Call a registered UDF by name; refused. pins: fnp-9-collections-json/C-007"""
    _refuse("call_udf")


def call_function(*args: Any, **kwargs: Any) -> Column:
    """Call any function by name; refused. pins: fnp-9-collections-json/C-007"""
    _refuse("call_function")


def sequence(*args: Any, **kwargs: Any) -> Column:
    """Generate a range array; refused pending the FNP-11 temporal arm.

    pins: fnp-9-collections-json/C-007
    """
    _refuse("sequence")


def install_into(namespace: dict[str, Any], exported: list[str]) -> None:
    """Copy the FNP-9/10 surface onto the canonical functions module."""
    sources: dict[str, Any] = {
        "array_insert": array_insert,
        "create_map": create_map,
        "map_concat": map_concat,
    }
    for name in FNP9_NAMES:
        namespace[name] = sources.get(name, globals().get(name))
        if name not in exported:
            exported.append(name)
