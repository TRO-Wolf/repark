"""JSON facade wrappers, and the installer for the FNP-9/10 surface."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from repark.errors import UnsupportedOperationException
from repark.spark.column import Column
from repark.spark.functions import _scalar, lit
from repark.spark.functions_collections import array_insert, create_map, map_concat

if TYPE_CHECKING:
    from repark.spark.types import DataType

FNP9_NAMES: tuple[str, ...] = (
    "array_insert",
    "create_map",
    "from_json",
    "get_json_object",
    "json_array_length",
    "json_object_keys",
    "map_concat",
    "to_json",
)


def _refuse_json_options(name: str, options: dict[str, str] | None) -> None:
    """Refuse a non-empty JSON option mapping rather than ignoring it silently."""
    if options:
        raise UnsupportedOperationException(
            f"{name} does not support JSON options yet: repark implements none of Spark's "
            f"JSON writer or inference options, and ignoring {sorted(options)} would change "
            "the answer silently. See docs/spark-sql-iceberg-parity.md "
            "(FNP10-JSON-OPTIONS-1)."
        )


def get_json_object(col: Column | str, path: str) -> Column:
    """Extract one JSONPath value as a string (PySpark ``functions.get_json_object``)."""
    return _scalar("get_json_object", col, path, lit_indices=frozenset({1}))


def json_array_length(col: Column | str) -> Column:
    """Length of a JSON array (PySpark ``functions.json_array_length``)."""
    return _scalar("json_array_length", col)


def json_object_keys(col: Column | str) -> Column:
    """Keys of a JSON object in document order (PySpark ``functions.json_object_keys``)."""
    return _scalar("json_object_keys", col)


def to_json(col: Column | str, options: dict[str, str] | None = None) -> Column:
    """Render a STRUCT, ARRAY, or MAP column as JSON (PySpark ``functions.to_json``)."""
    _refuse_json_options("to_json", options)
    return _scalar("to_json", col)


def from_json(
    col: Column | str,
    schema: DataType | str | Column,
    options: dict[str, str] | None = None,
) -> Column:
    """Parse a JSON string column against a schema (PySpark ``functions.from_json``)."""
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
