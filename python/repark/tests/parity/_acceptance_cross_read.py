"""Spark reads RePark-written tables and answers row-equal.

Given a RePark session and catalog, a PySpark session whose catalog points at
the same warehouse, and table names, each table is read on both engines and
compared on schema (names, common-vocabulary types, nullability) and rows
(order-insensitive, exact, floats bit-exact). One outcome per table: EQUAL,
DIFFERENT with the first differences, or SPARK-CANNOT-READ with Spark error
class and first line. Tables holding nanosecond timestamp columns report
has_ns_columns and are never skipped.

pins: ice-spark-cross-read-1/C-001
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Any, NamedTuple

import pyarrow as pa
import pyarrow.fs as pafs
from _acceptance_v3 import current_metadata_location

EQUAL = "EQUAL"
DIFFERENT = "DIFFERENT"
SPARK_CANNOT_READ = "SPARK-CANNOT-READ"

_NS_TYPE_NAMES = frozenset({"timestamp_ns", "timestamptz_ns"})
_MAX_ROW_DIFFERENCES = 5
_MAX_ERROR_LINE_CHARS = 300


class CrossReadColumn(NamedTuple):
    """One compared column: name, common-vocabulary type, nullability."""

    name: str
    common_type: str
    nullable: bool


class CrossReadOutcome(NamedTuple):
    """One table verdict plus the evidence behind it."""

    table: str
    status: str
    repark_columns: list[CrossReadColumn]
    spark_columns: list[CrossReadColumn]
    schema_equal: bool
    rows_equal: bool | None
    repark_row_count: int
    spark_row_count: int | None
    first_differences: list[str]
    spark_error_class: str | None
    spark_error_condition: str | None
    spark_error_first_line: str | None
    has_ns_columns: bool


def common_type_name(arrow_type: pa.DataType) -> str:
    """Common-vocabulary name of one Arrow type for the schema compare."""
    if pa.types.is_int8(arrow_type):
        return "tinyint"
    if pa.types.is_int16(arrow_type):
        return "smallint"
    if pa.types.is_int32(arrow_type):
        return "int"
    if pa.types.is_int64(arrow_type):
        return "bigint"
    if pa.types.is_float32(arrow_type):
        return "float"
    if pa.types.is_float64(arrow_type):
        return "double"
    if (
        pa.types.is_string(arrow_type)
        or pa.types.is_large_string(arrow_type)
        or pa.types.is_string_view(arrow_type)
    ):
        return "string"
    if (
        pa.types.is_binary(arrow_type)
        or pa.types.is_large_binary(arrow_type)
        or pa.types.is_binary_view(arrow_type)
        or pa.types.is_fixed_size_binary(arrow_type)
    ):
        return "binary"
    if pa.types.is_boolean(arrow_type):
        return "boolean"
    if pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
        return "date"
    if pa.types.is_time32(arrow_type) or pa.types.is_time64(arrow_type):
        return "time"
    if pa.types.is_timestamp(arrow_type):
        zone = ",tz" if arrow_type.tz is not None else ""
        return f"timestamp[{arrow_type.unit}{zone}]"
    if pa.types.is_decimal(arrow_type):
        return f"decimal({arrow_type.precision},{arrow_type.scale})"
    if (
        pa.types.is_list(arrow_type)
        or pa.types.is_large_list(arrow_type)
        or pa.types.is_fixed_size_list(arrow_type)
    ):
        return f"array<{common_type_name(arrow_type.value_type)}>"
    if pa.types.is_struct(arrow_type):
        fields = ",".join(
            f"{arrow_type.field(index).name}:{common_type_name(arrow_type.field(index).type)}"
            for index in range(arrow_type.num_fields)
        )
        return f"struct<{fields}>"
    if pa.types.is_map(arrow_type):
        key = common_type_name(arrow_type.key_type)
        item = common_type_name(arrow_type.item_type)
        return f"map<{key},{item}>"
    if pa.types.is_null(arrow_type):
        return "void"
    return str(arrow_type)


def columns_of(schema: pa.Schema) -> list[CrossReadColumn]:
    """Common-vocabulary columns of one Arrow schema, in order."""
    return [
        CrossReadColumn(
            name=field.name,
            common_type=common_type_name(field.type),
            nullable=field.nullable,
        )
        for field in schema
    ]


def normalize_cell(value: Any) -> Any:
    """Canonical comparable form of one cell; floats exact, instants zone-free."""
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        return repr(value)
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.astimezone(UTC).replace(tzinfo=None)
        return ("datetime", value.isoformat())
    if isinstance(value, date):
        return ("date", value.isoformat())
    if isinstance(value, time):
        return ("time", value.isoformat())
    if isinstance(value, (bytes, bytearray, memoryview)):
        return ("bytes", bytes(value).hex())
    if isinstance(value, (list, tuple)):
        return tuple(normalize_cell(item) for item in value)
    if isinstance(value, dict):
        return tuple(sorted(((key, normalize_cell(item)) for key, item in value.items()), key=repr))
    return ("repr", repr(value))


def normalized_rows(table: pa.Table) -> list[tuple[Any, ...]]:
    """Rows as name-aligned normalized tuples, sorted; column order is ignored."""
    names = sorted(table.schema.names)
    rows = [
        tuple((name, normalize_cell(record[name])) for name in names)
        for record in table.to_pylist()
    ]
    return sorted(rows, key=repr)


def compare_schemas(
    repark_columns: list[CrossReadColumn], spark_columns: list[CrossReadColumn]
) -> list[str]:
    """Schema difference lines; empty when names, types and nullability match."""
    diffs: list[str] = []
    left_names = [column.name for column in repark_columns]
    right_names = [column.name for column in spark_columns]
    if left_names != right_names:
        diffs.append(f"column names differ: repark={left_names} spark={right_names}")
    if len(repark_columns) != len(spark_columns):
        diffs.append(
            f"column count differs: repark={len(repark_columns)} spark={len(spark_columns)}"
        )
    for index, (left, right) in enumerate(zip(repark_columns, spark_columns, strict=False)):
        if left.common_type != right.common_type:
            diffs.append(
                f"column {index} ({left.name}): type repark={left.common_type} "
                f"spark={right.common_type}"
            )
        if left.nullable != right.nullable:
            diffs.append(
                f"column {index} ({left.name}): nullability repark={left.nullable} "
                f"spark={right.nullable}"
            )
    return diffs


def compare_row_sets(repark_table: pa.Table, spark_table: pa.Table) -> list[str]:
    """First row difference lines, order-insensitive; empty when the sets match."""
    left = normalized_rows(repark_table)
    right = normalized_rows(spark_table)
    if left == right:
        return []
    diffs: list[str] = []
    if len(left) != len(right):
        diffs.append(f"row count differs: repark={len(left)} spark={len(right)}")
    for index, (left_row, right_row) in enumerate(zip(left, right, strict=False)):
        if left_row != right_row:
            diffs.append(f"row {index}: repark={left_row!r} spark={right_row!r}")
        if len(diffs) >= _MAX_ROW_DIFFERENCES + 1:
            break
    return diffs[: _MAX_ROW_DIFFERENCES + 1]


def arrow_has_ns_columns(table: pa.Table) -> bool:
    """True when any top-level Arrow field is a nanosecond timestamp."""
    return any(
        pa.types.is_timestamp(field.type) and field.type.unit == "ns" for field in table.schema
    )


def iceberg_type_has_ns(node: Any) -> bool:
    """True when one Iceberg type node holds a nanosecond timestamp anywhere."""
    if isinstance(node, str):
        return node in _NS_TYPE_NAMES
    if isinstance(node, dict):
        if node.get("type") in _NS_TYPE_NAMES:
            return True
        return any(iceberg_type_has_ns(value) for value in node.values())
    if isinstance(node, list):
        return any(iceberg_type_has_ns(value) for value in node)
    return False


def metadata_has_ns_types(metadata_path: str) -> bool:
    """True when the metadata JSON current schema holds a nanosecond timestamp."""
    location = metadata_path if "://" in metadata_path else str(Path(metadata_path).absolute())
    filesystem, path = pafs.FileSystem.from_uri(location)
    with filesystem.open_input_file(path) as handle:
        doc = json.loads(handle.read())
    current = doc["current-schema-id"]
    schema = next(item for item in doc["schemas"] if item["schema-id"] == current)
    return iceberg_type_has_ns(schema["fields"])


def table_has_ns_columns(repark_table: pa.Table, metadata_file: str | None) -> bool:
    """True when Arrow storage or the metadata current schema shows nanoseconds."""
    if arrow_has_ns_columns(repark_table):
        return True
    return metadata_file is not None and metadata_has_ns_types(metadata_file)


def adopt_into_spark(
    spark: Any, spark_catalog: str, namespace: str, name: str, metadata_file: str
) -> str:
    """Register a RePark-written metadata file as a Spark table; return its name."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {spark_catalog}.{namespace}")
    qualified = f"{spark_catalog}.{namespace}.{name}"
    if not spark.catalog.tableExists(qualified):
        spark.sql(
            f"CALL {spark_catalog}.system.register_table("
            f"table => '{namespace}.{name}', metadata_file => '{metadata_file}')"
        )
    return qualified


def spark_error_condition(error: BaseException) -> str | None:
    """Spark condition of a PySpark error, or None when the error carries none."""
    getter = getattr(error, "getCondition", None)
    if not callable(getter):
        getter = getattr(error, "getErrorClass", None)
    if not callable(getter):
        return None
    try:
        condition = getter()
    except Exception:
        return None
    return str(condition) if condition else None


def spark_error_first_line(error: BaseException) -> str:
    """First error line, truncated; the class name when the message is empty."""
    lines = str(error).strip().splitlines()
    first = lines[0].strip() if lines else ""
    if not first:
        return type(error).__name__
    return first[:_MAX_ERROR_LINE_CHARS]


def cross_read_table_pair(
    repark: Any,
    repark_fqn: str,
    spark: Any,
    spark_fqn: str,
    metadata_file: str | None = None,
) -> CrossReadOutcome:
    """Read one table on both engines and compare; Spark errors become SPARK-CANNOT-READ."""
    repark_table = repark.sql(f"SELECT * FROM {repark_fqn}").to_arrow()
    has_ns = table_has_ns_columns(repark_table, metadata_file)
    repark_columns = columns_of(repark_table.schema)
    try:
        spark_table = spark.sql(f"SELECT * FROM {spark_fqn}").toArrow()
    except Exception as error:
        return CrossReadOutcome(
            table=repark_fqn,
            status=SPARK_CANNOT_READ,
            repark_columns=repark_columns,
            spark_columns=[],
            schema_equal=False,
            rows_equal=None,
            repark_row_count=repark_table.num_rows,
            spark_row_count=None,
            first_differences=[],
            spark_error_class=type(error).__name__,
            spark_error_condition=spark_error_condition(error),
            spark_error_first_line=spark_error_first_line(error),
            has_ns_columns=has_ns,
        )
    spark_columns = columns_of(spark_table.schema)
    schema_diffs = compare_schemas(repark_columns, spark_columns)
    row_diffs = compare_row_sets(repark_table, spark_table)
    status = EQUAL if not schema_diffs and not row_diffs else DIFFERENT
    return CrossReadOutcome(
        table=repark_fqn,
        status=status,
        repark_columns=repark_columns,
        spark_columns=spark_columns,
        schema_equal=not schema_diffs,
        rows_equal=not row_diffs,
        repark_row_count=repark_table.num_rows,
        spark_row_count=spark_table.num_rows,
        first_differences=schema_diffs + row_diffs,
        spark_error_class=None,
        spark_error_condition=None,
        spark_error_first_line=None,
        has_ns_columns=has_ns,
    )


def cross_read_local_tables(
    repark: Any,
    repark_catalog: str,
    spark: Any,
    spark_catalog: str,
    namespace: str,
    tables: list[str],
) -> list[CrossReadOutcome]:
    """Adopt each RePark table into Spark and compare; one outcome per table."""
    outcomes: list[CrossReadOutcome] = []
    for name in tables:
        repark_fqn = f"{repark_catalog}.{namespace}.{name}"
        metadata_file = current_metadata_location(repark, repark_fqn)
        spark_fqn = adopt_into_spark(spark, spark_catalog, namespace, name, metadata_file)
        outcomes.append(cross_read_table_pair(repark, repark_fqn, spark, spark_fqn, metadata_file))
    return outcomes


def format_outcome_table(outcomes: list[CrossReadOutcome]) -> str:
    """Render per-table outcomes as a plain-text table for the ledger."""
    lines = ["table | status | repark_rows | spark_rows | detail"]
    for outcome in outcomes:
        if outcome.status == EQUAL:
            detail = "row-equal"
        elif outcome.status == SPARK_CANNOT_READ:
            condition = f" {outcome.spark_error_condition}" if outcome.spark_error_condition else ""
            detail = f"{outcome.spark_error_class}{condition}: {outcome.spark_error_first_line}"
        else:
            detail = "; ".join(outcome.first_differences[:2]) or "differs"
        if outcome.has_ns_columns:
            detail = f"ns-columns; {detail}"
        spark_rows = outcome.spark_row_count if outcome.spark_row_count is not None else "-"
        lines.append(
            f"{outcome.table} | {outcome.status} | {outcome.repark_row_count} | "
            f"{spark_rows} | {detail}"
        )
    return "\n".join(lines)


def assert_outcomes_expected(outcomes: list[CrossReadOutcome]) -> None:
    """Fail unless every table is EQUAL or an expected nanosecond SPARK-CANNOT-READ."""
    bad = [
        outcome
        for outcome in outcomes
        if outcome.status == DIFFERENT
        or (outcome.status == SPARK_CANNOT_READ and not outcome.has_ns_columns)
    ]
    assert not bad, f"cross-read found {len(bad)} unexpected outcome(s):\n" + format_outcome_table(
        outcomes
    )
