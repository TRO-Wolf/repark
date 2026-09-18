"""ICE-NESTED-EVO-1 round 2 — nested DDL metadata (ids, order, doc, required) against Spark 4.1.2.

Every cell reads its expected answer from the `schema_cells` block of the recorded
`fixtures/torture/data/ice_nested_evo_1/oracle.json`: the current schema of the table's metadata
file after Spark ran the cell (field ids, names, `required`, `doc`, child order), the read schema,
and the refusal Spark raised. Each cell replays Spark's own statements on a fresh RePark
catalog and compares the RePark metadata file, the read on the SQL door and the DataFrame door,
and the refusal class and message. The two CREATE cells also create the same schema through the
DataFrame door (`writeTo(...).create()`) against Spark's recorded DataFrame-door metadata
(`dataframe_create_cells`): Spark's V2 CTAS makes every column nullable, so there the required
child is optional.

pins: ice-nested-evo-1/C-014, C-015, C-016, C-017, C-018, C-019, C-020
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import _record_ice_nested_evo_1 as recorder
import pytest

_ORACLE: dict[str, Any] = json.loads(recorder.ORACLE_FILE.read_text(encoding="utf-8"))
_SCHEMA_CELLS: dict[str, dict[str, Any]] = _ORACLE["schema_cells"]
_DATAFRAME_CREATE_CELLS: dict[str, dict[str, Any]] = _ORACLE["dataframe_create_cells"]
_LABELS: tuple[str, ...] = tuple(
    label.removeprefix("v2_") for label in _SCHEMA_CELLS if label.startswith("v2_")
)
_CREATE_LABELS = ("create_field_ids", "create_required_child")
_SPARK_SIMPLE_TYPES = {
    "integer": "int",
    "long": "bigint",
    "string": "string",
}
_ARROW_SIMPLE_TYPES = {
    "int32": "int",
    "int64": "bigint",
    "string": "string",
    "large_string": "string",
    "string_view": "string",
}


def _session() -> Any:
    """Return the facade session with format-version 3 CREATE allowed."""
    from repark import ReparkSession

    return (
        ReparkSession.builder.appName("ice-nested-evo-1-schema")
        .config("repark.sql.allowCreateFormatVersion3", "true")
        .getOrCreate()
    )


def _without_schema_ids(value: Any) -> Any:
    """Drop the `schema-id` and `identifier-field-ids` keys at every level of a schema JSON."""
    if isinstance(value, dict):
        return {
            key: _without_schema_ids(child)
            for key, child in value.items()
            if key not in ("schema-id", "identifier-field-ids")
        }
    if isinstance(value, list):
        return [_without_schema_ids(child) for child in value]
    return value


def _current_schema(warehouse: Path, table: str) -> dict[str, Any] | None:
    """Return the current schema JSON of a RePark table from its newest metadata file."""
    files = sorted(
        warehouse.rglob(f"ns/{table}/metadata/*.metadata.json"),
        key=lambda path: int(path.name.split("-")[0]),
    )
    if not files:
        return None
    metadata = json.loads(files[-1].read_text(encoding="utf-8"))
    for schema in metadata["schemas"]:
        if schema["schema-id"] == metadata["current-schema-id"]:
            return _without_schema_ids(schema)
    return None


def _spark_shape(data_type: Any) -> str:
    """Render a Spark schema-JSON type as a simple string that marks non-nullable children."""
    if isinstance(data_type, str):
        return _SPARK_SIMPLE_TYPES.get(data_type, data_type)
    kind = data_type["type"]
    if kind == "struct":
        children = ",".join(
            f"{field['name']}:{_spark_shape(field['type'])}"
            + ("" if field["nullable"] else " not null")
            for field in data_type["fields"]
        )
        return f"struct<{children}>"
    if kind == "array":
        return f"array<{_spark_shape(data_type['elementType'])}>"
    if kind == "map":
        return f"map<{_spark_shape(data_type['keyType'])},{_spark_shape(data_type['valueType'])}>"
    raise AssertionError(f"unmapped Spark type {data_type}")


def _arrow_shape(data_type: Any) -> str:
    """Render an Arrow type in the notation of `_spark_shape`."""
    import pyarrow as pa

    if pa.types.is_struct(data_type):
        children = ",".join(
            f"{data_type.field(index).name}:{_arrow_shape(data_type.field(index).type)}"
            + ("" if data_type.field(index).nullable else " not null")
            for index in range(data_type.num_fields)
        )
        return f"struct<{children}>"
    if pa.types.is_map(data_type):
        return f"map<{_arrow_shape(data_type.key_type)},{_arrow_shape(data_type.item_type)}>"
    if pa.types.is_list(data_type) or pa.types.is_large_list(data_type):
        return f"array<{_arrow_shape(data_type.value_type)}>"
    return _ARROW_SIMPLE_TYPES.get(str(data_type), str(data_type))


def _read_shape(table: Any) -> list[str]:
    """Return `name:type` for every column of an Arrow result."""
    return [f"{field.name}:{_arrow_shape(field.type)}" for field in table.schema]


def _spark_read_shape(schema: dict[str, Any]) -> list[str]:
    """Return `name:type` for every column of Spark's recorded read schema."""
    return [f"{field['name']}:{_spark_shape(field['type'])}" for field in schema["fields"]]


def _message_head(error: dict[str, Any]) -> str:
    """Return Spark's first message line up to and including its SQLSTATE."""
    message = error["message"]
    at = message.find("SQLSTATE: ")
    return message if at < 0 else message[: at + len("SQLSTATE: 00000")]


def _replay(spark: Any, format_version: str, warehouse: Path) -> dict[str, Any]:
    """Replay every schema cell of one format version on a fresh RePark catalog."""
    catalog = f"ice_nested_evo_1_schema_v{format_version}_{warehouse.name.replace('-', '_')}"
    spark.register_memory_catalog(catalog, warehouse)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
    outcomes: dict[str, Any] = {}
    for label, cell in _SCHEMA_CELLS.items():
        if not label.startswith(f"v{format_version}_"):
            continue
        error = None
        for statement in cell["statements"]:
            try:
                spark.sql(statement.replace("sc.ns.", f"{catalog}.ns.")).collect()
            except Exception as exc:
                error = exc
                break
        table = cell["table"]
        outcome: dict[str, Any] = {"error": error, "schema": _current_schema(warehouse, table)}
        if outcome["schema"] is not None:
            qualified = f"{catalog}.ns.{table}"
            outcome["sql"] = _read_shape(spark.sql(f"SELECT * FROM {qualified}").toArrow())
            outcome["dataframe"] = _read_shape(spark.table(qualified).toArrow())
        outcomes[label.removeprefix(f"v{format_version}_")] = outcome
    return outcomes


_REPLAYS: dict[str, dict[str, Any]] = {}


def _replayed(format_version: str, tmp_path_factory: pytest.TempPathFactory) -> dict[str, Any]:
    """Replay one format version once per worker and cache the outcomes."""
    if format_version not in _REPLAYS:
        warehouse = tmp_path_factory.mktemp(f"nested-schema-v{format_version}")
        _REPLAYS[format_version] = _replay(_session(), format_version, warehouse)
    return _REPLAYS[format_version]


def _params(labels: tuple[str, ...]) -> list[Any]:
    """Return `(label, format_version)` params for the given labels, v2 and v3."""
    return [
        pytest.param(label, format_version, id=f"{label}-v{format_version}")
        for format_version in recorder.FORMAT_VERSIONS
        for label in labels
    ]


@pytest.mark.parametrize(("label", "format_version"), _params(_LABELS))
def test_nested_ddl_metadata_matches_spark(
    label: str, format_version: str, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """The metadata file's current schema — ids, order, `required`, `doc` — is Spark's."""
    outcome = _replayed(format_version, tmp_path_factory)[label]
    cell = _SCHEMA_CELLS[f"v{format_version}_{label}"]
    assert outcome["schema"] == _without_schema_ids(cell["metadata_schema"]), (
        label,
        outcome["schema"],
        repr(outcome["error"]),
    )


@pytest.mark.parametrize(("label", "format_version"), _params(_LABELS))
def test_nested_ddl_outcome_matches_spark(
    label: str, format_version: str, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """RePark accepts and refuses what Spark does, with Spark's exception class and text."""
    outcome = _replayed(format_version, tmp_path_factory)[label]
    spark_error = _SCHEMA_CELLS[f"v{format_version}_{label}"]["error"]
    error = outcome["error"]
    if spark_error is None:
        assert error is None, (label, repr(error))
        return
    assert error is not None, (label, "accepted", spark_error)
    assert type(error).__name__ == spark_error["python_class"], (label, repr(error))
    assert _message_head(spark_error) in str(error), (label, str(error), spark_error["message"])


@pytest.mark.parametrize(("label", "format_version"), _params(_LABELS))
def test_nested_ddl_read_schema_matches_spark(
    label: str, format_version: str, tmp_path_factory: pytest.TempPathFactory
) -> None:
    """`SELECT *` and `table(...)` answer Spark's columns, types and child nullability."""
    outcome = _replayed(format_version, tmp_path_factory)[label]
    cell = _SCHEMA_CELLS[f"v{format_version}_{label}"]
    expected = _spark_read_shape(cell["read_schema"])
    assert outcome.get("sql") == expected, (label, "sql", outcome.get("sql"), expected)
    assert outcome.get("dataframe") == expected, (label, "dataframe", outcome.get("dataframe"))


@pytest.mark.parametrize(("label", "format_version"), _params(_CREATE_LABELS))
def test_dataframe_door_create_matches_spark_metadata(
    label: str, format_version: str, tmp_path: Path
) -> None:
    """`writeTo(...).create()` with Spark's nested schema writes Spark's DataFrame-door metadata."""
    from repark.spark.sql.types import StructType

    cell = _DATAFRAME_CREATE_CELLS[f"v{format_version}_{label}"]
    spark = _session()
    catalog = f"ice_nested_evo_1_df_{label}_v{format_version}"
    spark.register_memory_catalog(catalog, tmp_path)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.ns")
    frame = spark.createDataFrame([], StructType.fromJson(cell["read_schema"]))
    table = cell["table"]
    frame.writeTo(f"{catalog}.ns.{table}").using("iceberg").tableProperty(
        "format-version", format_version
    ).create()
    assert _current_schema(tmp_path, table) == _without_schema_ids(cell["metadata_schema"])
