from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkException


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-u5-alter-ddl").getOrCreate()
    session.register_memory_catalog("sc", tmp_path)
    session.sql("CREATE NAMESPACE sc.ns")
    return session


def _metadata(warehouse: Path, table: str) -> dict[str, Any]:
    paths = sorted(
        warehouse.rglob(f"ns/{table}/metadata/*.metadata.json"),
        key=lambda path: int(path.name.split("-")[0]),
    )
    if not paths:
        raise AssertionError(f"no metadata file for {table}")
    return json.loads(paths[-1].read_text(encoding="utf-8"))


def _current_schema(metadata: dict[str, Any]) -> dict[str, Any]:
    schema_id = metadata["current-schema-id"]
    for schema in metadata["schemas"]:
        if schema["schema-id"] == schema_id:
            return schema
    raise AssertionError(f"no current schema {schema_id}")


def _field_type(fields: list[dict[str, Any]], name: str) -> Any:
    for field in fields:
        if field["name"] == name:
            return field["type"]
    raise AssertionError(f"no field {name}")


def _metadata_type(schema: dict[str, Any], path: str) -> Any:
    parts = path.split(".")
    value = _field_type(schema["fields"], parts[0])
    for part in parts[1:]:
        if value["type"] == "struct":
            value = _field_type(value["fields"], part)
        elif value["type"] == "list" and part == "element":
            value = value["element"]
        elif value["type"] == "map" and part in ("key", "value"):
            value = value[part]
        else:
            raise AssertionError(f"no nested field {path}")
    return value


def _describe_types(spark: ReparkSession, table: str) -> dict[str, str]:
    return {
        str(row[0]): str(row[1])
        for row in spark.sql(f"DESCRIBE TABLE sc.ns.{table}").collect()
    }


@pytest.mark.parametrize(
    ("table", "definition", "statement", "path", "describe_type"),
    [
        pytest.param(
            "struct_type",
            "st STRUCT<a: INT>",
            "ALTER TABLE sc.ns.struct_type ALTER COLUMN st.a TYPE BIGINT",
            "st.a",
            "struct<a:bigint>",
            id="struct",
        ),
        pytest.param(
            "list_type",
            "arr ARRAY<INT>",
            "ALTER TABLE sc.ns.list_type ALTER COLUMN arr.element TYPE BIGINT",
            "arr.element",
            "array<bigint>",
            id="list-element",
        ),
        pytest.param(
            "map_type",
            "m MAP<STRING, INT>",
            "ALTER TABLE sc.ns.map_type ALTER COLUMN m.value TYPE BIGINT",
            "m.value",
            "map<string,bigint>",
            id="map-value",
        ),
    ],
)
def test_nested_alter_column_type_updates_metadata(
    spark: ReparkSession,
    tmp_path: Path,
    table: str,
    definition: str,
    statement: str,
    path: str,
    describe_type: str,
) -> None:
    spark.sql(f"CREATE TABLE sc.ns.{table} (id BIGINT, {definition}) USING iceberg")
    spark.sql(statement)
    schema = _current_schema(_metadata(tmp_path, table))
    assert _metadata_type(schema, path) == "long"
    assert _describe_types(spark, table)[path.split(".")[0]] == describe_type


def test_unset_tblproperties_if_exists_missing_key_keeps_metadata(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql("CREATE TABLE sc.ns.props (id BIGINT) USING iceberg")
    before = _metadata(tmp_path, "props")
    spark.sql("ALTER TABLE sc.ns.props UNSET TBLPROPERTIES IF EXISTS ('nope')")
    after_if_exists = _metadata(tmp_path, "props")
    assert after_if_exists["properties"] == before["properties"]
    spark.sql("ALTER TABLE sc.ns.props UNSET TBLPROPERTIES ('nope')")
    assert _metadata(tmp_path, "props")["properties"] == before["properties"]


def test_alter_namespace_properties_update_extended_describe(spark: ReparkSession) -> None:
    spark.sql("CREATE NAMESPACE sc.nsa")
    spark.sql("ALTER NAMESPACE sc.nsa SET DBPROPERTIES ('b' = '2')")
    first_rows = [list(row) for row in spark.sql("DESCRIBE NAMESPACE EXTENDED sc.nsa").collect()]
    assert ["Properties", "((b,2))"] in first_rows
    spark.sql("ALTER NAMESPACE sc.nsa SET PROPERTIES ('z' = '9', 'a' = '1')")
    rows = [list(row) for row in spark.sql("DESCRIBE NAMESPACE EXTENDED sc.nsa").collect()]
    assert ["Properties", "((a,1), (b,2), (z,9))"] in rows
    spark.sql("CREATE NAMESPACE sc.nsorder")
    spark.sql("ALTER NAMESPACE sc.nsorder SET PROPERTIES ('z' = '9', 'a' = '1')")
    order_rows = [
        list(row) for row in spark.sql("DESCRIBE NAMESPACE EXTENDED sc.nsorder").collect()
    ]
    assert ["Properties", "((a,1), (z,9))"] in order_rows


def test_nested_map_key_type_refusal_is_spark_shaped(spark: ReparkSession) -> None:
    spark.sql("CREATE TABLE sc.ns.map_key (m MAP<STRING, INT>) USING iceberg")
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER TABLE sc.ns.map_key ALTER COLUMN m.key TYPE BIGINT")
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        "Error during planning: [NOT_SUPPORTED_CHANGE_COLUMN] ALTER TABLE ALTER/CHANGE COLUMN is "
        "not supported for changing `sc`.`ns`.`map_key`'s column `m`.`key` with type \"STRING\" "
        "to `m`.`key` with type \"BIGINT\". SQLSTATE: 0A000"
    )
    assert caught.value.getCondition() == "NOT_SUPPORTED_CHANGE_COLUMN"
    assert caught.value.getSqlState() == "0A000"


def test_top_level_alter_column_type_keeps_the_existing_route(spark: ReparkSession) -> None:
    spark.sql("CREATE TABLE sc.ns.top_level (id INT, st STRUCT<a: INT>) USING iceberg")
    spark.sql("ALTER TABLE sc.ns.top_level ALTER COLUMN id TYPE BIGINT")
    assert _describe_types(spark, "top_level")["id"] == "bigint"
    with pytest.raises(PySparkException) as caught:
        spark.sql("ALTER TABLE sc.ns.top_level ALTER COLUMN st TYPE STRING")
    assert type(caught.value) is PySparkException
    assert str(caught.value) == (
        "DataInvalid => Cannot change column type: st: struct<int> -> string"
    )
    assert caught.value.getCondition() is None
    assert caught.value.getSqlState() is None
