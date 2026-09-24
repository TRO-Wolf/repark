from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException, PySparkException


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
        str(row[0]): str(row[1]) for row in spark.sql(f"DESCRIBE TABLE sc.ns.{table}").collect()
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


_TYPES_TABLE = (
    "CREATE TABLE sc.ns.types (id INT, st STRUCT<a: INT, b: BIGINT, s: STRING, c: BIGINT>, "
    "arr ARRAY<INT>, arrl ARRAY<BIGINT>, arrs ARRAY<STRING>, arrd ARRAY<BIGINT>, "
    "ki MAP<INT, INT>, kl MAP<BIGINT, INT>, ks MAP<STRING, INT>) USING iceberg"
)


def _not_supported(column: str, from_type: str, to_type: str) -> str:
    return (
        "Error during planning: [NOT_SUPPORTED_CHANGE_COLUMN] ALTER TABLE ALTER/CHANGE COLUMN is "
        f"not supported for changing `sc`.`ns`.`types`'s column {column} with type "
        f'"{from_type}" to {column} with type "{to_type}". SQLSTATE: 0A000'
    )


def _unsupported(message: str) -> str:
    return f"datafusion engine error: Execution error: Unsupported table change: {message}"


@pytest.mark.parametrize(
    ("clause", "expected_type", "message", "condition", "sql_state"),
    [
        pytest.param(
            "ki.key TYPE BIGINT",
            PySparkException,
            _unsupported("Cannot update map keys: map<int, int>"),
            None,
            None,
            id="map-key-widen",
        ),
        pytest.param(
            "kl.key TYPE STRING",
            PySparkException,
            _unsupported("Cannot change column type: kl.key: long -> string"),
            None,
            None,
            id="map-key-to-string",
        ),
        pytest.param(
            "ks.key TYPE BIGINT",
            AnalysisException,
            _not_supported("`ks`.`key`", "STRING", "BIGINT"),
            "NOT_SUPPORTED_CHANGE_COLUMN",
            "0A000",
            id="map-key-string-to-bigint",
        ),
        pytest.param(
            "kl.key TYPE INT",
            AnalysisException,
            _not_supported("`kl`.`key`", "BIGINT", "INT"),
            "NOT_SUPPORTED_CHANGE_COLUMN",
            "0A000",
            id="map-key-narrow",
        ),
        pytest.param(
            "st.b TYPE STRING",
            PySparkException,
            _unsupported("Cannot change column type: st.b: long -> string"),
            None,
            None,
            id="struct-to-string",
        ),
        pytest.param(
            "st.s TYPE BIGINT",
            AnalysisException,
            _not_supported("`st`.`s`", "STRING", "BIGINT"),
            "NOT_SUPPORTED_CHANGE_COLUMN",
            "0A000",
            id="struct-string-to-bigint",
        ),
        pytest.param(
            "st.c TYPE INT",
            AnalysisException,
            _not_supported("`st`.`c`", "BIGINT", "INT"),
            "NOT_SUPPORTED_CHANGE_COLUMN",
            "0A000",
            id="struct-narrow",
        ),
        pytest.param(
            "arrl.element TYPE STRING",
            PySparkException,
            _unsupported("Cannot change column type: arrl.element: long -> string"),
            None,
            None,
            id="list-to-string",
        ),
        pytest.param(
            "arrs.element TYPE BIGINT",
            AnalysisException,
            _not_supported("`arrs`.`element`", "STRING", "BIGINT"),
            "NOT_SUPPORTED_CHANGE_COLUMN",
            "0A000",
            id="list-string-to-bigint",
        ),
        pytest.param(
            "arrd.element TYPE INT",
            AnalysisException,
            _not_supported("`arrd`.`element`", "BIGINT", "INT"),
            "NOT_SUPPORTED_CHANGE_COLUMN",
            "0A000",
            id="list-narrow",
        ),
        pytest.param(
            "st.zz TYPE BIGINT",
            AnalysisException,
            "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or "
            "function parameter with name `st`.`zz` cannot be resolved. Did you mean one of the "
            "following? [`id`, `st`, `arr`, `arrl`, `arrs`, `arrd`, `ki`, `kl`, `ks`]. "
            "SQLSTATE: 42703",
            "UNRESOLVED_COLUMN.WITH_SUGGESTION",
            "42703",
            id="missing-field",
        ),
        pytest.param(
            "ki.KEY TYPE BIGINT",
            AnalysisException,
            "Error during planning: [INVALID_FIELD_NAME] Field name `ki`.`KEY` is invalid: `ki` "
            "is not a struct. SQLSTATE: 42000",
            "INVALID_FIELD_NAME",
            "42000",
            id="map-key-case",
        ),
    ],
)
def test_nested_alter_column_type_refusals_match_spark(
    spark: ReparkSession,
    tmp_path: Path,
    clause: str,
    expected_type: type[Exception],
    message: str,
    condition: str | None,
    sql_state: str | None,
) -> None:
    spark.sql(_TYPES_TABLE)
    before = _current_schema(_metadata(tmp_path, "types"))
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"ALTER TABLE sc.ns.types ALTER COLUMN {clause}")
    assert type(caught.value) is expected_type
    assert str(caught.value) == message
    assert caught.value.getCondition() == condition
    assert caught.value.getSqlState() == sql_state
    assert _current_schema(_metadata(tmp_path, "types")) == before


def test_nested_alter_column_type_on_a_missing_table_matches_spark(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER TABLE sc.ns.nope ALTER COLUMN st.a TYPE BIGINT")
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `sc`.`ns`.`nope` "
        "cannot be found. Verify the spelling and correctness of the schema and catalog. If you "
        "did not qualify the name with a schema, verify the current_schema() output, or qualify "
        "the name with the correct schema and catalog. To tolerate the error on drop use DROP "
        "VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    assert caught.value.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"
    assert caught.value.getSqlState() == "42P01"


@pytest.mark.parametrize(
    "statement",
    [
        pytest.param("ALTER NAMESPACE sc.nope SET DBPROPERTIES ('b' = '2')", id="namespace"),
        pytest.param("ALTER DATABASE sc.nope SET PROPERTIES ('b' = '2')", id="database"),
    ],
)
def test_alter_namespace_missing_namespace_matches_spark(
    spark: ReparkSession, statement: str
) -> None:
    with pytest.raises(AnalysisException) as caught:
        spark.sql(statement)
    assert type(caught.value) is AnalysisException
    assert str(caught.value) == (
        "Error during planning: [SCHEMA_NOT_FOUND] The schema `nope` cannot be found. Verify the "
        "spelling and correctness of the schema and catalog.\nIf you did not qualify the name "
        "with a catalog, verify the current_schema() output, or qualify the name with the correct "
        "catalog.\nTo tolerate the error on drop use DROP SCHEMA IF EXISTS. SQLSTATE: 42704"
    )
    assert caught.value.getCondition() == "SCHEMA_NOT_FOUND"
    assert caught.value.getSqlState() == "42704"


_RESERVED_LOCATION = (
    "[UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY] The feature is not supported: location is a "
    "reserved namespace property, please use the LOCATION clause to specify it. SQLSTATE: 0A000"
)
_RESERVED_OWNER = (
    "[UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY] The feature is not supported: owner is a "
    "reserved namespace property, it will be set to the current user. SQLSTATE: 0A000"
)


@pytest.mark.parametrize(
    ("tail", "message", "condition", "sql_state"),
    [
        pytest.param(
            "('location' = '/x')",
            _RESERVED_LOCATION,
            "UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY",
            "0A000",
            id="location",
        ),
        pytest.param(
            "('owner' = 'bob')",
            _RESERVED_OWNER,
            "UNSUPPORTED_FEATURE.SET_NAMESPACE_PROPERTY",
            "0A000",
            id="owner",
        ),
        pytest.param(
            "('a' = '1', 'a' = '2')",
            "[DUPLICATE_KEY] Found duplicate keys `a`. SQLSTATE: 23505",
            "DUPLICATE_KEY",
            "23505",
            id="duplicate",
        ),
        pytest.param(
            "()",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near ')'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="empty",
        ),
        pytest.param(
            "('w' = foo)",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'foo'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="word-value",
        ),
        pytest.param(
            "('k2' = 'v') extra",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'extra': extra input 'extra'. "
            "SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="trailing",
        ),
    ],
)
def test_alter_namespace_refuses_properties_like_spark(
    spark: ReparkSession, tail: str, message: str, condition: str, sql_state: str
) -> None:
    spark.sql("CREATE NAMESPACE sc.nsr")
    before = [list(row) for row in spark.sql("DESCRIBE NAMESPACE EXTENDED sc.nsr").collect()]
    with pytest.raises(ParseException) as caught:
        spark.sql(f"ALTER NAMESPACE sc.nsr SET DBPROPERTIES {tail}")
    assert type(caught.value) is ParseException
    assert str(caught.value) == message
    assert caught.value.getCondition() == condition
    assert caught.value.getSqlState() == sql_state
    after = [list(row) for row in spark.sql("DESCRIBE NAMESPACE EXTENDED sc.nsr").collect()]
    assert after == before


def test_alter_namespace_accepted_shapes_describe_like_spark(spark: ReparkSession) -> None:
    spark.sql("CREATE NAMESPACE sc.nsp")
    spark.sql("ALTER NAMESPACE sc.nsp SET DBPROPERTIES ('comment' = 'c', 'LOCATION' = '/x')")
    spark.sql("ALTER NAMESPACE sc.nsp SET DBPROPERTIES (a.b = 'v', 'n' = 5, 'tu' = TRUE)")
    rows = [list(row) for row in spark.sql("DESCRIBE NAMESPACE EXTENDED sc.nsp").collect()]
    assert rows == [
        ["Catalog Name", "sc"],
        ["Namespace Name", "nsp"],
        ["Comment", "c"],
        ["Properties", "((LOCATION,/x), (a.b,v), (n,5), (tu,true))"],
    ]


@pytest.mark.parametrize(
    ("tail", "message"),
    [
        pytest.param(
            "IF ('k')",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '(': missing 'EXISTS'. SQLSTATE: 42601",
            id="if-alone",
        ),
        pytest.param(
            "EXISTS ('k')",
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'EXISTS': extra input 'EXISTS'. "
            "SQLSTATE: 42601",
            id="exists-alone",
        ),
    ],
)
def test_unset_tblproperties_if_without_exists_matches_spark(
    spark: ReparkSession, tmp_path: Path, tail: str, message: str
) -> None:
    spark.sql("CREATE TABLE sc.ns.unset_if (id BIGINT) USING iceberg TBLPROPERTIES ('k' = 'v')")
    before = _metadata(tmp_path, "unset_if")["properties"]
    with pytest.raises(ParseException) as caught:
        spark.sql(f"ALTER TABLE sc.ns.unset_if UNSET TBLPROPERTIES {tail}")
    assert type(caught.value) is ParseException
    assert str(caught.value) == message
    assert caught.value.getCondition() == "PARSE_SYNTAX_ERROR"
    assert caught.value.getSqlState() == "42601"
    assert _metadata(tmp_path, "unset_if")["properties"] == before


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
