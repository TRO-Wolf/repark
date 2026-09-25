from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import (
    AnalysisException,
    IllegalArgumentException,
    ParseException,
    PySparkException,
    UnsupportedOperationException,
)

_COMMENT_TABLE = (
    "CREATE TABLE sc.ns.ac (id BIGINT, data STRING, cat STRING, st STRUCT<x: INT>) USING iceberg"
)

_SAME_COLUMN = (
    "Error during planning: [NOT_SUPPORTED_CHANGE_SAME_COLUMN] ALTER TABLE ALTER/CHANGE COLUMN "
    "is not supported for changing `sc`.`ns`.`ac`'s column {column} including its nested fields "
    "multiple times in the same command. SQLSTATE: 0A000"
)


@pytest.fixture()
def spark(tmp_path: Path) -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-ice-ddl-alter-2").getOrCreate()
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


def _metadata_files(warehouse: Path, table: str) -> int:
    return len(list(warehouse.rglob(f"ns/{table}/metadata/*.metadata.json")))


def _docs(warehouse: Path) -> dict[str, str | None]:
    schema = _current_schema(_metadata(warehouse, "ac"))
    docs: dict[str, str | None] = {}
    for field in schema["fields"]:
        docs[field["name"]] = field.get("doc")
        if isinstance(field["type"], dict) and field["type"]["type"] == "struct":
            for child in field["type"]["fields"]:
                docs[f"{field['name']}.{child['name']}"] = child.get("doc")
    return docs


def test_alter_column_comment_lands_the_doc_like_spark(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql(_COMMENT_TABLE)
    spark.sql("ALTER TABLE sc.ns.ac ALTER COLUMN data COMMENT 'new doc'")
    spark.sql("ALTER TABLE sc.ns.ac ALTER COLUMN st.x COMMENT 'nested doc'")
    spark.sql("ALTER TABLE sc.ns.ac ALTER COLUMN id COMMENT ''")
    spark.sql("ALTER TABLE sc.ns.ac CHANGE COLUMN cat COMMENT 'via change'")
    assert _docs(tmp_path) == {
        "id": "",
        "data": "new doc",
        "cat": "via change",
        "st": None,
        "st.x": "nested doc",
    }
    described = [list(row) for row in spark.sql("DESCRIBE TABLE sc.ns.ac").collect()]
    assert described == [
        ["id", "bigint", ""],
        ["data", "string", "new doc"],
        ["cat", "string", "via change"],
        ["st", "struct<x:int>", None],
    ]


def test_alter_column_comment_takes_the_dbt_statement_shapes(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql(_COMMENT_TABLE)
    files = _metadata_files(tmp_path, "ac")
    spark.sql("ALTER TABLE sc.ns.ac ALTER COLUMN data COMMENT 'a', st.x COMMENT 'b'")
    assert _metadata_files(tmp_path, "ac") == files + 1
    indent = "\n              "
    spark.sql(f"alter table sc.ns.ac alter column{indent}data{indent}comment 'it\\'s dbt';")
    spark.sql(f"alter table sc.ns.ac change column{indent}cat{indent}comment 'q';")
    spark.sql("ALTER TABLE sc.ns.ac CHANGE id COMMENT 'bare change'")
    spark.sql("ALTER TABLE sc.ns.ac ALTER st COMMENT 'bare alter'")
    assert _docs(tmp_path) == {
        "id": "bare change",
        "data": "it's dbt",
        "cat": "q",
        "st": "bare alter",
        "st.x": "b",
    }


@pytest.mark.parametrize(
    ("clause", "expected_type", "message", "condition", "sql_state"),
    [
        pytest.param(
            "nope COMMENT 'x'",
            AnalysisException,
            "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or "
            "function parameter with name `nope` cannot be resolved. Did you mean one of the "
            "following? [`id`, `data`, `cat`, `st`]. SQLSTATE: 42703",
            "UNRESOLVED_COLUMN.WITH_SUGGESTION",
            "42703",
            id="missing-column",
        ),
        pytest.param(
            "st.nope COMMENT 'x'",
            AnalysisException,
            "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or "
            "function parameter with name `st`.`nope` cannot be resolved. Did you mean one of the "
            "following? [`id`, `data`, `cat`, `st`]. SQLSTATE: 42703",
            "UNRESOLVED_COLUMN.WITH_SUGGESTION",
            "42703",
            id="missing-nested-field",
        ),
        pytest.param(
            "id.x COMMENT 'x'",
            AnalysisException,
            "Error during planning: [INVALID_FIELD_NAME] Field name `id`.`x` is invalid: `id` is "
            "not a struct. SQLSTATE: 42000",
            "INVALID_FIELD_NAME",
            "42000",
            id="primitive-parent",
        ),
        pytest.param(
            "cat COMMENT NULL",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'NULL'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="null-comment",
        ),
        pytest.param(
            "data COMMENT 5",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '5'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="number-comment",
        ),
        pytest.param(
            "data COMMENT",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="missing-comment",
        ),
        pytest.param(
            "st.x COMMENT 'x' FIRST",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'FIRST': extra input 'FIRST'. "
            "SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="trailing-position",
        ),
        pytest.param(
            "data TYPE STRING COMMENT 'x'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="type-then-comment",
        ),
        pytest.param(
            "data SET NOT NULL COMMENT 'x'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="set-not-null-then-comment",
        ),
        pytest.param(
            "data DROP NOT NULL COMMENT 'x'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="drop-not-null-then-comment",
        ),
        pytest.param(
            "data SET DEFAULT 'a' COMMENT 'x'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="set-default-then-comment",
        ),
        pytest.param(
            "data FIRST COMMENT 'x'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'COMMENT'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="first-then-comment",
        ),
        pytest.param(
            "'data' COMMENT 'x'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near ''data''. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="single-quoted-name",
        ),
        pytest.param(
            "data COMMENT 'x' TYPE STRING",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'TYPE'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="trailing-type-action",
        ),
        pytest.param(
            "data COMMENT 'x' DROP NOT NULL",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'DROP'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="trailing-drop-action",
        ),
        pytest.param(
            "data COMMENT 'x' TYPE",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'TYPE': extra input 'TYPE'. "
            "SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="trailing-bare-type",
        ),
        pytest.param(
            "data COMMENT 'a', cat garbage",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near 'garbage': extra input 'garbage'. "
            "SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="list-extra-input",
        ),
        pytest.param(
            "data COMMENT 'a', cat",
            ParseException,
            'SQL error: ParserError("Operation not allowed: ALTER TABLE table ALTER COLUMN '
            'requires a TYPE, a SET/DROP, a COMMENT, or a FIRST/AFTER.")',
            None,
            None,
            id="list-spec-without-action",
        ),
        pytest.param(
            "data COMMENT 'a', cat TYPE STRING",
            UnsupportedOperationException,
            "This feature is not implemented: ALTER TABLE … ALTER COLUMN mixes COMMENT with "
            "another change for `cat` in one column list; only a list of COMMENT changes is "
            "supported, so split the statement",
            None,
            None,
            id="mixed-list-type",
        ),
        pytest.param(
            "data COMMENT 'a', cat FIRST",
            UnsupportedOperationException,
            "This feature is not implemented: ALTER TABLE … ALTER COLUMN mixes COMMENT with "
            "another change for `cat` in one column list; only a list of COMMENT changes is "
            "supported, so split the statement",
            None,
            None,
            id="mixed-list-first",
        ),
        pytest.param(
            "id DROP NOT NULL, data COMMENT 'dn'",
            UnsupportedOperationException,
            "This feature is not implemented: ALTER TABLE … ALTER COLUMN mixes COMMENT with "
            "another change for `id` in one column list; only a list of COMMENT changes is "
            "supported, so split the statement",
            None,
            None,
            id="mixed-list-drop-not-null-first",
        ),
        pytest.param(
            "cat TYPE STRING, data COMMENT 'tf'",
            UnsupportedOperationException,
            "This feature is not implemented: ALTER TABLE … ALTER COLUMN mixes COMMENT with "
            "another change for `cat` in one column list; only a list of COMMENT changes is "
            "supported, so split the statement",
            None,
            None,
            id="mixed-list-type-first",
        ),
        pytest.param(
            "id DROP NOT NULL, data COMMENT",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near end of input. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="mixed-list-missing-literal",
        ),
        pytest.param(
            "data COMMENT 'dup', data COMMENT 'dup2'",
            AnalysisException,
            _SAME_COLUMN.format(column="`data`"),
            "NOT_SUPPORTED_CHANGE_SAME_COLUMN",
            "0A000",
            id="repeated-column",
        ),
        pytest.param(
            "Data COMMENT 'c1', data COMMENT 'c2'",
            AnalysisException,
            _SAME_COLUMN.format(column="`data`"),
            "NOT_SUPPORTED_CHANGE_SAME_COLUMN",
            "0A000",
            id="repeated-column-case",
        ),
        pytest.param(
            "st COMMENT 'p', st.x COMMENT 'c'",
            AnalysisException,
            _SAME_COLUMN.format(column="`st`"),
            "NOT_SUPPORTED_CHANGE_SAME_COLUMN",
            "0A000",
            id="repeated-parent-and-field",
        ),
        pytest.param(
            "\"data\" COMMENT 'dq'",
            ParseException,
            "[PARSE_SYNTAX_ERROR] Syntax error at or near '\"data\"'. SQLSTATE: 42601",
            "PARSE_SYNTAX_ERROR",
            "42601",
            id="double-quoted-name",
        ),
    ],
)
def test_alter_column_comment_refusals_match_spark(
    spark: ReparkSession,
    tmp_path: Path,
    clause: str,
    expected_type: type[Exception],
    message: str,
    condition: str | None,
    sql_state: str | None,
) -> None:
    spark.sql(_COMMENT_TABLE)
    before = _current_schema(_metadata(tmp_path, "ac"))
    files = _metadata_files(tmp_path, "ac")
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"ALTER TABLE sc.ns.ac ALTER COLUMN {clause}")
    assert type(caught.value) is expected_type
    assert str(caught.value) == message
    assert caught.value.getCondition() == condition
    assert caught.value.getSqlState() == sql_state
    assert _current_schema(_metadata(tmp_path, "ac")) == before
    assert _metadata_files(tmp_path, "ac") == files


@pytest.mark.parametrize(
    ("statement", "near"),
    [
        pytest.param(
            "ALTER TABLE IF EXISTS sc.ns.ac ALTER COLUMN id COMMENT 'ie'", "EXISTS", id="if-exists"
        ),
        pytest.param(
            "alter table if exists sc.ns.ac alter column id comment 'lw'",
            "exists",
            id="if-exists-lower",
        ),
        pytest.param(
            "ALTER TABLE sc.ns.ac PARTITION (id=1) ALTER COLUMN id COMMENT 'pp'",
            "ALTER",
            id="partition-spec",
        ),
        pytest.param(
            "ALTER TABLE sc.ns.ac PARTITION (id=1) ALTER COLUMN st.x COMMENT 'pnest'",
            "ALTER",
            id="partition-spec-nested",
        ),
    ],
)
def test_wrapped_column_comments_are_parse_errors_like_spark(
    spark: ReparkSession, tmp_path: Path, statement: str, near: str
) -> None:
    spark.sql(_COMMENT_TABLE)
    files = _metadata_files(tmp_path, "ac")
    with pytest.raises(ParseException) as caught:
        spark.sql(statement)
    assert str(caught.value) == (
        f"[PARSE_SYNTAX_ERROR] Syntax error at or near '{near}'. SQLSTATE: 42601"
    )
    assert caught.value.getCondition() == "PARSE_SYNTAX_ERROR"
    assert caught.value.getSqlState() == "42601"
    assert _metadata_files(tmp_path, "ac") == files


def test_alter_column_comment_on_a_map_key_matches_spark(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql("CREATE TABLE sc.ns.mk (m MAP<STRING, INT>) USING iceberg")
    with pytest.raises(PySparkException) as caught:
        spark.sql("ALTER TABLE sc.ns.mk ALTER COLUMN m.key COMMENT 'mk'")
    assert str(caught.value) == (
        "datafusion engine error: Execution error: Unsupported table change: "
        "Cannot update map keys: map<string, int>"
    )


_MAP_KEY_TABLE = (
    "CREATE TABLE sc.ns.ac (id BIGINT, data STRING, m MAP<STRING, STRUCT<z: INT>>, "
    "mm MAP<STRUCT<k: INT>, INT>, arr ARRAY<STRUCT<a: INT>>) USING iceberg"
)


def _field_id(warehouse: Path, top: str, container: str, child: str) -> int:
    fields = _current_schema(_metadata(warehouse, "ac"))["fields"]
    field = next(field for field in fields if field["name"] == top)
    nested = field["type"][container]["fields"]
    return next(entry["id"] for entry in nested if entry["name"] == child)


@pytest.mark.parametrize(
    ("clause", "shape"),
    [
        pytest.param("mm.key.k COMMENT 'k'", "alter-mm", id="map-key-field"),
        pytest.param(
            "data COMMENT 'd2', mm.key.k COMMENT 'k'", "alter-mm", id="map-key-field-in-list"
        ),
        pytest.param(
            "mm.key.k COMMENT 'y', m.key COMMENT 'x'", "update-m", id="map-keys-in-schema-order"
        ),
        pytest.param("mm.key.k TYPE BIGINT", "alter-mm", id="map-key-field-type"),
        pytest.param("m.key COMMENT 'x', m.key COMMENT 'y'", "same-m-key", id="repeated-map-key"),
        pytest.param(
            "m.key COMMENT 'x', nope COMMENT 'y'", "unresolved", id="map-key-then-missing"
        ),
    ],
)
def test_map_key_changes_refuse_in_spark_order(
    spark: ReparkSession, tmp_path: Path, clause: str, shape: str
) -> None:
    spark.sql(_MAP_KEY_TABLE)
    k_id = _field_id(tmp_path, "mm", "key", "k")
    z_id = _field_id(tmp_path, "m", "value", "z")
    unsupported = "datafusion engine error: Execution error: Unsupported table change: "
    expected = {
        "alter-mm": (
            f"{unsupported}Cannot alter map keys: map<struct<{k_id}: k: optional int>, int>"
        ),
        "update-m": (
            f"{unsupported}Cannot update map keys: map<string, struct<{z_id}: z: optional int>>"
        ),
        "same-m-key": _SAME_COLUMN.format(column="`m`.`key`"),
        "unresolved": (
            "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or "
            "function parameter with name `nope` cannot be resolved. Did you mean one of the "
            "following? [`id`, `data`, `m`, `mm`, `arr`]. SQLSTATE: 42703"
        ),
    }[shape]
    files = _metadata_files(tmp_path, "ac")
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"ALTER TABLE sc.ns.ac ALTER COLUMN {clause}")
    assert str(caught.value) == expected
    assert len(_metadata(tmp_path, "ac")["schemas"]) == 1
    assert _metadata_files(tmp_path, "ac") == files


def test_element_and_value_comments_add_no_schema_like_spark(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql(_MAP_KEY_TABLE)
    for clause in [
        "m.value COMMENT 'v'",
        "arr.element COMMENT 'e'",
        "m.value COMMENT 'v', arr.element COMMENT 'e'",
    ]:
        spark.sql(f"ALTER TABLE sc.ns.ac ALTER COLUMN {clause}")
        assert len(_metadata(tmp_path, "ac")["schemas"]) == 1, clause
    spark.sql("ALTER TABLE sc.ns.ac ALTER COLUMN m.value COMMENT 'v', data COMMENT 'dv'")
    metadata = _metadata(tmp_path, "ac")
    assert len(metadata["schemas"]) == 2
    fields = {field["name"]: field for field in _current_schema(metadata)["fields"]}
    assert fields["data"]["doc"] == "dv"
    assert "doc" not in json.dumps(fields["m"])
    assert "doc" not in json.dumps(fields["arr"])


@pytest.mark.parametrize(
    "clause",
    [
        pytest.param("ADD COLUMNS (z2 INT), ALTER COLUMN id COMMENT 'aa'", id="add-then-alter"),
        pytest.param("ALTER COLUMN id DROP NOT NULL foo COMMENT 'x'", id="action-word-comment"),
    ],
)
def test_other_column_comment_statements_answer_the_residual_refusal(
    spark: ReparkSession, tmp_path: Path, clause: str
) -> None:
    spark.sql(_COMMENT_TABLE)
    files = _metadata_files(tmp_path, "ac")
    with pytest.raises(UnsupportedOperationException) as caught:
        spark.sql(f"ALTER TABLE sc.ns.ac {clause}")
    assert str(caught.value) == (
        "This feature is not implemented: ALTER TABLE … ALTER COLUMN … COMMENT is supported "
        "only as ALTER TABLE <table> ALTER COLUMN <column> COMMENT '<doc>' or a list of such "
        "COMMENT specs; this statement shape is not supported"
    )
    assert _metadata_files(tmp_path, "ac") == files


@pytest.mark.parametrize(
    ("clause", "rendered"),
    [
        pytest.param("ALTER COLUMN `st.x` COMMENT 'a'", "`st.x`", id="comment-dotted-name"),
        pytest.param("ALTER COLUMN st.`x.y` COMMENT 'a'", "`st`.`x.y`", id="comment-dotted-field"),
        pytest.param("ALTER COLUMN st.`x.y` TYPE BIGINT", "`st`.`x.y`", id="type-dotted-field"),
        pytest.param("ADD COLUMN nope.z INT", "`nope`", id="add-missing-parent"),
        pytest.param("ALTER COLUMN nope FIRST", "`nope`", id="move-missing-column"),
    ],
)
def test_unresolved_columns_render_backquoted_parts_like_spark(
    spark: ReparkSession, clause: str, rendered: str
) -> None:
    spark.sql("CREATE TABLE sc.ns.ac (id BIGINT, st STRUCT<x: INT>, `p.q` INT) USING iceberg")
    with pytest.raises(AnalysisException) as caught:
        spark.sql(f"ALTER TABLE sc.ns.ac {clause}")
    assert str(caught.value) == (
        "Error during planning: [UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or "
        f"function parameter with name {rendered} cannot be resolved. Did you mean one of the "
        "following? [`id`, `st`, `p`.`q`]. SQLSTATE: 42703"
    )


def test_a_repeated_column_after_use_names_the_three_part_table(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql(_COMMENT_TABLE)
    spark.sql("USE sc.ns")
    for table in ["ac", "ns.ac"]:
        with pytest.raises(AnalysisException) as caught:
            spark.sql(f"ALTER TABLE {table} ALTER COLUMN data COMMENT 'a', data COMMENT 'b'")
        assert str(caught.value) == _SAME_COLUMN.format(column="`data`")
    assert len(_metadata(tmp_path, "ac")["schemas"]) == 1


def test_alter_column_comment_on_a_missing_table_matches_spark(spark: ReparkSession) -> None:
    with pytest.raises(AnalysisException) as caught:
        spark.sql("ALTER TABLE sc.ns.nope ALTER COLUMN data COMMENT 'x'")
    assert str(caught.value) == (
        "Error during planning: [TABLE_OR_VIEW_NOT_FOUND] The table or view `sc`.`ns`.`nope` "
        "cannot be found. Verify the spelling and correctness of the schema and catalog. If you "
        "did not qualify the name with a schema, verify the current_schema() output, or qualify "
        "the name with the correct schema and catalog. To tolerate the error on drop use DROP "
        "VIEW IF EXISTS or DROP TABLE IF EXISTS. SQLSTATE: 42P01"
    )
    assert caught.value.getCondition() == "TABLE_OR_VIEW_NOT_FOUND"


def test_alter_column_type_and_hive_change_stay_unchanged(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql("CREATE TABLE sc.ns.ac (id INT, data STRING) USING iceberg")
    spark.sql("ALTER TABLE sc.ns.ac ALTER COLUMN id TYPE BIGINT")
    spark.sql("ALTER TABLE sc.ns.ac CHANGE COLUMN data data STRING COMMENT 'hive-style'")
    schema = _current_schema(_metadata(tmp_path, "ac"))
    fields = {field["name"]: field for field in schema["fields"]}
    assert fields["id"]["type"] == "long"
    assert fields["data"]["doc"] == "hive-style"


def _snapshots(metadata: dict[str, Any]) -> list[dict[str, Any]]:
    return metadata.get("snapshots", [])


def _default_order(metadata: dict[str, Any]) -> list[list[Any]]:
    default = metadata["default-sort-order-id"]
    for order in metadata["sort-orders"]:
        if order["order-id"] == default:
            return [
                [field["transform"], field["source-id"], field["direction"], field["null-order"]]
                for field in order["fields"]
            ]
    raise AssertionError(f"no default sort order {default}")


def _refusal(spark: ReparkSession, statement: str) -> PySparkException:
    with pytest.raises(PySparkException) as caught:
        spark.sql(statement).collect()
    return caught.value


_EMPTY_SUMMARY = {
    "operation": "append",
    "manifests-created": "0",
    "manifests-kept": "0",
    "manifests-replaced": "0",
    "changed-partition-count": "0",
    "total-records": "0",
    "total-files-size": "0",
    "total-data-files": "0",
    "total-delete-files": "0",
    "total-position-deletes": "0",
    "total-equality-deletes": "0",
}


def test_create_format_version_one_writes_v1_like_spark(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql(
        "CREATE TABLE sc.ns.v1 (id BIGINT, data STRING, cat STRING) USING iceberg "
        "TBLPROPERTIES ('format-version'='1')"
    )
    spark.sql("INSERT INTO sc.ns.v1 VALUES (0,'d0','a'),(1,'d1','b'),(2,'d2','a')")
    metadata = _metadata(tmp_path, "v1")
    assert metadata["format-version"] == 1
    assert "last-sequence-number" not in metadata
    assert "schema" in metadata
    assert "partition-spec" in metadata
    assert "sequence-number" not in _snapshots(metadata)[0]
    assert metadata["snapshot-log"][0]["snapshot-id"] == metadata["current-snapshot-id"]
    seeded = spark.sql("SELECT * FROM sc.ns.v1 ORDER BY id").to_arrow()
    assert seeded.to_pylist() == [
        {"id": 0, "data": "d0", "cat": "a"},
        {"id": 1, "data": "d1", "cat": "b"},
        {"id": 2, "data": "d2", "cat": "a"},
    ]
    assert [str(field.type) for field in seeded.schema] == ["int64", "string", "string"]
    spark.sql("DELETE FROM sc.ns.v1 WHERE id = 1")
    deleted = _metadata(tmp_path, "v1")
    assert _snapshots(deleted)[-1]["summary"]["operation"] == "overwrite"
    assert _snapshots(deleted)[-1]["summary"]["total-delete-files"] == "0"
    assert spark.sql("SELECT id FROM sc.ns.v1 ORDER BY id").to_arrow().to_pylist() == [
        {"id": 0},
        {"id": 2},
    ]


@pytest.mark.parametrize(
    ("version", "expected_type", "message"),
    [
        pytest.param(
            "5",
            IllegalArgumentException,
            "Unsupported format version: v5 (supported: v4)",
            id="v5",
        ),
        pytest.param("abc", IllegalArgumentException, 'For input string: "abc"', id="abc"),
        pytest.param(
            "0",
            UnsupportedOperationException,
            "This feature is not implemented: TBLPROPERTIES 'format-version' = '0' is not "
            "supported (tables are created as Iceberg format v1, v2 or v3)",
            id="v0-residue",
        ),
        pytest.param(
            "4",
            UnsupportedOperationException,
            "This feature is not implemented: TBLPROPERTIES 'format-version' = '4' is not "
            "supported (tables are created as Iceberg format v1, v2 or v3)",
            id="v4-residue",
        ),
    ],
)
def test_create_format_version_refusals_match_spark(
    spark: ReparkSession,
    version: str,
    expected_type: type[Exception],
    message: str,
) -> None:
    caught = _refusal(
        spark,
        "CREATE TABLE sc.ns.bad (id BIGINT) USING iceberg "
        f"TBLPROPERTIES ('format-version'='{version}')",
    )
    assert type(caught) is expected_type
    assert str(caught) == message
    assert not spark.catalog.tableExists("sc.ns.bad")


def test_create_format_version_two_stays_the_default(spark: ReparkSession, tmp_path: Path) -> None:
    spark.sql(
        "CREATE TABLE sc.ns.v2 (id BIGINT) USING iceberg TBLPROPERTIES ('format-version'='2')"
    )
    spark.sql("CREATE TABLE sc.ns.vd (id BIGINT) USING iceberg")
    assert _metadata(tmp_path, "v2")["format-version"] == 2
    assert _metadata(tmp_path, "vd")["format-version"] == 2


def test_write_ordered_by_transforms_lands_the_order_spark_measured(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql("CREATE TABLE sc.ns.wo (id BIGINT, ts TIMESTAMP) USING iceberg")
    spark.sql("ALTER TABLE sc.ns.wo WRITE ORDERED BY bucket(4, id), days(ts) DESC NULLS FIRST")
    metadata = _metadata(tmp_path, "wo")
    assert _default_order(metadata) == [
        ["bucket[4]", 1, "asc", "nulls-first"],
        ["day", 2, "desc", "nulls-first"],
    ]
    assert metadata["properties"]["write.distribution-mode"] == "range"
    spark.sql("ALTER TABLE sc.ns.wo WRITE ORDERED BY truncate(id, 2) DESC, date_hour(ts)")
    assert _default_order(_metadata(tmp_path, "wo")) == [
        ["truncate[2]", 1, "desc", "nulls-last"],
        ["hour", 2, "asc", "nulls-first"],
    ]
    spark.sql("ALTER TABLE sc.ns.wo WRITE ORDERED BY id")
    assert _default_order(_metadata(tmp_path, "wo")) == [["identity", 1, "asc", "nulls-first"]]


@pytest.mark.parametrize(
    ("spec", "expected_type", "message"),
    [
        pytest.param(
            "void(id)",
            UnsupportedOperationException,
            "Transform is not supported: void(id)",
            id="void",
        ),
        pytest.param(
            "zorder(id, ts)", IllegalArgumentException, "Term must be unbound", id="zorder"
        ),
        pytest.param(
            "bucket(0, id)",
            IllegalArgumentException,
            "Unsupported width for transform: bucket(0, id)",
            id="bucket-zero",
        ),
        pytest.param(
            "bucket(4S, id)",
            IllegalArgumentException,
            "Cannot find width for transform: bucket(4, id)",
            id="short-width",
        ),
        pytest.param(
            "bucket()",
            AnalysisException,
            "Error during planning: \nno viable alternative at input ')'\n== SQL ==\n"
            "ALTER TABLE sc.ns.wo WRITE ORDERED BY bucket()",
            id="empty-arguments",
        ),
    ],
)
def test_write_ordered_by_transform_refusals_match_spark(
    spark: ReparkSession,
    tmp_path: Path,
    spec: str,
    expected_type: type[Exception],
    message: str,
) -> None:
    spark.sql("CREATE TABLE sc.ns.wo (id BIGINT, ts TIMESTAMP) USING iceberg")
    files = _metadata_files(tmp_path, "wo")
    caught = _refusal(spark, f"ALTER TABLE sc.ns.wo WRITE ORDERED BY {spec}")
    assert type(caught) is expected_type
    assert str(caught) == message
    assert _metadata_files(tmp_path, "wo") == files


def test_create_branch_on_an_empty_table_commits_an_empty_append_like_spark(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql("CREATE TABLE sc.ns.be (id BIGINT, data STRING, cat STRING) USING iceberg")
    spark.sql("ALTER TABLE sc.ns.be CREATE BRANCH b1")
    metadata = _metadata(tmp_path, "be")
    (snapshot,) = _snapshots(metadata)
    assert snapshot["sequence-number"] == 1
    assert {key: snapshot["summary"].get(key) for key in _EMPTY_SUMMARY} == _EMPTY_SUMMARY
    assert metadata["refs"] == {"b1": {"snapshot-id": snapshot["snapshot-id"], "type": "branch"}}
    assert metadata.get("current-snapshot-id") is None
    assert metadata.get("snapshot-log", []) == []
    branch = spark.sql("SELECT * FROM sc.ns.be.branch_b1").to_arrow()
    assert branch.num_rows == 0
    assert [str(field.type) for field in branch.schema] == ["int64", "string", "string"]
    assert spark.sql("SELECT * FROM sc.ns.be").to_arrow().num_rows == 0
    spark.sql("ALTER TABLE sc.ns.be CREATE BRANCH IF NOT EXISTS b1")
    spark.sql("ALTER TABLE sc.ns.be CREATE BRANCH b2 WITH SNAPSHOT RETENTION 3 SNAPSHOTS")
    refs = spark.sql(
        "SELECT name, type, min_snapshots_to_keep FROM sc.ns.be.refs ORDER BY name"
    ).collect()
    assert [list(row) for row in refs] == [["b1", "BRANCH", None], ["b2", "BRANCH", 3]]
    assert len(_snapshots(_metadata(tmp_path, "be"))) == 2


@pytest.mark.parametrize(
    ("statement", "message"),
    [
        pytest.param(
            "ALTER TABLE sc.ns.be CREATE TAG t1",
            "Cannot complete create or replace tag operation on ns.be, main has no snapshot",
            id="tag",
        ),
        pytest.param(
            "ALTER TABLE sc.ns.be CREATE OR REPLACE BRANCH b1",
            "Cannot complete replace branch operation on ns.be, main has no snapshot",
            id="replace-existing-branch",
        ),
        pytest.param(
            "ALTER TABLE sc.ns.be CREATE BRANCH b1", "Ref b1 already exists", id="duplicate"
        ),
    ],
)
def test_refs_on_an_empty_table_refuse_like_spark(
    spark: ReparkSession, tmp_path: Path, statement: str, message: str
) -> None:
    spark.sql("CREATE TABLE sc.ns.be (id BIGINT) USING iceberg")
    spark.sql("ALTER TABLE sc.ns.be CREATE BRANCH b1")
    files = _metadata_files(tmp_path, "be")
    caught = _refusal(spark, statement)
    assert type(caught) is IllegalArgumentException
    assert str(caught) == message
    assert _metadata_files(tmp_path, "be") == files


def test_create_branch_main_and_seeded_branches_keep_their_answers(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql("CREATE TABLE sc.ns.bm (id BIGINT) USING iceberg")
    spark.sql("ALTER TABLE sc.ns.bm CREATE BRANCH main")
    main = _metadata(tmp_path, "bm")
    (snapshot,) = _snapshots(main)
    assert main["current-snapshot-id"] == snapshot["snapshot-id"]
    assert main["snapshot-log"][0]["snapshot-id"] == snapshot["snapshot-id"]
    spark.sql("CREATE TABLE sc.ns.bs (id BIGINT) USING iceberg")
    spark.sql("INSERT INTO sc.ns.bs VALUES (1)")
    spark.sql("ALTER TABLE sc.ns.bs CREATE BRANCH b1")
    spark.sql("ALTER TABLE sc.ns.bs CREATE TAG t1")
    seeded = _metadata(tmp_path, "bs")
    current = seeded["current-snapshot-id"]
    assert len(_snapshots(seeded)) == 1
    assert {name: ref["snapshot-id"] for name, ref in seeded["refs"].items()} == {
        "main": current,
        "b1": current,
        "t1": current,
    }


def test_write_ordered_by_bind_refusal_keeps_the_fork_text_residue(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql("CREATE TABLE sc.ns.wo (id BIGINT, ts TIMESTAMP) USING iceberg")
    files = _metadata_files(tmp_path, "wo")
    caught = _refusal(spark, "ALTER TABLE sc.ns.wo WRITE ORDERED BY days(id)")
    assert type(caught) is PySparkException
    assert str(caught) == "DataInvalid => Cannot bind: day cannot transform long values from 'id'"
    assert _metadata_files(tmp_path, "wo") == files


def test_write_ordered_by_a_long_width_literal_lands_like_spark(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql("CREATE TABLE sc.ns.wo (id BIGINT, ts TIMESTAMP) USING iceberg")
    spark.sql("ALTER TABLE sc.ns.wo WRITE ORDERED BY bucket(4L, id)")
    assert _default_order(_metadata(tmp_path, "wo")) == [["bucket[4]", 1, "asc", "nulls-first"]]


def test_a_wap_branch_write_on_a_v1_table_refuses_and_writes_no_ref(
    spark: ReparkSession, tmp_path: Path
) -> None:
    spark.sql(
        "CREATE TABLE sc.ns.w1 (id BIGINT) USING iceberg "
        "TBLPROPERTIES ('format-version'='1', 'write.wap.enabled'='true')"
    )
    spark.sql("INSERT INTO sc.ns.w1 VALUES (1)")
    files = _metadata_files(tmp_path, "w1")
    spark.conf.set("spark.wap.branch", "w1")
    try:
        caught = _refusal(spark, "INSERT INTO sc.ns.w1 VALUES (2)")
    finally:
        spark.conf.unset("spark.wap.branch")
    assert type(caught) is UnsupportedOperationException
    assert str(caught) == (
        "This feature is not implemented: BRANCH on the format v1 table ns.w1 is not supported: "
        "the Iceberg fork writes v1 metadata without its refs, so the new ref would be lost"
    )
    assert _metadata_files(tmp_path, "w1") == files
    refs = spark.sql("SELECT name FROM sc.ns.w1.refs ORDER BY name").collect()
    assert [row[0] for row in refs] == ["main"]
    assert spark.sql("SELECT id FROM sc.ns.w1").to_arrow().to_pylist() == [{"id": 1}]
