from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, ParseException, PySparkException

_COMMENT_TABLE = (
    "CREATE TABLE sc.ns.ac (id BIGINT, data STRING, cat STRING, st STRUCT<x: INT>) USING iceberg"
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
    spark.sql("ALTER TABLE sc.ns.ac ALTER COLUMN data COMMENT 'a', st.x COMMENT 'b'")
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
    with pytest.raises(PySparkException) as caught:
        spark.sql(f"ALTER TABLE sc.ns.ac ALTER COLUMN {clause}")
    assert type(caught.value) is expected_type
    assert str(caught.value) == message
    assert caught.value.getCondition() == condition
    assert caught.value.getSqlState() == sql_state
    assert _current_schema(_metadata(tmp_path, "ac")) == before


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
