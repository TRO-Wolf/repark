"""FNP-GEN-1 steps 3-4 pins: tuple, csv and csv-schema kernels over the s34 oracle."""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, PySparkValueError, UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812

FIXTURE_PATH = Path(__file__).parent / "fnp_gen_1_s34_spark_oracle.json"
S34_FRAME = (
    "SELECT CAST(1 AS INT) AS id, "
    'CAST(\'{"a":1,"b":"x","c":{"d":2},"e":[1,2],"f":null,'
    '"g":1.50,"h":true,"a2":"q"}\' AS STRING) AS js, '
    "CAST('1,abc,2.5' AS STRING) AS csvrow "
    "UNION ALL SELECT CAST(2 AS INT), CAST('{\"a\":\"2\"}' AS STRING), CAST('x,y,z' AS STRING) "
    "UNION ALL SELECT CAST(3 AS INT), CAST(NULL AS STRING), CAST(NULL AS STRING) "
    "UNION ALL SELECT CAST(4 AS INT), CAST('[1,2]' AS STRING), CAST(',,' AS STRING) "
    "UNION ALL SELECT CAST(5 AS INT), CAST('{\"a\":1' AS STRING), CAST('7,\"q,r\",1e3' AS STRING) "
    "UNION ALL SELECT CAST(6 AS INT), CAST('{\"A\":9,\"a\":10}' AS STRING), CAST('8' AS STRING)"
)


def _oracle() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text())


def _pair(name: str, door: str, fragment: str) -> list[dict[str, Any]]:
    cells = [
        cell
        for cell in _oracle()["cells"]
        if cell["name"] == name and cell["door"] == door and fragment in cell["expr"]
    ]
    assert len(cells) == 2
    assert {cell["ansi"] for cell in cells} == {True, False}
    return cells


def _norm(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"row"}:
        return {key: _norm(item) for key, item in value["row"].items()}
    if isinstance(value, datetime.date) and not isinstance(value, datetime.datetime):
        return {"repr": repr(value)}
    if isinstance(value, list):
        return [_norm(item) for item in value]
    if isinstance(value, dict):
        return {key: _norm(item) for key, item in value.items()}
    return value


def _want_rows(cell: dict[str, Any]) -> list[Any]:
    return [_norm(row) for row in cell["rows"]]


def _row_key(row: list[Any]) -> list[str]:
    return [repr(value) for value in row]


def _check_value_cells(result: Any, cells: list[dict[str, Any]]) -> None:
    for cell in cells:
        actual = [
            (field.name, field.dataType.simpleString(), field.nullable)
            for field in result.schema.fields
        ]
        expected = [
            (column["name"], column["type"], column["nullable"]) for column in cell["columns"]
        ]
        assert actual == expected
    names = [field.name for field in result.schema.fields]
    actual_rows = sorted(
        ([_norm(row[name]) for name in names] for row in result.to_arrow().to_pylist()),
        key=_row_key,
    )
    for cell in cells:
        assert actual_rows == sorted(_want_rows(cell), key=_row_key)


def _condition(cell: dict[str, Any]) -> str:
    if cell.get("error_condition"):
        return str(cell["error_condition"]).split(".")[0]
    root = str(cell.get("root_cause") or "")
    start = root.index("[") + 1
    return root[start:].split("]")[0].split(".")[0]


def _check_error_cells(cells: list[dict[str, Any]], excinfo: Any) -> None:
    text = str(excinfo.value)
    for cell in cells:
        assert _condition(cell) in text


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fnp-gen-1-s34").getOrCreate()
    yield session
    session.stop()


@pytest.fixture
def frame(spark: ReparkSession) -> Any:
    return spark.sql(S34_FRAME)


def test_python_door_json_tuple_eight_fields(spark: ReparkSession, frame: Any) -> None:
    """Eight fields answer ``c0``..``c7`` with Jackson re-renders and NULLs."""
    cells = _pair("json_tuple", "python", "'a','b','c','e','f','g','h','zz'")
    result = frame.select("id", F.json_tuple("js", "a", "b", "c", "e", "f", "g", "h", "zz"))
    _check_value_cells(result, cells)


def test_python_door_json_tuple_duplicate_field(spark: ReparkSession, frame: Any) -> None:
    """A duplicated field answers twice, case-sensitively."""
    cells = _pair("json_tuple", "python", "'a', 'a'")
    result = frame.select("id", F.json_tuple("js", "a", "a"))
    _check_value_cells(result, cells)


def test_python_door_json_tuple_zero_fields_refuses(spark: ReparkSession) -> None:
    """Zero fields raise ``CANNOT_BE_EMPTY`` before reaching the engine."""
    cells = _pair("json_tuple", "python", "json_tuple(js))")
    assert all(cell["error_condition"] == "CANNOT_BE_EMPTY" for cell in cells)
    with pytest.raises(PySparkValueError) as excinfo:
        F.json_tuple("js")
    assert excinfo.value.getCondition() == "CANNOT_BE_EMPTY"
    assert "At least one field must be specified" in str(excinfo.value)


def test_python_door_json_tuple_alias_count_mismatch_refuses(
    spark: ReparkSession, frame: Any
) -> None:
    """A wrong alias count raises ``UDTF_ALIAS_NUMBER_MISMATCH``."""
    cells = _pair("json_tuple", "python", "js, 'a').alias('x', 'y')")
    assert all(cell["error_condition"] == "UDTF_ALIAS_NUMBER_MISMATCH" for cell in cells)
    with pytest.raises(AnalysisException, match="UDTF_ALIAS_NUMBER_MISMATCH") as excinfo:
        frame.select(F.json_tuple("js", "a").alias("x", "y")).collect()
    _check_error_cells(cells, excinfo)


def test_python_door_json_tuple_full_alias_renames(spark: ReparkSession, frame: Any) -> None:
    """``.alias('x', 'y')`` renames both outputs on the Python door."""
    cells = _pair("json_tuple", "python", "json_tuple(js, 'a', 'b').alias")
    result = frame.select("id", F.json_tuple("js", "a", "b").alias("x", "y"))
    _check_value_cells(result, cells)


def test_sql_door_json_tuple_three_fields(spark: ReparkSession) -> None:
    """The SQL SELECT list answers ``c0``..``c2`` with verbatim nested text."""
    cells = _pair("json_tuple", "sql", "json_tuple(js, 'a', 'c', 'e')")
    result = spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})"))
    _check_value_cells(result, cells)


@pytest.mark.xfail(
    strict=True,
    reason="FNP-GEN-1 step 3: SQL UDTF alias AS (x, y) needs the DataFusion SQL select-item "
    "seam (datafusion-sql select.rs multiple-alias refusal), owned by run 18c.",
)
def test_sql_door_json_tuple_udt_alias_single(spark: ReparkSession) -> None:
    """``AS (x)`` renames the single output on the SQL door."""
    cells = _pair("json_tuple", "sql", "AS (x)")
    result = spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})"))
    _check_value_cells(result, cells)


@pytest.mark.xfail(
    strict=True,
    reason="FNP-GEN-1 step 3: SQL UDTF alias AS (x, y) needs the DataFusion SQL select-item "
    "seam (datafusion-sql select.rs multiple-alias refusal), owned by run 18c.",
)
def test_sql_door_json_tuple_udt_alias_pair(spark: ReparkSession) -> None:
    """``AS (x, y)`` renames both outputs on the SQL door."""
    cells = _pair("json_tuple", "sql", "AS (x, y)")
    result = spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})"))
    _check_value_cells(result, cells)


def test_sql_door_json_tuple_non_string_field_refuses(spark: ReparkSession) -> None:
    """A non-string field raises ``DATATYPE_MISMATCH.NON_STRING_TYPE``."""
    cells = _pair("json_tuple", "sql", "json_tuple(js, id)")
    assert all(cell["error_condition"] == "DATATYPE_MISMATCH.NON_STRING_TYPE" for cell in cells)
    with pytest.raises(AnalysisException, match="NON_STRING_TYPE"):
        spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})")).collect()


def test_sql_door_json_tuple_null_field_refuses(spark: ReparkSession) -> None:
    """A NULL field raises ``DATATYPE_MISMATCH.NON_STRING_TYPE``."""
    cells = _pair("json_tuple", "sql", "json_tuple(js, NULL)")
    assert all(cell["error_condition"] == "DATATYPE_MISMATCH.NON_STRING_TYPE" for cell in cells)
    with pytest.raises(AnalysisException, match="NON_STRING_TYPE"):
        spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})")).collect()


def test_sql_door_json_tuple_zero_fields_refuses(spark: ReparkSession) -> None:
    """Zero fields raise ``WRONG_NUM_ARGS`` on the SQL door."""
    cells = _pair("json_tuple", "sql", "json_tuple(js) FROM")
    assert all(cell["error_condition"] == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION" for cell in cells)
    with pytest.raises(AnalysisException, match="WRONG_NUM_ARGS"):
        spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})")).collect()


def test_python_door_from_csv_permissive_partial_rows(spark: ReparkSession, frame: Any) -> None:
    """PERMISSIVE pads NULLs, keeps quoted commas and parses ``1e3``."""
    cells = _pair("from_csv", "python", "'a INT, b STRING, c DOUBLE')")
    result = frame.select("id", F.from_csv("csvrow", "a INT, b STRING, c DOUBLE"))
    _check_value_cells(result, cells)


def test_python_door_from_csv_failfast_raises(spark: ReparkSession, frame: Any) -> None:
    """FAILFAST raises the parse error at execution, naming the condition."""
    cells = _pair("from_csv", "python", "FAILFAST")
    with pytest.raises(Exception, match="MALFORMED_RECORD_IN_PARSING") as excinfo:
        frame.select(F.from_csv("csvrow", "a INT, b INT", {"mode": "FAILFAST"})).collect()
    _check_error_cells(cells, excinfo)


def test_python_door_from_csv_dropmalformed_refuses(spark: ReparkSession, frame: Any) -> None:
    """DROPMALFORMED raises ``PARSE_MODE_UNSUPPORTED`` on the Python door."""
    cells = _pair("from_csv", "python", "DROPMALFORMED")
    assert all(cell["error_condition"] == "PARSE_MODE_UNSUPPORTED" for cell in cells)
    with pytest.raises(AnalysisException, match="PARSE_MODE_UNSUPPORTED") as excinfo:
        frame.select(F.from_csv("csvrow", "a INT, b INT", {"mode": "DROPMALFORMED"})).collect()
    _check_error_cells(cells, excinfo)


def test_python_door_from_csv_null_value_option(spark: ReparkSession, frame: Any) -> None:
    """``nullValue`` spells an extra NULL without touching the other tokens."""
    cells = _pair("from_csv", "python", "nullValue")
    result = frame.select("id", F.from_csv("csvrow", "a STRING, b STRING", {"nullValue": "abc"}))
    _check_value_cells(result, cells)


def test_python_door_from_csv_corrupt_record_column(spark: ReparkSession, frame: Any) -> None:
    """A bad field lands the whole record in the corrupt column, others parsed."""
    cells = _pair("from_csv", "python", "columnNameOfCorruptRecord")
    result = frame.select(
        "id",
        F.from_csv(
            "csvrow",
            "a INT, b STRING, c DOUBLE, _corrupt_record STRING",
            {"columnNameOfCorruptRecord": "_corrupt_record"},
        ),
    )
    _check_value_cells(result, cells)


def test_python_door_from_csv_foldable_schema(spark: ReparkSession, frame: Any) -> None:
    """``from_csv(x, schema_of_csv(lit))`` folds the schema and names ``_cN``."""
    cells = _pair("from_csv", "python", "F.schema_of_csv")
    result = frame.select("id", F.from_csv("csvrow", F.schema_of_csv("1,abc,2.5")))
    _check_value_cells(result, cells)


def test_python_door_from_csv_date_format(spark: ReparkSession) -> None:
    """``dateFormat yyyy`` parses the year token to the year start date."""
    cells = _pair("from_csv", "python", "dateFormat")
    result = spark.sql(S34_FRAME).select(
        F.from_csv(F.lit("1,2024"), "a INT, b DATE", {"dateFormat": "yyyy"})
    )
    _check_value_cells(result, cells)


def test_python_door_from_csv_non_literal_schema_refuses(spark: ReparkSession, frame: Any) -> None:
    """A non-literal schema raises ``INVALID_SCHEMA.NON_STRING_LITERAL``."""
    cells = _pair("from_csv", "python", "col('csvrow')")
    assert all(cell["error_condition"] == "INVALID_SCHEMA.NON_STRING_LITERAL" for cell in cells)
    with pytest.raises(AnalysisException, match="NON_STRING_LITERAL"):
        frame.select(F.from_csv("csvrow", F.col("csvrow"))).collect()


def test_python_door_from_csv_bad_ddl_refuses(spark: ReparkSession, frame: Any) -> None:
    """A bad DDL raises ``PARSE_SYNTAX_ERROR`` on the Python door."""
    cells = _pair("from_csv", "python", "not a schema")
    assert all(cell["error_condition"] == "PARSE_SYNTAX_ERROR" for cell in cells)
    with pytest.raises(AnalysisException, match="PARSE_SYNTAX_ERROR"):
        frame.select(F.from_csv("csvrow", "not a schema !!")).collect()


def test_python_door_from_csv_non_string_input_refuses(spark: ReparkSession, frame: Any) -> None:
    """A non-string input raises ``DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE``."""
    cells = _pair("from_csv", "python", "F.from_csv('id'")
    assert all(
        cell["error_condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" for cell in cells
    )
    with pytest.raises(AnalysisException, match="UNEXPECTED_INPUT_TYPE"):
        frame.select(F.from_csv("id", "a INT")).collect()


def test_python_door_from_csv_unknown_option_ignored(spark: ReparkSession, frame: Any) -> None:
    """Unknown options are ignored and the row still parses."""
    cells = _pair("from_csv", "python", "bogus")
    result = frame.select("id", F.from_csv("csvrow", "a INT", {"bogus": "x"}))
    _check_value_cells(result, cells)


def test_python_door_from_csv_array_field_refuses(spark: ReparkSession, frame: Any) -> None:
    """A complex field raises ``UNSUPPORTED_DATATYPE`` as Spark's own class."""
    cells = _pair("from_csv", "python", "ARRAY<INT>")
    assert all(cell["error_condition"] == "UNSUPPORTED_DATATYPE" for cell in cells)
    assert all(cell["error_type"] == "UnsupportedOperationException" for cell in cells)
    with pytest.raises(UnsupportedOperationException, match="UNSUPPORTED_DATATYPE"):
        frame.select(F.from_csv("csvrow", "a ARRAY<INT>")).collect()


def test_sql_door_from_csv_failfast_raises(spark: ReparkSession) -> None:
    """FAILFAST raises the parse error at execution on the SQL door."""
    cells = _pair("from_csv", "sql", "FAILFAST")
    with pytest.raises(Exception, match="MALFORMED_RECORD_IN_PARSING") as excinfo:
        spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})")).collect()
    _check_error_cells(cells, excinfo)


def test_sql_door_from_csv_named_struct(spark: ReparkSession) -> None:
    """The SQL door parses the struct under its ``AS`` name."""
    cells = _pair("from_csv", "sql", "AS s FROM")
    result = spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})"))
    _check_value_cells(result, cells)


def test_sql_door_from_csv_non_literal_schema_refuses(spark: ReparkSession) -> None:
    """A column schema raises ``INVALID_SCHEMA.NON_STRING_LITERAL`` on SQL."""
    cells = _pair("from_csv", "sql", "from_csv(csvrow, csvrow)")
    assert all(cell["error_condition"] == "INVALID_SCHEMA.NON_STRING_LITERAL" for cell in cells)
    with pytest.raises(AnalysisException, match="NON_STRING_LITERAL"):
        spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})")).collect()


def test_sql_door_from_csv_single_arg_refuses(spark: ReparkSession) -> None:
    """One argument raises ``WRONG_NUM_ARGS`` on the SQL door."""
    cells = _pair("from_csv", "sql", "from_csv(csvrow) FROM")
    assert all(cell["error_condition"] == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION" for cell in cells)
    with pytest.raises(AnalysisException, match="WRONG_NUM_ARGS"):
        spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})")).collect()


def test_sql_door_from_csv_non_string_option_refuses(spark: ReparkSession) -> None:
    """A non-string option value raises ``INVALID_OPTIONS.NON_STRING_TYPE``."""
    cells = _pair("from_csv", "sql", "map('sep', 1)")
    assert all(cell["error_condition"] == "INVALID_OPTIONS.NON_STRING_TYPE" for cell in cells)
    with pytest.raises(AnalysisException, match="NON_STRING_TYPE"):
        spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})")).collect()


@pytest.mark.xfail(
    strict=True,
    reason="FNP-GEN-1 step 3: struct-field access on a call result needs the DataFusion SQL "
    "dot-access seam (datafusion-sql expr dot access on non-string exprs), owned by run 18c.",
)
def test_sql_door_from_csv_struct_field_access_refuses(spark: ReparkSession) -> None:
    """Selecting a missing struct field raises ``FIELD_NOT_FOUND``."""
    cells = _pair("from_csv", "sql", ").c FROM")
    assert all(cell["error_condition"] == "FIELD_NOT_FOUND" for cell in cells)
    with pytest.raises(AnalysisException, match="FIELD_NOT_FOUND"):
        spark.sql(cells[0]["expr"].replace("FRAME", f"({S34_FRAME})")).collect()


def test_python_door_schema_of_csv_type_ladder(spark: ReparkSession) -> None:
    """One CSV row infers Spark's ``CSVInferSchema`` ladder and renderer."""
    cells = _pair(
        "schema_of_csv", "python", "1,abc,2.5,true,2024-01-01,2024-01-01 10:00:00,,1e3,99999999999"
    )
    result = spark.sql(S34_FRAME).select(
        F.schema_of_csv("1,abc,2.5,true,2024-01-01,2024-01-01 10:00:00,,1e3,99999999999")
    )
    _check_value_cells(result, cells)


def test_python_door_schema_of_csv_empty_refuses(spark: ReparkSession) -> None:
    """``schema_of_csv('')`` matches Spark's ``INTERNAL_ERROR`` defect."""
    cells = _pair("schema_of_csv", "python", "F.schema_of_csv('')")
    with pytest.raises(Exception, match="INTERNAL_ERROR") as excinfo:
        spark.sql(S34_FRAME).select(F.schema_of_csv("")).collect()
    _check_error_cells(cells, excinfo)


def test_python_door_schema_of_csv_non_foldable_refuses(spark: ReparkSession, frame: Any) -> None:
    """A column raises ``DATATYPE_MISMATCH.NON_FOLDABLE_INPUT``."""
    cells = _pair("schema_of_csv", "python", "col('csvrow')")
    assert all(cell["error_condition"] == "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT" for cell in cells)
    with pytest.raises(AnalysisException, match="NON_FOLDABLE_INPUT"):
        frame.select(F.schema_of_csv(F.col("csvrow"))).collect()


def test_python_door_schema_of_csv_null_refuses(spark: ReparkSession) -> None:
    """A NULL literal raises ``DATATYPE_MISMATCH.UNEXPECTED_NULL``."""
    cells = _pair("schema_of_csv", "python", "lit(None)")
    assert all(cell["error_condition"] == "DATATYPE_MISMATCH.UNEXPECTED_NULL" for cell in cells)
    with pytest.raises(AnalysisException, match="UNEXPECTED_NULL"):
        spark.sql(S34_FRAME).select(F.schema_of_csv(F.lit(None).cast("string"))).collect()


def test_python_door_schema_of_csv_non_string_refuses(spark: ReparkSession) -> None:
    """A non-string literal raises ``DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE``."""
    cells = _pair("schema_of_csv", "python", "lit(5)")
    assert all(
        cell["error_condition"] == "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE" for cell in cells
    )
    with pytest.raises(AnalysisException, match="UNEXPECTED_INPUT_TYPE"):
        spark.sql(S34_FRAME).select(F.schema_of_csv(F.lit(5))).collect()


def test_python_door_schema_of_csv_quoted_field(spark: ReparkSession) -> None:
    """A quoted comma stays one STRING token; ``1.0`` infers DOUBLE."""
    cells = _pair("schema_of_csv", "python", "'\"a,b\",1.0'")
    result = spark.sql(S34_FRAME).select(F.schema_of_csv('"a,b",1.0'))
    _check_value_cells(result, cells)


def test_sql_door_schema_of_csv_value(spark: ReparkSession) -> None:
    """The SQL door renders the inferred DDL string."""
    cells = _pair("schema_of_csv", "sql", "schema_of_csv('1,abc,2.5,true,2024-01-01')")
    result = spark.sql(cells[0]["expr"])
    _check_value_cells(result, cells)


def test_sql_door_schema_of_csv_empty_refuses(spark: ReparkSession) -> None:
    """``schema_of_csv('')`` matches Spark's ``INTERNAL_ERROR`` defect on SQL."""
    cells = _pair("schema_of_csv", "sql", "schema_of_csv('')")
    with pytest.raises(Exception, match="INTERNAL_ERROR") as excinfo:
        spark.sql(cells[0]["expr"]).collect()
    _check_error_cells(cells, excinfo)


def test_sql_door_schema_of_csv_sep_option(spark: ReparkSession) -> None:
    """``sep`` splits the inference row on the SQL door."""
    cells = _pair("schema_of_csv", "sql", "map('sep', '|')")
    result = spark.sql(cells[0]["expr"])
    _check_value_cells(result, cells)


def test_sql_door_schema_of_csv_null_refuses(spark: ReparkSession) -> None:
    """``schema_of_csv(NULL)`` raises ``DATATYPE_MISMATCH.UNEXPECTED_NULL``."""
    cells = _pair("schema_of_csv", "sql", "schema_of_csv(NULL)")
    assert all(cell["error_condition"] == "DATATYPE_MISMATCH.UNEXPECTED_NULL" for cell in cells)
    with pytest.raises(AnalysisException, match="UNEXPECTED_NULL"):
        spark.sql(cells[0]["expr"]).collect()


def test_sql_door_schema_of_csv_non_map_options_refuses(spark: ReparkSession) -> None:
    """Non-``map()`` options raise ``INVALID_OPTIONS.NON_MAP_FUNCTION``."""
    cells = _pair("schema_of_csv", "sql", "schema_of_csv('a', 'b')")
    assert all(cell["error_condition"] == "INVALID_OPTIONS.NON_MAP_FUNCTION" for cell in cells)
    with pytest.raises(AnalysisException, match="NON_MAP_FUNCTION"):
        spark.sql(cells[0]["expr"]).collect()
