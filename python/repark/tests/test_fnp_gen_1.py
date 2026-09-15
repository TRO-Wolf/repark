"""FNP-GEN-1 step-1 pins: generators and semi-structured parsers over the o245 oracle."""

from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException, UnsupportedOperationException
from repark.spark import functions as F  # noqa: N812

FIXTURE_PATH = Path(__file__).parent / "fnp_gen_1_spark_oracle.json"
FRAME = (
    "SELECT * FROM VALUES "
    "(1, CAST(2.5 AS DOUBLE), CAST(-2.5 AS DOUBLE), CAST(12345.6789 AS DECIMAL(10,4)), "
    "'Hello World. How are you?', 'a,b,,c', '100', 'abcd-EFG-123', "
    '\'{"a":1,"b":"x","c":{"d":2}}\', \'1,abc,2.5\', \'<p><a>1</a><b>x</b></p>\', '
    "array(named_struct('x', 1, 'y', 'p'), named_struct('x', 2, 'y', 'q')), array(10, 20), "
    "TIMESTAMP'2024-01-01 10:07:30', 'k1'), "
    "(2, CAST(NULL AS DOUBLE), CAST(3.5 AS DOUBLE), CAST(NULL AS DECIMAL(10,4)), "
    "CAST(NULL AS STRING), CAST(NULL AS STRING), 'zz', CAST(NULL AS STRING), "
    "CAST(NULL AS STRING), CAST(NULL AS STRING), CAST(NULL AS STRING), "
    "CAST(NULL AS ARRAY<STRUCT<x:INT,y:STRING>>), array(), "
    "TIMESTAMP'2024-01-01 10:12:00', 'k1'), "
    "(3, CAST(0.125 AS DOUBLE), CAST(1.0 AS DOUBLE), CAST(-0.5 AS DECIMAL(10,4)), 'one', "
    "'x', '-17', 'x', '{\"a\":null}', ',,', '<p/>', array(), CAST(NULL AS ARRAY<INT>), "
    "TIMESTAMP'2024-01-01 10:31:00', 'k2') "
    "AS t(id, d, d2, dec, txt, csvs, num, masked, js, csvrow, xml, arr_s, arr_i, ts, key)"
)
NAMES = (
    "inline",
    "inline_outer",
    "posexplode",
    "posexplode_outer",
    "json_tuple",
    "from_csv",
    "schema_of_csv",
    "from_xml",
    "schema_of_xml",
)
REFUSAL_MARKER = "FNP-16-csv-xml-xpath"
NON_FOLDABLE = "DATATYPE_MISMATCH.NON_FOLDABLE_INPUT"


def _oracle() -> dict[str, Any]:
    return json.loads(FIXTURE_PATH.read_text())


def _cells(name: str, door: str) -> list[dict[str, Any]]:
    return [
        cell
        for cell in _oracle()["cells"]
        if cell["name"] == name
        and cell["door"] == door
        and cell["ansi"]
        and "LATERAL VIEW" not in cell["expr"]
    ]


def _split_top_level(body: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current: list[str] = []
    for char in body:
        if char in "[(":
            depth += 1
        elif char in "])":
            depth -= 1
        if char == "," and depth == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    parts.append("".join(current).strip())
    return [part for part in parts if part]


def _norm(value: Any) -> Any:
    if isinstance(value, dict) and set(value) == {"row"}:
        return {key: _norm(item) for key, item in value["row"].items()}
    if isinstance(value, list):
        return [_norm(item) for item in value]
    return value


def _want_rows(cell: dict[str, Any]) -> list[Any]:
    return [_norm(row) for row in cell["rows"]]


def _check_schema_and_rows(result: Any, cell: dict[str, Any]) -> None:
    actual = [
        (field.name, field.dataType.simpleString(), field.nullable)
        for field in result.schema.fields
    ]
    expected = [(column["name"], column["type"], column["nullable"]) for column in cell["columns"]]
    assert actual == expected
    assert result.to_arrow().to_pylist() == _want_rows(cell)


@pytest.fixture
def spark() -> ReparkSession:
    session = ReparkSession.builder.appName("pytest-fnp-gen-1").getOrCreate()
    yield session
    session.stop()


def test_nine_names_present_on_the_facade() -> None:
    """Every card name sits on ``repark.spark.functions`` with PySpark's parameters."""
    for name in NAMES:
        assert callable(getattr(F, name)), name


@pytest.mark.parametrize("name", NAMES)
def test_facade_signature_matches_the_spark_oracle(name: str) -> None:
    """Parameter names and defaults equal the recorded PySpark 4.1.2 signature."""
    body = _oracle()["signatures"][name].split("->")[0].strip().removeprefix("(").removesuffix(")")
    expected: list[tuple[str, Any]] = []
    for part in _split_top_level(body):
        label, _, default = part.partition("=")
        expected.append((label.split(":")[0].strip().lstrip("*"), default.strip()))
    actual = inspect.signature(getattr(F, name)).parameters
    assert list(actual) == [label for label, _ in expected]
    for label, default in expected:
        if default == "None":
            assert actual[label].default is None
        elif default == "":
            assert actual[label].default is inspect.Parameter.empty


@pytest.mark.parametrize("name", ["inline", "inline_outer"])
def test_python_door_generator_projects_struct_fields(spark: ReparkSession, name: str) -> None:
    """The Python door unnests the struct array into one column per struct field."""
    (cell,) = [cell for cell in _cells(name, "python") if "arr_s" in cell["expr"]]
    result = spark.sql(FRAME).select("id", getattr(F, name)("arr_s"))
    _check_schema_and_rows(result, cell)


@pytest.mark.parametrize("name", ["inline", "inline_outer"])
def test_sql_door_generator_projects_struct_fields(spark: ReparkSession, name: str) -> None:
    """The SQL SELECT list unnests the struct array into one column per struct field."""
    (cell,) = [cell for cell in _cells(name, "sql") if "arr_s" in cell["expr"]]
    result = spark.sql(cell["expr"].replace("FRAME", f"({FRAME})"))
    _check_schema_and_rows(result, cell)


def test_sql_door_inline_over_a_literal_array(spark: ReparkSession) -> None:
    """A literal struct array answers two rows under Spark's ``col1``/``col2`` names."""
    (cell,) = [cell for cell in _cells("inline", "sql") if "arr_s" not in cell["expr"]]
    result = spark.sql(cell["expr"])
    _check_schema_and_rows(result, cell)


@pytest.mark.parametrize("name", ["posexplode", "posexplode_outer"])
def test_python_door_posexplode_array(spark: ReparkSession, name: str) -> None:
    """The Python door answers ``pos`` plus ``col``, keeping a NULL row for the outer."""
    (cell,) = [
        cell
        for cell in _cells(name, "python")
        if "arr_i" in cell["expr"] and "alias" not in cell["expr"]
    ]
    result = spark.sql(FRAME).select("id", getattr(F, name)("arr_i"))
    _check_schema_and_rows(result, cell)


@pytest.mark.parametrize("name", ["posexplode", "posexplode_outer"])
def test_sql_door_posexplode_array(spark: ReparkSession, name: str) -> None:
    """The SQL SELECT list answers ``pos`` plus ``col``, keeping the outer NULL row."""
    (cell,) = [cell for cell in _cells(name, "sql") if "arr_i" in cell["expr"]]
    result = spark.sql(cell["expr"].replace("FRAME", f"({FRAME})"))
    _check_schema_and_rows(result, cell)


def test_python_door_posexplode_map(spark: ReparkSession) -> None:
    """A map answers ``pos`` plus ``key`` plus ``value`` on the Python door."""
    (cell,) = [cell for cell in _cells("posexplode", "python") if "create_map" in cell["expr"]]
    result = spark.sql(FRAME).select("id", F.posexplode(F.create_map(F.lit("a"), F.col("id"))))
    _check_schema_and_rows(result, cell)


def test_python_door_posexplode_multi_alias(spark: ReparkSession) -> None:
    """``.alias('p', 'v')`` renames both generator columns on the Python door."""
    (cell,) = [cell for cell in _cells("posexplode", "python") if "alias" in cell["expr"]]
    result = spark.sql(FRAME).select(F.posexplode("arr_i").alias("p", "v"))
    _check_schema_and_rows(result, cell)


def test_python_door_json_tuple_four_fields(spark: ReparkSession) -> None:
    """Four fields answer ``c0``..``c3`` as strings with NULL for the missing field."""
    (cell,) = [cell for cell in _cells("json_tuple", "python") if "zz" in cell["expr"]]
    result = spark.sql(FRAME).select("id", F.json_tuple("js", "a", "b", "c", "zz"))
    _check_schema_and_rows(result, cell)


def test_python_door_json_tuple_single_field_keeps_spark_name(spark: ReparkSession) -> None:
    """One field answers a single ``c0`` string column on the Python door."""
    (cell,) = [cell for cell in _cells("json_tuple", "python") if "zz" not in cell["expr"]]
    result = spark.sql(FRAME).select(F.json_tuple(F.col("js"), "a"))
    _check_schema_and_rows(result, cell)


def test_sql_door_json_tuple_three_fields(spark: ReparkSession) -> None:
    """Three fields answer ``c0``..``c2`` as strings on the SQL door."""
    (cell,) = [cell for cell in _cells("json_tuple", "sql") if "js," in cell["expr"]]
    result = spark.sql(cell["expr"].replace("FRAME", f"({FRAME})"))
    _check_schema_and_rows(result, cell)


def test_sql_door_json_tuple_invalid_json_answers_null(spark: ReparkSession) -> None:
    """Invalid JSON answers NULL instead of raising on the SQL door."""
    (cell,) = [cell for cell in _cells("json_tuple", "sql") if "js," not in cell["expr"]]
    result = spark.sql(cell["expr"])
    _check_schema_and_rows(result, cell)


def test_python_door_from_csv_parses_the_struct(spark: ReparkSession) -> None:
    """A DDL schema parses each row into a struct with PERMISSIVE bad-field NULLs."""
    cells = [cell for cell in _cells("from_csv", "python") if "FAILFAST" not in cell["expr"]]
    assert len(cells) == 2
    for cell in cells:
        schema = "a INT, b STRING, c DOUBLE" if "c DOUBLE" in cell["expr"] else "a INT, b STRING"
        if "lit(" in cell["expr"]:
            result = spark.sql(FRAME).select(
                F.from_csv(F.col("csvrow"), F.lit(schema), {"sep": ","})
            )
        else:
            result = spark.sql(FRAME).select(F.from_csv("csvrow", schema))
        assert result.schema.fields[0].dataType.simpleString() == cell["columns"][0]["type"]
        assert result.schema.fields[0].nullable == cell["columns"][0]["nullable"]
        assert result.to_arrow().to_pylist() == _want_rows(cell)


def test_sql_door_from_csv_parses_three_structs(spark: ReparkSession) -> None:
    """The SQL door parses the column struct, the ``sep`` struct and the bad-field struct."""
    (cell,) = _cells("from_csv", "sql")
    result = spark.sql(cell["expr"].replace("FRAME", f"({FRAME})"))
    assert [field.dataType.simpleString() for field in result.schema.fields] == [
        column["type"] for column in cell["columns"]
    ]
    assert result.to_arrow().to_pylist() == _want_rows(cell)


def test_from_csv_struct_carries_arrow_int32_and_double(spark: ReparkSession) -> None:
    """The parsed struct is Arrow ``int32``/``string``/``float64`` on both doors."""
    want = pa.struct(
        [pa.field("a", pa.int32()), pa.field("b", pa.string()), pa.field("c", pa.float64())]
    )
    column = spark.sql(FRAME).select(F.from_csv("csvrow", "a INT, b STRING, c DOUBLE").alias("r"))
    assert column.to_arrow().schema.field("r").type == want
    door = spark.sql(f"SELECT from_csv(csvrow, 'a INT, b STRING, c DOUBLE') AS r FROM ({FRAME})")
    assert door.to_arrow().schema.field("r").type == want


def test_python_door_from_csv_failfast_raises_the_parse_error(spark: ReparkSession) -> None:
    """FAILFAST raises the parse error at execution, never the stub refusal."""
    (cell,) = [cell for cell in _cells("from_csv", "python") if "FAILFAST" in cell["expr"]]
    assert cell.get("error_type") == "Py4JJavaError"
    with pytest.raises(Exception) as excinfo:
        spark.sql(FRAME).select(
            F.from_csv("csvrow", "a INT, b INT", {"mode": "FAILFAST"})
        ).collect()
    assert "not supported yet" not in str(excinfo.value)


@pytest.mark.parametrize("options", [None, {"sep": "|"}])
def test_python_door_schema_of_csv_infers_the_ddl(spark: ReparkSession, options: Any) -> None:
    """A foldable CSV literal infers Spark's DDL string on the Python door."""
    cells = _cells("schema_of_csv", "python")
    cell = next(cell for cell in cells if ("lit(" in cell["expr"]) == (options is not None))
    if options is None:
        result = spark.sql(FRAME).select(F.schema_of_csv("1,abc,2.5"))
    else:
        result = spark.sql(FRAME).select(F.schema_of_csv(F.lit("1|x"), {"sep": "|"}))
    _check_schema_and_rows(result, cell)


def test_sql_door_schema_of_csv_non_foldable_raises(spark: ReparkSession) -> None:
    """A non-foldable column raises ``DATATYPE_MISMATCH.NON_FOLDABLE_INPUT``."""
    (cell,) = [cell for cell in _cells("schema_of_csv", "sql") if "csvrow" in cell["expr"]]
    assert cell["error_condition"] == NON_FOLDABLE
    with pytest.raises(AnalysisException, match=NON_FOLDABLE):
        spark.sql(cell["expr"].replace("FRAME", f"({FRAME})"))


def test_sql_door_schema_of_csv_uninferable_literal_raises(spark: ReparkSession) -> None:
    """Spark itself refuses the uninferable literal, so the door must raise, not answer."""
    (cell,) = [cell for cell in _cells("schema_of_csv", "sql") if "csvrow" not in cell["expr"]]
    assert "error_type" in cell
    with pytest.raises(Exception) as excinfo:
        spark.sql(cell["expr"]).collect()
    assert "Invalid function" not in str(excinfo.value)


def test_python_door_from_xml_refuses_declared(spark: ReparkSession) -> None:
    """``from_xml`` raises the declared XML refusal that names its registry row."""
    with pytest.raises(UnsupportedOperationException, match=REFUSAL_MARKER):
        spark.sql(FRAME).select(F.from_xml("xml", "a INT, b STRING")).collect()


def test_python_door_schema_of_xml_refuses_declared(spark: ReparkSession) -> None:
    """``schema_of_xml`` raises the declared XML refusal that names its registry row."""
    with pytest.raises(UnsupportedOperationException, match=REFUSAL_MARKER):
        spark.sql("SELECT 1 AS one").select(F.schema_of_xml("<p><a>1</a></p>")).collect()


def test_sql_door_from_xml_refuses_declared(spark: ReparkSession) -> None:
    """The SQL door refuses ``from_xml`` with the engine's unsupported-operation error."""
    with pytest.raises(UnsupportedOperationException, match=REFUSAL_MARKER):
        spark.sql(f"SELECT from_xml(xml, 'a INT, b STRING') FROM ({FRAME})").collect()


def test_sql_door_schema_of_xml_refuses_declared(spark: ReparkSession) -> None:
    """The SQL door refuses ``schema_of_xml`` with the engine's unsupported-operation error."""
    with pytest.raises(UnsupportedOperationException, match=REFUSAL_MARKER):
        spark.sql("SELECT schema_of_xml('<p><a>1</a></p>')").collect()
