"""WO NTZ-1 slice 1: the TIMESTAMP_NTZ literal and explicit casts replay Spark.

The oracle is ``ntz_1_spark_oracle.json`` (Spark 4.1.2 + Iceberg 1.11.0, measured
2026-09-26): the Slice 1 literal/cast/refusal queries with their session zone and
Spark's answer, the v2/v3 one-row cell shapes, and the two DataFrame-door legs.
Every success step compares rows and dtypes exactly; column names compare too
except on unaliased CAST legs, where RePark keeps its existing cast naming (R2
residue: the embedded-UDF call renders where Spark renders its cast text).
Every refusal step compares SQLSTATE plus Spark's first message line after two
mechanical framings come off: Spark's trailing analysis position
(``; line 1 pos 7;``) and RePark's engine prefixes (``Error during planning: ``,
``datafusion engine error: ``, ``Execution error: ``). Two structural deltas
stay visible. The NTZ-to-numeric
refusal crosses the ``spark_expr_semantics`` analyzer rule, whose
``spark_expr_semantics`` / ``caused by`` header the DATE-pair refusals already
carry, so those steps compare the ``cannot cast`` clause rather than the full
first line and skip the condition. The malformed-string cast raises at execution
time, where RePark has no DateTimeException and the doubled ``Execution error:``
prefix defeats the condition parser, so that step compares SQLSTATE plus the
core text and skips class and condition. The invalid-literal window compares
all four lines byte for byte; the caret count is the literal byte length.

pins: ntz-1/C-001, C-002, C-003, C-004, C-005
"""

from __future__ import annotations

import datetime
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions
from repark.spark.types import TimestampNTZType

ORACLE_PATH: Path = Path(__file__).with_name("ntz_1_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))

_SPARK_POSITION_SUFFIX: re.Pattern[str] = re.compile(r"; line \d+ pos \d+;$")
_CANNOT_CAST: re.Pattern[str] = re.compile(r'cannot cast "[^"]*" to "[^"]*"')
_REPARK_PREFIXES: tuple[str, ...] = (
    "Error during planning: ",
    "datafusion engine error: ",
    "Execution error: ",
)
_RULE_HEADER: str = "spark_expr_semantics\ncaused by\n"


def _norm(value: Any, style: str) -> Any:
    """Normalize one collected value the way the recording probe did."""
    if isinstance(value, datetime.datetime):
        if style == "str":
            return str(value)
        return f"{value.isoformat()}|tz={value.tzinfo}"
    if isinstance(value, (datetime.date, datetime.time)):
        return value.isoformat()
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, dict):
        return {str(key): _norm(item, style) for key, item in value.items()}
    if hasattr(value, "asDict"):
        return {key: _norm(item, style) for key, item in value.asDict().items()}
    if isinstance(value, (list, tuple)):
        return [_norm(item, style) for item in value]
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    return repr(value)


def _rows(frame: Any, style: str) -> list[list[Any]]:
    """Collect a frame into probe-normalized rows."""
    return [[_norm(value, style) for value in row] for row in frame.collect()]


def _dtypes(frame: Any) -> list[list[str]]:
    """Read a frame's dtypes as name/type pairs."""
    return [[name, dtype] for name, dtype in frame.dtypes]


def _condition(error: BaseException) -> Any:
    """Read the attached Spark condition, if the error carries one."""
    method = getattr(error, "getCondition", None)
    if not callable(method):
        return None
    return method()


def _sql_state(error: BaseException) -> Any:
    """Read the attached SQLSTATE, if the error carries one."""
    method = getattr(error, "getSqlState", None)
    if not callable(method):
        return None
    return method()


def _spark_first_line(message: str) -> str:
    """Read Spark's first message line without its trailing plan position."""
    return _SPARK_POSITION_SUFFIX.sub("", message.splitlines()[0])


def _repark_core(message: str) -> list[str]:
    """Read RePark's message without engine framing, as lines."""
    text = message.removeprefix(_RULE_HEADER)
    for prefix in _REPARK_PREFIXES:
        while text.startswith(prefix):
            text = text.removeprefix(prefix)
    return text.splitlines()


def _cannot_cast_clause(first_line: str) -> str:
    """Read the cannot-cast clause of a DATATYPE_MISMATCH first line."""
    found = _CANNOT_CAST.search(first_line)
    assert found is not None
    return found.group(0)


def _open(zone: str, warehouse: Path, version3: bool = False) -> ReparkSession:
    """Open an ANSI facade session at the zone with a memory catalog."""
    builder = (
        ReparkSession.builder.appName("ntz-1")
        .config("spark.sql.session.timeZone", zone)
        .config("spark.sql.ansi.enabled", "true")
    )
    if version3:
        builder = builder.config("repark.sql.allowCreateFormatVersion3", "true")
    session = builder.getOrCreate()
    session.register_memory_catalog("sc", str(warehouse))
    session.sql("CREATE NAMESPACE IF NOT EXISTS sc.ns")
    return session


def _table_schema(warehouse: Path, table: str) -> list[list[Any]]:
    """Read the current Iceberg schema of table as name/type/required rows."""
    name = table.split(".")[-1]
    files = [
        path
        for path in warehouse.rglob("*.metadata.json")
        if path.parent.name == "metadata" and path.parent.parent.name.split("-")[0] == name
    ]
    newest = max(files, key=lambda path: (path.stat().st_mtime_ns, path.name))
    metadata = json.loads(newest.read_text(encoding="utf-8"))
    current = metadata["current-schema-id"]
    schema = next(entry for entry in metadata["schemas"] if entry["schema-id"] == current)
    return [[field["name"], field["type"], field["required"]] for field in schema["fields"]]


def _assert_error(step: dict[str, Any], error: BaseException) -> None:
    """Replay one oracle refusal against Spark's recorded answer."""
    spark = step["spark"]
    if spark["class_match"]:
        assert type(error).__name__ == spark["error"]
    if spark["condition_match"]:
        assert _condition(error) == spark["condition"]
    assert _sql_state(error) == spark["sqlstate"]
    core = _repark_core(str(error))
    want = _spark_first_line(spark["msg"])
    if str(error).startswith(_RULE_HEADER):
        assert _cannot_cast_clause(core[0]) == _cannot_cast_clause(want)
    else:
        assert core[0] == want
    if step["key"] == "lit_bad":
        assert core[1] == spark["msg"].splitlines()[1]
        assert core[2] == spark["msg"].splitlines()[2]
        assert core[3] == spark["msg"].splitlines()[3]


def _assert_select(session: ReparkSession, step: dict[str, Any]) -> None:
    """Replay one oracle SELECT against Spark's recorded answer."""
    key = step["key"]
    spark = step["spark"]
    if "error" in spark:
        try:
            session.sql(step["sql"]).collect()
        except Exception as error:
            _assert_error(step, error)
        else:
            raise AssertionError(f"{key} answered instead of refusing")
        return
    frame = session.sql(step["sql"])
    assert _rows(frame, step["style"]) == spark["rows"], key
    if spark.get("names_match", True):
        assert _dtypes(frame) == spark["schema"], key
    else:
        want_types = [dtype for _, dtype in spark["schema"]]
        assert [dtype for _, dtype in _dtypes(frame)] == want_types, key


@pytest.mark.parametrize("zone", ["UTC", "America/New_York"])
def test_select_cells_replay_spark(zone: str, tmp_path: Path) -> None:
    """Every Slice 1 literal/cast/refusal SELECT replays its Spark answer."""
    session = _open(zone, tmp_path)
    try:
        for step in [entry for entry in _ORACLE["selects"] if entry["zone"] == zone]:
            _assert_select(session, step)
    finally:
        session.stop()


@pytest.mark.parametrize("version", [2, 3])
def test_table_cells_replay_spark(version: int, tmp_path: Path) -> None:
    """The v2/v3 cell shape replays Spark's rows, columns, schema and filter."""
    session = _open("UTC", tmp_path, version3=(version == 3))
    try:
        cell = next(entry for entry in _ORACLE["cells"] if entry["version"] == version)
        session.sql(cell["create"]).collect()
        session.sql(cell["insert"]).collect()
        frame = session.sql(cell["rows_sql"])
        assert _rows(frame, "norm") == cell["rows"]
        assert _dtypes(frame) == cell["cols"]
        assert _table_schema(tmp_path, "sc.ns.t") == cell["meta"]
        assert _rows(session.sql(cell["filter_sql"]), "norm") == cell["filter"]
        assert _rows(session.sql(cell["where_lit_sql"]), "norm") == cell["where_lit"]
    finally:
        session.stop()


def test_dataframe_door_casts_answer_as_spark(tmp_path: Path) -> None:
    """functions.lit(...).cast(...) spells timestamp_ntz through the string and the type."""
    session = _open("UTC", tmp_path)
    try:
        literal = _ORACLE["dataframe"][0]["literal"]
        frame = session.sql("SELECT 1 AS v").select(
            functions.lit(literal).cast("timestamp_ntz").alias("v")
        )
        want = [[entry["v"]] for entry in _ORACLE["dataframe"][0]["spark_rows"]]
        assert _rows(frame, "norm") == want
        typed = session.sql("SELECT 1 AS v").select(
            functions.lit(literal).cast(TimestampNTZType()).alias("v")
        )
        assert _dtypes(typed) == _ORACLE["dataframe"][1]["spark_dtypes"]
    finally:
        session.stop()


@pytest.mark.parametrize(
    "sql",
    [
        "SELECT CAST(array('2024-01-01 00:00:00') AS ARRAY<TIMESTAMP_NTZ>) AS v",
        "SELECT CAST(named_struct('a', '2024-01-01') AS STRUCT<a: TIMESTAMP_NTZ>) AS v",
    ],
)
def test_nested_cast_target_keeps_the_r4_refusal(sql: str, tmp_path: Path) -> None:
    """A nested TIMESTAMP_NTZ target keeps the R4 refusal, not a value."""
    session = _open("UTC", tmp_path)
    try:
        with pytest.raises(AnalysisException) as caught:
            session.sql(sql).collect()
        assert _condition(caught.value) == "UNSUPPORTED_TIMESTAMP_NTZ"
        assert _sql_state(caught.value) == "0A000"
        assert _repark_core(str(caught.value))[0] == (
            "[UNSUPPORTED_TIMESTAMP_NTZ] TIMESTAMP_NTZ inside a nested cast target "
            "(ARRAY, STRUCT or MAP) is not supported yet; the scalar TIMESTAMP_NTZ "
            "literal and cast are. See TZ-6 (docs/spark-sql-iceberg-parity.md). "
            "SQLSTATE: 0A000"
        )
    finally:
        session.stop()
