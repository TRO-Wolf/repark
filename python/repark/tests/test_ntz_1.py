"""WO NTZ-1 slices 1 and 2: TIMESTAMP_NTZ literals, casts and store assignment.

The oracle is ``ntz_1_spark_oracle.json`` (Spark 4.1.2 + Iceberg 1.11.0, measured
2026-09-26): the Slice 1 literal/cast/refusal queries with their session zone and
Spark's answer, the v2/v3 one-row cell shapes, and the two DataFrame-door legs.
Every success step compares rows and dtypes exactly; column names compare too
except on unaliased CAST legs, where RePark keeps its existing cast naming (R2
residue: the embedded-UDF call renders where Spark renders its cast text).
Every refusal step compares SQLSTATE plus Spark's first message line after two
mechanical framings come off: Spark's trailing analysis position
(``; line 1 pos 7;``) and RePark's engine prefixes (``Error during planning: ``,
``datafusion engine error: ``, ``Execution error: ``) plus the
``spark_expr_semantics`` / ``caused by`` analyzer-rule header, which the
DATE-pair refusals already carry. Two structural deltas stay visible. The
NTZ-to-numeric steps compare the full first line and skip only the condition
per R-3. The malformed-string cast raises at execution
time, where RePark has no DateTimeException and the doubled ``Execution error:``
prefix defeats the condition parser, so that step compares SQLSTATE plus the
core text and skips class and condition. The invalid-literal window compares
all four lines byte for byte; the caret count is the CHAR length of the literal.

Slice 2 stores through every door and compares against the probe SQL and Spark's
recorded walls (``ntz-spark.json``, ``ntz2-spark.json``, ``ntz4-spark.json``,
``ntz6-spark.json``): values read back as ``CAST(c AS STRING)`` so host-zone
rendering never enters the comparison. The UTC UPDATE/MERGE legs are
rule-derived (session zone UTC leaves the wall untouched) rather than
probe-measured; every other wall below is Spark's recorded answer. Refusals
compare class, condition, SQLSTATE and the full first line with this file's
table name in Spark's template.

pins: ntz-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007
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
    assert core[0] == want
    if step["key"] in ("lit_bad", "lit_bad_wide"):
        assert core[1] == spark["msg"].splitlines()[1]
        assert core[2] == spark["msg"].splitlines()[2]
        assert core[3] == spark["msg"].splitlines()[3]


def _walls(session: ReparkSession, table: str) -> list[list[Any]]:
    """Read a table's id/wall rows with the wall rendered as Spark renders it."""
    return _rows(session.sql(f"SELECT id, CAST(c AS STRING) AS s FROM {table} ORDER BY id"), "str")


def _assert_store_refusal(session: ReparkSession, sql: str, table: str, source: str) -> None:
    """Replay one store refusal against Spark's recorded template and class."""
    try:
        session.sql(sql).collect()
    except Exception as error:
        assert type(error).__name__ == "AnalysisException"
        assert _condition(error) == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST"
        assert _sql_state(error) == "KD000"
        assert _repark_core(str(error))[0] == (
            "[INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST] Cannot write incompatible data "
            f'for the table {table}: Cannot safely cast `c` "{source}" to "TIMESTAMP_NTZ". '
            "SQLSTATE: KD000"
        )
    else:
        raise AssertionError(f"{sql} answered instead of refusing")


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


def test_store_values_and_select_answer_as_spark(tmp_path: Path) -> None:
    """TIMESTAMP, NULL and DATE store into NTZ columns as Spark's walls."""
    session = _open("UTC", tmp_path)
    try:
        session.sql("CREATE TABLE sc.ns.t (id INT, c TIMESTAMP_NTZ) USING iceberg").collect()
        session.sql("INSERT INTO sc.ns.t VALUES (1, TIMESTAMP'2024-01-01 12:00:00')").collect()
        session.sql("INSERT INTO sc.ns.t VALUES (3, NULL)").collect()
        session.sql("INSERT INTO sc.ns.t VALUES (30, DATE'2024-01-03')").collect()
        assert _walls(session, "sc.ns.t") == [
            [1, "2024-01-01 12:00:00"],
            [3, None],
            [30, "2024-01-03 00:00:00"],
        ]
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        session.sql("INSERT INTO sc.ns.t VALUES (4, TIMESTAMP'2024-01-01 12:00:00')").collect()
        session.sql("INSERT INTO sc.ns.t VALUES (5, TIMESTAMP'2024-01-01 12:00:00Z')").collect()
        session.sql("INSERT INTO sc.ns.t SELECT 40, TIMESTAMP'2024-01-04 12:00:00Z'").collect()
        session.sql("INSERT INTO sc.ns.t SELECT 41, DATE'2024-01-04'").collect()
        assert _walls(session, "sc.ns.t") == [
            [1, "2024-01-01 12:00:00"],
            [3, None],
            [4, "2024-01-01 12:00:00"],
            [5, "2024-01-01 07:00:00"],
            [30, "2024-01-03 00:00:00"],
            [40, "2024-01-04 07:00:00"],
            [41, "2024-01-04 00:00:00"],
        ]
    finally:
        session.stop()


@pytest.mark.parametrize(
    ("zone", "update_wall", "merge_first", "merge_second"),
    [
        ("UTC", "2024-05-05 12:00:00", "2024-06-06 12:00:00", "2024-07-07 12:00:00"),
        ("America/New_York", "2024-05-05 08:00:00", "2024-06-06 08:00:00", "2024-07-07 08:00:00"),
    ],
)
def test_update_and_merge_store_the_session_zone_wall(
    zone: str, update_wall: str, merge_first: str, merge_second: str, tmp_path: Path
) -> None:
    """UPDATE and both MERGE arms store the instant's wall in the session zone."""
    session = _open(zone, tmp_path)
    try:
        session.sql("CREATE TABLE sc.ns.t (id INT, c TIMESTAMP_NTZ) USING iceberg").collect()
        session.sql(
            "INSERT INTO sc.ns.t VALUES (0, TIMESTAMP_NTZ'2024-01-01 00:00:00'), "
            "(1, TIMESTAMP_NTZ'2024-01-01 00:00:00')"
        ).collect()
        session.sql("UPDATE sc.ns.t SET c = TIMESTAMP'2024-05-05 12:00:00Z' WHERE id = 0").collect()
        assert _walls(session, "sc.ns.t") == [
            [0, update_wall],
            [1, "2024-01-01 00:00:00"],
        ]
        session.sql(
            "MERGE INTO sc.ns.t t USING (SELECT 1 AS id, TIMESTAMP'2024-06-06 12:00:00Z' AS c "
            "UNION ALL SELECT 2, TIMESTAMP'2024-07-07 12:00:00Z') s ON t.id = s.id "
            "WHEN MATCHED THEN UPDATE SET c = s.c WHEN NOT MATCHED THEN INSERT *"
        ).collect()
        assert _walls(session, "sc.ns.t") == [
            [0, update_wall],
            [1, merge_first],
            [2, merge_second],
        ]
    finally:
        session.stop()


def test_dataframe_append_stores_the_session_zone_wall(tmp_path: Path) -> None:
    """A DataFrame append of a TIMESTAMP column stores the session-zone wall."""
    session = _open("America/New_York", tmp_path)
    try:
        session.sql("CREATE TABLE sc.ns.t (id INT, c TIMESTAMP_NTZ) USING iceberg").collect()
        session.sql("INSERT INTO sc.ns.t VALUES (0, TIMESTAMP_NTZ'2024-01-01 00:00:00')").collect()
        frame = session.createDataFrame(
            [(3, datetime.datetime(2024, 8, 8, 12, 0, tzinfo=datetime.UTC))],
            "id INT, c TIMESTAMP",
        )
        frame.writeTo("sc.ns.t").append()
        assert _walls(session, "sc.ns.t") == [
            [0, "2024-01-01 00:00:00"],
            [3, "2024-08-08 08:00:00"],
        ]
    finally:
        session.stop()


def test_ntz_values_store_into_timestamp_as_session_instants(tmp_path: Path) -> None:
    """NTZ values store into a TIMESTAMP column as the session-zone instant."""
    session = _open("America/New_York", tmp_path)
    try:
        session.sql("CREATE TABLE sc.ns.l (id INT, c TIMESTAMP) USING iceberg").collect()
        session.sql("INSERT INTO sc.ns.l VALUES (0, TIMESTAMP_NTZ'2024-01-01 12:00:00')").collect()
        assert _walls(session, "sc.ns.l") == [[0, "2024-01-01 12:00:00"]]
        session.conf.set("spark.sql.session.timeZone", "UTC")
        assert _walls(session, "sc.ns.l") == [[0, "2024-01-01 17:00:00"]]
        session.sql("INSERT INTO sc.ns.l VALUES (1, TIMESTAMP_NTZ'2024-01-01 12:00:00')").collect()
        assert _walls(session, "sc.ns.l") == [
            [0, "2024-01-01 17:00:00"],
            [1, "2024-01-01 12:00:00"],
        ]
    finally:
        session.stop()


def test_store_refusals_name_timestamp_ntz(tmp_path: Path) -> None:
    """Illegal sources refuse with Spark's CANNOT_SAFELY_CAST on every door."""
    session = _open("UTC", tmp_path)
    try:
        session.sql("CREATE TABLE sc.ns.t (id INT, c TIMESTAMP_NTZ) USING iceberg").collect()
        session.sql("INSERT INTO sc.ns.t VALUES (0, TIMESTAMP_NTZ'2024-01-01 00:00:00')").collect()
        table = "`sc`.`ns`.`t`"
        for sql, source in [
            ("INSERT INTO sc.ns.t VALUES (2, '2024-03-10 02:30:00')", "STRING"),
            ("INSERT INTO sc.ns.t VALUES (5, 1)", "INT"),
            ("INSERT INTO sc.ns.t VALUES (1, true)", "BOOLEAN"),
            ("INSERT INTO sc.ns.t SELECT 3, '2024-01-01'", "STRING"),
            ("INSERT INTO sc.ns.t SELECT 2, 1", "INT"),
        ]:
            _assert_store_refusal(session, sql, table, source)
        for sql in [
            "UPDATE sc.ns.t SET c = '2024-01-01 00:00:00' WHERE id = 0",
            "MERGE INTO sc.ns.t t USING (SELECT 0 AS id, '2024-01-01 00:00:00' AS c) s ON "
            "t.id = s.id WHEN MATCHED THEN UPDATE SET c = s.c",
            "MERGE INTO sc.ns.t t USING (SELECT 9 AS id, '2024-01-01 00:00:00' AS c) s ON "
            "t.id = s.id WHEN NOT MATCHED THEN INSERT *",
        ]:
            _assert_store_refusal(session, sql, "``", "STRING")
        frame = session.createDataFrame([(7, "2024-01-01 00:00:00")], "id INT, c STRING")
        try:
            frame.writeTo("sc.ns.t").append()
        except Exception as error:
            assert type(error).__name__ == "AnalysisException"
            assert _condition(error) == "INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_SAFELY_CAST"
            assert _sql_state(error) == "KD000"
        else:
            raise AssertionError("a STRING append answered instead of refusing")
    finally:
        session.stop()
