"""WO NTZ-1 slices 1 to 3: TIMESTAMP_NTZ literals, casts, store and storage.

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

Slice 3 pins the storage surface through SQL literals against the same probe
JSONs (``ntz-spark.json``, ``ntz2-spark.json``): the six partition transforms
(``.partitions`` rows, pruning, ``.files`` bounds, the New York ``days``
insert), identity and bucket partitions, v3 DML, CTAS, ``ADD COLUMN``,
``SHOW CREATE TABLE``, ``printSchema``, ``dtypes``, ``collect()`` and
``toArrow()`` types, and the filter / ORDER BY / min / max / interval legs.
``.partitions`` structs compare probe-normalized (dates render ISO, naive walls
keep no zone); unaliased expression names do not compare (the R2 naming class).
The single INSERT writes one file per partition, so on months/years the two
dated rows bound one file; the per-file walls follow Spark's measured days
pattern (work-order line 30).
The ``date_trunc`` typeof leg compares the value only (residue: Spark answers
``timestamp``, RePark ``timestamp_ntz``). The ``toArrow()`` legs compare field
types only (residue: RePark carries ``PARQUET:field_id`` metadata). The last
test adopts the Spark-written ``xc.ns.x`` fixture (one row
``2024-01-01 12:34:56.123456``, ``ntz-xc-spark.json`` beside the probes) at its
canonical path and replays Spark's reads, ``.files`` bounds and re-insert; the
``z`` bound compares as ``CAST … AS STRING`` in a UTC session, which is the
recorded instant rendering.

pins: ntz-1/C-001, C-002, C-003, C-004, C-005, C-006, C-007, C-008
"""

from __future__ import annotations

import datetime
import io
import json
import re
import shutil
import time
from collections.abc import Iterator
from contextlib import contextmanager, redirect_stdout, suppress
from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions
from repark.spark.types import TimestampNTZType

ORACLE_PATH: Path = Path(__file__).with_name("ntz_1_spark_oracle.json")
_ORACLE: dict[str, Any] = json.loads(ORACLE_PATH.read_text(encoding="utf-8"))
_XC_SRC: Path = Path(__file__).resolve().parent / "fixtures" / "ntz_1_spark_table"
_XC_ROOT: Path = Path("/tmp/repark-ntz-1-spark-table/wh")
_XC_WALL: str = "2024-01-01 12:34:56.123456"

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


class _DirLock:
    """Cross-process lock so concurrent facade tests do not clobber the fixture copy."""

    def __init__(self, path: Path) -> None:
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        started = time.monotonic()
        while True:
            try:
                self.path.mkdir()
                return
            except FileExistsError:
                if time.monotonic() - started > 120:
                    raise TimeoutError(
                        f"fixture lock {path} held for 2 minutes (no steal)"
                    ) from None
                time.sleep(0.025)

    def close(self) -> None:
        with suppress(OSError):
            self.path.rmdir()


@contextmanager
def _materialize_xc() -> Iterator[Path]:
    """Copy the Spark-written table to its canonical path; yield newest metadata file."""
    dest = _XC_ROOT / "ns" / "x"
    lock = _DirLock(Path(str(dest) + ".lock"))
    try:
        if dest.exists():
            shutil.rmtree(dest)
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(_XC_SRC, dest, ignore=shutil.ignore_patterns("*.crc", "map.md"))
        versions = sorted(
            (dest / "metadata").glob("v*.metadata.json"),
            key=lambda path: int(path.name[1:].split(".", 1)[0]),
        )
        assert versions, f"no Hadoop metadata under {dest}/metadata"
        yield versions[-1]
    finally:
        with suppress(OSError):
            if dest.exists():
                shutil.rmtree(dest)
        lock.close()


def _seed_filter_table(session: ReparkSession) -> None:
    """Create the five-row probe2 table through literals, the New York leg included."""
    session.sql("CREATE TABLE sc.ns.t (id INT, c TIMESTAMP_NTZ) USING iceberg").collect()
    session.sql(
        "INSERT INTO sc.ns.t VALUES (0, TIMESTAMP_NTZ'2024-01-01 12:34:56.123456'), "
        "(1, TIMESTAMP_NTZ'2024-01-02 00:00:00'), (2, NULL)"
    ).collect()
    session.sql("INSERT INTO sc.ns.t VALUES (3, DATE'2024-01-03')").collect()
    session.conf.set("spark.sql.session.timeZone", "America/New_York")
    session.sql("INSERT INTO sc.ns.t SELECT 4, TIMESTAMP'2024-01-04 12:00:00Z'").collect()
    session.conf.set("spark.sql.session.timeZone", "UTC")


@pytest.mark.parametrize(
    ("transform", "struct", "partitions", "bounds"),
    [
        (
            "days",
            "struct<c_day:date>",
            [[{"c_day": None}, 1], [{"c_day": "2024-01-01"}, 1], [{"c_day": "2024-01-02"}, 1]],
            [
                [None, None],
                ["2024-01-01T23:30:00|tz=None", "2024-01-01T23:30:00|tz=None"],
                ["2024-01-02T00:30:00|tz=None", "2024-01-02T00:30:00|tz=None"],
            ],
        ),
        (
            "hours",
            "struct<c_hour:int>",
            [[{"c_hour": None}, 1], [{"c_hour": 473375}, 1], [{"c_hour": 473376}, 1]],
            [
                [None, None],
                ["2024-01-01T23:30:00|tz=None", "2024-01-01T23:30:00|tz=None"],
                ["2024-01-02T00:30:00|tz=None", "2024-01-02T00:30:00|tz=None"],
            ],
        ),
        (
            "months",
            "struct<c_month:int>",
            [[{"c_month": None}, 1], [{"c_month": 648}, 2]],
            [
                [None, None],
                ["2024-01-01T23:30:00|tz=None", "2024-01-02T00:30:00|tz=None"],
            ],
        ),
        (
            "years",
            "struct<c_year:int>",
            [[{"c_year": None}, 1], [{"c_year": 54}, 2]],
            [
                [None, None],
                ["2024-01-01T23:30:00|tz=None", "2024-01-02T00:30:00|tz=None"],
            ],
        ),
    ],
)
def test_partition_transforms_answer_as_spark(
    transform: str,
    struct: str,
    partitions: list[list[Any]],
    bounds: list[list[Any]],
    tmp_path: Path,
) -> None:
    """Each time transform's partitions, pruning and file bounds replay Spark."""
    session = _open("UTC", tmp_path)
    try:
        table = f"sc.ns.p_{transform}"
        session.sql(
            f"CREATE TABLE {table} (id INT, c TIMESTAMP_NTZ) USING iceberg "
            f"PARTITIONED BY ({transform}(c))"
        ).collect()
        session.sql(
            f"INSERT INTO {table} VALUES (0, TIMESTAMP_NTZ'2024-01-01 23:30:00'), "
            "(1, TIMESTAMP_NTZ'2024-01-02 00:30:00'), (2, NULL)"
        ).collect()
        frame = session.sql(
            f"SELECT partition, record_count FROM {table}.partitions "
            "ORDER BY record_count, partition"
        )
        assert _rows(frame, "norm") == partitions
        assert _dtypes(frame) == [["partition", struct], ["record_count", "bigint"]]
        assert _rows(
            session.sql(f"SELECT id FROM {table} WHERE c >= TIMESTAMP_NTZ'2024-01-02 00:00:00'"),
            "norm",
        ) == [[1]]
        files = session.sql(
            f"SELECT readable_metrics.c.lower_bound, readable_metrics.c.upper_bound "
            f"FROM {table}.files ORDER BY readable_metrics.c.lower_bound NULLS FIRST"
        )
        assert _rows(files, "norm") == bounds
        assert _dtypes(files) == [
            ["lower_bound", "timestamp_ntz"],
            ["upper_bound", "timestamp_ntz"],
        ]
        assert _rows(
            session.sql(
                f"SELECT typeof(readable_metrics.c.lower_bound) FROM {table}.files LIMIT 1"
            ),
            "norm",
        ) == [["timestamp_ntz"]]
        if transform == "days":
            session.conf.set("spark.sql.session.timeZone", "America/New_York")
            session.sql(
                f"INSERT INTO {table} VALUES (3, TIMESTAMP_NTZ'2024-01-01 23:30:00')"
            ).collect()
            session.conf.set("spark.sql.session.timeZone", "UTC")
            assert _rows(
                session.sql(
                    f"SELECT partition, record_count FROM {table}.partitions "
                    "ORDER BY record_count, partition"
                ),
                "norm",
            ) == [[{"c_day": None}, 1], [{"c_day": "2024-01-02"}, 1], [{"c_day": "2024-01-01"}, 2]]
    finally:
        session.stop()


def test_identity_and_bucket_partitions_answer_as_spark(tmp_path: Path) -> None:
    """Identity and bucket partitions of an NTZ column replay Spark."""
    session = _open("UTC", tmp_path)
    try:
        session.sql(
            "CREATE TABLE sc.ns.pid (id INT, c TIMESTAMP_NTZ) USING iceberg PARTITIONED BY (c)"
        ).collect()
        session.sql(
            "INSERT INTO sc.ns.pid VALUES (0, TIMESTAMP_NTZ'2024-01-01 12:00:00')"
        ).collect()
        identity = session.sql("SELECT partition, record_count FROM sc.ns.pid.partitions")
        assert _rows(identity, "norm") == [[{"c": "2024-01-01T12:00:00|tz=None"}, 1]]
        assert _dtypes(identity) == [
            ["partition", "struct<c:timestamp_ntz>"],
            ["record_count", "bigint"],
        ]
        session.sql(
            "CREATE TABLE sc.ns.pb (id INT, c TIMESTAMP_NTZ) USING iceberg "
            "PARTITIONED BY (bucket(4, c))"
        ).collect()
        session.sql(
            "INSERT INTO sc.ns.pb VALUES (0, TIMESTAMP_NTZ'2024-01-01 12:00:00'), "
            "(1, TIMESTAMP_NTZ'2024-06-01 00:00:00')"
        ).collect()
        bucket = session.sql(
            "SELECT partition, record_count FROM sc.ns.pb.partitions ORDER BY partition"
        )
        assert _rows(bucket, "norm") == [[{"c_bucket": 2}, 1], [{"c_bucket": 3}, 1]]
        assert _dtypes(bucket) == [
            ["partition", "struct<c_bucket:int>"],
            ["record_count", "bigint"],
        ]
    finally:
        session.stop()


def test_filters_order_and_minmax_answer_as_spark(tmp_path: Path) -> None:
    """Filters, ORDER BY, min/max and interval arithmetic replay Spark's rows."""
    session = _open("UTC", tmp_path)
    try:
        _seed_filter_table(session)
        assert _walls(session, "sc.ns.t") == [
            [0, "2024-01-01 12:34:56.123456"],
            [1, "2024-01-02 00:00:00"],
            [2, None],
            [3, "2024-01-03 00:00:00"],
            [4, "2024-01-04 07:00:00"],
        ]
        legs = [
            ("SELECT id FROM sc.ns.t WHERE c = TIMESTAMP_NTZ'2024-01-01 12:34:56.123456'", [[0]]),
            (
                "SELECT id FROM sc.ns.t WHERE c > '2024-01-01 12:00:00' ORDER BY id",
                [[0], [1], [3], [4]],
            ),
            ("SELECT id FROM sc.ns.t WHERE c >= DATE'2024-01-02' ORDER BY id", [[1], [3], [4]]),
            (
                "SELECT id FROM sc.ns.t WHERE c BETWEEN TIMESTAMP_NTZ'2024-01-01 00:00:00' "
                "AND TIMESTAMP_NTZ'2024-01-02 00:00:00' ORDER BY id",
                [[0], [1]],
            ),
            (
                "SELECT id FROM sc.ns.t WHERE c IN (TIMESTAMP_NTZ'2024-01-02 00:00:00', "
                "TIMESTAMP_NTZ'2024-01-03 00:00:00') ORDER BY id",
                [[1], [3]],
            ),
            ("SELECT id FROM sc.ns.t WHERE c = TIMESTAMP'2024-01-02 00:00:00' ORDER BY id", [[1]]),
            ("SELECT id FROM sc.ns.t ORDER BY c NULLS FIRST, id", [[2], [0], [1], [3], [4]]),
        ]
        for sql, want in legs:
            assert _rows(session.sql(sql), "norm") == want, sql
        session.conf.set("spark.sql.session.timeZone", "America/New_York")
        for sql in [
            "SELECT id FROM sc.ns.t WHERE c = TIMESTAMP'2024-01-02 00:00:00' ORDER BY id",
            "SELECT id FROM sc.ns.t WHERE c = TIMESTAMP'2024-01-02 05:00:00Z' ORDER BY id",
        ]:
            assert _rows(session.sql(sql), "norm") == [[1]], sql
        session.conf.set("spark.sql.session.timeZone", "UTC")
        assert _rows(
            session.sql(
                "SELECT CAST(min(c) AS STRING), CAST(max(c) AS STRING), typeof(max(c)) FROM sc.ns.t"
            ),
            "norm",
        ) == [["2024-01-01 12:34:56.123456", "2024-01-04 07:00:00", "timestamp_ntz"]]
        assert _rows(
            session.sql(
                "SELECT CAST(c + INTERVAL 1 DAY AS STRING), typeof(c + INTERVAL 1 DAY) "
                "FROM sc.ns.t WHERE id = 0"
            ),
            "norm",
        ) == [["2024-01-02 12:34:56.123456", "timestamp_ntz"]]
        assert _rows(
            session.sql(
                "SELECT hour(c), CAST(date_trunc('DAY', c) AS STRING), CAST(c AS DATE) "
                "FROM sc.ns.t WHERE id = 0"
            ),
            "norm",
        ) == [[12, "2024-01-01 00:00:00", "2024-01-01"]]
        assert _rows(
            session.sql(
                "SELECT typeof(c), typeof(TIMESTAMP_NTZ'2024-01-01 00:00:00') "
                "FROM sc.ns.t WHERE id = 0"
            ),
            "norm",
        ) == [["timestamp_ntz", "timestamp_ntz"]]
    finally:
        session.stop()


def test_ctas_add_column_and_presentation_answer_as_spark(tmp_path: Path) -> None:
    """CTAS, ADD COLUMN and the DDL presentation replay Spark."""
    session = _open("UTC", tmp_path)
    try:
        session.sql("CREATE TABLE sc.ns.t (id INT, c TIMESTAMP_NTZ) USING iceberg").collect()
        session.sql(
            "INSERT INTO sc.ns.t VALUES (0, TIMESTAMP_NTZ'2024-01-01 12:34:56.123456')"
        ).collect()
        session.sql(
            "CREATE TABLE sc.ns.ctas USING iceberg AS "
            "SELECT TIMESTAMP_NTZ'2024-01-01 12:00:00' AS c"
        ).collect()
        session.sql(
            "CREATE TABLE sc.ns.ctas2 USING iceberg AS "
            "SELECT CAST('2024-01-01 00:00:00' AS TIMESTAMP_NTZ) AS c"
        ).collect()
        for table in ("sc.ns.ctas", "sc.ns.ctas2"):
            assert _table_schema(tmp_path, table) == [["c", "timestamp", False]]
            assert _rows(session.sql(f"DESCRIBE TABLE {table}"), "norm") == [
                ["c", "timestamp_ntz", None]
            ]
        session.sql("ALTER TABLE sc.ns.t ADD COLUMN c2 TIMESTAMP_NTZ").collect()
        assert _table_schema(tmp_path, "sc.ns.t") == [
            ["id", "int", False],
            ["c", "timestamp", False],
            ["c2", "timestamp", False],
        ]
        assert _rows(session.sql("DESCRIBE TABLE sc.ns.t"), "norm") == [
            ["id", "int", None],
            ["c", "timestamp_ntz", None],
            ["c2", "timestamp_ntz", None],
        ]
        create = session.sql("SHOW CREATE TABLE sc.ns.t").collect()[0][0]
        assert "c TIMESTAMP_NTZ" in create
        assert "USING iceberg" in create
        assert "'format-version' = '2'" in create
        session.sql("ALTER TABLE sc.ns.t ALTER COLUMN c TYPE TIMESTAMP_NTZ").collect()
        assert _table_schema(tmp_path, "sc.ns.t") == [
            ["id", "int", False],
            ["c", "timestamp", False],
            ["c2", "timestamp", False],
        ]
        assert _dtypes(session.sql("SELECT * FROM sc.ns.t")) == [
            ["id", "int"],
            ["c", "timestamp_ntz"],
            ["c2", "timestamp_ntz"],
        ]
        assert _rows(session.sql("SELECT c FROM sc.ns.t WHERE id = 0"), "norm") == [
            ["2024-01-01T12:34:56.123456|tz=None"]
        ]
        arrow = session.sql("SELECT id, c FROM sc.ns.t WHERE id = 0").to_arrow()
        assert arrow.schema.field("id").type == pa.int32()
        assert arrow.schema.field("c").type == pa.timestamp("us")
    finally:
        session.stop()


def test_printschema_spells_timestamp_ntz(tmp_path: Path) -> None:
    """printSchema renders the NTZ column exactly as Spark does."""
    session = _open("UTC", tmp_path)
    try:
        session.sql("CREATE TABLE sc.ns.t (id INT, c TIMESTAMP_NTZ) USING iceberg").collect()
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            session.table("sc.ns.t").printSchema()
        assert buffer.getvalue() == (
            "root\n |-- id: integer (nullable = true)\n |-- c: timestamp_ntz (nullable = true)\n\n"
        )
    finally:
        session.stop()


def test_v3_round_trip_answers_as_spark(tmp_path: Path) -> None:
    """A v3 table takes literals through INSERT, UPDATE, DELETE and MERGE."""
    session = _open("UTC", tmp_path, version3=True)
    try:
        session.sql(
            "CREATE TABLE sc.ns.t3 (id INT, c TIMESTAMP_NTZ) USING iceberg "
            "TBLPROPERTIES ('format-version'='3')"
        ).collect()
        session.sql(
            "INSERT INTO sc.ns.t3 VALUES (0, TIMESTAMP_NTZ'2024-01-01 12:34:56.123456'), (1, NULL)"
        ).collect()
        assert _table_schema(tmp_path, "sc.ns.t3") == [
            ["id", "int", False],
            ["c", "timestamp", False],
        ]
        assert _walls(session, "sc.ns.t3") == [[0, "2024-01-01 12:34:56.123456"], [1, None]]
        session.sql(
            "UPDATE sc.ns.t3 SET c = TIMESTAMP_NTZ'2025-05-05 05:05:05' WHERE id = 0"
        ).collect()
        session.sql("DELETE FROM sc.ns.t3 WHERE id = 1").collect()
        session.sql(
            "MERGE INTO sc.ns.t3 t USING "
            "(SELECT 9 AS id, TIMESTAMP_NTZ'2030-01-01 00:00:00' AS c) s "
            "ON t.id = s.id WHEN NOT MATCHED THEN INSERT *"
        ).collect()
        assert _walls(session, "sc.ns.t3") == [
            [0, "2025-05-05 05:05:05"],
            [9, "2030-01-01 00:00:00"],
        ]
    finally:
        session.stop()


def test_spark_written_table_reads_and_reinserts_as_spark(tmp_path: Path) -> None:
    """The Spark-written xc.ns.x fixture reads, bounds and re-inserts as Spark."""
    with _materialize_xc() as metadata_file:
        session = _open("UTC", tmp_path)
        try:
            session.sql(
                "CALL sc.system.register_table("
                f"table => 'ns.x', metadata_file => '{metadata_file}')"
            ).collect()
            assert _table_schema(_XC_ROOT, "sc.ns.x") == [
                ["id", "int", False],
                ["c", "timestamp", False],
                ["z", "timestamptz", False],
            ]
            frame = session.sql(
                "SELECT id, CAST(c AS STRING) AS c, CAST(z AS STRING) AS z FROM sc.ns.x ORDER BY id"
            )
            assert _rows(frame, "str") == [[0, _XC_WALL, _XC_WALL]]
            assert _dtypes(frame) == [["id", "int"], ["c", "string"], ["z", "string"]]
            assert _rows(session.sql("SELECT id FROM sc.ns.x WHERE c IS NOT NULL"), "norm") == [[0]]
            assert _rows(
                session.sql("SELECT CAST(c AS STRING), CAST(z AS STRING), hour(c) FROM sc.ns.x"),
                "str",
            ) == [[_XC_WALL, _XC_WALL, 12]]
            assert _dtypes(session.table("sc.ns.x")) == [
                ["id", "int"],
                ["c", "timestamp_ntz"],
                ["z", "timestamp"],
            ]
            arrow = session.table("sc.ns.x").to_arrow()
            assert arrow.schema.field("id").type == pa.int32()
            assert arrow.schema.field("c").type == pa.timestamp("us")
            assert arrow.schema.field("z").type == pa.timestamp("us", tz="UTC")
            bounds = session.sql(
                "SELECT readable_metrics.c.lower_bound AS c_lo, "
                "readable_metrics.c.upper_bound AS c_hi, "
                "readable_metrics.z.lower_bound AS z_lo, "
                "readable_metrics.z.upper_bound AS z_hi FROM sc.ns.x.files"
            )
            assert _rows(bounds, "str") == [
                [_XC_WALL, _XC_WALL, _XC_WALL, _XC_WALL],
            ]
            assert _dtypes(bounds) == [
                ["c_lo", "timestamp_ntz"],
                ["c_hi", "timestamp_ntz"],
                ["z_lo", "timestamp"],
                ["z_hi", "timestamp"],
            ]
            session.sql("INSERT INTO sc.ns.x SELECT id + 1, c, z FROM sc.ns.x").collect()
            assert _rows(
                session.sql(
                    "SELECT id, CAST(c AS STRING) AS c, CAST(z AS STRING) AS z "
                    "FROM sc.ns.x ORDER BY id"
                ),
                "str",
            ) == [[0, _XC_WALL, _XC_WALL], [1, _XC_WALL, _XC_WALL]]
        finally:
            session.stop()
