"""FNP-11A temporal constructors, intervals and arithmetic pins.

The suite passing whole alongside the neighbor suites pins the
no-regression clause. pins: fnp-11a/C-006
"""

from __future__ import annotations

import datetime
import inspect
import json
import re
from decimal import Decimal
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.errors import AnalysisException
from repark.spark import functions as F  # noqa: N812

CARD_NAMES: frozenset[str] = frozenset(
    {
        "make_timestamp",
        "make_timestamp_ltz",
        "try_make_timestamp",
        "try_make_timestamp_ltz",
        "make_timestamp_ntz",
        "try_make_timestamp_ntz",
        "make_ym_interval",
        "try_make_interval",
        "months_between",
        "convert_timezone",
        "localtimestamp",
        "timestamp_add",
        "timestamp_diff",
    }
)

ORACLE_PATH: Path = Path(__file__).with_name("fnp11_spark_oracle.json")
FRAME_VIEW: str = "fnp11a_frame"
BOX_ZONE: str = "America/New_York"
TM_TOKEN: re.Pattern[str] = re.compile(r"\btm\b")
D4_BLOCKED_SQL: frozenset[str] = frozenset(
    {"timestampdiff(DAY, ntz, TIMESTAMP_NTZ'2024-03-11 01:00:00')"}
)
FRAME_TOKEN: re.Pattern[str] = re.compile(
    r"\b(y|mo|d|h|mi|s|ts_str|dt|ts|tz|t_str|fmt_str|n|ntz|d1|d2)\b"
)

SPARK_PA_TYPE: dict[str, str] = {
    "timestamp": "timestamp[us, tz=UTC]",
    "timestamp_ntz": "timestamp[us]",
    "date": "date32[day]",
    "double": "double",
    "string": "string",
    "boolean": "bool",
    "bigint": "int64",
}
NTZ_NAMES: frozenset[str] = frozenset({"make_timestamp_ntz", "try_make_timestamp_ntz"})
FROZEN_SIGNATURE_NAMES: frozenset[str] = frozenset({"make_timestamp"})
TYPEOF_TOKEN: re.Pattern[str] = re.compile(r"\btypeof\b")
BARE_UNIT_NAMES: frozenset[str] = frozenset({"timestamp_add", "timestamp_diff"})
BARE_UNIT_CALL: re.Pattern[str] = re.compile(r"^(\w+)\(([A-Z]+),")
BARE_NULLARY_SQL: frozenset[str] = frozenset({"localtimestamp"})
LIT_ZONE_EXPRS: frozenset[str] = frozenset(
    {"F.timestamp_diff('HOUR', lit(datetime.datetime(2024, 1, 1)), col('ts'))"}
)


def _raw_cells() -> list[dict[str, Any]]:
    """Every oracle cell in scope: card names on the untouched-session recording."""
    payload: dict[str, Any] = json.loads(ORACLE_PATH.read_text())
    cells: list[dict[str, Any]] = payload["cells"]
    return [
        cell
        for cell in cells
        if cell.get("time_type_enabled") == "spark_default" and cell["name"] in CARD_NAMES
    ]


def _pinned_cells() -> list[dict[str, Any]]:
    """In-scope cells minus the out-of-scope, blocked and unrecordable sets."""
    kept: list[dict[str, Any]] = []
    for cell in _raw_cells():
        if TM_TOKEN.search(cell["expr"]):
            continue
        if TYPEOF_TOKEN.search(cell["expr"]):
            continue
        if cell["door"] == "sql" and cell["expr"] in D4_BLOCKED_SQL:
            continue
        if cell["name"] in NTZ_NAMES and cell.get("error_condition") == "UNSUPPORTED_TIME_TYPE":
            continue
        if cell["door"] == "sql" and cell["expr"] in BARE_NULLARY_SQL:
            continue
        if cell["door"] == "python" and cell["expr"] in LIT_ZONE_EXPRS:
            continue
        if (
            cell["door"] == "python"
            and cell["name"] in FROZEN_SIGNATURE_NAMES
            and "date=" in cell["expr"]
        ):
            continue
        kept.append(cell)
    return kept


PINNED_CELLS: list[dict[str, Any]] = _pinned_cells()


def _cell_id(cell: dict[str, Any]) -> str:
    """Stable pytest id: door, name, ansi, zone and the cell index."""
    return f"{cell['door']}-{cell['name']}-a{cell['ansi']}-z{cell['tz']}-{PINNED_CELLS.index(cell)}"


_LIVE: dict[str, Any] = {}


def _session(ansi: bool, tz: str) -> ReparkSession:
    """Session for one ansi/zone pair, reusing the live one while the key matches."""
    key = f"{ansi}/{tz}"
    cached: ReparkSession | None = _LIVE.get("session")
    if _LIVE.get("key") != key or cached is None or cached._inner is None:
        old: ReparkSession | None = _LIVE.get("session")
        if old is not None:
            old.stop()
            _LIVE.clear()
        session = (
            ReparkSession.builder.appName("fnp11a-temporal")
            .config("spark.sql.session.timeZone", tz)
            .config("spark.sql.ansi.enabled", "true" if ansi else "false")
            .getOrCreate()
        )
        session.createDataFrame(_frame_rows(), _frame_schema()).createOrReplaceTempView(FRAME_VIEW)
        _LIVE["key"] = key
        _LIVE["session"] = session
    return _LIVE["session"]


def _frame_schema() -> str:
    """DDL of the oracle frame without the TIME column (D-9 owns TIME)."""
    return (
        "y INT, mo INT, d INT, h INT, mi INT, s DECIMAL(16,6), ts_str STRING,"
        " dt DATE, ts TIMESTAMP, tz STRING, t_str STRING, fmt_str STRING, n INT,"
        " ntz TIMESTAMP_NTZ, d1 DATE, d2 DATE"
    )


def _frame_rows() -> list[tuple[Any, ...]]:
    """The two oracle frame rows: the value row and the all-NULL row."""
    full: tuple[Any, ...] = (
        2014,
        12,
        28,
        6,
        30,
        Decimal("45.887"),
        "2016-12-31 00:12:00",
        datetime.date(2024, 1, 31),
        datetime.datetime(2024, 3, 10, 1, 30, 0),
        "CET",
        "12:34:56.789",
        "31/12/2016 10:30",
        5,
        datetime.datetime(2024, 3, 10, 1, 30, 0),
        datetime.date(2024, 3, 31),
        datetime.date(2024, 2, 29),
    )
    return [full, tuple(None for _ in full)]


def _eval_namespace() -> dict[str, Any]:
    """Namespace the recorded Python-door expressions evaluate against."""
    return {"F": F, "col": F.col, "lit": F.lit, "datetime": datetime}


def _box_local_to_instant(value: datetime.datetime) -> datetime.datetime:
    """Localize a fixture LTZ wall clock in the recording box zone (D-8)."""
    return value.replace(tzinfo=ZoneInfo(BOX_ZONE)).astimezone(datetime.UTC)


def _expected_scalar(encoded: Any, spark_type: str, tz: str) -> Any:
    """Decode one fixture JSON value to the instant or wall clock RePark must answer."""
    if encoded is None:
        return None
    if isinstance(encoded, dict) and "datetime" in encoded:
        moment = datetime.datetime.fromisoformat(encoded["datetime"])
        if spark_type == "timestamp_ntz":
            return moment
        return _box_local_to_instant(moment)
    if isinstance(encoded, dict) and "date" in encoded:
        return datetime.date.fromisoformat(encoded["date"])
    if isinstance(encoded, dict) and "decimal" in encoded:
        return Decimal(encoded["decimal"])
    return encoded


def _actual_scalar(value: Any, spark_type: str, tz: str) -> Any:
    """Normalize one RePark value to the comparison domain of the expected value."""
    if isinstance(value, datetime.datetime) and spark_type == "timestamp":
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.UTC)
        return value.astimezone(datetime.UTC)
    return value


def _assert_error_cell(cell: dict[str, Any], failure: Exception) -> None:
    """A raising cell carries Spark's condition and Spark's message prefix (D-2)."""
    text = str(failure)
    condition = cell["error_condition"] or ""
    assert f"[{condition}]" in text
    prefix = (cell["message"] or "").split(".")[0]
    assert prefix in text


def _folded_literal(cell: dict[str, Any]) -> bool:
    """True when Spark constant-folded a literal-only expression to non-nullable."""
    if cell.get("nullable") is not False:
        return False
    if cell["door"] == "sql":
        return not _uses_frame(cell["expr"])
    return FRAME_TOKEN.search(cell["expr"]) is None


def _assert_value_cell(cell: dict[str, Any], table: pa.Table) -> None:
    """A value cell matches Spark's type, nullability and rows on the Arrow path."""
    field = table.schema.field(0)
    if not _folded_literal(cell):
        assert field.nullable == cell["nullable"]
    if cell["name"] == "localtimestamp" and "rows" in cell:
        if cell["type"] == "boolean":
            assert str(field.type) == "bool"
            assert table.column(0).to_pylist() == cell["rows"]
            return
        assert str(field.type) == SPARK_PA_TYPE["timestamp_ntz"]
        assert table.column(0).to_pylist()[0] is not None
        return
    assert str(field.type) == SPARK_PA_TYPE[cell["type"]]
    expected = [_expected_scalar(item, cell["type"], cell["tz"]) for item in cell["rows"]]
    actual = [
        _actual_scalar(item, cell["type"], cell["tz"]) for item in table.column(0).to_pylist()
    ]
    assert actual == expected


def _assert_string_rows(cell: dict[str, Any], table: pa.Table) -> None:
    """An interval cell matches Spark's CAST text, type string and nullability (D-5)."""
    if not _folded_literal(cell):
        assert table.schema.field(0).nullable == cell["nullable"]
    assert list(table.column(0).to_pylist()) == cell["rows_as_string"]


def _uses_frame(expr: str) -> bool:
    """True when the SQL expression names a frame column (else it runs frameless)."""
    return FRAME_TOKEN.search(expr) is not None


def _sql_table(session: ReparkSession, expr: str) -> pa.Table:
    """Run one SQL-door expression with the frame only when it names one."""
    if _uses_frame(expr):
        return session.sql(f"SELECT {expr} FROM {FRAME_VIEW}").toArrow()
    return session.sql(f"SELECT {expr}").toArrow()


def _run_cell(cell: dict[str, Any]) -> None:
    """Run one fixture cell on its door and assert the Spark answer."""
    session = _session(bool(cell["ansi"]), cell["tz"])
    if cell["door"] == "sql" and cell["name"] in BARE_UNIT_NAMES:
        cell = {**cell, "expr": BARE_UNIT_CALL.sub(r"\1('\2',", cell["expr"])}
    if "error_condition" in cell and cell["error_condition"] not in ("NOT_IMPLEMENTED",):
        try:
            if cell["door"] == "sql":
                table = _sql_table(session, cell["expr"])
            else:
                column = eval(cell["expr"], dict(_eval_namespace()))
                frame = session.table(FRAME_VIEW)
                table = frame.select(column.alias("v")).toArrow()
        except Exception as failure:
            _assert_error_cell(cell, failure)
        else:
            raise AssertionError(f"expected {cell['error_condition']}, got {table}")
        return
    if "rows_as_string" in cell:
        _assert_interval_cell(session, cell)
        return
    if "rows" not in cell:
        _assert_interval_shape(session, cell)
        return
    if cell["door"] == "sql":
        table = _sql_table(session, cell["expr"])
    else:
        column = eval(cell["expr"], dict(_eval_namespace()))
        frame = session.table(FRAME_VIEW)
        table = frame.select(column.alias("v")).toArrow()
    _assert_value_cell(cell, table)


def _assert_interval_shape(session: ReparkSession, cell: dict[str, Any]) -> None:
    """A fallback-failed interval cell keeps its type, nullability and NULL contract."""
    table = _sql_table(session, cell["expr"])
    field = table.schema.field(0)
    assert field.nullable == cell["nullable"]
    assert str(field.type) == "month_day_nano_interval"
    values = table.column(0).to_pylist()
    if cell["expr"] == "try_make_interval(2147483647, 12)":
        assert values == [None]
    else:
        assert values[0] is not None


def _assert_interval_cell(session: ReparkSession, cell: dict[str, Any]) -> None:
    """An interval cell answers Spark's CAST text on its door (D-5)."""
    if cell["door"] == "sql":
        if _uses_frame(cell["expr"]):
            text = f"SELECT CAST(({cell['expr']}) AS STRING) FROM {FRAME_VIEW}"
        else:
            text = f"SELECT CAST(({cell['expr']}) AS STRING)"
        table = session.sql(text).toArrow()
    else:
        column = eval(cell["expr"], dict(_eval_namespace()))
        frame = session.table(FRAME_VIEW)
        table = frame.select(column.cast("string").alias("v")).toArrow()
    _assert_string_rows(cell, table)


@pytest.fixture(scope="session", autouse=True)
def _stop_session_after_suite() -> Any:
    """Stop the cached session when the module run finishes."""
    yield
    old: ReparkSession | None = _LIVE.get("session")
    if old is not None:
        old.stop()
        _LIVE.clear()


def test_facade_signatures_match_spark() -> None:
    """pins: fnp-11a/C-001."""
    payload: dict[str, Any] = json.loads(ORACLE_PATH.read_text())
    signatures: dict[str, str | None] = payload["signatures"]
    for name in sorted(CARD_NAMES - FROZEN_SIGNATURE_NAMES):
        recorded = signatures[name]
        assert recorded is not None
        parameters = list(inspect.signature(getattr(F, name)).parameters.values())
        assert [item.name for item in parameters] == _signature_names(recorded)
        assert [_signature_default(item) for item in parameters] == _signature_defaults(recorded)


def _signature_names(recorded: str) -> list[str]:
    """Parameter names of the recorded PySpark signature."""
    inner = recorded.split("(", 1)[1].rsplit(")", 1)[0]
    return [part.split(":")[0].strip() for part in inner.split(",") if part.strip()]


def _signature_defaults(recorded: str) -> list[str]:
    """Normalized defaults of the recorded PySpark signature."""
    inner = recorded.split("(", 1)[1].rsplit(")", 1)[0]
    defaults: list[str] = []
    for part in inner.split(","):
        if not part.strip():
            continue
        if "=" in part:
            defaults.append(part.split("=", 1)[1].strip().split(" ")[0])
        else:
            defaults.append("<required>")
    return defaults


def _signature_default(parameter: inspect.Parameter) -> str:
    """Normalize one facade default to the recorded spelling."""
    if parameter.default is inspect.Parameter.empty:
        return "<required>"
    return repr(parameter.default).split(" ")[0]


@pytest.mark.parametrize("cell", PINNED_CELLS, ids=_cell_id)
def test_door_cell_matches_oracle(cell: dict[str, Any]) -> None:
    """pins: fnp-11a/C-002, C-003, C-004, C-005."""
    _run_cell(cell)


def test_timestampdiff_ntz_pair_answers_days() -> None:
    """pins: fnp-11a/C-003."""
    session = _session(True, "UTC")
    end = "make_timestamp_ntz(2024, 3, 11, 1, 0, 0)"
    text = f"SELECT timestampdiff('DAY', ntz, {end}) FROM {FRAME_VIEW}"
    table = session.sql(text).toArrow().rename_columns(["v"])
    assert table.column("v").to_pylist() == [0, None]
    assert str(table.schema.field("v").type) == "int64"


def test_timestampadd_string_unit_sql_matches_oracle() -> None:
    """pins: fnp-11a/C-003."""
    session = _session(True, "UTC")
    table = session.sql(f"SELECT timestampadd('DAY', 1, ts) FROM {FRAME_VIEW}").toArrow()
    assert str(table.schema.field(0).type) == "timestamp[us, tz=UTC]"
    actual = [_actual_scalar(item, "timestamp", "UTC") for item in table.column(0).to_pylist()]
    assert actual[0] == _box_local_to_instant(datetime.datetime(2024, 3, 10, 21, 30, 0))
    assert actual[1] is None


def test_timestampdiff_string_unit_sql_matches_oracle() -> None:
    """pins: fnp-11a/C-003."""
    session = _session(True, "UTC")
    text = f"SELECT timestampdiff('HOUR', TIMESTAMP'2024-03-11 01:00:00', ts) FROM {FRAME_VIEW}"
    table = session.sql(text).toArrow()
    assert str(table.schema.field(0).type) == "int64"
    assert table.column(0).to_pylist() == [-23, None]


def test_string_units_match_in_any_case() -> None:
    """pins: fnp-11a/C-003."""
    session = _session(True, "America/New_York")
    for unit in ("day", "Day", "DAY"):
        table = session.sql(
            f"SELECT timestampadd('{unit}', 1, TIMESTAMP'2024-03-10 01:30:00') AS v"
        ).toArrow()
        assert str(table.schema.field("v").type) == "timestamp[us, tz=UTC]"
        instant = table.column("v").to_pylist()[0].astimezone(datetime.UTC)
        assert instant == datetime.datetime(2024, 3, 11, 5, 30, tzinfo=datetime.UTC)
    for unit in ("hour", "Hour", "HOUR"):
        table = session.sql(
            "SELECT timestampdiff("
            f"'{unit}', TIMESTAMP'2024-03-10 01:30:00', TIMESTAMP'2024-03-11 01:30:00') AS v"
        ).toArrow()
        assert table.column("v").to_pylist() == [24]


def test_ntz_pair_ignores_session_zone() -> None:
    """pins: fnp-11a/C-003."""
    for tz in ("UTC", "America/New_York"):
        session = _session(True, tz)
        added = session.sql(
            "SELECT timestampadd('DAY', 1, make_timestamp_ntz(2024, 3, 10, 1, 30, 0)) AS v"
        ).toArrow()
        assert str(added.schema.field("v").type) == "timestamp[us]"
        assert added.column("v").to_pylist() == [datetime.datetime(2024, 3, 11, 1, 30, 0)]
        diffed = session.sql(
            "SELECT timestampdiff('DAY', make_timestamp_ntz(2024, 3, 10, 1, 30, 0), "
            "make_timestamp_ntz(2024, 3, 11, 1, 30, 0)) AS v"
        ).toArrow()
        assert diffed.column("v").to_pylist() == [1]


LIT_ZONE_ROWS: tuple[tuple[bool, str, int, int], ...] = (
    (True, "UTC", 1652, 1657),
    (False, "UTC", 1652, 1657),
    (True, "America/New_York", 1657, 1662),
    (False, "America/New_York", 1657, 1662),
)


@pytest.mark.parametrize("ansi,tz,spark_value,repark_value", LIT_ZONE_ROWS)
def test_timestampdiff_python_lit_keeps_repark_lit_zone(
    ansi: bool, tz: str, spark_value: int, repark_value: int
) -> None:
    """pins: fnp-11a/C-002."""
    session = _session(ansi, tz)
    frame = session.table(FRAME_VIEW)
    column = F.timestamp_diff("HOUR", F.lit(datetime.datetime(2024, 1, 1)), F.col("ts"))
    table = frame.select(column.alias("v")).toArrow()
    assert table.column("v").to_pylist() == [repark_value, None]
    assert repark_value != spark_value


def test_folded_literals_stay_nullable() -> None:
    """pins: fnp-11a/C-002 (EX-FN-24)."""
    session = _session(True, "UTC")
    timestamp_table = session.sql("SELECT make_timestamp(2019, 6, 30, 23, 59, 60)").toArrow()
    assert timestamp_table.schema.field(0).nullable is True
    months_table = session.sql(
        "SELECT months_between(DATE'2024-02-29', DATE'2024-01-31')"
    ).toArrow()
    assert months_table.schema.field(0).nullable is True
    interval_table = session.sql("SELECT CAST(make_ym_interval() AS STRING)").toArrow()
    assert interval_table.schema.field(0).nullable is True


def test_bare_timestampadd_unit_refuses() -> None:
    """pins: fnp-11a/C-003 (EX-FN-27)."""
    session = _session(True, "UTC")
    with pytest.raises(AnalysisException, match="No field named year"):
        session.sql("SELECT timestampadd(YEAR, 1, TIMESTAMP'2024-01-01 00:00:00')")
    with pytest.raises(AnalysisException, match="No field named year"):
        session.sql(
            "SELECT timestampdiff(YEAR, TIMESTAMP'2020-01-15 12:00:00', "
            "TIMESTAMP'2024-01-15 12:00:00')"
        )


def test_bare_localtimestamp_answers_call() -> None:
    """pins: fnp-11a/C-004 (EX-FN-25)."""
    session = _session(True, "UTC")
    table = session.sql("SELECT localtimestamp").toArrow()
    assert str(table.schema.field(0).type) == SPARK_PA_TYPE["timestamp_ntz"]
    assert table.column(0).to_pylist()[0] is not None


def test_make_timestamp_keeps_its_frozen_signature() -> None:
    """pins: fnp-11a/C-001; fnp-11b/C-001 (Q-15a-3 widening)."""
    parameters = list(inspect.signature(F.make_timestamp).parameters.values())
    assert [item.name for item in parameters] == [
        "years",
        "months",
        "days",
        "hours",
        "mins",
        "secs",
        "timezone",
        "date",
        "time",
    ]
    assert [_signature_default(item) for item in parameters] == ["None"] * 9


def test_make_timestamp_date_time_keywords_answer() -> None:
    """pins: fnp-11a/C-002; fnp-11b/C-002 (Q-15a-3 widening retires the EX-FN-28 refusal)."""
    session = ReparkSession.builder.appName("pytest-fnp11b-widened").getOrCreate()
    frame = session.createDataFrame([(datetime.date(2014, 12, 28),)], "dt DATE")
    rows = frame.select(
        F.make_timestamp(date=F.col("dt"), time=F.lit("06:30:45.887")).alias("ts")
    ).collect()
    assert len(rows) == 1 and rows[0]["ts"] is not None
    answered = session.sql(
        "SELECT make_timestamp(DATE'2014-12-28', TIME'06:30:45.887') AS ts"
    ).collect()
    assert len(answered) == 1 and answered[0]["ts"] is not None
