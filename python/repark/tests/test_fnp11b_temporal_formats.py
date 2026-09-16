"""FNP-11B datetime formats, the TIME family, BL-13 and BL-14 pins.

Red-first oracle pins over fnp11b_spark_oracle.json on both doors, both ANSI
settings and both zones. pins: fnp-11b/C-002, C-003, C-004, C-005, C-006
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
from repark.spark import functions as F  # noqa: N812

CARD_NAMES: frozenset[str] = frozenset(
    {
        "to_date",
        "to_timestamp",
        "unix_timestamp",
        "to_timestamp_ltz",
        "to_timestamp_ntz",
        "try_to_timestamp",
        "make_time",
        "to_time",
        "time_diff",
        "time_trunc",
        "current_time",
        "to_char",
        "to_varchar",
        "to_number",
        "to_binary",
        "make_timestamp",
    }
)
SIG_NAMES: frozenset[str] = CARD_NAMES
TRY_AVG_NAMES: frozenset[str] = frozenset({"try_avg", "avg", "date_plus_interval"})

ORACLE_PATH: Path = Path(__file__).with_name("fnp11b_spark_oracle.json")
FRAME_VIEW: str = "fnp11b_frame"
FRAME_TM_VIEW: str = "fnp11b_tm_frame"
O245_VIEW: str = "fnp11b_o245_frame"
BOX_ZONE: str = "America/New_York"
TM_TOKEN: re.Pattern[str] = re.compile(r"\btm\b")
TYPEOF_CALL: re.Pattern[str] = re.compile(r"^typeof\((.*)\)$")
FRAME_TOKEN: re.Pattern[str] = re.compile(
    r"\b(y|mo|d|h|mi|s|ts_str|dt|ts|tz|t_str|fmt_str|n|ntz|d1|d2|tm)\b"
)

SPARK_PA_TYPE: dict[str, str] = {
    "timestamp": "timestamp[us, tz=UTC]",
    "timestamp_ntz": "timestamp[us]",
    "date": "date32[day]",
    "double": "double",
    "string": "string",
    "boolean": "bool",
    "bigint": "int64",
    "binary": "binary",
    "time(6)": "time64[ns]",
    "time(3)": "time64[ns]",
    "time(0)": "time64[ns]",
    "interval day to second": "month_day_nano_interval",
    "interval year to month": "month_day_nano_interval",
}


def _raw_cells() -> list[dict[str, Any]]:
    """Every oracle cell in scope: card names on the default TIME recording."""
    payload: dict[str, Any] = json.loads(ORACLE_PATH.read_text())
    return list(payload["cells"])


PINNED_CELLS: list[dict[str, Any]] = _raw_cells()


def _cell_id(cell: dict[str, Any]) -> str:
    """Stable pytest id: block, door, name, ansi, zone and the cell index."""
    zone = cell.get("tz", "na")
    return f"{cell['door']}-{cell['name']}-a{cell['ansi']}-z{zone}-{PINNED_CELLS.index(cell)}"


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
            ReparkSession.builder.appName("fnp11b-temporal-formats")
            .config("spark.sql.session.timeZone", tz)
            .config("spark.sql.ansi.enabled", "true" if ansi else "false")
            .getOrCreate()
        )
        session.createDataFrame(_frame_rows(), _frame_schema()).createOrReplaceTempView(FRAME_VIEW)
        frame = session.table(FRAME_VIEW)
        frame.select(
            "*",
            F.expr("CASE WHEN y IS NULL THEN CAST(NULL AS TIME) ELSE TIME'12:34:56.789' END").alias(
                "tm"
            ),
        ).createOrReplaceTempView(FRAME_TM_VIEW)
        session.createDataFrame(_o245_rows(), _o245_schema()).createOrReplaceTempView(O245_VIEW)
        _LIVE["key"] = key
        _LIVE["session"] = session
    return _LIVE["session"]


def _frame_schema() -> str:
    """DDL of the oracle frame without the TIME column."""
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


def _o245_schema() -> str:
    """DDL of the math-oracle frame columns the to_char family reads."""
    return "d DOUBLE, dec DECIMAL(10,4), num STRING, ts TIMESTAMP"


def _o245_rows() -> list[tuple[Any, ...]]:
    """The three math-oracle frame rows the to_char family reads."""
    return [
        (2.5, Decimal("12345.6789"), "100", datetime.datetime(2024, 1, 1, 10, 7, 30)),
        (None, None, "zz", datetime.datetime(2024, 1, 1, 10, 12, 0)),
        (0.125, Decimal("-0.5"), "-17", datetime.datetime(2024, 1, 1, 10, 31, 0)),
    ]


def _eval_namespace() -> dict[str, Any]:
    """Namespace the recorded Python-door expressions evaluate against."""
    return {"F": F, "col": F.col, "lit": F.lit, "datetime": datetime}


def _box_local_to_instant(value: datetime.datetime) -> datetime.datetime:
    """Localize a fixture LTZ wall clock in the recording box zone."""
    return value.replace(tzinfo=ZoneInfo(BOX_ZONE)).astimezone(datetime.UTC)


def _expected_scalar(encoded: Any, spark_type: str) -> Any:
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
    if isinstance(encoded, dict) and "time" in encoded:
        return datetime.time.fromisoformat(encoded["time"])
    if isinstance(encoded, dict) and "timedelta_us" in encoded:
        total: int = int(encoded["timedelta_us"])
        return (0, total // 86400000000, (total % 86400000000) * 1000)
    if isinstance(encoded, dict) and "bytes_hex" in encoded:
        return bytes.fromhex(str(encoded["bytes_hex"]))
    if isinstance(encoded, dict) and "decimal" in encoded:
        return Decimal(encoded["decimal"])
    return encoded


def _actual_scalar(value: Any, spark_type: str) -> Any:
    """Normalize one RePark value to the comparison domain of the expected value."""
    if isinstance(value, datetime.datetime) and spark_type == "timestamp":
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.UTC)
        return value.astimezone(datetime.UTC)
    if isinstance(value, pa.MonthDayNano):
        return (value.months, value.days, value.nanoseconds)
    return value


def _assert_error_cell(cell: dict[str, Any], failure: Exception) -> None:
    """A raising cell carries Spark's condition and Spark's message prefix."""
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
    if cell["name"] == "current_time" and cell.get("type", "").startswith("time"):
        assert str(field.type) == SPARK_PA_TYPE[cell["type"]]
        assert table.column(0).to_pylist()[0] is not None
        return
    if not _folded_literal(cell):
        assert field.nullable == cell["nullable"]
    assert str(field.type) == SPARK_PA_TYPE[cell["type"]]
    expected = [_expected_scalar(item, cell["type"]) for item in cell["rows"]]
    actual = [_actual_scalar(item, cell["type"]) for item in table.column(0).to_pylist()]
    assert actual == expected


def _assert_block2_cell(cell: dict[str, Any], table: pa.Table) -> None:
    """A math-block cell matches every recorded column type, nullability and row."""
    assert len(table.schema) == len(cell["columns"])
    for field, column in zip(table.schema, cell["columns"], strict=True):
        assert str(field.type) == SPARK_PA_TYPE[column["type"]]
        if not _folded_block2_literal(cell):
            assert field.nullable == column["nullable"]
    assert table.num_rows == len(cell["rows"])
    for actual_row, expected_row in zip(table.to_pylist(), cell["rows"], strict=True):
        assert [_decode_block2(actual_row[name]) for name in table.schema.names] == [
            _decode_block2(item) for item in expected_row
        ]


def _folded_block2_literal(cell: dict[str, Any]) -> bool:
    """True when a math-block Python cell reads no frame column."""
    if cell["door"] == "sql":
        return False
    return FRAME_TOKEN.search(cell["expr"]) is None


def _decode_block2(value: Any) -> Any:
    """Decode one math-block fixture or engine value to comparable bytes or scalar."""
    if isinstance(value, dict) and "bytes_hex" in value:
        return bytes.fromhex(str(value["bytes_hex"]))
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    return value


def _uses_frame(expr: str) -> bool:
    """True when the SQL expression names a frame column (else it runs frameless)."""
    return FRAME_TOKEN.search(expr) is not None


POISONED_NESTING: frozenset[str] = frozenset(
    {
        "F.current_time().isNotNull()",
        "F.typeof(F.current_time(3))",
        "F.typeof(F.current_time())",
    }
)


def _frame_for_python(cell: dict[str, Any]) -> str:
    """Frame view for a Python-door cell: the TIME frame for tm reads and poisoned nests."""
    if "error_condition" in cell:
        if cell["error_condition"].startswith("UNRESOLVED"):
            return FRAME_VIEW
        if TM_TOKEN.search(cell["expr"]) is not None or cell["expr"] in POISONED_NESTING:
            return FRAME_TM_VIEW
        return FRAME_VIEW
    if TM_TOKEN.search(cell["expr"]):
        return FRAME_TM_VIEW
    return FRAME_VIEW


def _sql_table(session: ReparkSession, expr: str) -> pa.Table:
    """Run one SQL-door block-1 expression with the frame only when it names one."""
    if _uses_frame(expr):
        view = FRAME_TM_VIEW if TM_TOKEN.search(expr) else FRAME_VIEW
        return session.sql(f"SELECT {expr} FROM {view}").toArrow()
    if "FROM VALUES" in expr:
        return session.sql(f"SELECT {expr}").toArrow()
    return session.sql(f"SELECT {expr}").toArrow()


def _assert_typeof_translation(cell: dict[str, Any], session: ReparkSession) -> None:
    """A typeof(TIME) SQL cell answers a non-null time64 for its inner expression."""
    inner = TYPEOF_CALL.match(cell["expr"])
    assert inner is not None
    table = session.sql(f"SELECT {inner.group(1)}").toArrow()
    assert str(table.schema.field(0).type) == "time64[ns]"
    values = table.column(0).to_pylist()
    assert len(values) == 1
    assert values[0] is not None


def _strip_recording_comment(expr: str) -> str:
    """Drop the recorder's trailing ``--`` so the expression nests inside CAST."""
    stripped = expr.rstrip()
    if stripped.endswith("--"):
        return stripped[: -len("--")]
    return expr


def _assert_interval_rows_as_string(session: ReparkSession, cell: dict[str, Any]) -> None:
    """A collect-failing interval cell answers Spark's CAST text on its door."""
    if cell["door"] == "sql":
        inner = _strip_recording_comment(cell["expr"])
        table = session.sql(f"SELECT CAST((SELECT {inner}) AS STRING)").toArrow()
    else:
        column = eval(cell["expr"], dict(_eval_namespace()))
        frame = session.table(FRAME_VIEW)
        table = frame.select(column.cast("string").alias("v")).toArrow()
    actual = list(table.column(0).to_pylist())
    assert actual == cell["rows_as_string"]


def _run_block1_cell(cell: dict[str, Any]) -> None:
    """Run one datetime-block fixture cell on its door and assert the Spark answer."""
    session = _session(bool(cell["ansi"]), cell["tz"])
    if cell["door"] == "sql" and "error_condition" not in cell and TYPEOF_CALL.match(cell["expr"]):
        _assert_typeof_translation(cell, session)
        return
    if "rows_as_string" in cell:
        _assert_interval_rows_as_string(session, cell)
        return
    if "error_condition" in cell:
        try:
            if cell["door"] == "sql":
                table = _sql_table(session, cell["expr"])
            else:
                column = eval(cell["expr"], dict(_eval_namespace()))
                frame = session.table(_frame_for_python(cell))
                table = frame.select(column.alias("v")).toArrow()
        except Exception as failure:
            _assert_error_cell(cell, failure)
        else:
            raise AssertionError(f"expected {cell['error_condition']}, got {table}")
        return
    if cell["door"] == "sql":
        table = _sql_table(session, cell["expr"])
    else:
        column = eval(cell["expr"], dict(_eval_namespace()))
        frame = session.table(_frame_for_python(cell))
        table = frame.select(column.alias("v")).toArrow()
    _assert_value_cell(cell, table)


def _run_block2_cell(cell: dict[str, Any]) -> None:
    """Run one math-block fixture cell on its door and assert the Spark answer."""
    session = _session(bool(cell["ansi"]), "UTC")
    if "error_condition" in cell:
        try:
            if cell["door"] == "sql":
                table = session.sql(cell["expr"].replace("FRAME", O245_VIEW)).toArrow()
            else:
                column = eval(cell["expr"], dict(_eval_namespace()))
                frame = session.table(O245_VIEW)
                table = frame.select(column.alias("v")).toArrow()
        except Exception as failure:
            _assert_error_cell(cell, failure)
        else:
            raise AssertionError(f"expected {cell['error_condition']}, got {table}")
        return
    if cell["door"] == "sql":
        table = session.sql(cell["expr"].replace("FRAME", O245_VIEW)).toArrow()
    else:
        column = eval(cell["expr"], dict(_eval_namespace()))
        frame = session.table(O245_VIEW)
        table = frame.select(column).toArrow()
    _assert_block2_cell(cell, table)


def _run_cell(cell: dict[str, Any]) -> None:
    """Run one fixture cell on its door and assert the Spark answer."""
    if "tz" in cell:
        _run_block1_cell(cell)
    else:
        _run_block2_cell(cell)


@pytest.fixture(scope="session", autouse=True)
def _stop_session_after_suite() -> Any:
    """Stop the cached session when the module run finishes."""
    yield
    old: ReparkSession | None = _LIVE.get("session")
    if old is not None:
        old.stop()
        _LIVE.clear()


def test_facade_signatures_match_spark() -> None:
    """pins: fnp-11b/C-001."""
    payload: dict[str, Any] = json.loads(ORACLE_PATH.read_text())
    signatures: dict[str, str | None] = payload["signatures"]
    for name in sorted(SIG_NAMES):
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


STRICT_XFAIL_CELLS: dict[int, str] = {
    0: "R-17a-22 divergence, registry FNP-11B-UNRESOLVED-1, owner ERR-UNRESOLVED-COL-1",
    1: "R-17a-22 divergence, registry FNP-11B-UNRESOLVED-1, owner ERR-UNRESOLVED-COL-1",
    2: "R-17a-22 divergence, registry FNP-11B-UNRESOLVED-1, owner ERR-UNRESOLVED-COL-1",
    3: "R-17a-22 divergence, registry FNP-11B-UNRESOLVED-1, owner ERR-UNRESOLVED-COL-1",
    4: "R-17a-22 divergence, registry FNP-11B-UNRESOLVED-1, owner ERR-UNRESOLVED-COL-1",
    187: "R-17a-16 divergence, registry FNP-11B-YM-AVG-1, owner run 17b types slice",
    188: "R-17a-16 divergence, registry FNP-11B-YM-AVG-1, owner run 17b types slice",
    191: "NULL INTERVAL DAY seam, registry FNP-11B-NULL-AVG-1, owner run 17c",
    192: "NULL INTERVAL DAY seam, registry FNP-11B-NULL-AVG-1, owner run 17c",
    197: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
    198: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
    199: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
    200: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
    203: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
    204: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
    205: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
    206: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
    207: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
    208: "BL-14 seam, registry FNP-11B-BL14-1, owner run 17c",
}
"""Strict-xfail pins for the ruled residuals: each names its ruling, its
registry row and its owning slice. Strict is the point — when the owner lands
the fix the pin goes XPASS and fails, which retires the residual."""


def _cell_param(cell: dict[str, Any]) -> Any:
    """One pytest param per fixture cell, xfail-marked for ruled residuals."""
    index = PINNED_CELLS.index(cell)
    reason = STRICT_XFAIL_CELLS.get(index)
    if reason is None:
        return pytest.param(cell, id=_cell_id(cell))
    mark = pytest.mark.xfail(strict=True, reason=reason)
    return pytest.param(cell, marks=mark, id=_cell_id(cell))


@pytest.mark.parametrize("cell", [_cell_param(cell) for cell in PINNED_CELLS])
def test_door_cell_matches_oracle(cell: dict[str, Any]) -> None:
    """pins: fnp-11b/C-002, C-003, C-004, C-005, C-006."""
    _run_cell(cell)


def test_current_time_beside_aggregate_is_global_agg() -> None:
    """PYPERF-001: ``current_time()`` beside an aggregate is a global aggregation."""
    session = _session(True, "UTC")
    frame = session.table(FRAME_VIEW)
    table = frame.select(F.sum("n").alias("total"), F.current_time().alias("now")).toArrow()
    assert table.num_rows == 1
    assert table.column(1).to_pylist()[0] is not None


def test_try_avg_sliding_frame_retracts_leaving_rows() -> None:
    """L-003: a sliding try_avg answers the live frame, not a sticky overflow."""
    session = _session(True, "UTC")
    table = session.sql(
        "SELECT try_avg(v) OVER (ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS w"
        " FROM (VALUES (INTERVAL '1' DAY), (INTERVAL '2' DAY)) AS x(v)"
    ).toArrow()
    assert table.column(0).to_pylist() == [
        pa.MonthDayNano((0, 1, 0)),
        pa.MonthDayNano((0, 1, 43_200_000_000_000)),
    ]


def test_bare_format_string_is_a_column_reference() -> None:
    """PYPERF-003: a bare format string names a column, per live PySpark 4.1.2."""
    session = _session(True, "UTC")
    frame = session.table(FRAME_VIEW)
    with pytest.raises(Exception, match="dd/MM/yyyy HH:mm"):
        frame.select(F.to_timestamp_ltz("fmt_str", "dd/MM/yyyy HH:mm").alias("v")).toArrow()
    with pytest.raises(Exception, match="9999"):
        frame.select(F.to_char("n", "9999.99").alias("v")).toArrow()
    with pytest.raises(Exception, match="yyyy-MM-dd HH:mm:ss"):
        frame.select(F.try_to_timestamp("ts_str", "yyyy-MM-dd HH:mm:ss").alias("v")).toArrow()


def test_cast_to_time_refuses_on_both_doors() -> None:
    """L-004: the same CAST-to-TIME text refuses UNSUPPORTED_TIME_TYPE at build on both doors."""
    session = _session(True, "UTC")
    with pytest.raises(Exception, match="UNSUPPORTED_TIME_TYPE"):
        session.sql("SELECT CAST(NULL AS TIME)")
    with pytest.raises(Exception, match="UNSUPPORTED_TIME_TYPE"):
        F.expr("CAST(NULL AS TIME)")


@pytest.mark.parametrize("ansi", [True, False])
@pytest.mark.parametrize("zone", ["UTC", "America/New_York"])
def test_to_timestamp_ntz_date_only_string_parses_midnight(ansi: bool, zone: str) -> None:
    """L-002: a date-only string is never an offset; live PySpark 4.1.2 answers midnight."""
    session = _session(ansi, zone)
    table = session.sql("SELECT to_timestamp_ntz('2020-01-01')").toArrow()
    assert table.column(0).to_pylist() == [datetime.datetime(2020, 1, 1)]
    frame = session.range(1)
    column = F.to_timestamp_ntz(F.lit("2016-12-31"))
    assert frame.select(column.alias("v")).toArrow().column(0).to_pylist() == [
        datetime.datetime(2016, 12, 31)
    ]
