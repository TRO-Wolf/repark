"""FNP-11A round-2 finding pins against the run-15a Spark oracle.

SQL cells use Spark spellings (bare units, TIMESTAMP_NTZ literals, typeof);
the runner translates them to engine spellings: quoted units,
make_timestamp_ntz(...) constructors, typeof dropped in favour of the Arrow
schema assertion. The kernel batch-efficiency rework behind these pins is
covered by this module and the R1 suite staying green.
pins: fnp-11a/C-016
"""

from __future__ import annotations

import datetime
import json
import re
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812

R2_PATH: Path = Path(__file__).with_name("fnp11a_r2_spark_oracle.json")
BOX_ZONE: str = "America/New_York"
NTZ_LITERAL: re.Pattern[str] = re.compile(
    r"TIMESTAMP_NTZ'(\d{4})-(\d{2})-(\d{2})[ T](\d{2}):(\d{2}):(\d{2})(?:\.(\d+))?'"
)
BARE_UNIT: re.Pattern[str] = re.compile(r"\btimestamp(add|diff)\(\s*([A-Za-z]+)\s*,")
SPARK_PA_TYPE: dict[str, str] = {
    "timestamp": "timestamp[us, tz=UTC]",
    "timestamp_ntz": "timestamp[us]",
    "bigint": "int64",
    "string": "string",
}


def _all_cells() -> list[dict[str, Any]]:
    """Every recorded round-2 cell."""
    payload: dict[str, Any] = json.loads(R2_PATH.read_text())
    return payload["cells"]


ALL_CELLS: list[dict[str, Any]] = _all_cells()


def _cells(finding: str) -> list[dict[str, Any]]:
    """Cells settling one review finding, in recording order."""
    return [cell for cell in ALL_CELLS if cell["finding"] == finding]


def _cell_id(cell: dict[str, Any]) -> str:
    """Stable pytest id: finding, door, ansi, zone and the cell index."""
    return f"{cell['finding']}-{cell['door']}-a{cell['ansi']}-z{cell['tz']}-{ALL_CELLS.index(cell)}"


def _ntz_call(match: re.Match[str]) -> str:
    """Rewrite one TIMESTAMP_NTZ literal as a facade-built constructor call."""
    year, month, day, hour, minute, second, frac = match.groups()
    secs = f"{int(second)}.{frac}" if frac else second
    parts = ", ".join(
        [str(int(year)), str(int(month)), str(int(day)), str(int(hour)), str(int(minute)), secs]
    )
    return f"make_timestamp_ntz({parts})"


def _drop_typeof(text: str) -> tuple[str, bool]:
    """Drop a trailing `, typeof(<balanced>) AS t` projection."""
    marker = ", typeof("
    start = text.rfind(marker)
    if start < 0:
        return text, False
    depth = 0
    for pos in range(start + len(", "), len(text)):
        char = text[pos]
        if char == "(":
            depth += 1
        elif char == ")":
            depth -= 1
            if depth == 0:
                tail = text[pos + 1 :].strip()
                assert tail.upper().startswith("AS T"), tail
                return text[:start], True
    raise AssertionError(f"unbalanced typeof in {text!r}")


def _translate_sql(expr: str) -> tuple[str, bool]:
    """Engine spelling of one recorded SQL expression plus whether typeof dropped."""
    expr = NTZ_LITERAL.sub(_ntz_call, expr)
    expr = BARE_UNIT.sub(lambda pair: f"timestamp{pair.group(1)}('{pair.group(2)}',", expr)
    return _drop_typeof(expr)


def _typeof_name(field_type: str) -> str:
    """Spark typeof spelling of one Arrow timestamp type."""
    if field_type == "timestamp[us]":
        return "timestamp_ntz"
    if field_type.startswith("timestamp[us, tz="):
        return "timestamp"
    return field_type


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
            ReparkSession.builder.appName("fnp11a-r2")
            .config("spark.sql.session.timeZone", tz)
            .config("spark.sql.ansi.enabled", "true" if ansi else "false")
            .getOrCreate()
        )
        _LIVE["key"] = key
        _LIVE["session"] = session
    return _LIVE["session"]


def _box_local_to_instant(value: datetime.datetime) -> datetime.datetime:
    """Localize a fixture LTZ wall clock in the recording box zone."""
    return value.replace(tzinfo=ZoneInfo(BOX_ZONE)).astimezone(datetime.UTC)


def _expected_scalar(encoded: Any, spark_type: str) -> Any:
    """Decode one fixture JSON value to the instant, wall clock or scalar RePark answers."""
    if encoded is None:
        return None
    if isinstance(encoded, dict) and "datetime" in encoded:
        moment = datetime.datetime.fromisoformat(encoded["datetime"])
        if spark_type == "timestamp_ntz":
            return moment
        return _box_local_to_instant(moment)
    return encoded


def _actual_scalar(value: Any, spark_type: str) -> Any:
    """Normalize one RePark value to the comparison domain of the expected value."""
    if isinstance(value, datetime.datetime) and spark_type == "timestamp":
        if value.tzinfo is None:
            return value.replace(tzinfo=datetime.UTC)
        return value.astimezone(datetime.UTC)
    return value


def _run_cell(cell: dict[str, Any]) -> tuple[pa.Table, bool]:
    """Evaluate one translated cell on its door, flagging a dropped typeof."""
    session = _session(cell["ansi"], cell["tz"])
    if cell["door"] == "sql":
        text, dropped = _translate_sql(cell["expr"])
        return session.sql(text).toArrow(), dropped
    column = eval(cell["expr"], {"F": F, "lit": F.lit, "col": F.col})
    frame = session.createDataFrame([(1,)], "n INT")
    return frame.select(column.alias("v")).toArrow(), False


def _assert_value_cell(cell: dict[str, Any], table: pa.Table, dropped: bool) -> None:
    """A value cell matches Spark's type, nullability and rows on the Arrow path."""
    columns = cell["columns"] or []
    if dropped:
        assert len(columns) == 2 and columns[1]["name"] == "t"
        assert _typeof_name(str(table.schema.field(0).type)) == cell["rows"][0][1]
        columns = columns[:1]
    assert table.num_columns == len(columns)
    assert table.num_rows == len(cell["rows"])
    for index, spec in enumerate(columns):
        field = table.schema.field(index)
        assert str(field.type) == SPARK_PA_TYPE[spec["type"]]
        if spec["nullable"] is not False:
            assert field.nullable == spec["nullable"]
        expected = [_expected_scalar(row[index], spec["type"]) for row in cell["rows"]]
        actual = [_actual_scalar(item, spec["type"]) for item in table.column(index).to_pylist()]
        assert actual == expected


def _assert_error_cell(cell: dict[str, Any], failure: Exception) -> None:
    """A raising cell carries Spark's condition and Spark's message prefix."""
    text = str(failure)
    condition = cell.get("error_condition") or ""
    if condition:
        assert f"[{condition}]" in text
        prefix = (cell.get("message") or "").split(".")[0]
        assert prefix in text
    else:
        assert (cell.get("message") or "") in text


def _check_cell(cell: dict[str, Any]) -> None:
    """Run one round-2 cell against its translated spelling."""
    if cell.get("error_condition") or (cell.get("error_type") and cell.get("columns") is None):
        with pytest.raises(Exception) as caught:
            _run_cell(cell)
        _assert_error_cell(cell, caught.value)
        return
    table, dropped = _run_cell(cell)
    _assert_value_cell(cell, table, dropped)


@pytest.mark.parametrize("cell", _cells("L-001"), ids=_cell_id)
def test_r2_l001_ntz_add_preserves_type(cell: dict[str, Any]) -> None:
    """pins: fnp-11a/C-008, C-012."""
    _check_cell(cell)


@pytest.mark.parametrize("cell", _cells("L-002"), ids=_cell_id)
def test_r2_l002_calendar_diff(cell: dict[str, Any]) -> None:
    """pins: fnp-11a/C-009, C-012."""
    _check_cell(cell)


@pytest.mark.parametrize("cell", _cells("L-003"), ids=_cell_id)
def test_r2_l003_mixed_sign_cast(cell: dict[str, Any]) -> None:
    """pins: fnp-11a/C-010."""
    _check_cell(cell)


@pytest.mark.parametrize("cell", _cells("L-004"), ids=_cell_id)
def test_r2_l004_offset_range(cell: dict[str, Any]) -> None:
    """pins: fnp-11a/C-011."""
    _check_cell(cell)


@pytest.mark.parametrize("cell", _cells("L-007"), ids=_cell_id)
def test_r2_l007_zone_spellings(cell: dict[str, Any]) -> None:
    """pins: fnp-11a/C-011."""
    _check_cell(cell)


@pytest.mark.parametrize("cell", _cells("L-008"), ids=_cell_id)
def test_r2_l008_try_secs_null(cell: dict[str, Any]) -> None:
    """pins: fnp-11a/C-013."""
    _check_cell(cell)


@pytest.mark.parametrize("cell", _cells("L-010"), ids=_cell_id)
def test_r2_l010_month_time_of_day(cell: dict[str, Any]) -> None:
    """pins: fnp-11a/C-013."""
    _check_cell(cell)


@pytest.mark.parametrize("cell", _cells("L-011"), ids=_cell_id)
def test_r2_l011_year_magnitude(cell: dict[str, Any]) -> None:
    """pins: fnp-11a/C-013."""
    _check_cell(cell)


@pytest.fixture(scope="session", autouse=True)
def _stop_session_after_suite() -> Any:
    """Stop the cached session when the module run finishes."""
    yield
    old: ReparkSession | None = _LIVE.get("session")
    if old is not None:
        old.stop()
        _LIVE.clear()


def test_r2_l009_zero_arg_asymmetry() -> None:
    """pins: fnp-11a/C-014."""
    session = _session(True, "UTC")
    frame = session.createDataFrame([(1,)], "n INT")
    raw = frame.select(F.try_make_interval().alias("v")).toArrow()
    assert raw.num_rows == 1
    text = frame.select(F.try_make_interval().cast("string").alias("v")).toArrow()
    assert text.column("v").to_pylist() == ["0 seconds"]
    with pytest.raises(Exception) as caught:
        session.sql("SELECT try_make_interval() AS v").toArrow()
    assert "[WRONG_NUM_ARGS.WITHOUT_SUGGESTION]" in str(caught.value)


@pytest.mark.parametrize(
    "call,display",
    [
        ("F.try_make_interval()", "try_make_interval(0)"),
        ("F.try_make_interval(F.lit(1), F.lit(2))", "try_make_interval(1, 2)"),
        ("F.make_ym_interval()", "make_ym_interval()"),
        ("F.make_ym_interval(F.lit(3))", "make_ym_interval(3)"),
        ("F.months_between(F.col('a'), F.col('b'))", "months_between(a, b)"),
    ],
)
def test_r2_facade_shortest_call_shape(call: str, display: str) -> None:
    """pins: fnp-11a/C-015."""
    namespace = {"F": F}
    column = eval(call, namespace)
    assert column.spark_display_part() == display
