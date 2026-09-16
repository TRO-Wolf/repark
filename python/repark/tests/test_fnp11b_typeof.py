"""TYPEOF pins over fnp11b_typeof_spark_oracle.json on the recorded door.

pins: fnp-11b/C-005
"""

from __future__ import annotations

import inspect
import json
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812

ORACLE_PATH: Path = Path(__file__).with_name("fnp11b_typeof_spark_oracle.json")

_LIVE: dict[str, Any] = {}


def _cells() -> list[dict[str, Any]]:
    """Every recorded typeof cell in file order."""
    payload: dict[str, Any] = json.loads(ORACLE_PATH.read_text())
    return list(payload["cells"])


def _session() -> ReparkSession:
    """One shared session: typeof spells types, so neither zone nor ANSI moves it."""
    cached: ReparkSession | None = _LIVE.get("session")
    if cached is None or cached._inner is None:
        session = ReparkSession.builder.appName("fnp11b-typeof").getOrCreate()
        session.createDataFrame(
            [(1, "a", 1.5, True)], "i INT, s STRING, d DOUBLE, b BOOLEAN"
        ).createOrReplaceTempView("typeof_frame")
        _LIVE["session"] = session
        return session
    return cached


def _typeof_value(table: Any) -> str:
    """The single answered type name on the Arrow path."""
    assert len(table.schema) == 1
    assert str(table.schema.field(0).type) == "string"
    values = table.column(0).to_pylist()
    assert len(values) == 1
    assert values[0] is not None
    return str(values[0])


@pytest.fixture(scope="session", autouse=True)
def _stop_session_after_suite() -> Any:
    """Stop the cached session when the module run finishes."""
    yield
    old: ReparkSession | None = _LIVE.get("session")
    if old is not None:
        old.stop()
        _LIVE.clear()


def _cell_by_id(cell_id: str) -> dict[str, Any]:
    """Fetch one oracle cell by its recorded id."""
    for cell in _cells():
        if cell["id"] == cell_id:
            return cell
    raise AssertionError(f"unknown typeof cell {cell_id}")


OK_IDS: tuple[str, ...] = (
    "TYPEOF-SQL-00",
    "TYPEOF-SQL-01",
    "TYPEOF-SQL-02",
    "TYPEOF-SQL-03",
    "TYPEOF-SQL-04",
    "TYPEOF-SQL-05",
    "TYPEOF-SQL-06",
    "TYPEOF-SQL-07",
    "TYPEOF-SQL-08",
    "TYPEOF-SQL-09",
    "TYPEOF-SQL-10",
    "TYPEOF-SQL-11",
    "TYPEOF-SQL-12",
    "TYPEOF-SQL-17",
    "TYPEOF-SQL-18",
    "TYPEOF-SQL-19",
    "TYPEOF-SQL-20",
    "TYPEOF-SQL-21",
    "TYPEOF-SQL-22",
    "TYPEOF-SQL-23",
    "TYPEOF-SQL-25",
    "TYPEOF-SQL-26",
    "TYPEOF-SQL-27",
    "TYPEOF-SQL-28",
    "TYPEOF-PY-i",
    "TYPEOF-PY-s",
    "TYPEOF-PY-d",
    "TYPEOF-PY-b",
    "TYPEOF-PY-lit_dec",
    "TYPEOF-PY-lit_int",
    "TYPEOF-PY-lit_none",
    "TYPEOF-PY-arr",
)


@pytest.mark.parametrize("cell_id", OK_IDS)
def test_typeof_cell_matches_oracle(cell_id: str) -> None:
    """One answering cell spells Spark's type name on its recorded door."""
    cell = _cell_by_id(cell_id)
    assert cell["outcome"] == "ok"
    session = _session()
    if cell["kind"] == "typeof_sql":
        table = session.sql(cell["expr"]).toArrow()
    else:
        column = eval(cell["expr"], {"F": F, "Decimal": Decimal})
        table = session.table("typeof_frame").select(column.alias("v")).toArrow()
    assert _typeof_value(table) == cell["typeof"]


def test_typeof_signature_is_one_required_column() -> None:
    """pins: the TYPEOF-SIG shape ``typeof(col: ColumnOrName)``."""
    cell = _cell_by_id("TYPEOF-SIG")
    assert cell["signature"].startswith("(col: ")
    parameters = list(inspect.signature(F.typeof).parameters.values())
    assert [item.name for item in parameters] == ["col"]
    assert parameters[0].default is inspect.Parameter.empty


@pytest.mark.parametrize("cell_id", ("TYPEOF-ERR-00", "TYPEOF-ERR-01"))
def test_typeof_wrong_arity_raises_spark_condition(cell_id: str) -> None:
    """Wrong arity raises Spark's WRONG_NUM_ARGS on the SQL door."""
    cell = _cell_by_id(cell_id)
    assert cell["outcome"] == "error"
    session = _session()
    with pytest.raises(Exception, match="WRONG_NUM_ARGS") as caught:
        session.sql(cell["expr"]).toArrow()
    body = str(cell["error"]).split("; line")[0]
    assert body in str(caught.value)


def test_typeof_ntz_literal_is_blocked_on_the_dialect_seam() -> None:
    """TYPEOF-SQL-13 stays blocked: the TIMESTAMP_NTZ literal is 17c's dialect seam."""
    cell = _cell_by_id("TYPEOF-SQL-13")
    assert cell["typeof"] == "timestamp_ntz"
    session = _session()
    with pytest.raises(Exception, match="TIMESTAMP_NTZ") as caught:
        session.sql(cell["expr"]).toArrow()
    assert "Unsupported SQL type" in str(caught.value)


def test_typeof_binary_function_owner_is_recorded() -> None:
    """TYPEOF-SQL-24 stays blocked: no registry owns the binary function name."""
    cell = _cell_by_id("TYPEOF-SQL-24")
    assert cell["typeof"] == "binary"
    session = _session()
    with pytest.raises(Exception, match="Invalid function") as caught:
        session.sql(cell["expr"]).toArrow()
    assert "binary" in str(caught.value)


@pytest.mark.parametrize("cell_id", ("TYPEOF-SQL-14", "TYPEOF-SQL-15", "TYPEOF-SQL-16"))
def test_typeof_interval_spelling_is_blocked_on_the_unit_seam(cell_id: str) -> None:
    """Interval units arrive unit-less, so the day/year/month spellings stay blocked."""
    cell = _cell_by_id(cell_id)
    session = _session()
    with pytest.raises(Exception, match="not implemented") as caught:
        session.sql(cell["expr"]).toArrow()
    assert "MonthDayNano" in str(caught.value)
