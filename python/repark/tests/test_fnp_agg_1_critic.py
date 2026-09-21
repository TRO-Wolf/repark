"""FNP-AGG-1 slice (d) remediation pins: the grouping_id exact-order rule.

Eight live-PySpark 4.1.2 cells (2026-09-16, local session, ANSI on): grouping_id
arguments must equal the grouping columns exactly and in order on both doors,
else GROUPING_ID_COLUMN_MISMATCH. Value cells pin name, type, nullability and
rows exactly. This slice pins the grouping cells only; slices (a), (b) and (c)
append their own cells.
pins: fnp-agg-1/C-002, C-003, C-004
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812
from repark.spark.functions import col, lit

CRITIC_ORACLE_PATH = Path(__file__).with_name("fnp_agg_1_critic_spark_oracle.json")
CRITIC_F1 = (
    "SELECT * FROM VALUES "
    "(1, 'a', 10), (1, 'b', 20), (1, 'b', NULL), (1, 'c', 30), (2, NULL, 5) "
    "AS t(g, k, v)"
)

def _payload() -> dict[str, Any]:
    return json.loads(CRITIC_ORACLE_PATH.read_text())


def _cells() -> list[dict[str, Any]]:
    return [cell for cell in _payload()["cells"] if cell["name"] == "grouping_id"]


def _cell_id(cell: dict[str, Any]) -> str:
    return f"{cell['door']}-{cell['name']}-{_cells().index(cell)}"


def _is_error(cell: dict[str, Any]) -> bool:
    return "error_condition" in cell


CRITIC_ERROR = [cell for cell in _cells() if _is_error(cell)]


def _value_params() -> list[Any]:
    params: list[Any] = []
    for cell in _cells():
        if _is_error(cell):
            continue
        params.append(pytest.param(cell, id=_cell_id(cell)))
    return params


CRITIC_VALUE = _value_params()

_LIVE: dict[str, Any] = {}


def _session() -> ReparkSession:
    cached: ReparkSession | None = _LIVE.get("session")
    if cached is None or cached._inner is None:
        session = (
            ReparkSession.builder.appName("fnp-agg-1-critic")
            .config("spark.sql.ansi.enabled", "true")
            .getOrCreate()
        )
        session.sql(f"SELECT * FROM ({CRITIC_F1})").createOrReplaceTempView("F1")
        _LIVE["session"] = session
        return session
    return cached


@pytest.fixture(scope="module", autouse=True)
def _stop_session_after_module() -> Any:
    yield
    old: ReparkSession | None = _LIVE.get("session")
    if old is not None:
        old.stop()
        _LIVE.clear()


def _normalize(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (bytes, bytearray)):
        return {"bytes_hex": bytes(value).hex()}
    if isinstance(value, (list, tuple)):
        return [_normalize(item) for item in value]
    if hasattr(value, "asDict"):
        return {"row": {key: _normalize(item) for key, item in value.asDict().items()}}
    if isinstance(value, dict):
        return {"map": [[_normalize(key), _normalize(item)] for key, item in value.items()]}
    return {"repr": repr(value)}


def _run_cell(session: ReparkSession, cell: dict[str, Any]) -> Any:
    if cell["door"] == "sql":
        return session.sql(cell["expr"])
    frame = session.table("F1")
    return eval(cell["expr"], {"F": F, "col": col, "lit": lit, "df": frame})


def _assert_schema(result: Any, cell: dict[str, Any]) -> None:
    assert [(field.name, field.dataType.simpleString()) for field in result.schema.fields] == [
        (column["name"], column["type"]) for column in cell["columns"]
    ]
    for field, column in zip(result.schema.fields, cell["columns"], strict=True):
        assert field.nullable == column["nullable"]


def _key_runs(rows: list[list[Any]], key_width: int) -> list[tuple[str, list[list[Any]]]]:
    runs: list[tuple[str, list[list[Any]]]] = []
    for row in rows:
        key = repr(row[:key_width])
        if runs and runs[-1][0] == key:
            runs[-1][1].append(row)
        else:
            runs.append((key, [row]))
    return runs


def _assert_rows_up_to_ties(
    actual: list[list[Any]], expected: list[list[Any]], key_width: int
) -> None:
    actual_runs = _key_runs(actual, key_width)
    expected_runs = _key_runs(expected, key_width)
    assert [key for key, _ in actual_runs] == [key for key, _ in expected_runs]
    for (_, actual_run), (_, expected_run) in zip(actual_runs, expected_runs, strict=True):
        assert sorted(actual_run, key=repr) == sorted(expected_run, key=repr)


@pytest.mark.parametrize("cell", CRITIC_ERROR, ids=_cell_id)
def test_critic_error_cell_carries_spark_condition(cell: dict[str, Any]) -> None:
    """Refusal cells fail with Spark's own condition on their door. pins: fnp-agg-1/C-004."""
    with pytest.raises(Exception) as caught:
        _run_cell(_session(), cell).collect()
    assert f"[{cell['error_condition']}]" in str(caught.value)


@pytest.mark.parametrize("cell", CRITIC_VALUE, ids=_cell_id)
def test_critic_value_cell_matches_oracle(cell: dict[str, Any]) -> None:
    """Remediation value cells answer schema and rows. pins: fnp-agg-1/C-002, C-003."""
    session = _session()
    result = _run_cell(session, cell)
    _assert_schema(result, cell)
    actual_rows = [[_normalize(value) for value in row] for row in result.collect()]
    if cell["door"] == "sql":
        _assert_rows_up_to_ties(actual_rows, cell["rows"], 1)
    else:
        assert sorted(actual_rows, key=repr) == sorted(cell["rows"], key=repr)
