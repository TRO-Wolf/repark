"""FNP-AGG-1 run-18a P1 pins: any_value and product on both doors over two partitions.

Thirty live-PySpark 4.1.2 cells (2026-09-16, local session, both ANSI settings) over the
unit frame repartitioned by ``g``. Ungrouped ``any_value`` values depend on partition scan
order, so those cells pin name, type and nullability exactly and the value as membership
in the frame's non-null candidates; every other cell pins exactly. pins: fnp-agg-1/C-002,
fnp-agg-1/C-003, fnp-agg-1/C-004
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812
from repark.spark.functions import col, lit
from repark.spark.window import Window

P1_ORACLE_PATH = Path(__file__).with_name("fnp_agg_1_p1_spark_oracle.json")
P1_FRAME = (
    "SELECT * FROM VALUES "
    "(1, 'a', 10, CAST(1.5 AS DOUBLE), 'x', CAST(3 AS BIGINT)), "
    "(1, 'b', 20, CAST(2.5 AS DOUBLE), 'y', CAST(1 AS BIGINT)), "
    "(1, 'b', NULL, CAST(NULL AS DOUBLE), NULL, CAST(2 AS BIGINT)), "
    "(1, 'c', 30, CAST(-4.0 AS DOUBLE), 'x', CAST(NULL AS BIGINT)), "
    "(2, NULL, 5, CAST(0.0 AS DOUBLE), 'z', CAST(7 AS BIGINT)), "
    "(3, NULL, NULL, CAST(NULL AS DOUBLE), NULL, CAST(NULL AS BIGINT)) "
    "AS t(g, k, v, d, s, o)"
)
ANY_VALUE_UNGROUPED_CANDIDATES: dict[str, frozenset[Any]] = {
    "any_value(v)": frozenset({10, 20, 30, 5}),
    "any_value(d)": frozenset({1.5, 2.5, -4.0, 0.0}),
    "any_value((v + 1))": frozenset({11, 21, 31, 6}),
}


def _payload() -> dict[str, Any]:
    return json.loads(P1_ORACLE_PATH.read_text())


def _is_error(cell: dict[str, Any]) -> bool:
    return "error_condition" in cell


def _cells() -> list[dict[str, Any]]:
    return _payload()["cells"]


def _cell_id(cell: dict[str, Any]) -> str:
    return f"{cell['door']}-{cell['name']}-a{cell['ansi']}-{_cells().index(cell)}"


def _is_membership(cell: dict[str, Any]) -> bool:
    return (
        not _is_error(cell)
        and cell["name"] == "any_value"
        and "groupBy" not in cell["expr"]
        and ".over(" not in cell["expr"]
    )


P1_WINDOW_ORDERBY_BLOCKED = frozenset({11, 26})
P1_WINDOW_ORDERBY_REASON = (
    "blocked on run 18b: DataFrame.orderBy cannot sort by a column outside the "
    "projection (the window orders by unprojected k)"
)


def _exact_params() -> list[Any]:
    params: list[Any] = []
    for index, cell in enumerate(_cells()):
        if _is_error(cell) or _is_membership(cell):
            continue
        if index in P1_WINDOW_ORDERBY_BLOCKED:
            params.append(
                pytest.param(
                    cell,
                    marks=pytest.mark.xfail(strict=True, reason=P1_WINDOW_ORDERBY_REASON),
                    id=_cell_id(cell),
                )
            )
        else:
            params.append(pytest.param(cell, id=_cell_id(cell)))
    return params


P1_VALUE_EXACT = _exact_params()
P1_VALUE_MEMBERSHIP = [cell for cell in _cells() if _is_membership(cell)]
P1_ERROR = [cell for cell in _cells() if _is_error(cell)]

_LIVE: dict[str, Any] = {}


def _session(ansi: bool) -> ReparkSession:
    key = f"p1/{ansi}"
    cached: ReparkSession | None = _LIVE.get("session")
    if _LIVE.get("key") != key or cached is None or cached._inner is None:
        old: ReparkSession | None = _LIVE.get("session")
        if old is not None:
            old.stop()
            _LIVE.clear()
        session = (
            ReparkSession.builder.appName("fnp-agg-1-p1")
            .config("spark.sql.ansi.enabled", "true" if ansi else "false")
            .getOrCreate()
        )
        _LIVE["key"] = key
        _LIVE["session"] = session
    current: ReparkSession | None = _LIVE.get("session")
    assert current is not None
    return current


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
        return session.sql(cell["expr"].replace("FRAME", f"({P1_FRAME})"))
    frame = session.sql(P1_FRAME)
    return eval(cell["expr"], {"F": F, "col": col, "lit": lit, "Window": Window, "df": frame})


def _assert_schema(result: Any, cell: dict[str, Any]) -> None:
    assert [(field.name, field.dataType.simpleString()) for field in result.schema.fields] == [
        (column["name"], column["type"]) for column in cell["columns"]
    ]
    for field, column in zip(result.schema.fields, cell["columns"], strict=True):
        if column["name"] != "g":
            assert field.nullable == column["nullable"]


@pytest.mark.parametrize("cell", P1_VALUE_EXACT)
def test_p1_exact_cell_matches_oracle(cell: dict[str, Any]) -> None:
    """Exact cells answer name, type and rows. pins: fnp-agg-1/C-002, C-003."""
    result = _run_cell(_session(cell["ansi"]), cell)
    _assert_schema(result, cell)
    assert [[_normalize(value) for value in row] for row in result.collect()] == cell["rows"]


@pytest.mark.parametrize("cell", P1_VALUE_MEMBERSHIP, ids=_cell_id)
def test_p1_ungrouped_any_value_cell_matches_oracle_shape(cell: dict[str, Any]) -> None:
    """Ungrouped any_value pins schema; value as membership. pins: fnp-agg-1/C-002, C-003."""
    result = _run_cell(_session(cell["ansi"]), cell)
    _assert_schema(result, cell)
    names = [column["name"] for column in cell["columns"]]
    assert len(result.collect()) == len(cell["rows"])
    for row, expected in zip(result.collect(), cell["rows"], strict=True):
        for name, value, want in zip(names, row, expected, strict=True):
            if name in ANY_VALUE_UNGROUPED_CANDIDATES:
                assert _normalize(value) in ANY_VALUE_UNGROUPED_CANDIDATES[name]
            else:
                assert _normalize(value) == want


@pytest.mark.parametrize("cell", P1_ERROR, ids=_cell_id)
def test_p1_error_cell_carries_spark_condition(cell: dict[str, Any]) -> None:
    """Refusal cells fail with Spark's own condition on their door. pins: fnp-agg-1/C-004."""
    with pytest.raises(Exception) as caught:
        _run_cell(_session(cell["ansi"]), cell).collect()
    assert f"[{cell['error_condition']}]" in str(caught.value)
