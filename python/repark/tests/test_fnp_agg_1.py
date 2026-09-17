"""FNP-AGG-1 step-1 pins: the missing aggregates answer PySpark 4.1.2 on both doors.

Every pin replays a recorded live-PySpark 4.1.2 cell: ``fnp_agg_1_spark_oracle.json``
holds the orchestrator recording of 2026-09-14 over AGG_FRAME, and the
``sum_distinct`` / ``sumDistinct`` pins replay ``fnp_alias_1_spark_oracle.json``
over ALIAS_FRAME. Value pins compare column names, Spark types, nullability
except the VALUES-derived group key, and rows against the fixture cells, never
RePark against RePark. Step 1 is red-first: eleven names are absent from the
facade, kurtosis/skewness/mode are engine-gap stubs, and the SQL door has none
of the new routines.

pins: fnp-agg-1/C-001, fnp-agg-1/C-002, fnp-agg-1/C-003, fnp-agg-1/C-004
"""

from __future__ import annotations

import inspect
import json
import re
from pathlib import Path
from typing import Any

import pyarrow as pa
import pytest

from repark import ReparkSession
from repark.spark import functions as F  # noqa: N812

AGG_ORACLE_PATH = Path(__file__).with_name("fnp_agg_1_spark_oracle.json")
ALIAS_ORACLE_PATH = Path(__file__).with_name("fnp_alias_1_spark_oracle.json")
AGG_FRAME = (
    "SELECT * FROM VALUES "
    "(1, 'a', 10, CAST(1.5 AS DOUBLE), 'x', CAST(3 AS BIGINT)), "
    "(1, 'b', 20, CAST(2.5 AS DOUBLE), 'y', CAST(1 AS BIGINT)), "
    "(1, 'b', NULL, CAST(NULL AS DOUBLE), NULL, CAST(2 AS BIGINT)), "
    "(1, 'c', 30, CAST(-4.0 AS DOUBLE), 'x', CAST(NULL AS BIGINT)), "
    "(2, NULL, 5, CAST(0.0 AS DOUBLE), 'z', CAST(7 AS BIGINT)), "
    "(3, NULL, NULL, CAST(NULL AS DOUBLE), NULL, CAST(NULL AS BIGINT)) "
    "AS t(g, k, v, d, s, o)"
)
ALIAS_FRAME = (
    "SELECT * FROM VALUES (1, 1, CAST(10 AS BIGINT), CAST(-8 AS INT), CAST(180.0 AS DOUBLE), "
    "CAST(3.14159 AS DOUBLE), 'a'), (1, 2, CAST(10 AS BIGINT), CAST(7 AS INT), "
    "CAST(90.0 AS DOUBLE), CAST(1.0 AS DOUBLE), 'b'), (1, NULL, NULL, NULL, NULL, NULL, 'a'), "
    "(2, 3, CAST(-1 AS BIGINT), CAST(2147483647 AS INT), CAST(-45.0 AS DOUBLE), "
    "CAST(-0.5 AS DOUBLE), NULL) AS t(g, v, b, i, deg, rad, s)"
)
CARD_NAMES = frozenset(
    {
        "any_value",
        "max_by",
        "min_by",
        "percentile",
        "product",
        "listagg",
        "listagg_distinct",
        "string_agg_distinct",
        "grouping_id",
        "histogram_numeric",
        "kurtosis",
        "skewness",
        "mode",
        "sum_distinct",
        "sumDistinct",
        "count_min_sketch",
    }
)
SUM_NAMES = frozenset({"sum_distinct", "sumDistinct"})
SUM_DISTINCT_DEPRECATION = "Deprecated in 3.2, use sum_distinct instead."


def _agg_payload() -> dict[str, Any]:
    return json.loads(AGG_ORACLE_PATH.read_text())


def _alias_payload() -> dict[str, Any]:
    return json.loads(ALIAS_ORACLE_PATH.read_text())


def _is_error(cell: dict[str, Any]) -> bool:
    return "error_condition" in cell or (cell.get("error_type") and cell.get("columns") is None)


def _agg_cells() -> list[dict[str, Any]]:
    return [cell for cell in _agg_payload()["cells"] if cell["name"] in CARD_NAMES]


def _alias_sum_cells() -> list[dict[str, Any]]:
    return [cell for cell in _alias_payload()["cells"] if cell["name"] in SUM_NAMES]


def _cell_id(cells: list[dict[str, Any]], cell: dict[str, Any]) -> str:
    return f"{cell['door']}-{cell['name']}-a{cell['ansi']}-{cells.index(cell)}"


AGG_VALUE_PYTHON = [
    cell for cell in _agg_cells() if cell["door"] == "python" and not _is_error(cell)
]
AGG_VALUE_SQL = [cell for cell in _agg_cells() if cell["door"] == "sql" and not _is_error(cell)]
AGG_ERROR = [cell for cell in _agg_cells() if _is_error(cell)]
ALIAS_VALUE_PYTHON = [
    cell for cell in _alias_sum_cells() if cell["door"] == "python" and not _is_error(cell)
]
ALIAS_VALUE_SQL = [
    cell for cell in _alias_sum_cells() if cell["door"] == "sql" and not _is_error(cell)
]
ALIAS_ERROR = [cell for cell in _alias_sum_cells() if _is_error(cell)]

_LIVE: dict[str, Any] = {}


def _session(frame_key: str, ansi: bool) -> ReparkSession:
    key = f"{frame_key}/{ansi}"
    cached: ReparkSession | None = _LIVE.get("session")
    if _LIVE.get("key") != key or cached is None or cached._inner is None:
        old: ReparkSession | None = _LIVE.get("session")
        if old is not None:
            old.stop()
            _LIVE.clear()
        session = (
            ReparkSession.builder.appName("fnp-agg-1")
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


def _run_grouping_python(frame: Any, expr: str) -> Any:
    if expr == "cube(g,k).agg(grouping_id())":
        return frame.cube("g", "k").agg(F.grouping_id(), F.sum("v")).orderBy("g", "k")
    if expr == "rollup(g,k).agg(grouping_id('g','k'))":
        return frame.rollup("g", "k").agg(F.grouping_id("g", "k")).orderBy("g", "k")
    if expr == "rollup(g,k).agg(grouping_id('k'))":
        return frame.rollup("g", "k").agg(F.grouping_id("k")).orderBy("g", "k")
    if expr == "groupBy(g).agg(grouping_id())":
        return frame.groupBy("g").agg(F.grouping_id()).orderBy("g")
    raise AssertionError(f"unmapped grouping_id expr {expr!r}")


def _run_python_cell(session: ReparkSession, frame_sql: str, cell: dict[str, Any]) -> Any:
    frame = session.sql(frame_sql)
    if cell["name"] == "grouping_id":
        return _run_grouping_python(frame, cell["expr"])
    column = eval(cell["expr"], {"F": F, "col": F.col, "lit": F.lit})
    if any(column["name"] == "g" for column in cell.get("columns", [])):
        return frame.groupBy("g").agg(column).orderBy("g")
    return frame.agg(column)


def _run_sql_cell(session: ReparkSession, frame_sql: str, cell: dict[str, Any]) -> Any:
    return session.sql(cell["expr"].replace("FRAME", f"({frame_sql})"))


_SKETCH_NAME_XFAIL = (
    "planner surface: projection alias Debug-renders CAST/decimal literal args — run 18c"
)


def _sketch_cells(cells: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """The card's ``count_min_sketch`` value cells in oracle order."""
    return [cell for cell in cells if cell["name"] == "count_min_sketch"]


def _assert_sketch_cell(result: Any, cell: dict[str, Any]) -> None:
    """Sketch shared pins: rows, nullability and the currently-clean schema half, strict."""
    columns = cell["columns"]
    actual_schema = [(field.name, field.dataType.simpleString()) for field in result.schema.fields]
    if cell["door"] == "python":
        assert [name for name, _ in actual_schema] == [column["name"] for column in columns]
    if not any(column["type"] == "binary" for column in columns):
        assert [kind for _, kind in actual_schema] == [column["type"] for column in columns]
    actual_rows = [[_normalize(value) for value in row] for row in result.collect()]
    assert actual_rows == cell["rows"]
    for field, column in zip(result.schema.fields, columns, strict=True):
        if column["name"] != "g":
            assert field.nullable == column["nullable"]


def _assert_distinct_elements(actual_rows: list[list[Any]], expected_rows: list[list[Any]]) -> None:
    """DISTINCT strings compare as multisets: Spark contracts no element order."""
    assert len(actual_rows) == len(expected_rows)
    for actual, expected in zip(actual_rows, expected_rows, strict=True):
        assert len(actual) == len(expected)
        for actual_value, expected_value in zip(actual, expected, strict=True):
            if actual_value is None or expected_value is None:
                assert actual_value == expected_value
            elif isinstance(actual_value, str) and isinstance(expected_value, str):
                if "-" in expected_value:
                    assert sorted(actual_value.split("-")) == sorted(expected_value.split("-"))
                else:
                    assert sorted(actual_value) == sorted(expected_value)
            else:
                assert actual_value == expected_value


def _histogram_value(value: Any) -> Any:
    """Canonicalize one histogram_numeric answer for the struct-vs-map collect shape."""
    shaped = _normalize(value)
    if not isinstance(shaped, list):
        return shaped
    buckets: list[Any] = []
    for bucket in shaped:
        pairs = bucket.get("map") if isinstance(bucket, dict) else None
        if (
            isinstance(pairs, list)
            and all(isinstance(pair, list) and len(pair) == 2 for pair in pairs)
            and {pair[0] for pair in pairs} == {"x", "y"}
        ):
            buckets.append({"row": {pair[0]: pair[1] for pair in pairs}})
        else:
            buckets.append(bucket)
    return buckets


def _assert_value_cell(result: Any, cell: dict[str, Any]) -> None:
    if cell["name"] == "count_min_sketch":
        _assert_sketch_cell(result, cell)
        return
    expected_schema = [(column["name"], column["type"]) for column in cell["columns"]]
    assert [(field.name, field.dataType.simpleString()) for field in result.schema.fields] == (
        expected_schema
    )
    for field, column in zip(result.schema.fields, cell["columns"], strict=True):
        if column["name"] != "g":
            assert field.nullable == column["nullable"]
    rows = result.collect()
    if cell["name"] == "histogram_numeric":
        assert [[_histogram_value(value) for value in row] for row in rows] == cell["rows"]
        return
    actual_rows = [[_normalize(value) for value in row] for row in rows]
    if cell["name"] in ("listagg_distinct", "string_agg_distinct"):
        _assert_distinct_elements(actual_rows, cell["rows"])
    elif cell["name"] == "grouping_id":
        _assert_rows_up_to_ties(actual_rows, cell["rows"], 2)
    else:
        assert actual_rows == cell["rows"]


def _key_runs(rows: list[list[Any]], key_width: int) -> list[tuple[str, list[list[Any]]]]:
    """Group consecutive rows by the repr of their first key_width values."""
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
    """Order-insensitive within runs tied on every sort key. pins: fnp-agg-1/C-002."""
    actual_runs = _key_runs(actual, key_width)
    expected_runs = _key_runs(expected, key_width)
    assert [key for key, _ in actual_runs] == [key for key, _ in expected_runs]
    for (_, actual_run), (_, expected_run) in zip(actual_runs, expected_runs, strict=True):
        assert sorted(actual_run, key=repr) == sorted(expected_run, key=repr)


def _assert_error_cell(cell: dict[str, Any], failure: Exception) -> None:
    assert f"[{cell['error_condition']}]" in str(failure)


def _split_signature_params(inner: str) -> list[str]:
    parts: list[str] = []
    depth = 0
    current = ""
    for char in inner:
        if char in "[({":
            depth += 1
        elif char in "])}":
            depth -= 1
        if char == "," and depth == 0:
            parts.append(current)
            current = ""
        else:
            current += char
    if current.strip():
        parts.append(current)
    return [part.strip() for part in parts if part.strip()]


def _expected_default(token: str) -> Any:
    text = token.strip()
    if text == "None":
        return None
    if text == "True":
        return True
    if text == "False":
        return False
    if re.fullmatch(r"-?\d+", text):
        return int(text)
    if len(text) >= 2 and text[0] == text[-1] and text[0] in ("'", '"'):
        return text[1:-1]
    raise AssertionError(f"unmapped signature default {token!r}")


def _signature_shape(recorded: str | None) -> list[tuple[str, Any]]:
    assert recorded is not None
    inner = recorded.split("(", 1)[1].rsplit(")", 1)[0]
    shape: list[tuple[str, Any]] = []
    for part in _split_signature_params(inner):
        if part.startswith("*"):
            shape.append(("*" + part.lstrip("*").split(":")[0].strip(), inspect.Parameter.empty))
            continue
        name, _, default = part.partition("=")
        name = name.split(":")[0].strip()
        if not default:
            shape.append((name, inspect.Parameter.empty))
        else:
            shape.append((name, _expected_default(default)))
    return shape


def test_facade_names_present_and_exported() -> None:
    """Every card name sits on the module and in its ``__all__``. pins: fnp-agg-1/C-001."""
    for name in sorted(CARD_NAMES):
        assert callable(getattr(F, name)), name
        assert name in F.__all__, name


@pytest.mark.parametrize("name", sorted(CARD_NAMES))
def test_facade_signature_matches_spark(name: str) -> None:
    """Parameters and defaults equal the recorded PySpark signature. pins: fnp-agg-1/C-001."""
    if name in SUM_NAMES:
        recorded = _alias_payload()["signatures"][name]
    else:
        recorded = _agg_payload()["signatures"][name]
    expected = _signature_shape(recorded)
    parameters = list(inspect.signature(getattr(F, name)).parameters.values())
    assert len(parameters) == len(expected)
    for parameter, (expected_name, expected_default) in zip(parameters, expected, strict=True):
        if expected_name.startswith("*"):
            assert parameter.kind is inspect.Parameter.VAR_POSITIONAL
            assert parameter.name == expected_name.lstrip("*")
        else:
            assert parameter.name == expected_name
            if expected_default is inspect.Parameter.empty:
                assert parameter.default is inspect.Parameter.empty
            else:
                assert parameter.default == expected_default


@pytest.mark.parametrize(
    "cell", AGG_VALUE_PYTHON, ids=lambda cell: _cell_id(AGG_VALUE_PYTHON, cell)
)
def test_python_door_value_cell_matches_oracle(cell: dict[str, Any]) -> None:
    """The Python door answers the recorded Spark column, type and rows. pins: fnp-agg-1/C-002."""
    session = _session("agg", cell["ansi"])
    _assert_value_cell(_run_python_cell(session, AGG_FRAME, cell), cell)


_SQL_LISTAGG_XFAIL = (
    "SQL door has no listagg routine and no WITHIN GROUP ordered-set syntax — "
    "ledger Remediation R-18a-26"
)
_SQL_ANY_VALUE_XFAIL = (
    "planner rejects duplicate projection names Spark allows "
    "(both columns are any_value(v)) — ledger Remediation R-18a-26"
)


def _sql_value_params() -> list[Any]:
    params: list[Any] = []
    for cell in AGG_VALUE_SQL:
        if cell["name"] == "listagg":
            params.append(
                pytest.param(cell, marks=pytest.mark.xfail(strict=True, reason=_SQL_LISTAGG_XFAIL))
            )
        elif cell["name"] == "any_value":
            params.append(
                pytest.param(
                    cell, marks=pytest.mark.xfail(strict=True, reason=_SQL_ANY_VALUE_XFAIL)
                )
            )
        else:
            params.append(pytest.param(cell))
    return params


@pytest.mark.parametrize(
    "cell", _sql_value_params(), ids=lambda cell: _cell_id(AGG_VALUE_SQL, cell)
)
def test_sql_door_value_cell_matches_oracle(cell: dict[str, Any]) -> None:
    """The SQL door answers the recorded Spark column, type and rows. pins: fnp-agg-1/C-003."""
    session = _session("agg", cell["ansi"])
    _assert_value_cell(_run_sql_cell(session, AGG_FRAME, cell), cell)


_SKETCH_BINARY_CELLS = _sketch_cells(AGG_VALUE_PYTHON) + [
    cell
    for cell in _sketch_cells(AGG_VALUE_SQL)
    if any(column["type"] == "binary" for column in cell["columns"])
]


@pytest.mark.parametrize(
    "cell", _SKETCH_BINARY_CELLS, ids=lambda cell: _cell_id(_SKETCH_BINARY_CELLS, cell)
)
def test_sketch_binary_type_matches_oracle(cell: dict[str, Any]) -> None:
    """Sketch columns report binary on both doors. pins: fnp-agg-1/C-002, C-003."""
    session = _session("agg", cell["ansi"])
    run = _run_python_cell if cell["door"] == "python" else _run_sql_cell
    result = run(session, AGG_FRAME, cell)
    assert [field.dataType.simpleString() for field in result.schema.fields] == [
        column["type"] for column in cell["columns"]
    ]


_SKETCH_SQL_CELLS = _sketch_cells(AGG_VALUE_SQL)


@pytest.mark.xfail(strict=True, reason=_SKETCH_NAME_XFAIL)
@pytest.mark.parametrize(
    "cell", _SKETCH_SQL_CELLS, ids=lambda cell: _cell_id(_SKETCH_SQL_CELLS, cell)
)
def test_sql_door_sketch_names_match_oracle(cell: dict[str, Any]) -> None:
    """SQL-door sketch names drop the Debug alias on the planner surface. pins: fnp-agg-1/C-003."""
    session = _session("agg", cell["ansi"])
    result = _run_sql_cell(session, AGG_FRAME, cell)
    assert [field.name for field in result.schema.fields] == [
        column["name"] for column in cell["columns"]
    ]


def _grouping_sql_cells() -> list[dict[str, Any]]:
    """The SQL-door cube cells carrying the tinyint grouping column."""
    return [
        cell
        for cell in AGG_VALUE_SQL
        if cell["name"] == "grouping_id"
        and any(column["name"] == "grouping(g)" for column in cell["columns"])
    ]


def test_sql_grouping_reports_tinyint() -> None:
    """SQL-door grouping(g) is tinyint once LOGICAL-WIDTH-1 lands. pins: fnp-agg-1/C-003."""
    session = _session("agg", True)
    result = _run_sql_cell(session, AGG_FRAME, _grouping_sql_cells()[0])
    assert [
        field.dataType.simpleString()
        for field in result.schema.fields
        if field.name == "grouping(g)"
    ] == ["tinyint"]


def test_python_cube_grouping_reaches_rust_rule() -> None:
    """A Python-door cube answers single-arg grouping as int8 Arrow. pins: fnp-agg-1/C-002."""
    session = _session("agg", True)
    table = session.sql(AGG_FRAME).cube("g", "k").agg(F.grouping("g")).to_arrow()
    assert table.schema.field("grouping(g)").type == pa.int8()
    assert sorted(table.column("grouping(g)").to_pylist()) == [0] * 8 + [1] * 5


@pytest.mark.parametrize(
    "cell", ALIAS_VALUE_PYTHON, ids=lambda cell: _cell_id(ALIAS_VALUE_PYTHON, cell)
)
def test_sum_distinct_python_door_matches_alias_oracle(cell: dict[str, Any]) -> None:
    """``sum_distinct`` answers the alias-oracle column, type and rows. pins: fnp-agg-1/C-002."""
    session = _session("alias", cell["ansi"])
    _assert_value_cell(_run_python_cell(session, ALIAS_FRAME, cell), cell)


@pytest.mark.xfail(
    strict=True,
    reason="projection names keep the inner-frame qualifier — ledger Remediation R-18a-26",
)
@pytest.mark.parametrize("cell", ALIAS_VALUE_SQL, ids=lambda cell: _cell_id(ALIAS_VALUE_SQL, cell))
def test_sum_distinct_sql_door_matches_alias_oracle(cell: dict[str, Any]) -> None:
    """``sum(DISTINCT)`` answers on SQL; ``sum_distinct`` stays refused. pins: fnp-agg-1/C-003."""
    session = _session("alias", cell["ansi"])
    if _is_error(cell):
        with pytest.raises(Exception) as caught:
            _run_sql_cell(session, ALIAS_FRAME, cell).collect()
        _assert_error_cell(cell, caught.value)
    else:
        _assert_value_cell(_run_sql_cell(session, ALIAS_FRAME, cell), cell)


@pytest.mark.parametrize(
    "cell", AGG_ERROR + ALIAS_ERROR, ids=lambda cell: _cell_id(AGG_ERROR + ALIAS_ERROR, cell)
)
def test_error_cell_carries_spark_condition(cell: dict[str, Any]) -> None:
    """A raising cell fails with Spark's own condition on its door. pins: fnp-agg-1/C-004."""
    frame_key = "alias" if cell in ALIAS_ERROR else "agg"
    frame_sql = ALIAS_FRAME if cell in ALIAS_ERROR else AGG_FRAME
    session = _session(frame_key, cell["ansi"])
    with pytest.raises(Exception) as caught:
        if cell["door"] == "sql":
            _run_sql_cell(session, frame_sql, cell).collect()
        else:
            _run_python_cell(session, frame_sql, cell).collect()
    _assert_error_cell(cell, caught.value)


def test_deprecated_sum_alias_warns_spark_message() -> None:
    """The ``sumDistinct`` alias warns Spark's deprecation message. pins: fnp-agg-1/C-001."""
    with pytest.warns(FutureWarning, match=f"^{re.escape(SUM_DISTINCT_DEPRECATION)}$"):
        F.sumDistinct("v")
